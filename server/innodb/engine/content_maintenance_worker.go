package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

const (
	contentMaintenanceLeaseDuration = 5 * time.Minute
	contentMaintenanceMaxAttempts   = 8
)

var (
	errContentMaintenanceStaleSource = errors.New("CONTENT maintenance source is stale")
	errContentMaintenanceUnsupported = errors.New("unsupported CONTENT maintenance processor")
)

type contentMaintenanceResultManifest struct {
	Version          int                              `json:"version"`
	JobID            string                           `json:"job_id"`
	Source           content.SourceRef                `json:"source"`
	ProcessorID      string                           `json:"processor_id"`
	ProcessorVersion string                           `json:"processor_version"`
	Object           content.ObjectRef                `json:"object"`
	Blocks           []contentMaintenanceBlockLocator `json:"blocks"`
}

type contentMaintenanceBlockLocator struct {
	ID      string `json:"id"`
	Ordinal uint32 `json:"ordinal"`
	Offset  uint64 `json:"offset"`
	Length  uint64 `json:"length"`
}

func (e *XMySQLEngine) startContentMaintenanceWorker(ctx context.Context) {
	if e == nil || e.QueryExecutor == nil || e.QueryExecutor.contentMaintenanceQueue == nil || e.contentMaintenanceCancel != nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	workerCtx, cancel := context.WithCancel(ctx)
	e.contentMaintenanceCancel = cancel
	e.contentMaintenanceWG.Add(1)
	go func() {
		defer e.contentMaintenanceWG.Done()
		for {
			processed, err := e.QueryExecutor.ProcessNextContentMaintenance(workerCtx, "xmysql-content-worker")
			if err != nil && workerCtx.Err() == nil {
				logger.Warnf("CONTENT maintenance worker could not process a job: %v", err)
			}
			if processed {
				continue
			}
			timer := time.NewTimer(time.Second)
			select {
			case <-workerCtx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
		}
	}()
}

// ProcessNextContentMaintenance claims and processes one committed CONTENT
// derivation request. Failed transient work is durably retried with backoff;
// stale source versions and unsupported processors become visible failures.
func (e *XMySQLExecutor) ProcessNextContentMaintenance(ctx context.Context, owner string) (bool, error) {
	if e == nil || e.contentMaintenanceQueue == nil {
		return false, fmt.Errorf("CONTENT maintenance queue is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	lease, found, err := e.contentMaintenanceQueue.Claim(owner, time.Now().UTC(), contentMaintenanceLeaseDuration)
	if err != nil || !found {
		return found, err
	}
	if err := e.processContentMaintenanceLease(ctx, &lease); err == nil {
		return true, nil
	} else {
		now := time.Now().UTC()
		var transitionErr error
		if errors.Is(err, errContentMaintenanceStaleSource) || errors.Is(err, errContentMaintenanceUnsupported) || lease.Job.Attempts >= contentMaintenanceMaxAttempts {
			_, transitionErr = e.contentMaintenanceQueue.Fail(lease, now, err)
		} else {
			_, transitionErr = e.contentMaintenanceQueue.Retry(lease, now, contentMaintenanceRetryDelay(lease.Job.Attempts), err)
		}
		if transitionErr != nil {
			return true, fmt.Errorf("process CONTENT maintenance failed: %v; record job outcome: %w", err, transitionErr)
		}
		return true, nil
	}
}

func (e *XMySQLExecutor) processContentMaintenanceLease(ctx context.Context, lease *content.MaintenanceLease) error {
	if lease == nil {
		return fmt.Errorf("CONTENT maintenance lease is nil")
	}
	job := lease.Job
	if !strings.EqualFold(job.ProcessorID, "xmysql.text-chunker") || job.ProcessorVersion != "1" {
		return fmt.Errorf("%w: %s@%s", errContentMaintenanceUnsupported, job.ProcessorID, job.ProcessorVersion)
	}
	request, err := content.NewTextChunkMaintenanceRequest(job.Source, content.TextChunkOptions{MaxBytes: content.DefaultTextChunkBytes})
	if err != nil {
		return fmt.Errorf("validate CONTENT text chunk request: %w", err)
	}
	if !strings.EqualFold(request.ConfigurationDigest, job.ConfigurationSHA) {
		return fmt.Errorf("%w: text chunk configuration digest does not match processor defaults", errContentMaintenanceUnsupported)
	}
	value, err := e.readCurrentTextMaintenanceSource(ctx, job.Source)
	if err != nil {
		return err
	}
	store, err := contentSegmentStoreForDataDir(e.getDataDir())
	if err != nil {
		return fmt.Errorf("open CONTENT store for text maintenance: %w", err)
	}
	writer, err := store.NewWriter(ctx, content.BlockMIMEType, 1)
	if err != nil {
		return fmt.Errorf("open derived block stream: %w", err)
	}
	writerOpen := true
	defer func() {
		if writerOpen {
			_ = writer.Abort()
		}
	}()
	manifest := contentMaintenanceResultManifest{
		Version: 1, JobID: job.ID, Source: job.Source,
		ProcessorID: job.ProcessorID, ProcessorVersion: job.ProcessorVersion,
		Blocks: make([]contentMaintenanceBlockLocator, 0),
	}
	options := content.TextChunkOptions{MaxBytes: content.DefaultTextChunkBytes}
	lastHeartbeat := time.Now().UTC()
	var offset uint64
	_, err = content.VisitTextChunksFromStore(ctx, store, value, job.Source, options, func(block content.Block) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Since(lastHeartbeat) >= contentMaintenanceLeaseDuration/3 {
			renewed, heartbeatErr := e.contentMaintenanceQueue.Heartbeat(*lease, time.Now().UTC(), contentMaintenanceLeaseDuration)
			if heartbeatErr != nil {
				return fmt.Errorf("renew CONTENT maintenance lease: %w", heartbeatErr)
			}
			*lease = renewed
			lastHeartbeat = time.Now().UTC()
		}
		encoded, encodeErr := block.MarshalBinary()
		if encodeErr != nil {
			return fmt.Errorf("encode derived CONTENT block %d: %w", block.Ordinal, encodeErr)
		}
		if len(encoded) > int(^uint32(0)) {
			return fmt.Errorf("derived CONTENT block %d exceeds the framing limit", block.Ordinal)
		}
		var frame [4]byte
		binary.BigEndian.PutUint32(frame[:], uint32(len(encoded)))
		if err := writeContentMaintenanceBytes(writer, frame[:]); err != nil {
			return err
		}
		offset += uint64(len(frame))
		locator := contentMaintenanceBlockLocator{ID: block.ID, Ordinal: block.Ordinal, Offset: offset, Length: uint64(len(encoded))}
		if err := writeContentMaintenanceBytes(writer, encoded); err != nil {
			return err
		}
		offset += uint64(len(encoded))
		manifest.Blocks = append(manifest.Blocks, locator)
		return nil
	})
	if err != nil {
		return fmt.Errorf("derive CONTENT text blocks: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(manifest.Blocks) == 0 {
		if err := writer.Abort(); err != nil {
			return fmt.Errorf("discard empty derived CONTENT block stream: %w", err)
		}
		writerOpen = false
	} else {
		object, err := writer.Commit()
		if err != nil {
			return fmt.Errorf("publish derived CONTENT block stream: %w", err)
		}
		writerOpen = false
		manifest.Object = object
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("encode CONTENT maintenance result manifest: %w", err)
	}
	if err := e.verifyCurrentTextMaintenanceSource(ctx, job.Source); err != nil {
		return err
	}
	if time.Since(lastHeartbeat) >= contentMaintenanceLeaseDuration/3 {
		renewed, heartbeatErr := e.contentMaintenanceQueue.Heartbeat(*lease, time.Now().UTC(), contentMaintenanceLeaseDuration)
		if heartbeatErr != nil {
			return fmt.Errorf("renew CONTENT maintenance lease before publication: %w", heartbeatErr)
		}
		*lease = renewed
	}
	if _, err := e.contentMaintenanceQueue.CompleteWithResult(*lease, time.Now().UTC(), manifestBytes); err != nil {
		return fmt.Errorf("publish CONTENT maintenance result: %w", err)
	}
	return nil
}

func (e *XMySQLExecutor) readCurrentTextMaintenanceSource(ctx context.Context, source content.SourceRef) (content.Value, error) {
	dml, err := e.newStorageIntegratedDMLExecutor()
	if err != nil {
		return content.Value{}, fmt.Errorf("open CONTENT source table access: %w", err)
	}
	tableMeta, _, btree, err := dml.tableAccess(ctx, source.Schema, source.Table)
	if err != nil {
		return content.Value{}, fmt.Errorf("open CONTENT source %s.%s: %w", source.Schema, source.Table, err)
	}
	var sourceColumn *metadata.ColumnMeta
	for _, column := range tableMeta.Columns {
		if column != nil && strings.EqualFold(strings.TrimSpace(column.Name), strings.TrimSpace(source.Column)) {
			sourceColumn = column
			break
		}
	}
	if !columnHasDataType(sourceColumn, metadata.TypeContent) {
		return content.Value{}, fmt.Errorf("%w: source column %s is no longer CONTENT", errContentMaintenanceStaleSource, source.Column)
	}
	lookupKey, err := vectorSearchPrimaryKeyLookupKey(source.PrimaryKey, tableMeta)
	if err != nil {
		return content.Value{}, fmt.Errorf("decode CONTENT source primary key: %w", err)
	}
	scanner := NewClusteredIndexScanner(btree, tableMeta)
	row, _, found, err := scanner.FindByLookupKeyAndStorageKey(ctx, lookupKey, source.PrimaryKey)
	if err != nil {
		return content.Value{}, fmt.Errorf("read CONTENT source row: %w", err)
	}
	if !found || row == nil {
		return content.Value{}, fmt.Errorf("%w: source row no longer exists", errContentMaintenanceStaleSource)
	}
	raw, exists := clusteredRowColumnValue(row, sourceColumn.Name)
	if !exists {
		return content.Value{}, fmt.Errorf("%w: source CONTENT column is missing", errContentMaintenanceStaleSource)
	}
	var value content.Value
	switch typed := raw.(type) {
	case content.Value:
		value = typed
	case *content.Value:
		if typed == nil {
			return content.Value{}, fmt.Errorf("%w: source CONTENT column is NULL", errContentMaintenanceStaleSource)
		}
		value = *typed
	default:
		return content.Value{}, fmt.Errorf("%w: source CONTENT column has type %T", errContentMaintenanceStaleSource, raw)
	}
	current, err := content.NewSourceRef(source.Schema, source.Table, source.Column, source.PrimaryKey, value)
	if err != nil {
		return content.Value{}, fmt.Errorf("validate current CONTENT source identity: %w", err)
	}
	if !source.Matches(current) {
		return content.Value{}, fmt.Errorf("%w: current row revision or digest differs from the queued source", errContentMaintenanceStaleSource)
	}
	return value, nil
}

func (e *XMySQLExecutor) verifyCurrentTextMaintenanceSource(ctx context.Context, source content.SourceRef) error {
	if _, err := e.readCurrentTextMaintenanceSource(ctx, source); err != nil {
		return fmt.Errorf("verify CONTENT source before derived publication: %w", err)
	}
	return nil
}

// CONTENT_MAINTENANCE_STATUS(content_column[, max_bytes[, overlap_bytes[, min_boundary_bytes]]])
// reports the durable derivation state for the exact CONTENT version in the
// current row. It never creates a queue entry.
func evaluateContentMaintenanceStatusSQL(function *sqlparser.FuncExpr, values map[string]interface{}) (interface{}, error) {
	if function == nil || len(function.Exprs) < 1 || len(function.Exprs) > 4 {
		return nil, fmt.Errorf("CONTENT_MAINTENANCE_STATUS requires a CONTENT column and optional chunk settings")
	}
	first, ok := function.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("CONTENT_MAINTENANCE_STATUS first argument must be a CONTENT column")
	}
	column, ok := first.Expr.(*sqlparser.ColName)
	if !ok {
		return nil, fmt.Errorf("CONTENT_MAINTENANCE_STATUS first argument must be a CONTENT column")
	}
	args := make([]interface{}, len(function.Exprs))
	for index, raw := range function.Exprs {
		argument, ok := raw.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("CONTENT_MAINTENANCE_STATUS arguments must be scalar expressions")
		}
		value, err := evaluateExpressionWithRow(argument.Expr, values)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		args[index] = value
	}
	var sourceValue content.Value
	switch value := args[0].(type) {
	case content.Value:
		sourceValue = value
	case *content.Value:
		if value == nil {
			return nil, nil
		}
		sourceValue = *value
	default:
		return nil, fmt.Errorf("CONTENT_MAINTENANCE_STATUS requires a typed CONTENT value; got %T", args[0])
	}
	if !content.IsTextMIMEType(sourceValue.MIMEType) {
		return "NOT_APPLICABLE", nil
	}
	rowContext, ok := contentSourceContextForColumn(values, column)
	if !ok || len(rowContext.PrimaryKey) == 0 {
		return nil, fmt.Errorf("CONTENT_MAINTENANCE_STATUS requires stored-row context for the source column")
	}
	source, err := content.NewSourceRef(rowContext.Schema, rowContext.Table, column.Name.String(), rowContext.PrimaryKey, sourceValue)
	if err != nil {
		return nil, fmt.Errorf("CONTENT_MAINTENANCE_STATUS source verification failed: %w", err)
	}
	options := content.TextChunkOptions{MaxBytes: content.DefaultTextChunkBytes}
	if len(args) > 1 {
		maxBytes, err := contentRangeArgument(args[1], "max_bytes")
		if err != nil || maxBytes == 0 || maxBytes > uint64(content.MaxReadRangeBytes) {
			return nil, fmt.Errorf("CONTENT_MAINTENANCE_STATUS max_bytes must be between 1 and %d", content.MaxReadRangeBytes)
		}
		options.MaxBytes = int(maxBytes)
	}
	if len(args) > 2 {
		overlap, err := contentRangeArgument(args[2], "overlap_bytes")
		if err != nil || overlap > uint64(content.MaxReadRangeBytes) {
			return nil, fmt.Errorf("CONTENT_MAINTENANCE_STATUS overlap_bytes exceeds %d", content.MaxReadRangeBytes)
		}
		options.OverlapBytes = int(overlap)
	}
	if len(args) > 3 {
		minimum, err := contentRangeArgument(args[3], "min_boundary_bytes")
		if err != nil || minimum > uint64(content.MaxReadRangeBytes) {
			return nil, fmt.Errorf("CONTENT_MAINTENANCE_STATUS min_boundary_bytes exceeds %d", content.MaxReadRangeBytes)
		}
		options.MinBoundaryBytes = int(minimum)
	}
	dataDir, _ := values[storedFunctionDataDirKey].(string)
	if strings.TrimSpace(dataDir) == "" {
		return "NOT_QUEUED", nil
	}
	store, err := contentSegmentStoreForDataDir(dataDir)
	if err != nil {
		return nil, err
	}
	queue, err := store.OpenMaintenanceQueue()
	if err != nil {
		return nil, err
	}
	request, err := content.NewTextChunkMaintenanceRequest(source, options)
	if err != nil {
		return nil, err
	}
	job, found, err := queue.GetForRequest(request)
	if err != nil {
		return nil, err
	}
	if !found {
		return "NOT_QUEUED", nil
	}
	return string(job.Status), nil
}

func visitPublishedTextChunks(ctx context.Context, store *content.SegmentStore, source content.SourceRef, options content.TextChunkOptions, visit func(content.Block) error) (int, bool, error) {
	if store == nil {
		return 0, false, nil
	}
	request, err := content.NewTextChunkMaintenanceRequest(source, options)
	if err != nil {
		return 0, false, err
	}
	queue, err := store.OpenMaintenanceQueue()
	if err != nil {
		return 0, false, err
	}
	job, found, err := queue.GetForRequest(request)
	if err != nil || !found || job.Status != content.MaintenanceReady || !job.ResultStored {
		return 0, false, err
	}
	result, found, err := queue.ReadResult(job.ID)
	if err != nil || !found {
		if err == nil {
			err = fmt.Errorf("READY maintenance job has no readable result")
		}
		return 0, false, err
	}
	manifest, err := decodeContentMaintenanceResultManifest(result)
	if err != nil {
		return 0, false, err
	}
	if manifest.Version != 1 || manifest.JobID != job.ID || !manifest.Source.Matches(source) ||
		manifest.ProcessorID != job.ProcessorID || manifest.ProcessorVersion != job.ProcessorVersion ||
		len(manifest.Blocks) > content.MaxTextChunkCount {
		return 0, false, fmt.Errorf("published text block manifest identity is invalid")
	}
	if len(manifest.Blocks) == 0 {
		if manifest.Object.ID != "" {
			return 0, false, fmt.Errorf("empty text block manifest unexpectedly references an object")
		}
		return 0, true, nil
	}
	if manifest.Object.ID == "" || manifest.Object.MIMEType != content.BlockMIMEType || manifest.Object.Revision != 1 {
		return 0, false, fmt.Errorf("published text block object identity is invalid")
	}
	var previousEnd uint64
	for index, locator := range manifest.Blocks {
		if locator.Ordinal != uint32(index) || locator.ID == "" || locator.Length == 0 || locator.Length > uint64(content.MaxReadRangeBytes) ||
			locator.Offset < 4 || locator.Offset > manifest.Object.Size || locator.Length > manifest.Object.Size-locator.Offset ||
			index > 0 && locator.Offset < previousEnd+4 {
			return 0, false, fmt.Errorf("published text block manifest contains an invalid locator")
		}
		frame, err := store.ReadRange(ctx, manifest.Object, locator.Offset-4, 4)
		if err != nil {
			return index, false, fmt.Errorf("read published text block frame: %w", err)
		}
		if len(frame) != 4 || binary.BigEndian.Uint32(frame) != uint32(locator.Length) {
			return index, false, fmt.Errorf("published text block frame length does not match manifest")
		}
		encoded, err := store.ReadRange(ctx, manifest.Object, locator.Offset, locator.Length)
		if err != nil {
			return index, false, fmt.Errorf("read published text block: %w", err)
		}
		var block content.Block
		if err := block.UnmarshalBinary(encoded); err != nil {
			return index, false, fmt.Errorf("decode published text block: %w", err)
		}
		if block.ID != locator.ID || block.Ordinal != locator.Ordinal || block.Kind != content.BlockText ||
			!block.Source.Matches(source) || block.ProcessorID != job.ProcessorID || block.ProcessorVersion != job.ProcessorVersion {
			return index, false, fmt.Errorf("published text block does not match its source or manifest")
		}
		if err := visit(block); err != nil {
			return index, false, err
		}
		previousEnd = locator.Offset + locator.Length
	}
	return len(manifest.Blocks), true, nil
}

func decodeContentMaintenanceResultManifest(data []byte) (contentMaintenanceResultManifest, error) {
	var manifest contentMaintenanceResultManifest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return contentMaintenanceResultManifest{}, fmt.Errorf("decode published text block manifest: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return contentMaintenanceResultManifest{}, fmt.Errorf("published text block manifest contains trailing data")
	}
	return manifest, nil
}

func writeContentMaintenanceBytes(writer interface{ Write([]byte) (int, error) }, data []byte) error {
	written, err := writer.Write(data)
	if err != nil {
		return err
	}
	if written != len(data) {
		return fmt.Errorf("short write while persisting derived CONTENT block stream")
	}
	return nil
}

func contentMaintenanceRetryDelay(attempt uint32) time.Duration {
	if attempt == 0 {
		attempt = 1
	}
	if attempt > 8 {
		attempt = 8
	}
	delay := time.Second << (attempt - 1)
	if delay > 5*time.Minute {
		return 5 * time.Minute
	}
	return delay
}
