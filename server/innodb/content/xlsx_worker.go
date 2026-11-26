package content

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
)

const xlsxWorkerArgument = "--xmysql-internal-xlsx-worker"

type XLSXRowParser interface {
	ParseXLSXRows(context.Context, *SegmentStore, Value, SourceRef, XLSXOptions) ([]Block, error)
}

type InProcessXLSXRowParser struct{}

func (InProcessXLSXRowParser) ParseXLSXRows(ctx context.Context, store *SegmentStore, value Value, source SourceRef, options XLSXOptions) ([]Block, error) {
	return ParseXLSXRows(ctx, store, value, source, options)
}

type IsolatedXLSXRowParser struct {
	Executable string
	Args       []string
	Env        []string
}

type xlsxWorkerRequest struct {
	Source   SourceRef   `json:"source"`
	MIME     string      `json:"mime"`
	Revision uint64      `json:"revision"`
	Size     uint64      `json:"size"`
	Digest   string      `json:"digest"`
	Options  XLSXOptions `json:"options"`
}

// ParseXLSXRows isolates ZIP/XML expansion, transfers the immutable CONTENT
// through a bounded pipe, and validates returned blocks against the source.
func (parser IsolatedXLSXRowParser) ParseXLSXRows(ctx context.Context, store *SegmentStore, value Value, source SourceRef, options XLSXOptions) ([]Block, error) {
	options, err := validateXLSXInput(value, source, options)
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if parser.Executable == "" {
		parser.Executable, err = os.Executable()
		if err != nil {
			return nil, fmt.Errorf("locate XMySQL XLSX worker executable: %w", err)
		}
	}
	if len(parser.Args) == 0 {
		parser.Args = []string{xlsxWorkerArgument}
	}
	select {
	case pdfWorkerSlots <- struct{}{}:
		defer func() { <-pdfWorkerSlots }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	workerCtx, cancel := context.WithTimeout(ctx, pdfWorkerTimeout)
	defer cancel()
	command := exec.CommandContext(workerCtx, parser.Executable, parser.Args...)
	command.Env = append(os.Environ(), parser.Env...)
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("create XLSX worker input pipe: %w", err)
	}
	stdout := &boundedWorkerBuffer{limit: maxPDFWorkerOutputSize, worker: "XLSX"}
	stderr := &truncatedPDFWorkerBuffer{limit: maxPDFWorkerErrorBytes}
	command.Stdout, command.Stderr = stdout, stderr
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("start XLSX worker: %w", err)
	}
	writeErr := writeXLSXWorkerRequest(workerCtx, stdin, store, value, source, options)
	closeErr := stdin.Close()
	waitErr := command.Wait()
	if contextErr := workerCtx.Err(); contextErr != nil {
		return nil, contextErr
	}
	if writeErr != nil {
		return nil, fmt.Errorf("send XLSX to worker: %w", writeErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close XLSX worker input: %w", closeErr)
	}
	if waitErr != nil {
		if stderr.Len() > 0 {
			return nil, fmt.Errorf("XLSX worker failed: %s: %w", stderr.String(), waitErr)
		}
		return nil, fmt.Errorf("XLSX worker failed: %w", waitErr)
	}
	var blocks []Block
	if err := json.Unmarshal(stdout.Bytes(), &blocks); err != nil {
		return nil, fmt.Errorf("decode XLSX worker result: %w", err)
	}
	if len(blocks) > options.MaxRows {
		return nil, fmt.Errorf("XLSX worker returned %d rows beyond the configured limit %d", len(blocks), options.MaxRows)
	}
	for _, block := range blocks {
		if err := block.Validate(); err != nil || !block.Source.Matches(source) {
			return nil, fmt.Errorf("XLSX worker returned invalid or foreign row evidence")
		}
	}
	return blocks, nil
}

func writeXLSXWorkerRequest(ctx context.Context, writer io.Writer, store *SegmentStore, value Value, source SourceRef, options XLSXOptions) error {
	request := xlsxWorkerRequest{Source: source, MIME: value.MIMEType, Revision: value.Revision,
		Size: value.Size(), Digest: hex.EncodeToString(value.Digest[:]), Options: options}
	header, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode XLSX worker request: %w", err)
	}
	if len(header) == 0 || len(header) > maxPDFWorkerHeaderSize {
		return fmt.Errorf("XLSX worker request header exceeds %d bytes", maxPDFWorkerHeaderSize)
	}
	var frame [4]byte
	binary.BigEndian.PutUint32(frame[:], uint32(len(header)))
	if err := writePDFWorkerBytes(writer, frame[:]); err != nil {
		return err
	}
	if err := writePDFWorkerBytes(writer, header); err != nil {
		return err
	}
	for offset := uint64(0); offset < value.Size(); {
		if err := ctx.Err(); err != nil {
			return err
		}
		length := value.Size() - offset
		if length > MaxReadRangeBytes {
			length = MaxReadRangeBytes
		}
		chunk, err := value.ReadRangeFromStore(ctx, store, offset, length)
		if err != nil {
			return err
		}
		if uint64(len(chunk)) != length {
			return fmt.Errorf("XLSX CONTENT range at %d returned %d bytes, expected %d", offset, len(chunk), length)
		}
		if err := writePDFWorkerBytes(writer, chunk); err != nil {
			return err
		}
		offset += length
	}
	return nil
}

// RunXLSXWorker handles one framed request and returns one JSON response.
func RunXLSXWorker(stdin io.Reader, stdout io.Writer) error {
	blocks, err := runXLSXWorker(stdin)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(blocks)
}

func runXLSXWorker(input io.Reader) ([]Block, error) {
	var frame [4]byte
	if _, err := io.ReadFull(input, frame[:]); err != nil {
		return nil, fmt.Errorf("read XLSX worker header length: %w", err)
	}
	headerLength := binary.BigEndian.Uint32(frame[:])
	if headerLength == 0 || headerLength > maxPDFWorkerHeaderSize {
		return nil, fmt.Errorf("XLSX worker header length %d is invalid", headerLength)
	}
	header := make([]byte, int(headerLength))
	if _, err := io.ReadFull(input, header); err != nil {
		return nil, fmt.Errorf("read XLSX worker header: %w", err)
	}
	var request xlsxWorkerRequest
	if err := json.Unmarshal(header, &request); err != nil {
		return nil, fmt.Errorf("decode XLSX worker header: %w", err)
	}
	if request.Size == 0 || request.Size > xlsxMaxSourceBytes {
		return nil, fmt.Errorf("XLSX worker input size %d exceeds the configured limit", request.Size)
	}
	digest, err := hex.DecodeString(request.Digest)
	if err != nil || len(digest) != 32 {
		return nil, fmt.Errorf("XLSX worker source digest is invalid")
	}
	sourceBytes, err := io.ReadAll(io.LimitReader(input, int64(request.Size)+1))
	if err != nil {
		return nil, fmt.Errorf("read XLSX worker source bytes: %w", err)
	}
	if uint64(len(sourceBytes)) != request.Size {
		return nil, fmt.Errorf("XLSX worker source has %d bytes, expected %d", len(sourceBytes), request.Size)
	}
	var extra [1]byte
	if count, err := input.Read(extra[:]); count != 0 {
		return nil, fmt.Errorf("XLSX worker input contains trailing bytes")
	} else if err != nil && err != io.EOF {
		return nil, fmt.Errorf("check XLSX worker input boundary: %w", err)
	}
	if err := request.Source.Validate(); err != nil {
		return nil, fmt.Errorf("XLSX worker source identity is invalid: %w", err)
	}
	value, err := NewValue(request.MIME, request.Revision, sourceBytes)
	if err != nil {
		return nil, fmt.Errorf("validate XLSX worker source: %w", err)
	}
	if !bytes.Equal(value.Digest[:], digest) || request.Source.Revision != value.Revision || request.Source.Digest != value.Digest {
		return nil, fmt.Errorf("XLSX worker source identity does not match supplied bytes")
	}
	blocks, err := ParseXLSXRows(context.Background(), nil, value, request.Source, request.Options)
	if err != nil {
		return nil, fmt.Errorf("parse XLSX worker source: %w", err)
	}
	return blocks, nil
}
