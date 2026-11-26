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

const docxWorkerArgument = "--xmysql-internal-docx-worker"

type DOCXBlockParser interface {
	ParseDOCXBlocks(context.Context, *SegmentStore, Value, SourceRef, DOCXOptions) ([]Block, error)
}

type InProcessDOCXBlockParser struct{}

func (InProcessDOCXBlockParser) ParseDOCXBlocks(ctx context.Context, store *SegmentStore, value Value, source SourceRef, options DOCXOptions) ([]Block, error) {
	return ParseDOCXBlocks(ctx, store, value, source, options)
}

type IsolatedDOCXBlockParser struct {
	Executable string
	Args       []string
	Env        []string
}

type docxWorkerRequest struct {
	Source   SourceRef   `json:"source"`
	MIME     string      `json:"mime"`
	Revision uint64      `json:"revision"`
	Size     uint64      `json:"size"`
	Digest   string      `json:"digest"`
	Options  DOCXOptions `json:"options"`
}

// ParseDOCXBlocks runs ZIP and XML parsing in a helper process, streams the
// immutable source value through stdin, and accepts only source-bound blocks.
func (parser IsolatedDOCXBlockParser) ParseDOCXBlocks(ctx context.Context, store *SegmentStore, value Value, source SourceRef, options DOCXOptions) ([]Block, error) {
	options, err := validateDOCXInput(value, source, options)
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
			return nil, fmt.Errorf("locate XMySQL DOCX worker executable: %w", err)
		}
	}
	if len(parser.Args) == 0 {
		parser.Args = []string{docxWorkerArgument}
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
		return nil, fmt.Errorf("create DOCX worker input pipe: %w", err)
	}
	stdout := &boundedPDFWorkerBuffer{limit: maxPDFWorkerOutputSize}
	stderr := &truncatedPDFWorkerBuffer{limit: maxPDFWorkerErrorBytes}
	command.Stdout, command.Stderr = stdout, stderr
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("start DOCX worker: %w", err)
	}
	writeErr := writeDOCXWorkerRequest(workerCtx, stdin, store, value, source, options)
	closeErr := stdin.Close()
	waitErr := command.Wait()
	if contextErr := workerCtx.Err(); contextErr != nil {
		return nil, contextErr
	}
	if writeErr != nil {
		return nil, fmt.Errorf("send DOCX to worker: %w", writeErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close DOCX worker input: %w", closeErr)
	}
	if waitErr != nil {
		if stderr.Len() > 0 {
			return nil, fmt.Errorf("DOCX worker failed: %s: %w", stderr.String(), waitErr)
		}
		return nil, fmt.Errorf("DOCX worker failed: %w", waitErr)
	}
	var blocks []Block
	if err := json.Unmarshal(stdout.Bytes(), &blocks); err != nil {
		return nil, fmt.Errorf("decode DOCX worker result: %w", err)
	}
	if len(blocks) > options.MaxParagraphs {
		return nil, fmt.Errorf("DOCX worker returned %d blocks beyond the configured limit %d", len(blocks), options.MaxParagraphs)
	}
	for _, block := range blocks {
		if err := block.Validate(); err != nil || !block.Source.Matches(source) {
			return nil, fmt.Errorf("DOCX worker returned invalid or foreign paragraph evidence")
		}
	}
	return blocks, nil
}

func writeDOCXWorkerRequest(ctx context.Context, writer io.Writer, store *SegmentStore, value Value, source SourceRef, options DOCXOptions) error {
	request := docxWorkerRequest{Source: source, MIME: value.MIMEType, Revision: value.Revision,
		Size: value.Size(), Digest: hex.EncodeToString(value.Digest[:]), Options: options}
	header, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode DOCX worker request: %w", err)
	}
	if len(header) == 0 || len(header) > maxPDFWorkerHeaderSize {
		return fmt.Errorf("DOCX worker request header exceeds %d bytes", maxPDFWorkerHeaderSize)
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
			return fmt.Errorf("DOCX CONTENT range at %d returned %d bytes, expected %d", offset, len(chunk), length)
		}
		if err := writePDFWorkerBytes(writer, chunk); err != nil {
			return err
		}
		offset += length
	}
	return nil
}

// RunDOCXWorker serves one framed DOCX parse request and writes one JSON result.
func RunDOCXWorker(stdin io.Reader, stdout io.Writer) error {
	blocks, err := runDOCXWorker(stdin)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(blocks)
}

func runDOCXWorker(input io.Reader) ([]Block, error) {
	var frame [4]byte
	if _, err := io.ReadFull(input, frame[:]); err != nil {
		return nil, fmt.Errorf("read DOCX worker request header length: %w", err)
	}
	headerLength := binary.BigEndian.Uint32(frame[:])
	if headerLength == 0 || headerLength > maxPDFWorkerHeaderSize {
		return nil, fmt.Errorf("DOCX worker header length %d is invalid", headerLength)
	}
	headerBytes := make([]byte, int(headerLength))
	if _, err := io.ReadFull(input, headerBytes); err != nil {
		return nil, fmt.Errorf("read DOCX worker request header: %w", err)
	}
	var request docxWorkerRequest
	if err := json.Unmarshal(headerBytes, &request); err != nil {
		return nil, fmt.Errorf("decode DOCX worker request header: %w", err)
	}
	if request.Size == 0 || request.Size > defaultDOCXMaxSize {
		return nil, fmt.Errorf("DOCX worker input size %d exceeds the configured limit", request.Size)
	}
	digest, err := hex.DecodeString(request.Digest)
	if err != nil || len(digest) != 32 {
		return nil, fmt.Errorf("DOCX worker source digest is invalid")
	}
	docxBytes, err := io.ReadAll(io.LimitReader(input, int64(request.Size)+1))
	if err != nil {
		return nil, fmt.Errorf("read DOCX worker source bytes: %w", err)
	}
	if uint64(len(docxBytes)) != request.Size {
		return nil, fmt.Errorf("DOCX worker source has %d bytes, expected %d", len(docxBytes), request.Size)
	}
	var extra [1]byte
	if count, err := input.Read(extra[:]); count != 0 {
		return nil, fmt.Errorf("DOCX worker input contains trailing bytes")
	} else if err != nil && err != io.EOF {
		return nil, fmt.Errorf("check DOCX worker input boundary: %w", err)
	}
	if err := request.Source.Validate(); err != nil {
		return nil, fmt.Errorf("DOCX worker source identity is invalid: %w", err)
	}
	value, err := NewValue(request.MIME, request.Revision, docxBytes)
	if err != nil {
		return nil, fmt.Errorf("validate DOCX worker source: %w", err)
	}
	if !bytes.Equal(value.Digest[:], digest) || request.Source.Revision != value.Revision || request.Source.Digest != value.Digest {
		return nil, fmt.Errorf("DOCX worker source identity does not match the supplied bytes")
	}
	blocks, err := ParseDOCXBlocks(context.Background(), nil, value, request.Source, request.Options)
	if err != nil {
		return nil, fmt.Errorf("parse DOCX worker source: %w", err)
	}
	return blocks, nil
}
