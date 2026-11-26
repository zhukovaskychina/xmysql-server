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
	"strings"
	"time"
)

const (
	pdfWorkerArgument      = "--xmysql-internal-pdf-worker"
	maxPDFWorkerHeaderSize = 1 << 20
	maxPDFWorkerOutputSize = 64 << 20
	maxPDFWorkerErrorBytes = 64 << 10
	pdfWorkerTimeout       = 90 * time.Second
	pdfWorkerConcurrency   = 2
)

var pdfWorkerSlots = make(chan struct{}, pdfWorkerConcurrency)

type IsolatedPDFPageParser struct {
	Executable string
	Args       []string
	Env        []string
}

type pdfWorkerRequest struct {
	Source   SourceRef  `json:"source"`
	MIME     string     `json:"mime"`
	Revision uint64     `json:"revision"`
	Size     uint64     `json:"size"`
	Digest   string     `json:"digest"`
	Options  PDFOptions `json:"options"`
}

// ParsePDFPages runs the PDF parser in a bounded helper process. The parent
// streams the immutable CONTENT value to stdin; the helper returns only
// source-bound page blocks. Cancellation kills the helper process.
func (parser IsolatedPDFPageParser) ParsePDFPages(ctx context.Context, store *SegmentStore, value Value, source SourceRef, options PDFOptions) ([]Block, error) {
	options, err := validatePDFInput(value, source, options)
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
			return nil, fmt.Errorf("locate XMySQL PDF worker executable: %w", err)
		}
	}
	if len(parser.Args) == 0 {
		parser.Args = []string{pdfWorkerArgument}
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
		return nil, fmt.Errorf("create PDF worker input pipe: %w", err)
	}
	stdout := &boundedWorkerBuffer{limit: maxPDFWorkerOutputSize, worker: "PDF"}
	stderr := &truncatedPDFWorkerBuffer{limit: maxPDFWorkerErrorBytes}
	command.Stdout = stdout
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("start PDF worker: %w", err)
	}
	writeErr := writePDFWorkerRequest(workerCtx, stdin, store, value, source, options)
	closeErr := stdin.Close()
	waitErr := command.Wait()
	if contextErr := workerCtx.Err(); contextErr != nil {
		return nil, contextErr
	}
	if writeErr != nil {
		return nil, fmt.Errorf("send PDF to worker: %w", writeErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close PDF worker input: %w", closeErr)
	}
	if waitErr != nil {
		message := strings.TrimSpace(stderr.String())
		if message != "" {
			return nil, fmt.Errorf("PDF worker failed: %s: %w", message, waitErr)
		}
		return nil, fmt.Errorf("PDF worker failed: %w", waitErr)
	}
	var blocks []Block
	if err := json.Unmarshal(stdout.Bytes(), &blocks); err != nil {
		return nil, fmt.Errorf("decode PDF worker result: %w", err)
	}
	if len(blocks) > options.MaxPages {
		return nil, fmt.Errorf("PDF worker returned %d pages beyond the configured limit %d", len(blocks), options.MaxPages)
	}
	for _, block := range blocks {
		if err := block.Validate(); err != nil || !block.Source.Matches(source) {
			return nil, fmt.Errorf("PDF worker returned invalid or foreign page evidence")
		}
	}
	return blocks, nil
}

func writePDFWorkerRequest(ctx context.Context, writer io.Writer, store *SegmentStore, value Value, source SourceRef, options PDFOptions) error {
	request := pdfWorkerRequest{
		Source: source, MIME: value.MIMEType, Revision: value.Revision, Size: value.Size(),
		Digest: hex.EncodeToString(value.Digest[:]), Options: options,
	}
	header, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode PDF worker request: %w", err)
	}
	if len(header) == 0 || len(header) > maxPDFWorkerHeaderSize {
		return fmt.Errorf("PDF worker request header exceeds %d bytes", maxPDFWorkerHeaderSize)
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
			return fmt.Errorf("PDF CONTENT range at %d returned %d bytes, expected %d", offset, len(chunk), length)
		}
		if err := writePDFWorkerBytes(writer, chunk); err != nil {
			return err
		}
		offset += length
	}
	return nil
}

func writePDFWorkerBytes(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written <= 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

// RunPDFWorker serves one framed request on stdin and writes one JSON result
// to stdout. The process exits after this call; the server invokes it only
// through the private command-line mode.
func RunPDFWorker(stdin io.Reader, stdout io.Writer) error {
	blocks, err := runPDFWorker(stdin, stdout)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(blocks)
}

func runPDFWorker(input io.Reader, _ io.Writer) ([]Block, error) {
	var frame [4]byte
	if _, err := io.ReadFull(input, frame[:]); err != nil {
		return nil, fmt.Errorf("read PDF worker request header length: %w", err)
	}
	headerLength := binary.BigEndian.Uint32(frame[:])
	if headerLength == 0 || headerLength > maxPDFWorkerHeaderSize {
		return nil, fmt.Errorf("PDF worker header length %d is invalid", headerLength)
	}
	headerBytes := make([]byte, int(headerLength))
	if _, err := io.ReadFull(input, headerBytes); err != nil {
		return nil, fmt.Errorf("read PDF worker request header: %w", err)
	}
	var request pdfWorkerRequest
	if err := json.Unmarshal(headerBytes, &request); err != nil {
		return nil, fmt.Errorf("decode PDF worker request header: %w", err)
	}
	if request.Size == 0 || request.Size > defaultPDFMaxBytes {
		return nil, fmt.Errorf("PDF worker input size %d exceeds the configured limit", request.Size)
	}
	digest, err := hex.DecodeString(request.Digest)
	if err != nil || len(digest) != len([32]byte{}) {
		return nil, fmt.Errorf("PDF worker source digest is invalid")
	}
	pdfBytes, err := io.ReadAll(io.LimitReader(input, int64(request.Size)+1))
	if err != nil {
		return nil, fmt.Errorf("read PDF worker source bytes: %w", err)
	}
	if uint64(len(pdfBytes)) != request.Size {
		return nil, fmt.Errorf("PDF worker source has %d bytes, expected %d", len(pdfBytes), request.Size)
	}
	var extra [1]byte
	if count, err := input.Read(extra[:]); count != 0 {
		return nil, fmt.Errorf("PDF worker input contains trailing bytes")
	} else if err != nil && err != io.EOF {
		return nil, fmt.Errorf("check PDF worker input boundary: %w", err)
	}
	if err := request.Source.Validate(); err != nil {
		return nil, fmt.Errorf("PDF worker source identity is invalid: %w", err)
	}
	value, err := NewValue(request.MIME, request.Revision, pdfBytes)
	if err != nil {
		return nil, fmt.Errorf("validate PDF worker source: %w", err)
	}
	if !bytes.Equal(value.Digest[:], digest) ||
		request.Source.Revision != value.Revision || request.Source.Digest != value.Digest {
		return nil, fmt.Errorf("PDF worker source identity does not match the supplied bytes")
	}
	blocks, err := ParsePDFPages(context.Background(), nil, value, request.Source, request.Options)
	if err != nil {
		return nil, fmt.Errorf("parse PDF worker source: %w", err)
	}
	return blocks, nil
}

type boundedWorkerBuffer struct {
	bytes.Buffer
	limit  int
	worker string
}

func (buffer *boundedWorkerBuffer) Write(p []byte) (int, error) {
	if len(p) > buffer.limit-buffer.Len() {
		return 0, fmt.Errorf("%s worker output exceeds the configured limit", buffer.worker)
	}
	return buffer.Buffer.Write(p)
}

type truncatedPDFWorkerBuffer struct {
	bytes.Buffer
	limit int
}

func (buffer *truncatedPDFWorkerBuffer) Write(p []byte) (int, error) {
	remaining := buffer.limit - buffer.Len()
	if remaining > 0 {
		if remaining > len(p) {
			remaining = len(p)
		}
		_, _ = buffer.Buffer.Write(p[:remaining])
	}
	return len(p), nil
}
