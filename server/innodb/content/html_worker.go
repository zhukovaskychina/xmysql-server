package content

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"os"
	"os/exec"
)

const (
	htmlWorkerArgument      = "--xmysql-internal-html-worker"
	maxHTMLWorkerOutputSize = 64 << 20
)

type IsolatedHTMLBlockParser struct {
	Executable string
	Args       []string
	Env        []string
}

type htmlWorkerRequest struct {
	Source   SourceRef   `json:"source"`
	MIME     string      `json:"mime"`
	Revision uint64      `json:"revision"`
	Size     uint64      `json:"size"`
	Digest   string      `json:"digest"`
	Options  HTMLOptions `json:"options"`
}

// ParseHTMLBlocks isolates tokenization from the query process and streams the
// original CONTENT bytes to the worker in bounded ranges.
func (parser IsolatedHTMLBlockParser) ParseHTMLBlocks(ctx context.Context, store *SegmentStore, value Value, source SourceRef, options HTMLOptions) ([]Block, error) {
	options, err := validateHTMLInput(value, source, options)
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
			return nil, fmt.Errorf("locate XMySQL HTML worker executable: %w", err)
		}
	}
	if len(parser.Args) == 0 {
		parser.Args = []string{htmlWorkerArgument}
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
		return nil, fmt.Errorf("create HTML worker input pipe: %w", err)
	}
	stdout := &boundedWorkerBuffer{limit: maxHTMLWorkerOutputSize, worker: "HTML"}
	stderr := &truncatedPDFWorkerBuffer{limit: maxPDFWorkerErrorBytes}
	command.Stdout, command.Stderr = stdout, stderr
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("start HTML worker: %w", err)
	}
	writeErr := writeHTMLWorkerRequest(workerCtx, stdin, store, value, source, options)
	closeErr := stdin.Close()
	waitErr := command.Wait()
	if contextErr := workerCtx.Err(); contextErr != nil {
		return nil, contextErr
	}
	if writeErr != nil {
		return nil, fmt.Errorf("send HTML to worker: %w", writeErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close HTML worker input: %w", closeErr)
	}
	if waitErr != nil {
		if stderr.Len() > 0 {
			return nil, fmt.Errorf("HTML worker failed: %s: %w", stderr.String(), waitErr)
		}
		return nil, fmt.Errorf("HTML worker failed: %w", waitErr)
	}
	var blocks []Block
	if err := json.Unmarshal(stdout.Bytes(), &blocks); err != nil {
		return nil, fmt.Errorf("decode HTML worker result: %w", err)
	}
	if len(blocks) > options.MaxBlocks {
		return nil, fmt.Errorf("HTML worker returned %d blocks beyond the configured limit %d", len(blocks), options.MaxBlocks)
	}
	for _, block := range blocks {
		if err := block.Validate(); err != nil || !block.Source.Matches(source) {
			return nil, fmt.Errorf("HTML worker returned invalid or foreign source evidence")
		}
	}
	return blocks, nil
}

func writeHTMLWorkerRequest(ctx context.Context, writer io.Writer, store *SegmentStore, value Value, source SourceRef, options HTMLOptions) error {
	request := htmlWorkerRequest{Source: source, MIME: value.MIMEType, Revision: value.Revision,
		Size: value.Size(), Digest: hex.EncodeToString(value.Digest[:]), Options: options}
	header, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode HTML worker request: %w", err)
	}
	if len(header) == 0 || len(header) > maxPDFWorkerHeaderSize {
		return fmt.Errorf("HTML worker request header exceeds %d bytes", maxPDFWorkerHeaderSize)
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
			return fmt.Errorf("HTML CONTENT range at %d returned %d bytes, expected %d", offset, len(chunk), length)
		}
		if err := writePDFWorkerBytes(writer, chunk); err != nil {
			return err
		}
		offset += length
	}
	return nil
}

// RunHTMLWorker handles one framed source and returns source-bound HTML blocks.
func RunHTMLWorker(stdin io.Reader, stdout io.Writer) error {
	blocks, err := runHTMLWorker(stdin)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(blocks)
}

func runHTMLWorker(input io.Reader) ([]Block, error) {
	var frame [4]byte
	if _, err := io.ReadFull(input, frame[:]); err != nil {
		return nil, fmt.Errorf("read HTML worker header length: %w", err)
	}
	headerLength := binary.BigEndian.Uint32(frame[:])
	if headerLength == 0 || headerLength > maxPDFWorkerHeaderSize {
		return nil, fmt.Errorf("HTML worker header length %d is invalid", headerLength)
	}
	header := make([]byte, int(headerLength))
	if _, err := io.ReadFull(input, header); err != nil {
		return nil, fmt.Errorf("read HTML worker header: %w", err)
	}
	var request htmlWorkerRequest
	if err := json.Unmarshal(header, &request); err != nil {
		return nil, fmt.Errorf("decode HTML worker header: %w", err)
	}
	if request.Size == 0 || request.Size > defaultHTMLMaxSource {
		return nil, fmt.Errorf("HTML worker input size %d exceeds the configured limit", request.Size)
	}
	if _, err := validateHTMLInputMetadata(request.MIME, request.Revision, request.Size, request.Options); err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(request.Digest)
	if err != nil || len(digest) != len([32]byte{}) {
		return nil, fmt.Errorf("HTML worker source digest is invalid")
	}
	data, err := io.ReadAll(io.LimitReader(input, int64(request.Size)+1))
	if err != nil {
		return nil, fmt.Errorf("read HTML worker source bytes: %w", err)
	}
	if uint64(len(data)) != request.Size {
		return nil, fmt.Errorf("HTML worker source has %d bytes, expected %d", len(data), request.Size)
	}
	var extra [1]byte
	if count, err := input.Read(extra[:]); count != 0 {
		return nil, fmt.Errorf("HTML worker input contains trailing bytes")
	} else if err != nil && err != io.EOF {
		return nil, fmt.Errorf("check HTML worker input boundary: %w", err)
	}
	if err := request.Source.Validate(); err != nil {
		return nil, fmt.Errorf("HTML worker source identity is invalid: %w", err)
	}
	value, err := NewValue(request.MIME, request.Revision, data)
	if err != nil {
		return nil, fmt.Errorf("validate HTML worker source: %w", err)
	}
	if !bytes.Equal(value.Digest[:], digest) || request.Source.Revision != value.Revision || request.Source.Digest != value.Digest {
		return nil, fmt.Errorf("HTML worker source identity does not match the supplied bytes")
	}
	blocks, err := ParseHTMLBlocks(context.Background(), nil, value, request.Source, request.Options)
	if err != nil {
		return nil, fmt.Errorf("parse HTML worker source: %w", err)
	}
	return blocks, nil
}

func validateHTMLInputMetadata(mimeType string, revision, size uint64, options HTMLOptions) (HTMLOptions, error) {
	mediaType, _, err := mime.ParseMediaType(mimeType)
	if err != nil || (mediaType != "text/html" && mediaType != "application/xhtml+xml") {
		return HTMLOptions{}, fmt.Errorf("HTML worker MIME type is unsupported")
	}
	if revision == 0 {
		return HTMLOptions{}, fmt.Errorf("HTML worker revision must be positive")
	}
	if options.MaxSourceBytes == 0 {
		options.MaxSourceBytes = defaultHTMLMaxSource
	}
	if options.MaxExtractedTextBytes == 0 {
		options.MaxExtractedTextBytes = defaultHTMLMaxText
	}
	if options.MaxBlocks == 0 {
		options.MaxBlocks = defaultHTMLMaxBlocks
	}
	if options.MaxTokens == 0 {
		options.MaxTokens = defaultHTMLMaxTokens
	}
	if options.MaxSourceBytes > defaultHTMLMaxSource || options.MaxExtractedTextBytes > defaultHTMLMaxText ||
		options.MaxBlocks > defaultHTMLMaxBlocks || options.MaxTokens > defaultHTMLMaxTokens ||
		options.MaxSourceBytes == 0 || options.MaxExtractedTextBytes < 1 || options.MaxBlocks < 1 || options.MaxTokens < 1 {
		return HTMLOptions{}, fmt.Errorf("HTML worker limits exceed the built-in resource ceilings")
	}
	if size == 0 || size > options.MaxSourceBytes {
		return HTMLOptions{}, fmt.Errorf("HTML worker source size %d is outside the supported range 1..%d", size, options.MaxSourceBytes)
	}
	return options, nil
}
