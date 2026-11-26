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

const pptxWorkerArgument = "--xmysql-internal-pptx-worker"

type PPTXBlockParser interface {
	ParsePPTXBlocks(context.Context, *SegmentStore, Value, SourceRef, PPTXOptions) ([]Block, error)
}

type InProcessPPTXBlockParser struct{}

func (InProcessPPTXBlockParser) ParsePPTXBlocks(ctx context.Context, store *SegmentStore, value Value, source SourceRef, options PPTXOptions) ([]Block, error) {
	return ParsePPTXBlocks(ctx, store, value, source, options)
}

type IsolatedPPTXBlockParser struct {
	Executable string
	Args       []string
	Env        []string
}

type pptxWorkerRequest struct {
	Source   SourceRef   `json:"source"`
	MIME     string      `json:"mime"`
	Revision uint64      `json:"revision"`
	Size     uint64      `json:"size"`
	Digest   string      `json:"digest"`
	Options  PPTXOptions `json:"options"`
}

func (parser IsolatedPPTXBlockParser) ParsePPTXBlocks(ctx context.Context, store *SegmentStore, value Value, source SourceRef, options PPTXOptions) ([]Block, error) {
	options, err := validatePPTXInput(value, source, options)
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
			return nil, fmt.Errorf("locate XMySQL PPTX worker executable: %w", err)
		}
	}
	if len(parser.Args) == 0 {
		parser.Args = []string{pptxWorkerArgument}
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
		return nil, fmt.Errorf("create PPTX worker input pipe: %w", err)
	}
	stdout := &boundedWorkerBuffer{limit: maxPDFWorkerOutputSize, worker: "PPTX"}
	stderr := &truncatedPDFWorkerBuffer{limit: maxPDFWorkerErrorBytes}
	command.Stdout, command.Stderr = stdout, stderr
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("start PPTX worker: %w", err)
	}
	writeErr := writePPTXWorkerRequest(workerCtx, stdin, store, value, source, options)
	closeErr := stdin.Close()
	waitErr := command.Wait()
	if contextErr := workerCtx.Err(); contextErr != nil {
		return nil, contextErr
	}
	if writeErr != nil {
		return nil, fmt.Errorf("send PPTX to worker: %w", writeErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close PPTX worker input: %w", closeErr)
	}
	if waitErr != nil {
		if stderr.Len() > 0 {
			return nil, fmt.Errorf("PPTX worker failed: %s: %w", stderr.String(), waitErr)
		}
		return nil, fmt.Errorf("PPTX worker failed: %w", waitErr)
	}
	var blocks []Block
	if err := json.Unmarshal(stdout.Bytes(), &blocks); err != nil {
		return nil, fmt.Errorf("decode PPTX worker result: %w", err)
	}
	if len(blocks) > options.MaxParagraphs {
		return nil, fmt.Errorf("PPTX worker returned %d blocks beyond the configured limit %d", len(blocks), options.MaxParagraphs)
	}
	for _, block := range blocks {
		if err := block.Validate(); err != nil || !block.Source.Matches(source) {
			return nil, fmt.Errorf("PPTX worker returned invalid or foreign source evidence")
		}
	}
	return blocks, nil
}

func writePPTXWorkerRequest(ctx context.Context, writer io.Writer, store *SegmentStore, value Value, source SourceRef, options PPTXOptions) error {
	request := pptxWorkerRequest{Source: source, MIME: value.MIMEType, Revision: value.Revision,
		Size: value.Size(), Digest: hex.EncodeToString(value.Digest[:]), Options: options}
	header, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode PPTX worker request: %w", err)
	}
	if len(header) == 0 || len(header) > maxPDFWorkerHeaderSize {
		return fmt.Errorf("PPTX worker request header exceeds %d bytes", maxPDFWorkerHeaderSize)
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
			return fmt.Errorf("PPTX CONTENT range at %d returned %d bytes, expected %d", offset, len(chunk), length)
		}
		if err := writePDFWorkerBytes(writer, chunk); err != nil {
			return err
		}
		offset += length
	}
	return nil
}

func RunPPTXWorker(stdin io.Reader, stdout io.Writer) error {
	blocks, err := runPPTXWorker(stdin)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(blocks)
}

func runPPTXWorker(input io.Reader) ([]Block, error) {
	var frame [4]byte
	if _, err := io.ReadFull(input, frame[:]); err != nil {
		return nil, fmt.Errorf("read PPTX worker header length: %w", err)
	}
	headerLength := binary.BigEndian.Uint32(frame[:])
	if headerLength == 0 || headerLength > maxPDFWorkerHeaderSize {
		return nil, fmt.Errorf("PPTX worker header length %d is invalid", headerLength)
	}
	header := make([]byte, int(headerLength))
	if _, err := io.ReadFull(input, header); err != nil {
		return nil, fmt.Errorf("read PPTX worker header: %w", err)
	}
	var request pptxWorkerRequest
	if err := json.Unmarshal(header, &request); err != nil {
		return nil, fmt.Errorf("decode PPTX worker header: %w", err)
	}
	if request.Size == 0 || request.Size > defaultPPTXSource {
		return nil, fmt.Errorf("PPTX worker input size %d exceeds the configured limit", request.Size)
	}
	digest, err := hex.DecodeString(request.Digest)
	if err != nil || len(digest) != len([32]byte{}) {
		return nil, fmt.Errorf("PPTX worker source digest is invalid")
	}
	data, err := io.ReadAll(io.LimitReader(input, int64(request.Size)+1))
	if err != nil {
		return nil, fmt.Errorf("read PPTX worker source bytes: %w", err)
	}
	if uint64(len(data)) != request.Size {
		return nil, fmt.Errorf("PPTX worker source has %d bytes, expected %d", len(data), request.Size)
	}
	var extra [1]byte
	if count, err := input.Read(extra[:]); count != 0 {
		return nil, fmt.Errorf("PPTX worker input contains trailing bytes")
	} else if err != nil && err != io.EOF {
		return nil, fmt.Errorf("check PPTX worker input boundary: %w", err)
	}
	if err := request.Source.Validate(); err != nil {
		return nil, fmt.Errorf("PPTX worker source identity is invalid: %w", err)
	}
	value, err := NewValue(request.MIME, request.Revision, data)
	if err != nil {
		return nil, fmt.Errorf("validate PPTX worker source: %w", err)
	}
	if !bytes.Equal(value.Digest[:], digest) || request.Source.Revision != value.Revision || request.Source.Digest != value.Digest {
		return nil, fmt.Errorf("PPTX worker source identity does not match the supplied bytes")
	}
	blocks, err := ParsePPTXBlocks(context.Background(), nil, value, request.Source, request.Options)
	if err != nil {
		return nil, fmt.Errorf("parse PPTX worker source: %w", err)
	}
	return blocks, nil
}
