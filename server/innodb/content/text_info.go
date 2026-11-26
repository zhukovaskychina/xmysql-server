package content

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"mime"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	textProcessorID      = "xmysql.plain-text"
	textProcessorVersion = "1"
	defaultTextMaxSource = uint64(32 << 20)
	defaultTextMaxOutput = (16 << 20) - 1
	defaultTextMaxBlocks = 100000
)

type TextOptions struct {
	MaxSourceBytes        uint64
	MaxExtractedTextBytes int
	MaxBlocks             int
}

// ParseTextBlocks splits plain text into paragraphs separated by blank lines.
// Every block remains bound to its exact row revision and source byte range.
func ParseTextBlocks(ctx context.Context, store *SegmentStore, value Value, source SourceRef, options TextOptions) ([]Block, error) {
	options, err := validateTextInput(value, source, options)
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	reader := bufio.NewReaderSize(&contentRangeReader{ctx: ctx, store: store, value: value}, 64<<10)
	blocks := make([]Block, 0)
	var paragraph bytes.Buffer
	var paragraphStart, paragraphEnd, offset uint64
	var totalTextBytes int

	flush := func() error {
		if paragraph.Len() == 0 {
			return nil
		}
		paragraphBytes := paragraph.Bytes()
		leading, trailing := trimTextBounds(paragraphBytes)
		trimmed := paragraphBytes[leading:trailing]
		paragraph.Reset()
		if len(trimmed) == 0 {
			return nil
		}
		if !utf8.Valid(trimmed) {
			return fmt.Errorf("plain text source must be valid UTF-8")
		}
		totalTextBytes += len(trimmed)
		if totalTextBytes > options.MaxExtractedTextBytes {
			return fmt.Errorf("plain text output exceeds the configured limit %d", options.MaxExtractedTextBytes)
		}
		start, end := paragraphStart+uint64(leading), paragraphStart+uint64(trailing)
		block, err := NewBlock(source, uint32(len(blocks)+1), BlockText, string(trimmed),
			Locator{ByteStart: &start, ByteEnd: &end}, textProcessorID, textProcessorVersion, "", "")
		if err != nil {
			return fmt.Errorf("bind plain text paragraph %d: %w", len(blocks)+1, err)
		}
		blocks = append(blocks, block)
		if len(blocks) > options.MaxBlocks {
			return fmt.Errorf("plain text produced more than %d paragraphs", options.MaxBlocks)
		}
		return nil
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		lineStart := offset
		line, readErr := reader.ReadString('\n')
		offset += uint64(len(line))
		if len(bytes.TrimSpace([]byte(line))) == 0 {
			if err := flush(); err != nil {
				return nil, err
			}
		} else {
			if paragraph.Len() == 0 {
				paragraphStart = lineStart
			}
			if _, err := paragraph.WriteString(line); err != nil {
				return nil, err
			}
			paragraphEnd = offset
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("read plain text source: %w", readErr)
		}
	}
	if paragraphEnd > paragraphStart {
		if err := flush(); err != nil {
			return nil, err
		}
	}
	return blocks, nil
}

func validateTextInput(value Value, source SourceRef, options TextOptions) (TextOptions, error) {
	if err := value.Validate(); err != nil {
		return TextOptions{}, err
	}
	if err := source.Validate(); err != nil {
		return TextOptions{}, err
	}
	if source.Revision != value.Revision || source.Digest != value.Digest {
		return TextOptions{}, fmt.Errorf("%w: text source does not match the CONTENT revision and digest", ErrInvalidBlock)
	}
	mediaType, _, err := mime.ParseMediaType(value.MIMEType)
	if err != nil || (!strings.EqualFold(mediaType, "text/plain") && !strings.EqualFold(mediaType, "text/markdown")) {
		return TextOptions{}, fmt.Errorf("plain text parser requires text/plain or text/markdown MIME type")
	}
	if options.MaxSourceBytes == 0 {
		options.MaxSourceBytes = defaultTextMaxSource
	}
	if options.MaxExtractedTextBytes == 0 {
		options.MaxExtractedTextBytes = defaultTextMaxOutput
	}
	if options.MaxBlocks == 0 {
		options.MaxBlocks = defaultTextMaxBlocks
	}
	if options.MaxSourceBytes > defaultTextMaxSource || options.MaxExtractedTextBytes > defaultTextMaxOutput ||
		options.MaxBlocks > defaultTextMaxBlocks || options.MaxSourceBytes == 0 || options.MaxExtractedTextBytes < 1 || options.MaxBlocks < 1 {
		return TextOptions{}, fmt.Errorf("plain text limits must be positive and cannot exceed built-in resource ceilings")
	}
	if value.Size() == 0 || value.Size() > options.MaxSourceBytes {
		return TextOptions{}, fmt.Errorf("plain text source size %d is outside the supported range 1..%d", value.Size(), options.MaxSourceBytes)
	}
	return options, nil
}

func trimTextBounds(value []byte) (int, int) {
	start := 0
	for start < len(value) {
		r, size := utf8.DecodeRune(value[start:])
		if !unicode.IsSpace(r) {
			break
		}
		start += size
	}
	end := len(value)
	for end > start {
		r, size := utf8.DecodeLastRune(value[:end])
		if !unicode.IsSpace(r) {
			break
		}
		end -= size
	}
	return start, end
}
