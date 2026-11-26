package content

import (
	"context"
	"fmt"
	"io"
	"mime"
	"strings"
	"unicode/utf8"

	"golang.org/x/net/html"
)

const (
	htmlProcessorID      = "xmysql.html-text"
	htmlProcessorVersion = "1"
	defaultHTMLMaxSource = uint64(32 << 20)
	defaultHTMLMaxText   = (16 << 20) - 1
	defaultHTMLMaxBlocks = 100000
	defaultHTMLMaxTokens = 1000000
	maxHTMLTokenBytes    = 1 << 20
	maxHTMLNesting       = 256
)

type HTMLOptions struct {
	MaxSourceBytes        uint64
	MaxExtractedTextBytes int
	MaxBlocks             int
	MaxTokens             int
}

type HTMLBlockParser interface {
	ParseHTMLBlocks(context.Context, *SegmentStore, Value, SourceRef, HTMLOptions) ([]Block, error)
}

type InProcessHTMLBlockParser struct{}

func (InProcessHTMLBlockParser) ParseHTMLBlocks(ctx context.Context, store *SegmentStore, value Value, source SourceRef, options HTMLOptions) ([]Block, error) {
	return ParseHTMLBlocks(ctx, store, value, source, options)
}

// ParseHTMLBlocks extracts source-bound text units from UTF-8 HTML. It uses
// the HTML tokenizer without constructing a document tree, skips executable
// and hidden subtrees, and records byte ranges in the original CONTENT value.
func ParseHTMLBlocks(ctx context.Context, store *SegmentStore, value Value, source SourceRef, options HTMLOptions) ([]Block, error) {
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
	reader := &htmlValueReader{ctx: ctx, store: store, value: value}
	tokenizer := html.NewTokenizer(reader)
	tokenizer.SetMaxBuf(maxHTMLTokenBytes)

	rootCounts := make(map[string]uint64)
	elements := make([]htmlElementFrame, 0, 32)
	blocks := make([]Block, 0)
	var flow *htmlBlockFrame
	var totalTextBytes int
	var tokenCount int
	var offset uint64
	var skipTags []string

	flush := func(frame *htmlBlockFrame, end uint64) error {
		text := frame.text.String()
		if frame.preserveSpace {
			text = strings.TrimSpace(text)
		} else {
			text = strings.Join(strings.Fields(text), " ")
		}
		if text == "" {
			return nil
		}
		if !utf8.ValidString(text) {
			return fmt.Errorf("HTML extracted text is not valid UTF-8")
		}
		totalTextBytes += len(text)
		if totalTextBytes > options.MaxExtractedTextBytes {
			return fmt.Errorf("HTML extracted text exceeds the configured limit %d", options.MaxExtractedTextBytes)
		}
		start, finish := frame.start, end
		block, err := NewBlock(source, uint32(len(blocks)+1), frame.kind, text,
			Locator{ByteStart: &start, ByteEnd: &finish}, htmlProcessorID, htmlProcessorVersion, "", "")
		if err != nil {
			return fmt.Errorf("bind HTML block %d: %w", len(blocks)+1, err)
		}
		block.Attributes = map[string]string{"structure_path": frame.path, "html_tag": frame.tag}
		blocks = append(blocks, block)
		if len(blocks) > options.MaxBlocks {
			return fmt.Errorf("HTML produced more than %d text blocks", options.MaxBlocks)
		}
		return nil
	}
	flushFlow := func(end uint64) error {
		if flow == nil {
			return nil
		}
		frame := flow
		flow = nil
		return flush(frame, end)
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		tokenType := tokenizer.Next()
		raw := tokenizer.Raw()
		start := offset
		offset += uint64(len(raw))
		tokenCount++
		if tokenCount > options.MaxTokens {
			return nil, fmt.Errorf("HTML token count exceeds the configured limit %d", options.MaxTokens)
		}
		if tokenType == html.ErrorToken {
			if err := tokenizer.Err(); err != nil && err != io.EOF {
				return nil, fmt.Errorf("tokenize HTML source: %w", err)
			}
			break
		}
		if len(raw) > maxHTMLTokenBytes {
			return nil, fmt.Errorf("HTML token exceeds %d bytes", maxHTMLTokenBytes)
		}
		switch tokenType {
		case html.StartTagToken, html.SelfClosingTagToken:
			token := tokenizer.Token()
			tag := strings.ToLower(token.Data)
			if len(skipTags) > 0 {
				if tokenType == html.StartTagToken && !isHTMLVoidElement(tag) {
					skipTags = append(skipTags, tag)
				}
				continue
			}
			if tag == "script" || tag == "style" || tag == "template" || tag == "noscript" || tag == "iframe" || tag == "object" {
				if tokenType == html.StartTagToken {
					skipTags = append(skipTags, tag)
				}
				continue
			}
			if tag == "br" || tag == "hr" {
				if active := activeHTMLBlock(elements); active != nil {
					active.text.WriteByte(' ')
				}
				continue
			}
			if !isHTMLVoidElement(tag) && tokenType == html.StartTagToken && len(elements) >= maxHTMLNesting {
				return nil, fmt.Errorf("HTML nesting exceeds %d elements", maxHTMLNesting)
			}
			parentPath := ""
			counts := rootCounts
			if len(elements) > 0 {
				parent := &elements[len(elements)-1]
				parentPath, counts = parent.path, parent.children
			}
			counts[tag]++
			path := fmt.Sprintf("%s/%s[%d]", parentPath, tag, counts[tag])
			blockKind, isBlock := htmlBlockKind(tag)
			var block *htmlBlockFrame
			if isBlock {
				if err := flushFlow(start); err != nil {
					return nil, err
				}
				block = &htmlBlockFrame{tag: tag, path: path, start: start, kind: blockKind,
					preserveSpace: tag == "pre"}
			}
			if tokenType == html.StartTagToken && !isHTMLVoidElement(tag) {
				elements = append(elements, htmlElementFrame{tag: tag, path: path, children: make(map[string]uint64), block: block})
			}
		case html.EndTagToken:
			token := tokenizer.Token()
			tag := strings.ToLower(token.Data)
			if len(skipTags) > 0 {
				for index := len(skipTags) - 1; index >= 0; index-- {
					if skipTags[index] == tag {
						skipTags = skipTags[:index]
						break
					}
				}
				continue
			}
			match := -1
			for index := len(elements) - 1; index >= 0; index-- {
				if elements[index].tag == tag {
					match = index
					break
				}
			}
			if match < 0 {
				continue
			}
			for index := len(elements) - 1; index >= match; index-- {
				if frame := elements[index].block; frame != nil {
					if err := flush(frame, offset); err != nil {
						return nil, err
					}
				}
			}
			elements = elements[:match]
		case html.TextToken:
			if len(skipTags) > 0 {
				continue
			}
			text := string(tokenizer.Text())
			if !utf8.ValidString(text) {
				return nil, fmt.Errorf("HTML source text must be UTF-8")
			}
			active := activeHTMLBlock(elements)
			if active == nil {
				if flow == nil {
					flow = &htmlBlockFrame{tag: "text", path: "/text[1]", start: start, kind: BlockText}
				}
				active = flow
			}
			if _, err := active.text.WriteString(text); err != nil {
				return nil, err
			}
		}
	}
	for index := len(elements) - 1; index >= 0; index-- {
		if frame := elements[index].block; frame != nil {
			if err := flush(frame, offset); err != nil {
				return nil, err
			}
		}
	}
	if err := flushFlow(offset); err != nil {
		return nil, err
	}
	return blocks, nil
}

func validateHTMLInput(value Value, source SourceRef, options HTMLOptions) (HTMLOptions, error) {
	if err := value.Validate(); err != nil {
		return HTMLOptions{}, err
	}
	if err := source.Validate(); err != nil {
		return HTMLOptions{}, err
	}
	if source.Revision != value.Revision || source.Digest != value.Digest {
		return HTMLOptions{}, fmt.Errorf("%w: HTML source does not match the CONTENT revision and digest", ErrInvalidBlock)
	}
	mediaType, _, err := mime.ParseMediaType(value.MIMEType)
	if err != nil || (!strings.EqualFold(mediaType, "text/html") && !strings.EqualFold(mediaType, "application/xhtml+xml")) {
		return HTMLOptions{}, fmt.Errorf("HTML parser requires text/html or application/xhtml+xml MIME type")
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
		return HTMLOptions{}, fmt.Errorf("HTML limits must be positive and cannot exceed built-in resource ceilings")
	}
	if value.Size() == 0 || value.Size() > options.MaxSourceBytes {
		return HTMLOptions{}, fmt.Errorf("HTML source size %d is outside the supported range 1..%d", value.Size(), options.MaxSourceBytes)
	}
	return options, nil
}

type htmlElementFrame struct {
	tag      string
	path     string
	children map[string]uint64
	block    *htmlBlockFrame
}

type htmlBlockFrame struct {
	tag           string
	path          string
	start         uint64
	kind          BlockKind
	preserveSpace bool
	text          strings.Builder
}

type htmlValueReader struct {
	ctx    context.Context
	store  *SegmentStore
	value  Value
	offset uint64
	buffer []byte
}

func (reader *htmlValueReader) Read(destination []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	if len(destination) == 0 {
		return 0, nil
	}
	if len(reader.buffer) == 0 {
		if reader.offset >= reader.value.Size() {
			return 0, io.EOF
		}
		length := reader.value.Size() - reader.offset
		if length > 64<<10 {
			length = 64 << 10
		}
		buffer, err := reader.value.ReadRangeFromStore(reader.ctx, reader.store, reader.offset, length)
		if err != nil {
			return 0, err
		}
		if uint64(len(buffer)) != length {
			return 0, fmt.Errorf("read HTML source range at %d returned %d bytes, expected %d", reader.offset, len(buffer), length)
		}
		reader.buffer = buffer
		reader.offset += length
	}
	count := copy(destination, reader.buffer)
	reader.buffer = reader.buffer[count:]
	return count, nil
}

func activeHTMLBlock(elements []htmlElementFrame) *htmlBlockFrame {
	for index := len(elements) - 1; index >= 0; index-- {
		if elements[index].block != nil {
			return elements[index].block
		}
	}
	return nil
}

func htmlBlockKind(tag string) (BlockKind, bool) {
	switch tag {
	case "p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "pre", "blockquote", "figcaption":
		return BlockText, true
	case "td", "th":
		return BlockTable, true
	default:
		return "", false
	}
}

func isHTMLVoidElement(tag string) bool {
	switch tag {
	case "area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr":
		return true
	default:
		return false
	}
}
