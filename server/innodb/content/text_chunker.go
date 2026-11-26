package content

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	DefaultTextChunkBytes       = 4096
	MaxTextChunkCount           = 65536
	textChunkerProcessorID      = "xmysql.text-chunker"
	textChunkerProcessorVersion = "1"
)

// NewTextChunkMaintenanceRequest captures the deterministic processor
// configuration in the durable job identity used by the database queue.
func NewTextChunkMaintenanceRequest(source SourceRef, options TextChunkOptions) (MaintenanceRequest, error) {
	normalized, err := normalizeTextChunkOptions(options)
	if err != nil {
		return MaintenanceRequest{}, err
	}
	configuration, err := json.Marshal(normalized)
	if err != nil {
		return MaintenanceRequest{}, err
	}
	digest := sha256.Sum256(configuration)
	return MaintenanceRequest{
		Source: source, ProcessorID: textChunkerProcessorID,
		ProcessorVersion: textChunkerProcessorVersion, ConfigurationDigest: hex.EncodeToString(digest[:]),
	}, nil
}

// TextChunkOptions controls deterministic text segmentation. MaxBytes is a
// byte bound, not a model-token estimate. Boundary preference is paragraph,
// sentence, then whitespace; an unbroken span is split at a UTF-8 rune edge.
type TextChunkOptions struct {
	MaxBytes         int
	OverlapBytes     int
	MinBoundaryBytes int
}

// ChunkText creates source-bound text evidence blocks from an inline text
// value. Source offsets refer to the original stored bytes, including a UTF-8
// BOM when present. Non-text media must be processed by a format-specific
// parser instead of being guessed as text.
func ChunkText(value Value, source SourceRef, options TextChunkOptions) ([]Block, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	if value.ExternalRef != nil {
		return nil, ErrExternalContentRequiresStore
	}
	if source.Revision != value.Revision || source.Digest != value.Digest {
		return nil, fmt.Errorf("%w: text chunk source does not match the CONTENT revision and digest", ErrInvalidBlock)
	}
	if err := source.Validate(); err != nil {
		return nil, err
	}
	text, err := ExtractText(value)
	if err != nil {
		return nil, err
	}
	baseOffset := 0
	if len(value.Bytes) >= 3 && value.Bytes[0] == 0xef && value.Bytes[1] == 0xbb && value.Bytes[2] == 0xbf {
		baseOffset = 3
	}
	return chunkUTF8Text(text, baseOffset, source, options)
}

// TextChunkAt returns one deterministic source-bound chunk without building
// the other block records. It still validates and reads the complete bounded
// text value so its boundaries match ChunkText exactly.
func TextChunkAt(value Value, source SourceRef, ordinal uint32, options TextChunkOptions) (Block, bool, error) {
	if err := value.Validate(); err != nil {
		return Block{}, false, err
	}
	if value.ExternalRef != nil {
		return Block{}, false, ErrExternalContentRequiresStore
	}
	if source.Revision != value.Revision || source.Digest != value.Digest {
		return Block{}, false, fmt.Errorf("%w: text chunk source does not match the CONTENT revision and digest", ErrInvalidBlock)
	}
	if err := source.Validate(); err != nil {
		return Block{}, false, err
	}
	text, err := ExtractText(value)
	if err != nil {
		return Block{}, false, err
	}
	baseOffset := 0
	if len(value.Bytes) >= 3 && value.Bytes[0] == 0xef && value.Bytes[1] == 0xbb && value.Bytes[2] == 0xbf {
		baseOffset = 3
	}
	start, end, exists, options, err := textChunkRangeAt(text, ordinal, options)
	if err != nil || !exists {
		return Block{}, exists, err
	}
	block, err := newTextChunkBlock(text[start:end], start, end, baseOffset, source, ordinal, options)
	return block, err == nil, err
}

// ChunkTextFromStore reads a text value only within MaxInlineTextBytes and
// applies the same deterministic chunking and source checks as ChunkText.
func ChunkTextFromStore(ctx context.Context, store *SegmentStore, value Value, source SourceRef, options TextChunkOptions) ([]Block, error) {
	blocks := make([]Block, 0)
	_, err := VisitTextChunksFromStore(ctx, store, value, source, options, func(block Block) error {
		blocks = append(blocks, block)
		return nil
	})
	return blocks, err
}

// VisitTextChunksFromStore visits deterministic source-bound chunks in order.
// External values are read through bounded ranges and emitted one at a time,
// so maintenance workers can persist each block without materializing either
// the complete source text or the full block list. Returning an error stops
// the walk and is propagated unchanged.
func VisitTextChunksFromStore(ctx context.Context, store *SegmentStore, value Value, source SourceRef, options TextChunkOptions, visit func(Block) error) (int, error) {
	if err := value.Validate(); err != nil {
		return 0, err
	}
	if visit == nil {
		return 0, fmt.Errorf("text chunk visitor is required")
	}
	if value.ExternalRef != nil {
		return walkStoredTextChunks(ctx, store, value, &source, options, visit)
	}
	blocks, err := ChunkText(value, source, options)
	if err != nil {
		return 0, err
	}
	for _, block := range blocks {
		if err := visit(block); err != nil {
			return int(block.Ordinal) + 1, err
		}
	}
	return len(blocks), nil
}

// TextChunkAtFromStore is the segmented-value form of TextChunkAt.
func TextChunkAtFromStore(ctx context.Context, store *SegmentStore, value Value, source SourceRef, ordinal uint32, options TextChunkOptions) (Block, bool, error) {
	if err := value.Validate(); err != nil {
		return Block{}, false, err
	}
	if value.ExternalRef == nil {
		return TextChunkAt(value, source, ordinal, options)
	}
	var selected Block
	found := false
	_, err := walkStoredTextChunks(ctx, store, value, &source, options, func(block Block) error {
		if block.Ordinal == ordinal {
			selected = block
			found = true
			return errStopTextChunkWalk
		}
		return nil
	})
	if err == errStopTextChunkWalk {
		err = nil
	}
	return selected, found, err
}

// TextChunkCount returns the number of blocks produced by the deterministic
// chunker without constructing block records or source locators.
func TextChunkCount(value Value, options TextChunkOptions) (int, error) {
	if err := value.Validate(); err != nil {
		return 0, err
	}
	if value.ExternalRef != nil {
		return 0, ErrExternalContentRequiresStore
	}
	text, err := ExtractText(value)
	if err != nil {
		return 0, err
	}
	return countUTF8TextChunks(text, options)
}

// TextChunkCountFromStore counts blocks for a segmented text value while
// keeping materialization within MaxInlineTextBytes.
func TextChunkCountFromStore(ctx context.Context, store *SegmentStore, value Value, options TextChunkOptions) (int, error) {
	if err := value.Validate(); err != nil {
		return 0, err
	}
	if value.ExternalRef == nil {
		return TextChunkCount(value, options)
	}
	return walkStoredTextChunks(ctx, store, value, nil, options, nil)
}

var errStopTextChunkWalk = fmt.Errorf("stop after requested CONTENT text chunk")

// walkStoredTextChunks builds one source-bound text block at a time. Its read
// window is bounded by MaxBytes plus at most one UTF-8 rune of lookahead, so a
// large external CONTENT value does not need to be materialized in memory.
func walkStoredTextChunks(ctx context.Context, store *SegmentStore, value Value, source *SourceRef, options TextChunkOptions, visit func(Block) error) (int, error) {
	if value.ExternalRef == nil || store == nil {
		return 0, ErrExternalContentRequiresStore
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !isTextMIME(value.MIMEType) {
		return 0, fmt.Errorf("%w: %q", ErrUnsupportedTextMIME, value.MIMEType)
	}
	if source != nil {
		if source.Revision != value.Revision || source.Digest != value.Digest {
			return 0, fmt.Errorf("%w: text chunk source does not match the CONTENT revision and digest", ErrInvalidBlock)
		}
		if err := source.Validate(); err != nil {
			return 0, err
		}
	}
	options, err := normalizeTextChunkOptions(options)
	if err != nil {
		return 0, err
	}
	baseOffset := 0
	if value.Size() >= 3 {
		header, err := value.ReadRangeFromStore(ctx, store, 0, 3)
		if err != nil {
			return 0, err
		}
		if len(header) == 3 && header[0] == 0xef && header[1] == 0xbb && header[2] == 0xbf {
			baseOffset = 3
		}
	}
	textSize := value.Size() - uint64(baseOffset)
	if textSize == 0 {
		return 0, nil
	}
	count := 0
	for start := uint64(0); start < textSize; {
		if count >= MaxTextChunkCount {
			return count, fmt.Errorf("%w: text chunk count exceeds the supported maximum %d", ErrInvalidValue, MaxTextChunkCount)
		}
		if err := ctx.Err(); err != nil {
			return count, err
		}
		remaining := textSize - start
		windowLength := uint64(options.MaxBytes)
		if windowLength > remaining {
			windowLength = remaining
		}
		window, err := value.ReadRangeFromStore(ctx, store, uint64(baseOffset)+start, windowLength)
		if err != nil {
			return count, err
		}
		if windowLength < remaining {
			lookaheadLength := uint64(utf8.UTFMax - 1)
			if lookaheadLength > remaining-windowLength {
				lookaheadLength = remaining - windowLength
			}
			lookahead, readErr := value.ReadRangeFromStore(ctx, store, uint64(baseOffset)+start+windowLength, lookaheadLength)
			if readErr != nil {
				return count, readErr
			}
			window = append(window, lookahead...)
		}
		limit := int(windowLength)
		if windowLength < remaining {
			limit = utf8BoundaryAtOrBefore(string(window), limit)
		}
		if limit <= 0 || !utf8.Valid(window[:limit]) {
			return count, ErrInvalidTextEncoding
		}
		end := limit
		if windowLength < remaining {
			end = preferredTextBoundary(string(window), 0, limit, options.MinBoundaryBytes)
		}
		if end <= 0 {
			return count, fmt.Errorf("%w: text chunker failed to advance at byte %d", ErrInvalidValue, start)
		}
		if visit != nil {
			if source == nil {
				return count, fmt.Errorf("text chunk source is required")
			}
			block, blockErr := newTextChunkBlock(string(window[:end]), 0, end, baseOffset+int(start), *source, uint32(count), options)
			if blockErr != nil {
				return count, fmt.Errorf("create text chunk %d: %w", count, blockErr)
			}
			if err := visit(block); err != nil {
				return count + 1, err
			}
		}
		count++
		if uint64(end) == remaining {
			break
		}
		next := nextTextChunkStart(string(window), 0, end, options.OverlapBytes)
		if next <= 0 || uint64(next) > remaining {
			return count, fmt.Errorf("%w: text chunker failed to advance at byte %d", ErrInvalidValue, start)
		}
		start += uint64(next)
	}
	return count, nil
}

func chunkUTF8Text(text string, baseOffset int, source SourceRef, options TextChunkOptions) ([]Block, error) {
	options, err := normalizeTextChunkOptions(options)
	if err != nil {
		return nil, err
	}
	if !utf8.ValidString(text) {
		return nil, ErrInvalidTextEncoding
	}
	if text == "" {
		return []Block{}, nil
	}
	blocks := make([]Block, 0, (len(text)+options.MaxBytes-1)/options.MaxBytes)
	for start := 0; start < len(text); {
		limit := start + options.MaxBytes
		if limit >= len(text) {
			limit = len(text)
		} else {
			limit = utf8BoundaryAtOrBefore(text, limit)
		}
		end := limit
		if limit < len(text) {
			end = preferredTextBoundary(text, start, limit, options.MinBoundaryBytes)
		}
		if end <= start {
			return nil, fmt.Errorf("%w: text chunker failed to advance at byte %d", ErrInvalidValue, start)
		}
		if uint64(len(blocks)) > uint64(^uint32(0)) {
			return nil, fmt.Errorf("%w: text chunk count exceeds the supported ordinal range", ErrInvalidBlock)
		}
		if len(blocks) >= MaxTextChunkCount {
			return nil, fmt.Errorf("%w: text chunk count exceeds the supported maximum %d", ErrInvalidValue, MaxTextChunkCount)
		}
		block, err := newTextChunkBlock(text[start:end], start, end, baseOffset, source, uint32(len(blocks)), options)
		if err != nil {
			return nil, fmt.Errorf("create text chunk %d: %w", len(blocks), err)
		}
		blocks = append(blocks, block)
		if end == len(text) {
			break
		}
		start = nextTextChunkStart(text, start, end, options.OverlapBytes)
	}
	return blocks, nil
}

func newTextChunkBlock(text string, start, end, baseOffset int, source SourceRef, ordinal uint32, options TextChunkOptions) (Block, error) {
	byteStart := uint64(baseOffset + start)
	byteEnd := uint64(baseOffset + end)
	block, err := NewBlock(source, ordinal, BlockText, text, Locator{
		ByteStart: &byteStart,
		ByteEnd:   &byteEnd,
	}, textChunkerProcessorID, textChunkerProcessorVersion, "", "")
	if err != nil {
		return Block{}, err
	}
	block.Attributes = map[string]string{
		"chunk_max_bytes":     strconv.Itoa(options.MaxBytes),
		"chunk_overlap_bytes": strconv.Itoa(options.OverlapBytes),
		"min_boundary_bytes":  strconv.Itoa(options.MinBoundaryBytes),
		"boundary_policy":     "paragraph_sentence_whitespace_utf8",
	}
	block.ID = block.Identity()
	return block, nil
}

func textChunkRangeAt(text string, ordinal uint32, options TextChunkOptions) (int, int, bool, TextChunkOptions, error) {
	options, err := normalizeTextChunkOptions(options)
	if err != nil {
		return 0, 0, false, options, err
	}
	if !utf8.ValidString(text) {
		return 0, 0, false, options, ErrInvalidTextEncoding
	}
	var current uint32
	for start := 0; start < len(text); current++ {
		if current >= MaxTextChunkCount {
			return 0, 0, false, options, fmt.Errorf("%w: text chunk count exceeds the supported maximum %d", ErrInvalidValue, MaxTextChunkCount)
		}
		limit := start + options.MaxBytes
		if limit >= len(text) {
			limit = len(text)
		} else {
			limit = utf8BoundaryAtOrBefore(text, limit)
		}
		end := limit
		if limit < len(text) {
			end = preferredTextBoundary(text, start, limit, options.MinBoundaryBytes)
		}
		if end <= start {
			return 0, 0, false, options, fmt.Errorf("%w: text chunker failed to advance at byte %d", ErrInvalidValue, start)
		}
		if current == ordinal {
			return start, end, true, options, nil
		}
		if end == len(text) {
			break
		}
		start = nextTextChunkStart(text, start, end, options.OverlapBytes)
	}
	return 0, 0, false, options, nil
}

func countUTF8TextChunks(text string, options TextChunkOptions) (int, error) {
	options, err := normalizeTextChunkOptions(options)
	if err != nil {
		return 0, err
	}
	if !utf8.ValidString(text) {
		return 0, ErrInvalidTextEncoding
	}
	count := 0
	for start := 0; start < len(text); {
		limit := start + options.MaxBytes
		if limit >= len(text) {
			limit = len(text)
		} else {
			limit = utf8BoundaryAtOrBefore(text, limit)
		}
		end := limit
		if limit < len(text) {
			end = preferredTextBoundary(text, start, limit, options.MinBoundaryBytes)
		}
		if end <= start {
			return 0, fmt.Errorf("%w: text chunker failed to advance at byte %d", ErrInvalidValue, start)
		}
		count++
		if count > MaxTextChunkCount {
			return 0, fmt.Errorf("%w: text chunk count exceeds the supported maximum %d", ErrInvalidValue, MaxTextChunkCount)
		}
		if end == len(text) {
			break
		}
		next := nextTextChunkStart(text, start, end, options.OverlapBytes)
		start = next
	}
	return count, nil
}

func nextTextChunkStart(text string, start, end, overlap int) int {
	next := end - overlap
	for next > start && !utf8.RuneStart(text[next]) {
		next--
	}
	if next <= start {
		_, firstRuneBytes := utf8.DecodeRuneInString(text[start:end])
		next = start + firstRuneBytes
	}
	return next
}

func normalizeTextChunkOptions(options TextChunkOptions) (TextChunkOptions, error) {
	if options.MaxBytes == 0 {
		options.MaxBytes = DefaultTextChunkBytes
	}
	if options.MaxBytes < utf8.UTFMax || uint64(options.MaxBytes) > MaxReadRangeBytes {
		return TextChunkOptions{}, fmt.Errorf("text chunk MaxBytes must be between %d and %d", utf8.UTFMax, MaxReadRangeBytes)
	}
	if options.OverlapBytes < 0 || options.OverlapBytes >= options.MaxBytes {
		return TextChunkOptions{}, fmt.Errorf("text chunk OverlapBytes must be non-negative and smaller than MaxBytes")
	}
	if options.MinBoundaryBytes == 0 {
		options.MinBoundaryBytes = options.MaxBytes / 2
		if options.MinBoundaryBytes == 0 {
			options.MinBoundaryBytes = 1
		}
	}
	if options.MinBoundaryBytes < 1 || options.MinBoundaryBytes > options.MaxBytes {
		return TextChunkOptions{}, fmt.Errorf("text chunk MinBoundaryBytes must be between 1 and MaxBytes")
	}
	return options, nil
}

func utf8BoundaryAtOrBefore(text string, limit int) int {
	if limit >= len(text) {
		return len(text)
	}
	for limit > 0 && !utf8.RuneStart(text[limit]) {
		limit--
	}
	return limit
}

func preferredTextBoundary(text string, start, limit, minimum int) int {
	paragraph, sentence, whitespace := 0, 0, 0
	for offset := start; offset < limit; {
		r, size := utf8.DecodeRuneInString(text[offset:limit])
		next := offset + size
		if next-start >= minimum {
			if r == '\n' && next < limit && text[next] == '\n' {
				paragraph = next + 1
			} else if isSentenceBoundary(r) {
				sentence = next
			} else if unicodeSpace(r) {
				whitespace = next
			}
		}
		offset = next
	}
	if paragraph > start {
		return paragraph
	}
	if sentence > start {
		return sentence
	}
	if whitespace > start {
		return whitespace
	}
	return limit
}

func isSentenceBoundary(r rune) bool {
	return strings.ContainsRune(".!?。！？；;", r)
}

func unicodeSpace(r rune) bool {
	return unicode.IsSpace(r)
}
