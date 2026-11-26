package content

import (
	"context"
	"fmt"
	"io"
	"mime"
	"strings"

	"github.com/sassoftware/pdf-xtract"
)

const (
	pdfProcessorID      = "xmysql.pdf-text"
	pdfProcessorVersion = "1"
	defaultPDFMaxBytes  = uint64(64 << 20)
	defaultPDFMaxPages  = 1000
	defaultPDFPageBytes = 4 << 20
	defaultPDFTextBytes = 32 << 20
	defaultPDFMemory    = int64(64 << 20)
)

// PDFOptions bounds text-layer extraction. Empty pages remain as page blocks
// with OCR_REQUIRED status; this parser does not perform OCR or render pixels.
type PDFOptions struct {
	MaxSourceBytes uint64
	MaxPages       int
	MaxPageBytes   int
	MaxTextBytes   int
	MaxMemoryBytes int64
}

// ParsePDFPages extracts a PDF text layer into one source-bound block per
// page. Its ReaderAt adapter reads external CONTENT in bounded ranges, so the
// source object is not copied into an unmanaged temporary file.
func ParsePDFPages(ctx context.Context, store *SegmentStore, value Value, source SourceRef, options PDFOptions) ([]Block, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	if err := source.Validate(); err != nil {
		return nil, err
	}
	if source.Revision != value.Revision || source.Digest != value.Digest {
		return nil, fmt.Errorf("%w: PDF source does not match the CONTENT revision and digest", ErrInvalidBlock)
	}
	mediaType, _, err := mime.ParseMediaType(value.MIMEType)
	if err != nil || !strings.EqualFold(mediaType, "application/pdf") {
		return nil, fmt.Errorf("PDF parser requires application/pdf MIME type")
	}
	options = normalizePDFOptions(options)
	if options.MaxSourceBytes > defaultPDFMaxBytes || options.MaxPages > defaultPDFMaxPages ||
		options.MaxPageBytes > defaultPDFPageBytes || options.MaxTextBytes > defaultPDFTextBytes ||
		options.MaxMemoryBytes > defaultPDFMemory || options.MaxSourceBytes == 0 || options.MaxPages < 1 ||
		options.MaxPageBytes < 1 || options.MaxTextBytes < 1 || options.MaxMemoryBytes < 1 {
		return nil, fmt.Errorf("PDF limits must be positive and cannot exceed the built-in resource ceilings")
	}
	if value.Size() == 0 || value.Size() > options.MaxSourceBytes {
		return nil, fmt.Errorf("PDF source size %d is outside the supported range 1..%d", value.Size(), options.MaxSourceBytes)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	readerAt := pdfValueReaderAt{ctx: ctx, store: store, value: value}
	reader, err := xtract.NewReaderBounded(readerAt, int64(value.Size()), options.MaxMemoryBytes)
	if err != nil {
		return nil, fmt.Errorf("open PDF: %w", err)
	}
	defer reader.ResetAllocator()
	pageCount := reader.NumPage()
	if pageCount < 1 {
		return nil, fmt.Errorf("PDF has no pages")
	}
	if pageCount > options.MaxPages {
		return nil, fmt.Errorf("PDF page count %d exceeds the configured limit %d", pageCount, options.MaxPages)
	}
	blocks := make([]Block, 0, pageCount)
	var totalTextBytes int
	for pageNumber := 1; pageNumber <= pageCount; pageNumber++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		text, err := extractPDFPageText(reader.Page(pageNumber))
		if err != nil {
			return nil, fmt.Errorf("extract PDF page %d: %w", pageNumber, err)
		}
		text = strings.TrimSpace(text)
		if len(text) > options.MaxPageBytes {
			return nil, fmt.Errorf("PDF page %d text size %d exceeds the configured limit %d", pageNumber, len(text), options.MaxPageBytes)
		}
		if len(text) > options.MaxTextBytes-totalTextBytes {
			return nil, fmt.Errorf("PDF extracted text exceeds the configured limit %d", options.MaxTextBytes)
		}
		totalTextBytes += len(text)
		page := uint32(pageNumber)
		block, err := NewBlock(source, page, BlockText, text, Locator{Page: &page}, pdfProcessorID, pdfProcessorVersion, "", "")
		if err != nil {
			return nil, fmt.Errorf("create PDF page %d block: %w", pageNumber, err)
		}
		status := "TEXT_EXTRACTED"
		if text == "" {
			status = "OCR_REQUIRED"
		}
		block.Attributes = map[string]string{"extraction_status": status, "source_kind": "pdf_text_layer"}
		block.ID = block.Identity()
		if err := block.Validate(); err != nil {
			return nil, fmt.Errorf("validate PDF page %d block: %w", pageNumber, err)
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}

func normalizePDFOptions(options PDFOptions) PDFOptions {
	if options.MaxSourceBytes == 0 {
		options.MaxSourceBytes = defaultPDFMaxBytes
	}
	if options.MaxPages == 0 {
		options.MaxPages = defaultPDFMaxPages
	}
	if options.MaxPageBytes == 0 {
		options.MaxPageBytes = defaultPDFPageBytes
	}
	if options.MaxTextBytes == 0 {
		options.MaxTextBytes = defaultPDFTextBytes
	}
	if options.MaxMemoryBytes == 0 {
		options.MaxMemoryBytes = defaultPDFMemory
	}
	return options
}

type pdfValueReaderAt struct {
	ctx   context.Context
	store *SegmentStore
	value Value
}

func (reader pdfValueReaderAt) ReadAt(destination []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, fmt.Errorf("negative PDF read offset %d", offset)
	}
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	if uint64(offset) >= reader.value.Size() {
		return 0, io.EOF
	}
	total := 0
	for total < len(destination) {
		current := uint64(offset) + uint64(total)
		if current >= reader.value.Size() {
			return total, io.EOF
		}
		remaining := len(destination) - total
		if uint64(remaining) > MaxReadRangeBytes {
			remaining = int(MaxReadRangeBytes)
		}
		if available := reader.value.Size() - current; uint64(remaining) > available {
			remaining = int(available)
		}
		chunk, err := reader.value.ReadRangeFromStore(reader.ctx, reader.store, current, uint64(remaining))
		if err != nil {
			return total, err
		}
		if len(chunk) == 0 {
			return total, io.ErrUnexpectedEOF
		}
		copied := copy(destination[total:], chunk)
		total += copied
	}
	return total, nil
}

func extractPDFPageText(page xtract.Page) (text string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			text = ""
			err = fmt.Errorf("PDF parser panic: %v", recovered)
		}
	}()
	fonts := make(map[string]*xtract.Font)
	for _, name := range page.Fonts() {
		font := page.Font(name)
		fonts[name] = &font
	}
	return page.GetPlainText(fonts)
}
