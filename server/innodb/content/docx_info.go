package content

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"mime"
	"strings"
)

const (
	docxMIMEType       = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	docxProcessorID    = "xmysql.docx-text"
	docxProcessorVer   = "1"
	defaultDOCXMaxSize = uint64(64 << 20)
	defaultDOCXXMLSize = uint64(32 << 20)
	defaultDOCXParas   = 100000
	defaultDOCXText    = 32 << 20
	maxDOCXEntries     = 10000
	wordXMLNamespace   = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
)

type DOCXOptions struct {
	MaxSourceBytes        uint64
	MaxDocumentXMLBytes   uint64
	MaxParagraphs         int
	MaxExtractedTextBytes int
}

// ParseDOCXBlocks extracts one source-bound block per non-empty Word paragraph.
// Table paragraphs retain their table kind and all paragraphs carry their
// OOXML element path for precise evidence navigation.
func ParseDOCXBlocks(ctx context.Context, store *SegmentStore, value Value, source SourceRef, options DOCXOptions) ([]Block, error) {
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
	archive, err := zip.NewReader(pdfValueReaderAt{ctx: ctx, store: store, value: value}, int64(value.Size()))
	if err != nil {
		return nil, fmt.Errorf("open DOCX ZIP package: %w", err)
	}
	if len(archive.File) > maxDOCXEntries {
		return nil, fmt.Errorf("DOCX has %d ZIP entries; maximum is %d", len(archive.File), maxDOCXEntries)
	}
	var document *zip.File
	for _, file := range archive.File {
		if file.Name != "word/document.xml" {
			continue
		}
		if document != nil {
			return nil, fmt.Errorf("DOCX package contains a duplicate word/document.xml part")
		}
		document = file
	}
	if document == nil {
		return nil, fmt.Errorf("DOCX package has no word/document.xml part")
	}
	if document.UncompressedSize64 > options.MaxDocumentXMLBytes {
		return nil, fmt.Errorf("DOCX document XML size %d exceeds the configured limit %d", document.UncompressedSize64, options.MaxDocumentXMLBytes)
	}
	reader, err := document.Open()
	if err != nil {
		return nil, fmt.Errorf("open DOCX document XML: %w", err)
	}
	defer reader.Close()
	xmlBytes, err := io.ReadAll(io.LimitReader(reader, int64(options.MaxDocumentXMLBytes)+1))
	if err != nil {
		return nil, fmt.Errorf("read DOCX document XML: %w", err)
	}
	if uint64(len(xmlBytes)) > options.MaxDocumentXMLBytes {
		return nil, fmt.Errorf("DOCX document XML exceeds the configured limit %d", options.MaxDocumentXMLBytes)
	}
	return parseDOCXDocumentXML(ctx, xmlBytes, source, options)
}

func validateDOCXInput(value Value, source SourceRef, options DOCXOptions) (DOCXOptions, error) {
	if err := value.Validate(); err != nil {
		return DOCXOptions{}, err
	}
	if err := source.Validate(); err != nil {
		return DOCXOptions{}, err
	}
	if source.Revision != value.Revision || source.Digest != value.Digest {
		return DOCXOptions{}, fmt.Errorf("%w: DOCX source does not match the CONTENT revision and digest", ErrInvalidBlock)
	}
	mediaType, _, err := mime.ParseMediaType(value.MIMEType)
	if err != nil || !strings.EqualFold(mediaType, docxMIMEType) {
		return DOCXOptions{}, fmt.Errorf("DOCX parser requires %s MIME type", docxMIMEType)
	}
	if options.MaxSourceBytes == 0 {
		options.MaxSourceBytes = defaultDOCXMaxSize
	}
	if options.MaxDocumentXMLBytes == 0 {
		options.MaxDocumentXMLBytes = defaultDOCXXMLSize
	}
	if options.MaxParagraphs == 0 {
		options.MaxParagraphs = defaultDOCXParas
	}
	if options.MaxExtractedTextBytes == 0 {
		options.MaxExtractedTextBytes = defaultDOCXText
	}
	if options.MaxSourceBytes > defaultDOCXMaxSize || options.MaxDocumentXMLBytes > defaultDOCXXMLSize ||
		options.MaxParagraphs > defaultDOCXParas || options.MaxExtractedTextBytes > defaultDOCXText ||
		options.MaxSourceBytes == 0 || options.MaxDocumentXMLBytes == 0 || options.MaxParagraphs < 1 || options.MaxExtractedTextBytes < 1 {
		return DOCXOptions{}, fmt.Errorf("DOCX limits must be positive and cannot exceed the built-in resource ceilings")
	}
	if value.Size() == 0 || value.Size() > options.MaxSourceBytes {
		return DOCXOptions{}, fmt.Errorf("DOCX source size %d is outside the supported range 1..%d", value.Size(), options.MaxSourceBytes)
	}
	return options, nil
}

type docxXMLPathNode struct {
	name   string
	index  uint64
	counts map[string]uint64
}

func parseDOCXDocumentXML(ctx context.Context, data []byte, source SourceRef, options DOCXOptions) ([]Block, error) {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	stack := make([]docxXMLPathNode, 0, 32)
	blocks := make([]Block, 0)
	var paragraph bytes.Buffer
	var paragraphPath string
	var paragraphKind BlockKind
	var inParagraph, inText bool
	rootSeen, bodySeen := false, false
	bodyDepth := 0
	paragraphCount, totalTextBytes := 0, 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("parse DOCX document XML: %w", err)
		}
		switch token := token.(type) {
		case xml.StartElement:
			if len(stack) >= 256 {
				return nil, fmt.Errorf("DOCX document XML nesting exceeds 256 elements")
			}
			parentDepth := len(stack)
			index := uint64(1)
			if len(stack) > 0 {
				parent := &stack[len(stack)-1]
				if parent.counts == nil {
					parent.counts = make(map[string]uint64)
				}
				parent.counts[token.Name.Local]++
				index = parent.counts[token.Name.Local]
			}
			stack = append(stack, docxXMLPathNode{name: token.Name.Local, index: index})
			if parentDepth == 0 {
				if token.Name.Space != wordXMLNamespace || token.Name.Local != "document" {
					return nil, fmt.Errorf("DOCX document XML root is not a WordprocessingML document")
				}
				rootSeen = true
			}
			if token.Name.Space != wordXMLNamespace {
				continue
			}
			switch token.Name.Local {
			case "body":
				if parentDepth == 1 && stack[0].name == "document" {
					bodySeen = true
					bodyDepth = len(stack)
				}
			case "p":
				if bodyDepth == 0 {
					continue
				}
				if inParagraph {
					return nil, fmt.Errorf("DOCX document XML contains nested paragraphs")
				}
				paragraphCount++
				if paragraphCount > options.MaxParagraphs {
					return nil, fmt.Errorf("DOCX paragraph count exceeds the configured limit %d", options.MaxParagraphs)
				}
				inParagraph, inText = true, false
				paragraph.Reset()
				paragraphPath = docxXMLPath(stack)
				paragraphKind = BlockText
				for _, node := range stack {
					if node.name == "tbl" {
						paragraphKind = BlockTable
						break
					}
				}
			case "t":
				if inParagraph && bodyDepth > 0 {
					inText = true
				}
			case "tab":
				if inParagraph && bodyDepth > 0 {
					paragraph.WriteByte('\t')
				}
			case "br", "cr":
				if inParagraph && bodyDepth > 0 {
					paragraph.WriteByte('\n')
				}
			}
		case xml.CharData:
			if inParagraph && inText {
				if len(token) > options.MaxExtractedTextBytes-totalTextBytes-paragraph.Len() {
					return nil, fmt.Errorf("DOCX extracted text exceeds the configured limit %d", options.MaxExtractedTextBytes)
				}
				paragraph.Write([]byte(token))
			}
		case xml.EndElement:
			if token.Name.Space == wordXMLNamespace {
				switch token.Name.Local {
				case "t":
					inText = false
				case "p":
					if inParagraph {
						text := strings.TrimSpace(paragraph.String())
						if text != "" {
							if len(text) > options.MaxExtractedTextBytes-totalTextBytes {
								return nil, fmt.Errorf("DOCX extracted text exceeds the configured limit %d", options.MaxExtractedTextBytes)
							}
							totalTextBytes += len(text)
							ordinal := uint32(len(blocks) + 1)
							block, err := NewBlock(source, ordinal, paragraphKind, text, Locator{}, docxProcessorID, docxProcessorVer, "", "")
							if err != nil {
								return nil, fmt.Errorf("create DOCX paragraph block %d: %w", ordinal, err)
							}
							block.Attributes = map[string]string{"content_type": "docx_paragraph", "structure_path": paragraphPath}
							block.ID = block.Identity()
							blocks = append(blocks, block)
						}
						inParagraph, inText = false, false
					}
				case "body":
					if len(stack) == bodyDepth {
						bodyDepth = 0
					}
				}
			}
			if len(stack) == 0 {
				return nil, fmt.Errorf("DOCX XML path stack is inconsistent")
			}
			stack = stack[:len(stack)-1]
		}
	}
	if !rootSeen || !bodySeen {
		return nil, fmt.Errorf("DOCX document XML is missing the WordprocessingML document body")
	}
	return blocks, nil
}

func docxXMLPath(stack []docxXMLPathNode) string {
	var path strings.Builder
	for _, node := range stack {
		fmt.Fprintf(&path, "/%s[%d]", node.name, node.index)
	}
	return path.String()
}
