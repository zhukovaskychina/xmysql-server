package content

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"mime"
	"path"
	"strings"
)

const (
	pptxMIMEType      = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
	pptxProcessorID   = "xmysql.pptx-text"
	pptxProcessorVer  = "1"
	defaultPPTXSource = uint64(64 << 20)
	defaultPPTXXML    = uint64(32 << 20)
	defaultPPTXSlides = 1000
	defaultPPTXParas  = 100000
	defaultPPTXText   = (16 << 20) - 1
	maxPPTXEntries    = 10000
	maxPPTXXMLTokens  = 1000000
	maxPPTXXMLDepth   = 256
	maxPPTXShapeName  = 4096
	pptxNamespace     = "http://schemas.openxmlformats.org/presentationml/2006/main"
	drawingNamespace  = "http://schemas.openxmlformats.org/drawingml/2006/main"
	relNamespace      = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
	pkgRelNamespace   = "http://schemas.openxmlformats.org/package/2006/relationships"
	pptxSlideRelType  = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/slide"
)

type PPTXOptions struct {
	MaxSourceBytes        uint64
	MaxXMLBytes           uint64
	MaxSlides             int
	MaxParagraphs         int
	MaxExtractedTextBytes int
}

// ParsePPTXBlocks extracts text from slides in presentation order, retaining
// slide and shape paths. It never follows external relationships or opens
// linked media, notes, macros, or embedded objects.
func ParsePPTXBlocks(ctx context.Context, store *SegmentStore, value Value, source SourceRef, options PPTXOptions) ([]Block, error) {
	options, err := validatePPTXInput(value, source, options)
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	archive, err := zip.NewReader(pptxValueReaderAt{ctx: ctx, store: store, value: value}, int64(value.Size()))
	if err != nil {
		return nil, fmt.Errorf("open PPTX package: %w", err)
	}
	if len(archive.File) > maxPPTXEntries {
		return nil, fmt.Errorf("PPTX has %d ZIP entries; maximum is %d", len(archive.File), maxPPTXEntries)
	}
	files := make(map[string]*zip.File, len(archive.File))
	for _, file := range archive.File {
		if _, exists := files[file.Name]; exists {
			return nil, fmt.Errorf("PPTX contains duplicate package part %q", file.Name)
		}
		files[file.Name] = file
	}
	presentationXML, err := readPPTXPart(files, "ppt/presentation.xml", options.MaxXMLBytes)
	if err != nil {
		return nil, err
	}
	remainingXML := options.MaxXMLBytes - uint64(len(presentationXML))
	presentationRels, err := readPPTXPart(files, "ppt/_rels/presentation.xml.rels", remainingXML)
	if err != nil {
		return nil, err
	}
	remainingXML -= uint64(len(presentationRels))
	order, err := pptxSlideOrder(presentationXML, presentationRels, files, options.MaxSlides, options.MaxXMLBytes)
	if err != nil {
		return nil, err
	}
	blocks := make([]Block, 0)
	totalTextBytes := 0
	paragraphCount := 0
	for slideIndex, partName := range order {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, err := readPPTXPart(files, partName, remainingXML)
		if err != nil {
			return nil, err
		}
		remainingXML -= uint64(len(data))
		slideBlocks, paragraphBytes, paragraphs, err := parsePPTXSlide(ctx, data, source, uint32(slideIndex+1), len(blocks), options)
		if err != nil {
			return nil, fmt.Errorf("parse PPTX slide %d: %w", slideIndex+1, err)
		}
		paragraphCount += paragraphs
		totalTextBytes += paragraphBytes
		if paragraphCount > options.MaxParagraphs {
			return nil, fmt.Errorf("PPTX paragraph count exceeds the configured limit %d", options.MaxParagraphs)
		}
		if totalTextBytes > options.MaxExtractedTextBytes {
			return nil, fmt.Errorf("PPTX extracted text exceeds the configured limit %d", options.MaxExtractedTextBytes)
		}
		blocks = append(blocks, slideBlocks...)
	}
	return blocks, nil
}

func validatePPTXInput(value Value, source SourceRef, options PPTXOptions) (PPTXOptions, error) {
	if err := value.Validate(); err != nil {
		return PPTXOptions{}, err
	}
	if err := source.Validate(); err != nil {
		return PPTXOptions{}, err
	}
	if source.Revision != value.Revision || source.Digest != value.Digest {
		return PPTXOptions{}, fmt.Errorf("%w: PPTX source does not match the CONTENT revision and digest", ErrInvalidBlock)
	}
	mediaType, _, err := mime.ParseMediaType(value.MIMEType)
	if err != nil || !strings.EqualFold(mediaType, pptxMIMEType) {
		return PPTXOptions{}, fmt.Errorf("PPTX parser requires %s MIME type", pptxMIMEType)
	}
	if options.MaxSourceBytes == 0 {
		options.MaxSourceBytes = defaultPPTXSource
	}
	if options.MaxXMLBytes == 0 {
		options.MaxXMLBytes = defaultPPTXXML
	}
	if options.MaxSlides == 0 {
		options.MaxSlides = defaultPPTXSlides
	}
	if options.MaxParagraphs == 0 {
		options.MaxParagraphs = defaultPPTXParas
	}
	if options.MaxExtractedTextBytes == 0 {
		options.MaxExtractedTextBytes = defaultPPTXText
	}
	if options.MaxSourceBytes > defaultPPTXSource || options.MaxXMLBytes > defaultPPTXXML || options.MaxSlides > defaultPPTXSlides ||
		options.MaxParagraphs > defaultPPTXParas || options.MaxExtractedTextBytes > defaultPPTXText ||
		options.MaxSourceBytes == 0 || options.MaxXMLBytes == 0 || options.MaxSlides < 1 || options.MaxParagraphs < 1 || options.MaxExtractedTextBytes < 1 {
		return PPTXOptions{}, fmt.Errorf("PPTX limits must be positive and cannot exceed built-in resource ceilings")
	}
	if value.Size() == 0 || value.Size() > options.MaxSourceBytes {
		return PPTXOptions{}, fmt.Errorf("PPTX source size %d is outside the supported range 1..%d", value.Size(), options.MaxSourceBytes)
	}
	return options, nil
}

func readPPTXPart(files map[string]*zip.File, name string, limit uint64) ([]byte, error) {
	file := files[name]
	if file == nil {
		return nil, fmt.Errorf("PPTX package is missing %s", name)
	}
	if file.UncompressedSize64 > limit {
		return nil, fmt.Errorf("PPTX part %s has %d bytes; maximum is %d", name, file.UncompressedSize64, limit)
	}
	reader, err := file.Open()
	if err != nil {
		return nil, fmt.Errorf("open PPTX part %s: %w", name, err)
	}
	defer reader.Close()
	data, err := io.ReadAll(io.LimitReader(reader, int64(limit)+1))
	if err != nil {
		return nil, fmt.Errorf("read PPTX part %s: %w", name, err)
	}
	if uint64(len(data)) > limit {
		return nil, fmt.Errorf("PPTX part %s exceeds the configured limit %d", name, limit)
	}
	return data, nil
}

type pptxSlideID struct {
	RelationshipID string
}

type pptxRelationship struct {
	ID         string
	Type       string
	Target     string
	TargetMode string
}

func pptxSlideOrder(presentationXML, relationshipsXML []byte, files map[string]*zip.File, maxSlides int, maxXMLBytes uint64) ([]string, error) {
	relationships := make(map[string]pptxRelationship)
	relDecoder := xml.NewDecoder(bytes.NewReader(relationshipsXML))
	relationshipTokens, relationshipDepth := 0, 0
	for {
		token, err := relDecoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("parse PPTX presentation relationships: %w", err)
		}
		relationshipTokens++
		if relationshipTokens > maxPPTXXMLTokens {
			return nil, fmt.Errorf("PPTX presentation relationships exceed the XML token limit")
		}
		switch token.(type) {
		case xml.StartElement:
			relationshipDepth++
			if relationshipDepth > maxPPTXXMLDepth {
				return nil, fmt.Errorf("PPTX presentation relationship nesting exceeds %d", maxPPTXXMLDepth)
			}
		case xml.EndElement:
			relationshipDepth--
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name.Space != pkgRelNamespace || start.Name.Local != "Relationship" {
			continue
		}
		relationship := pptxRelationship{}
		for _, attribute := range start.Attr {
			switch attribute.Name.Local {
			case "Id":
				relationship.ID = attribute.Value
			case "Type":
				relationship.Type = attribute.Value
			case "Target":
				relationship.Target = attribute.Value
			case "TargetMode":
				relationship.TargetMode = attribute.Value
			}
		}
		if relationship.ID == "" || relationship.Target == "" {
			return nil, fmt.Errorf("PPTX presentation relationship is missing Id or Target")
		}
		if _, exists := relationships[relationship.ID]; exists {
			return nil, fmt.Errorf("PPTX presentation contains duplicate relationship %q", relationship.ID)
		}
		relationships[relationship.ID] = relationship
	}

	ids := make([]pptxSlideID, 0)
	presentationDecoder := xml.NewDecoder(bytes.NewReader(presentationXML))
	rootSeen := false
	presentationTokens, presentationDepth := 0, 0
	for {
		token, err := presentationDecoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("parse PPTX presentation: %w", err)
		}
		presentationTokens++
		if presentationTokens > maxPPTXXMLTokens {
			return nil, fmt.Errorf("PPTX presentation exceeds the XML token limit")
		}
		start, ok := token.(xml.StartElement)
		if _, isStart := token.(xml.StartElement); isStart {
			presentationDepth++
			if presentationDepth > maxPPTXXMLDepth {
				return nil, fmt.Errorf("PPTX presentation nesting exceeds %d", maxPPTXXMLDepth)
			}
		} else if _, isEnd := token.(xml.EndElement); isEnd {
			presentationDepth--
		}
		if !ok {
			continue
		}
		if !rootSeen {
			if start.Name.Space != pptxNamespace || start.Name.Local != "presentation" {
				return nil, fmt.Errorf("PPTX presentation XML has an invalid root")
			}
			rootSeen = true
		}
		if start.Name.Space != pptxNamespace || start.Name.Local != "sldId" {
			continue
		}
		id := ""
		for _, attribute := range start.Attr {
			if attribute.Name.Space == relNamespace && attribute.Name.Local == "id" {
				id = attribute.Value
				break
			}
		}
		if id == "" {
			return nil, fmt.Errorf("PPTX slide entry has no relationship id")
		}
		ids = append(ids, pptxSlideID{RelationshipID: id})
		if len(ids) > maxSlides {
			return nil, fmt.Errorf("PPTX slide count exceeds the configured limit %d", maxSlides)
		}
	}
	if !rootSeen || len(ids) == 0 {
		return nil, fmt.Errorf("PPTX presentation has no slides")
	}
	order := make([]string, 0, len(ids))
	seenTargets := make(map[string]struct{}, len(ids))
	for _, slide := range ids {
		relationship, found := relationships[slide.RelationshipID]
		if !found {
			return nil, fmt.Errorf("PPTX slide relationship %q is missing", slide.RelationshipID)
		}
		if strings.EqualFold(relationship.TargetMode, "External") {
			return nil, fmt.Errorf("PPTX slide relationship %q is external", slide.RelationshipID)
		}
		if relationship.Type != pptxSlideRelType {
			return nil, fmt.Errorf("PPTX relationship %q is not a slide relationship", slide.RelationshipID)
		}
		if strings.ContainsAny(relationship.Target, "\\?#:") || strings.HasPrefix(relationship.Target, "/") {
			return nil, fmt.Errorf("PPTX slide relationship %q has an unsafe target", slide.RelationshipID)
		}
		target := path.Clean(path.Join("ppt", relationship.Target))
		if !strings.HasPrefix(target, "ppt/slides/slide") || !strings.HasSuffix(target, ".xml") || strings.Contains(target, "../") {
			return nil, fmt.Errorf("PPTX slide relationship %q targets an unsupported part", slide.RelationshipID)
		}
		if _, exists := seenTargets[target]; exists {
			return nil, fmt.Errorf("PPTX presentation references slide part %s more than once", target)
		}
		if files[target] == nil {
			return nil, fmt.Errorf("PPTX presentation references missing slide part %s", target)
		}
		if files[target].UncompressedSize64 > maxXMLBytes {
			return nil, fmt.Errorf("PPTX slide part %s exceeds the configured XML limit", target)
		}
		seenTargets[target] = struct{}{}
		order = append(order, target)
	}
	return order, nil
}

func parsePPTXSlide(ctx context.Context, data []byte, source SourceRef, slideNumber uint32, firstOrdinal int, options PPTXOptions) ([]Block, int, int, error) {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	blocks := make([]Block, 0)
	var paragraph strings.Builder
	var shapeIndex, rowIndex, cellIndex, paragraphIndex int
	var inShape, inGraphicFrame, inTable, inParagraph, inText, rootSeen bool
	var shapeName, shapeID string
	var paragraphPath string
	paragraphKind := BlockText
	paragraphCount, textBytes := 0, 0
	tokenCount, depth := 0, 0
	flush := func() error {
		text := strings.TrimSpace(paragraph.String())
		paragraph.Reset()
		if text == "" {
			return nil
		}
		if len(text) > options.MaxExtractedTextBytes-textBytes {
			return fmt.Errorf("PPTX extracted text exceeds the configured limit %d", options.MaxExtractedTextBytes)
		}
		textBytes += len(text)
		paragraphCount++
		if paragraphCount > options.MaxParagraphs {
			return fmt.Errorf("PPTX paragraph count exceeds the configured limit %d", options.MaxParagraphs)
		}
		ordinal := uint32(firstOrdinal + len(blocks) + 1)
		page := slideNumber
		block, err := NewBlock(source, ordinal, paragraphKind, text, Locator{Page: &page}, pptxProcessorID, pptxProcessorVer, "", "")
		if err != nil {
			return fmt.Errorf("bind PPTX paragraph %d: %w", ordinal, err)
		}
		block.Attributes = map[string]string{"structure_path": paragraphPath, "shape_name": shapeName, "shape_id": shapeID}
		block.ID = block.Identity()
		blocks = append(blocks, block)
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, 0, 0, err
		}
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, 0, fmt.Errorf("parse slide XML: %w", err)
		}
		tokenCount++
		if tokenCount > maxPPTXXMLTokens {
			return nil, 0, 0, fmt.Errorf("slide XML exceeds the token limit")
		}
		switch token := token.(type) {
		case xml.StartElement:
			depth++
			if depth > maxPPTXXMLDepth {
				return nil, 0, 0, fmt.Errorf("slide XML nesting exceeds %d", maxPPTXXMLDepth)
			}
			if !rootSeen {
				if token.Name.Space != pptxNamespace || token.Name.Local != "sld" {
					return nil, 0, 0, fmt.Errorf("slide XML has an invalid root")
				}
				rootSeen = true
			}
			if token.Name.Space == pptxNamespace && isPPTXShapeElement(token.Name.Local) {
				shapeIndex++
				inShape = token.Name.Local == "sp" || token.Name.Local == "cxnSp"
				inGraphicFrame = token.Name.Local == "graphicFrame"
				shapeName, shapeID = "", ""
				rowIndex, cellIndex, paragraphIndex = 0, 0, 0
			}
			if (inShape || inGraphicFrame) && token.Name.Space == pptxNamespace && token.Name.Local == "cNvPr" {
				for _, attribute := range token.Attr {
					if attribute.Name.Local == "name" {
						if len(attribute.Value) > maxPPTXShapeName {
							return nil, 0, 0, fmt.Errorf("shape name exceeds %d bytes", maxPPTXShapeName)
						}
						shapeName = attribute.Value
					}
					if attribute.Name.Local == "id" {
						if len(attribute.Value) > 128 {
							return nil, 0, 0, fmt.Errorf("shape ID exceeds 128 bytes")
						}
						shapeID = attribute.Value
					}
				}
			}
			if token.Name.Space == drawingNamespace && token.Name.Local == "tbl" {
				inTable = true
				rowIndex, cellIndex = 0, 0
			}
			if inTable && token.Name.Space == drawingNamespace && token.Name.Local == "tr" {
				rowIndex++
				cellIndex = 0
			}
			if inTable && token.Name.Space == drawingNamespace && token.Name.Local == "tc" {
				cellIndex++
			}
			if token.Name.Space == drawingNamespace && token.Name.Local == "p" {
				inParagraph = true
				paragraph.Reset()
				paragraphIndex++
				paragraphKind = BlockText
				path := fmt.Sprintf("/slide[%d]/shape[%d]", slideNumber, shapeIndex)
				if inTable {
					paragraphKind = BlockTable
					path += fmt.Sprintf("/row[%d]/cell[%d]", rowIndex, cellIndex)
				}
				paragraphPath = fmt.Sprintf("%s/paragraph[%d]", path, paragraphIndex)
			}
			if token.Name.Space == drawingNamespace && token.Name.Local == "t" && inParagraph {
				inText = true
			}
			if token.Name.Space == drawingNamespace && token.Name.Local == "br" && inParagraph {
				paragraph.WriteByte('\n')
			}
		case xml.CharData:
			if inParagraph && inText {
				if len(token) > options.MaxExtractedTextBytes-textBytes-paragraph.Len() {
					return nil, 0, 0, fmt.Errorf("PPTX extracted text exceeds the configured limit %d", options.MaxExtractedTextBytes)
				}
				paragraph.Write([]byte(token))
			}
		case xml.EndElement:
			depth--
			if token.Name.Space == drawingNamespace && token.Name.Local == "t" {
				inText = false
			}
			if token.Name.Space == drawingNamespace && token.Name.Local == "p" && inParagraph {
				if err := flush(); err != nil {
					return nil, 0, 0, err
				}
				inParagraph = false
				inText = false
			}
			if token.Name.Space == drawingNamespace && token.Name.Local == "tbl" {
				inTable = false
			}
			if token.Name.Space == pptxNamespace && (token.Name.Local == "sp" || token.Name.Local == "cxnSp") {
				inShape = false
			}
			if token.Name.Space == pptxNamespace && token.Name.Local == "graphicFrame" {
				inGraphicFrame = false
			}
		}
	}
	if !rootSeen {
		return nil, 0, 0, fmt.Errorf("slide XML is empty")
	}
	return blocks, textBytes, paragraphCount, nil
}

func isPPTXShapeElement(name string) bool {
	switch name {
	case "sp", "graphicFrame", "pic", "cxnSp", "grpSp":
		return true
	default:
		return false
	}
}

type pptxValueReaderAt struct {
	ctx   context.Context
	store *SegmentStore
	value Value
}

func (reader pptxValueReaderAt) ReadAt(buffer []byte, offset int64) (int, error) {
	if offset < 0 || uint64(offset) >= reader.value.Size() {
		return 0, io.EOF
	}
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	length := uint64(len(buffer))
	remaining := reader.value.Size() - uint64(offset)
	if length > remaining {
		length = remaining
	}
	var data []byte
	if reader.value.ExternalRef == nil {
		start, end := int(offset), int(uint64(offset)+length)
		data = reader.value.Bytes[start:end]
	} else {
		var err error
		data, err = reader.value.ReadRangeFromStore(reader.ctx, reader.store, uint64(offset), length)
		if err != nil {
			return 0, err
		}
	}
	count := copy(buffer, data)
	if count < len(buffer) {
		return count, io.EOF
	}
	return count, nil
}
