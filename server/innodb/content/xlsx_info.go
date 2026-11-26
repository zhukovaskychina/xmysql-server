package content

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"mime"
	"path"
	"strconv"
	"strings"
)

const (
	xlsxMIMEType       = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	xlsxProcessorID    = "xmysql.xlsx-rows"
	xlsxProcessorVer   = "1"
	xlsxMaxSourceBytes = uint64(64 << 20)
	xlsxMaxPartBytes   = uint64(32 << 20)
	xlsxMaxTotalXML    = uint64(64 << 20)
	xlsxMaxZipEntries  = 10000
	xlsxMaxSheets      = 1024
	xlsxMaxRows        = 100000
	xlsxMaxCells       = 200000
	xlsxMaxTextBytes   = 32 << 20
	xlsxWorkbookNS     = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
	xlsxOfficeRelNS    = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
	xlsxPackageRelNS   = "http://schemas.openxmlformats.org/package/2006/relationships"
	xlsxRowSchemaID    = "xmysql.xlsx.row"
	xlsxRowSchemaV     = 1
	xlsxRowMediaType   = "application/vnd.xmysql.xlsx-row+json"
)

type XLSXOptions struct {
	MaxSourceBytes   uint64
	MaxXMLPartBytes  uint64
	MaxTotalXMLBytes uint64
	MaxSheets        int
	MaxRows          int
	MaxCells         int
	MaxTextBytes     int
}

type XLSXCell struct {
	Address            string `json:"address"`
	Type               string `json:"type"`
	Value              string `json:"value"`
	RawValue           string `json:"raw_value,omitempty"`
	Formula            string `json:"formula,omitempty"`
	FormulaType        string `json:"formula_type,omitempty"`
	FormulaRef         string `json:"formula_ref,omitempty"`
	FormulaSharedIndex string `json:"formula_shared_index,omitempty"`
	StyleID            string `json:"style_id,omitempty"`
}

type XLSXRow struct {
	SheetName string     `json:"sheet_name"`
	RowNumber uint32     `json:"row_number"`
	Cells     []XLSXCell `json:"cells"`
}

type xlsxSheet struct {
	name string
	rid  string
}

type xlsxRelationship struct {
	target   string
	external bool
	typeURI  string
}

type xlsxArchiveReader struct {
	archive  *zip.Reader
	files    map[string]*zip.File
	options  XLSXOptions
	totalXML uint64
}

// ParseXLSXRows returns one source-bound structured block per non-empty row
// in workbook order. Cell addresses, source data types, formulas and cached
// values remain in a typed JSON payload while deterministic row text is
// available to lexical retrieval and model context.
func ParseXLSXRows(ctx context.Context, store *SegmentStore, value Value, source SourceRef, options XLSXOptions) ([]Block, error) {
	options, err := validateXLSXInput(value, source, options)
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
		return nil, fmt.Errorf("open XLSX ZIP package: %w", err)
	}
	if len(archive.File) > xlsxMaxZipEntries {
		return nil, fmt.Errorf("XLSX has %d ZIP entries; maximum is %d", len(archive.File), xlsxMaxZipEntries)
	}
	reader := &xlsxArchiveReader{archive: archive, files: make(map[string]*zip.File, len(archive.File)), options: options}
	for _, file := range archive.File {
		if _, duplicate := reader.files[file.Name]; duplicate {
			return nil, fmt.Errorf("XLSX package contains duplicate part %q", file.Name)
		}
		reader.files[file.Name] = file
	}
	workbookPath := "xl/workbook.xml"
	workbookBytes, err := reader.readPart(ctx, workbookPath)
	if err != nil {
		return nil, err
	}
	sheets, err := parseXLSXWorkbook(workbookBytes)
	if err != nil {
		return nil, err
	}
	if len(sheets) == 0 || len(sheets) > options.MaxSheets {
		return nil, fmt.Errorf("XLSX sheet count %d is outside the supported range 1..%d", len(sheets), options.MaxSheets)
	}
	relationshipPath := "xl/_rels/workbook.xml.rels"
	relationshipBytes, err := reader.readPart(ctx, relationshipPath)
	if err != nil {
		return nil, err
	}
	relationships, err := parseXLSXRelationships(relationshipBytes)
	if err != nil {
		return nil, err
	}
	sharedStrings := []string(nil)
	sharedRelationshipIDs := make([]string, 0, 1)
	for id, relationship := range relationships {
		if strings.HasSuffix(strings.ToLower(relationship.typeURI), "/sharedstrings") {
			sharedRelationshipIDs = append(sharedRelationshipIDs, id)
		}
	}
	if len(sharedRelationshipIDs) > 1 {
		return nil, fmt.Errorf("XLSX workbook has multiple shared strings relationships")
	}
	if len(sharedRelationshipIDs) == 1 {
		relationship := relationships[sharedRelationshipIDs[0]]
		if relationship.external {
			return nil, fmt.Errorf("XLSX shared strings relationship cannot be external")
		}
		part, err := resolveXLSXPartTarget("xl/workbook.xml", relationship.target)
		if err != nil {
			return nil, err
		}
		sharedBytes, err := reader.readPart(ctx, part)
		if err != nil {
			return nil, err
		}
		sharedStrings, err = parseXLSXSharedStrings(sharedBytes)
		if err != nil {
			return nil, err
		}
	}
	blocks := make([]Block, 0)
	globalRows, globalCells, totalText := 0, 0, 0
	globalPhysicalRows := 0
	for _, sheet := range sheets {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		relationship, found := relationships[sheet.rid]
		if !found || relationship.external || !strings.HasSuffix(strings.ToLower(relationship.typeURI), "/worksheet") {
			return nil, fmt.Errorf("XLSX worksheet %q has a missing or external relationship", sheet.name)
		}
		worksheetPath, err := resolveXLSXPartTarget("xl/workbook.xml", relationship.target)
		if err != nil {
			return nil, fmt.Errorf("resolve XLSX worksheet %q: %w", sheet.name, err)
		}
		worksheetBytes, err := reader.readPart(ctx, worksheetPath)
		if err != nil {
			return nil, err
		}
		rows, cells, textBytes, err := parseXLSXWorksheet(ctx, worksheetBytes, sheet.name, sharedStrings, options, &globalPhysicalRows)
		if err != nil {
			return nil, fmt.Errorf("parse XLSX worksheet %q: %w", sheet.name, err)
		}
		globalRows += len(rows)
		globalCells += cells
		totalText += textBytes
		if globalRows > options.MaxRows {
			return nil, fmt.Errorf("XLSX row count exceeds the configured limit %d", options.MaxRows)
		}
		if globalCells > options.MaxCells {
			return nil, fmt.Errorf("XLSX cell count exceeds the configured limit %d", options.MaxCells)
		}
		if totalText > options.MaxTextBytes {
			return nil, fmt.Errorf("XLSX extracted text exceeds the configured limit %d", options.MaxTextBytes)
		}
		for _, row := range rows {
			block, err := xlsxRowBlock(source, uint32(len(blocks)+1), row)
			if err != nil {
				return nil, err
			}
			blocks = append(blocks, block)
		}
	}
	return blocks, nil
}

func validateXLSXInput(value Value, source SourceRef, options XLSXOptions) (XLSXOptions, error) {
	if err := value.Validate(); err != nil {
		return XLSXOptions{}, err
	}
	if err := source.Validate(); err != nil {
		return XLSXOptions{}, err
	}
	if source.Revision != value.Revision || source.Digest != value.Digest {
		return XLSXOptions{}, fmt.Errorf("%w: XLSX source does not match the CONTENT revision and digest", ErrInvalidBlock)
	}
	mediaType, _, err := mime.ParseMediaType(value.MIMEType)
	if err != nil || !strings.EqualFold(mediaType, xlsxMIMEType) {
		return XLSXOptions{}, fmt.Errorf("XLSX parser requires %s MIME type", xlsxMIMEType)
	}
	if options.MaxSourceBytes == 0 {
		options.MaxSourceBytes = xlsxMaxSourceBytes
	}
	if options.MaxXMLPartBytes == 0 {
		options.MaxXMLPartBytes = xlsxMaxPartBytes
	}
	if options.MaxTotalXMLBytes == 0 {
		options.MaxTotalXMLBytes = xlsxMaxTotalXML
	}
	if options.MaxSheets == 0 {
		options.MaxSheets = xlsxMaxSheets
	}
	if options.MaxRows == 0 {
		options.MaxRows = xlsxMaxRows
	}
	if options.MaxCells == 0 {
		options.MaxCells = xlsxMaxCells
	}
	if options.MaxTextBytes == 0 {
		options.MaxTextBytes = xlsxMaxTextBytes
	}
	if options.MaxSourceBytes > xlsxMaxSourceBytes || options.MaxXMLPartBytes > xlsxMaxPartBytes || options.MaxTotalXMLBytes > xlsxMaxTotalXML ||
		options.MaxSheets > xlsxMaxSheets || options.MaxRows > xlsxMaxRows || options.MaxCells > xlsxMaxCells || options.MaxTextBytes > xlsxMaxTextBytes ||
		options.MaxSourceBytes == 0 || options.MaxXMLPartBytes == 0 || options.MaxTotalXMLBytes == 0 || options.MaxSheets < 1 || options.MaxRows < 1 || options.MaxCells < 1 || options.MaxTextBytes < 1 {
		return XLSXOptions{}, fmt.Errorf("XLSX limits must be positive and cannot exceed the built-in resource ceilings")
	}
	if value.Size() == 0 || value.Size() > options.MaxSourceBytes {
		return XLSXOptions{}, fmt.Errorf("XLSX source size %d is outside the supported range 1..%d", value.Size(), options.MaxSourceBytes)
	}
	return options, nil
}

func (reader *xlsxArchiveReader) readPart(ctx context.Context, name string) ([]byte, error) {
	file := reader.files[name]
	if file == nil {
		return nil, fmt.Errorf("XLSX package is missing required part %q", name)
	}
	if file.UncompressedSize64 > reader.options.MaxXMLPartBytes {
		return nil, fmt.Errorf("XLSX part %q size %d exceeds the configured limit %d", name, file.UncompressedSize64, reader.options.MaxXMLPartBytes)
	}
	if reader.totalXML > reader.options.MaxTotalXMLBytes-file.UncompressedSize64 {
		return nil, fmt.Errorf("XLSX XML parts exceed the configured total limit %d", reader.options.MaxTotalXMLBytes)
	}
	part, err := file.Open()
	if err != nil {
		return nil, fmt.Errorf("open XLSX part %q: %w", name, err)
	}
	data, readErr := io.ReadAll(io.LimitReader(part, int64(reader.options.MaxXMLPartBytes)+1))
	closeErr := part.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read XLSX part %q: %w", name, readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close XLSX part %q: %w", name, closeErr)
	}
	if uint64(len(data)) != file.UncompressedSize64 || uint64(len(data)) > reader.options.MaxXMLPartBytes {
		return nil, fmt.Errorf("XLSX part %q expanded to %d bytes, expected %d within limit", name, len(data), file.UncompressedSize64)
	}
	reader.totalXML += uint64(len(data))
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return data, nil
}

func parseXLSXWorkbook(data []byte) ([]xlsxSheet, error) {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	sheets := make([]xlsxSheet, 0)
	rootSeen := false
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("parse XLSX workbook: %w", err)
		}
		start, ok := token.(xml.StartElement)
		if !ok {
			continue
		}
		if !rootSeen {
			if start.Name.Space != xlsxWorkbookNS || start.Name.Local != "workbook" {
				return nil, fmt.Errorf("XLSX workbook XML has an invalid root")
			}
			rootSeen = true
		}
		if start.Name.Space != xlsxWorkbookNS || start.Name.Local != "sheet" {
			continue
		}
		name, _ := xlsxAttr(start.Attr, "", "name")
		rid, _ := xlsxAttr(start.Attr, xlsxOfficeRelNS, "id")
		if strings.TrimSpace(name) == "" || strings.TrimSpace(rid) == "" {
			return nil, fmt.Errorf("XLSX sheet is missing name or relationship ID")
		}
		for _, existing := range sheets {
			if strings.EqualFold(existing.name, name) {
				return nil, fmt.Errorf("XLSX workbook has duplicate sheet name %q", name)
			}
		}
		sheets = append(sheets, xlsxSheet{name: name, rid: rid})
	}
	if !rootSeen {
		return nil, fmt.Errorf("XLSX workbook XML is empty")
	}
	return sheets, nil
}

func parseXLSXRelationships(data []byte) (map[string]xlsxRelationship, error) {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	relationships := make(map[string]xlsxRelationship)
	rootSeen := false
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("parse XLSX workbook relationships: %w", err)
		}
		start, ok := token.(xml.StartElement)
		if !ok {
			continue
		}
		if !rootSeen {
			if start.Name.Space != xlsxPackageRelNS || start.Name.Local != "Relationships" {
				return nil, fmt.Errorf("XLSX relationship XML has an invalid root")
			}
			rootSeen = true
		}
		if start.Name.Space != xlsxPackageRelNS || start.Name.Local != "Relationship" {
			continue
		}
		id, _ := xlsxAttr(start.Attr, "", "Id")
		target, _ := xlsxAttr(start.Attr, "", "Target")
		typeURI, _ := xlsxAttr(start.Attr, "", "Type")
		targetMode, _ := xlsxAttr(start.Attr, "", "TargetMode")
		if id == "" || target == "" {
			return nil, fmt.Errorf("XLSX relationship is missing ID or target")
		}
		if _, duplicate := relationships[id]; duplicate {
			return nil, fmt.Errorf("XLSX package has duplicate relationship ID %q", id)
		}
		relationships[id] = xlsxRelationship{target: target, typeURI: typeURI, external: strings.EqualFold(targetMode, "External")}
	}
	if !rootSeen {
		return nil, fmt.Errorf("XLSX relationship XML is empty")
	}
	return relationships, nil
}

func resolveXLSXPartTarget(basePart, target string) (string, error) {
	target = strings.TrimSpace(target)
	if target == "" || strings.ContainsAny(target, "\\\x00?#:") {
		return "", fmt.Errorf("XLSX relationship target %q is not a package path", target)
	}
	var resolved string
	if strings.HasPrefix(target, "/") {
		resolved = path.Clean(strings.TrimPrefix(target, "/"))
	} else {
		resolved = path.Clean(path.Join(path.Dir(basePart), target))
	}
	if resolved == "." || resolved == ".." || strings.HasPrefix(resolved, "../") || !strings.HasPrefix(resolved, "xl/") {
		return "", fmt.Errorf("XLSX relationship target %q escapes the workbook package", target)
	}
	return resolved, nil
}

func parseXLSXSharedStrings(data []byte) ([]string, error) {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	values := make([]string, 0)
	var current strings.Builder
	rootSeen, inString, inText := false, false, false
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("parse XLSX shared strings: %w", err)
		}
		switch token := token.(type) {
		case xml.StartElement:
			if !rootSeen {
				if token.Name.Space != xlsxWorkbookNS || token.Name.Local != "sst" {
					return nil, fmt.Errorf("XLSX shared strings XML has an invalid root")
				}
				rootSeen = true
			}
			if token.Name.Space != xlsxWorkbookNS {
				continue
			}
			switch token.Name.Local {
			case "si":
				inString = true
				current.Reset()
			case "t":
				if inString {
					inText = true
				}
			}
		case xml.CharData:
			if inString && inText {
				current.Write([]byte(token))
			}
		case xml.EndElement:
			if token.Name.Space != xlsxWorkbookNS {
				continue
			}
			switch token.Name.Local {
			case "t":
				inText = false
			case "si":
				if inString {
					values = append(values, current.String())
					inString = false
				}
			}
		}
	}
	if !rootSeen {
		return nil, fmt.Errorf("XLSX shared strings XML is empty")
	}
	return values, nil
}

func parseXLSXWorksheet(ctx context.Context, data []byte, sheetName string, shared []string, options XLSXOptions, globalPhysicalRows *int) ([]XLSXRow, int, int, error) {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	rows := make([]XLSXRow, 0)
	rootSeen, inSheetData := false, false
	var row XLSXRow
	var cell XLSXCell
	var valueText, formulaText, inlineText strings.Builder
	capture := ""
	rowSequence, totalCells, totalText := 0, 0, 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, totalCells, totalText, err
		}
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, totalCells, totalText, fmt.Errorf("parse worksheet XML: %w", err)
		}
		switch token := token.(type) {
		case xml.StartElement:
			if !rootSeen {
				if token.Name.Space != xlsxWorkbookNS || token.Name.Local != "worksheet" {
					return nil, totalCells, totalText, fmt.Errorf("worksheet XML has an invalid root")
				}
				rootSeen = true
			}
			if token.Name.Space != xlsxWorkbookNS {
				continue
			}
			switch token.Name.Local {
			case "sheetData":
				inSheetData = true
			case "row":
				if !inSheetData {
					continue
				}
				rowSequence++
				*globalPhysicalRows++
				if *globalPhysicalRows > options.MaxRows {
					return nil, totalCells, totalText, fmt.Errorf("workbook row count exceeds the configured limit %d", options.MaxRows)
				}
				rowNumber := uint64(rowSequence)
				if raw, ok := xlsxAttr(token.Attr, "", "r"); ok {
					parsed, parseErr := strconv.ParseUint(raw, 10, 32)
					if parseErr != nil || parsed == 0 {
						return nil, totalCells, totalText, fmt.Errorf("invalid worksheet row number %q", raw)
					}
					rowNumber = parsed
				}
				row = XLSXRow{SheetName: sheetName, RowNumber: uint32(rowNumber), Cells: make([]XLSXCell, 0)}
			case "c":
				if row.RowNumber == 0 {
					continue
				}
				cell = XLSXCell{}
				cell.Address, _ = xlsxAttr(token.Attr, "", "r")
				if !validXLSXCellAddress(cell.Address) {
					return nil, totalCells, totalText, fmt.Errorf("invalid cell address %q", cell.Address)
				}
				cell.Type, _ = xlsxAttr(token.Attr, "", "t")
				cell.StyleID, _ = xlsxAttr(token.Attr, "", "s")
				valueText.Reset()
				formulaText.Reset()
				inlineText.Reset()
			case "v":
				if row.RowNumber != 0 {
					capture = "value"
				}
			case "f":
				if row.RowNumber != 0 {
					capture = "formula"
					cell.FormulaType, _ = xlsxAttr(token.Attr, "", "t")
					cell.FormulaRef, _ = xlsxAttr(token.Attr, "", "ref")
					cell.FormulaSharedIndex, _ = xlsxAttr(token.Attr, "", "si")
				}
			case "t":
				if row.RowNumber != 0 && cell.Address != "" {
					capture = "inline"
				}
			}
		case xml.CharData:
			switch capture {
			case "value":
				valueText.Write([]byte(token))
			case "formula":
				formulaText.Write([]byte(token))
			case "inline":
				inlineText.Write([]byte(token))
			}
		case xml.EndElement:
			if token.Name.Space != xlsxWorkbookNS {
				continue
			}
			switch token.Name.Local {
			case "v", "f", "t":
				capture = ""
			case "c":
				if row.RowNumber == 0 || cell.Address == "" {
					continue
				}
				totalCells++
				if totalCells > options.MaxCells {
					return nil, totalCells, totalText, fmt.Errorf("XLSX cell count exceeds the configured limit %d", options.MaxCells)
				}
				cell.RawValue = valueText.String()
				cell.Formula = formulaText.String()
				if err := resolveXLSXCell(&cell, inlineText.String(), shared); err != nil {
					return nil, totalCells, totalText, fmt.Errorf("cell %s: %w", cell.Address, err)
				}
				totalText += len(cell.Value) + len(cell.Formula)
				if totalText > options.MaxTextBytes {
					return nil, totalCells, totalText, fmt.Errorf("XLSX extracted text exceeds the configured limit %d", options.MaxTextBytes)
				}
				row.Cells = append(row.Cells, cell)
			case "row":
				if row.RowNumber != 0 && len(row.Cells) > 0 {
					rows = append(rows, row)
				}
				row = XLSXRow{}
			case "sheetData":
				inSheetData = false
			}
		}
	}
	if !rootSeen {
		return nil, totalCells, totalText, fmt.Errorf("worksheet XML is empty")
	}
	if row.RowNumber != 0 && len(row.Cells) > 0 {
		rows = append(rows, row)
	}
	return rows, totalCells, totalText, nil
}

func resolveXLSXCell(cell *XLSXCell, inlineValue string, shared []string) error {
	switch cell.Type {
	case "s":
		index, err := strconv.Atoi(strings.TrimSpace(cell.RawValue))
		if err != nil || index < 0 || index >= len(shared) {
			return fmt.Errorf("shared string index %q is invalid", cell.RawValue)
		}
		cell.Type, cell.Value = "shared_string", shared[index]
	case "inlineStr":
		cell.Type, cell.Value = "inline_string", inlineValue
	case "str":
		cell.Type, cell.Value = "string", cell.RawValue
	case "b":
		if cell.RawValue != "0" && cell.RawValue != "1" {
			return fmt.Errorf("boolean value %q is invalid", cell.RawValue)
		}
		cell.Type = "boolean"
		if cell.RawValue == "1" {
			cell.Value = "TRUE"
		} else {
			cell.Value = "FALSE"
		}
	case "e":
		cell.Type, cell.Value = "error", cell.RawValue
	case "d":
		cell.Type, cell.Value = "date", cell.RawValue
	case "":
		cell.Type, cell.Value = "number", cell.RawValue
	case "n":
		cell.Type, cell.Value = "number", cell.RawValue
	default:
		return fmt.Errorf("unsupported cell data type %q", cell.Type)
	}
	return nil
}

func xlsxRowBlock(source SourceRef, ordinal uint32, row XLSXRow) (Block, error) {
	encoded, err := json.Marshal(row)
	if err != nil {
		return Block{}, fmt.Errorf("encode XLSX row payload: %w", err)
	}
	payload, err := NewTypedPayload(xlsxRowSchemaID, xlsxRowSchemaV, xlsxRowMediaType, encoded)
	if err != nil {
		return Block{}, err
	}
	parts := make([]string, 0, len(row.Cells))
	for _, cell := range row.Cells {
		part := cell.Address + " [" + cell.Type + "] " + strconv.Quote(cell.Value)
		if cell.Formula != "" {
			part += " formula=" + cell.Formula
		} else if cell.FormulaType != "" {
			part += " formula_type=" + cell.FormulaType
			if cell.FormulaRef != "" {
				part += " formula_ref=" + cell.FormulaRef
			}
		}
		parts = append(parts, part)
	}
	text := "Sheet " + strconv.Quote(row.SheetName) + ", row " + strconv.FormatUint(uint64(row.RowNumber), 10) + ": " + strings.Join(parts, "; ")
	block, err := NewBlock(source, ordinal, BlockTable, text, Locator{}, xlsxProcessorID, xlsxProcessorVer, "", "")
	if err != nil {
		return Block{}, fmt.Errorf("create XLSX row block %s/%d: %w", row.SheetName, row.RowNumber, err)
	}
	block.Payload = &payload
	block.Attributes = map[string]string{"content_type": "xlsx_row", "sheet_name": row.SheetName, "row_number": strconv.FormatUint(uint64(row.RowNumber), 10), "structure_path": "/workbook/sheets[" + strconv.Quote(row.SheetName) + "]/row[" + strconv.FormatUint(uint64(row.RowNumber), 10) + "]"}
	block.ID = block.Identity()
	if err := block.Validate(); err != nil {
		return Block{}, err
	}
	return block, nil
}

func xlsxAttr(attributes []xml.Attr, namespace, local string) (string, bool) {
	for _, attribute := range attributes {
		if attribute.Name.Space == namespace && attribute.Name.Local == local {
			return attribute.Value, true
		}
	}
	return "", false
}

func validXLSXCellAddress(address string) bool {
	address = strings.ToUpper(strings.TrimSpace(address))
	if address == "" {
		return false
	}
	index := 0
	column := 0
	for index < len(address) && address[index] >= 'A' && address[index] <= 'Z' {
		column = column*26 + int(address[index]-'A'+1)
		if column > 16384 {
			return false
		}
		index++
	}
	if index == 0 || index == len(address) {
		return false
	}
	row, err := strconv.ParseUint(address[index:], 10, 32)
	return err == nil && row > 0 && row <= 1048576
}
