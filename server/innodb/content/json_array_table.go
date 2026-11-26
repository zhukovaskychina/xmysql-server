package content

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	jsonArrayProcessorID      = "xmysql.json-array-parser"
	jsonArrayProcessorVersion = "1"
)

// ParseJSONArrayTableFromStore converts a JSON array of objects into a typed
// table block. Every cell remains JSON-typed, missing properties and explicit
// nulls stay distinct, and each row locator points into the original bytes.
func ParseJSONArrayTableFromStore(ctx context.Context, store *SegmentStore, value Value, source SourceRef) (Block, error) {
	if err := value.Validate(); err != nil {
		return Block{}, err
	}
	if source.Revision != value.Revision || source.Digest != value.Digest {
		return Block{}, fmt.Errorf("%w: JSON source does not match the CONTENT revision and digest", ErrInvalidBlock)
	}
	if err := source.Validate(); err != nil {
		return Block{}, err
	}
	mediaType, _, err := mime.ParseMediaType(value.MIMEType)
	if err != nil || !(strings.EqualFold(mediaType, "application/json") || strings.HasSuffix(strings.ToLower(mediaType), "+json")) {
		return Block{}, fmt.Errorf("JSON table parser requires application/json or a +json MIME type")
	}
	data, err := value.ReadAllFromStore(ctx, store, MaxInlineTextBytes)
	if err != nil {
		return Block{}, err
	}
	baseOffset := 0
	if len(data) >= 3 && bytes.Equal(data[:3], []byte{0xef, 0xbb, 0xbf}) {
		baseOffset = 3
		data = data[3:]
	}
	if !utf8.Valid(data) {
		return Block{}, ErrInvalidTextEncoding
	}
	if err := validateJSONUniqueKeys(data); err != nil {
		return Block{}, err
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	opening, err := decoder.Token()
	if err != nil {
		return Block{}, fmt.Errorf("read JSON table root: %w", err)
	}
	if opening != json.Delim('[') {
		return Block{}, fmt.Errorf("JSON table parser requires an array of objects")
	}
	rows := make([]map[string]json.RawMessage, 0)
	rowLocators := make([]Locator, 0)
	keySet := make(map[string]struct{})
	for ordinal := uint64(1); decoder.More(); ordinal++ {
		if ordinal > MaxStructuredTableRows {
			return Block{}, fmt.Errorf("JSON array exceeds %d rows", MaxStructuredTableRows)
		}
		startOffset := decoder.InputOffset()
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return Block{}, fmt.Errorf("decode JSON row %d: %w", ordinal, err)
		}
		endOffset := decoder.InputOffset()
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil || object == nil {
			return Block{}, fmt.Errorf("JSON array row %d must be an object", ordinal)
		}
		for key := range object {
			keySet[key] = struct{}{}
			if len(keySet) > MaxStructuredTableColumns {
				return Block{}, fmt.Errorf("JSON object array exceeds %d distinct properties", MaxStructuredTableColumns)
			}
		}
		if int64(len(rows)+1)*int64(len(keySet)) > MaxStructuredTableCells {
			return Block{}, fmt.Errorf("JSON object array exceeds %d table cells", MaxStructuredTableCells)
		}
		start := uint64(baseOffset) + uint64(startOffset)
		end := uint64(baseOffset) + uint64(endOffset)
		rows = append(rows, object)
		rowLocators = append(rowLocators, Locator{ByteStart: &start, ByteEnd: &end})
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim(']') {
		return Block{}, fmt.Errorf("JSON array is not properly terminated")
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return Block{}, fmt.Errorf("JSON table input contains trailing data")
		}
		return Block{}, fmt.Errorf("read JSON table trailing data: %w", err)
	}
	if len(rows) == 0 {
		return Block{}, fmt.Errorf("JSON table parser requires a non-empty array of objects")
	}
	keys := make([]string, 0, len(keySet))
	for key := range keySet {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	columns := make([]TableColumn, len(keys))
	caseFoldedNames := make(map[string]string, len(keys))
	for index, key := range keys {
		if strings.TrimSpace(key) == "" {
			return Block{}, fmt.Errorf("JSON property names cannot be empty or whitespace-only")
		}
		if len(key) > 256 {
			return Block{}, fmt.Errorf("JSON property name exceeds 256 bytes")
		}
		folded := strings.ToLower(key)
		if earlier, exists := caseFoldedNames[folded]; exists {
			return Block{}, fmt.Errorf("JSON properties %q and %q collide under SQL identifier comparison", earlier, key)
		}
		caseFoldedNames[folded] = key
		columns[index] = TableColumn{Name: key, SQLType: "JSON", Meaning: "JSON object property; source type preserved"}
	}
	if len(columns) == 0 {
		return Block{}, fmt.Errorf("JSON objects contain no properties")
	}
	tableRows := make([]TableRow, len(rows))
	for rowIndex, object := range rows {
		cells := make([]TableCell, len(columns))
		for columnIndex, key := range keys {
			raw, exists := object[key]
			if !exists {
				cells[columnIndex] = TableCell{State: TableCellSourceMissing}
			} else if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				cells[columnIndex] = TableCell{State: TableCellSourceNull}
			} else {
				cells[columnIndex] = TableCell{State: TableCellPresent, Value: append(json.RawMessage(nil), raw...)}
			}
		}
		tableRows[rowIndex] = TableRow{Ordinal: uint64(rowIndex + 1), Cells: cells, Locator: rowLocators[rowIndex]}
	}
	table := StructuredTable{
		TableID: hex.EncodeToString(source.Digest[:]), Title: "JSON array of objects",
		Columns: columns, Rows: tableRows,
	}
	payload, err := NewStructuredTablePayload(table)
	if err != nil {
		return Block{}, err
	}
	start, end := uint64(0), value.Size()
	block, err := NewBlock(source, 1, BlockTable, "", Locator{ByteStart: &start, ByteEnd: &end}, jsonArrayProcessorID, jsonArrayProcessorVersion, "", "")
	if err != nil {
		return Block{}, err
	}
	block.Payload = &payload
	block.Attributes = map[string]string{"format": "json-array-of-objects", "value_type": "json", "property_order": "unicode-codepoint"}
	block.ID = block.Identity()
	if err := block.Validate(); err != nil {
		return Block{}, err
	}
	return block, nil
}

func validateJSONUniqueKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := consumeUniqueJSONValue(decoder); err != nil {
		return fmt.Errorf("validate JSON table input: %w", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("JSON table input contains trailing data")
		}
		return fmt.Errorf("validate JSON trailing data: %w", err)
	}
	return nil
}

func consumeUniqueJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("JSON object property name is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate JSON property %q", key)
			}
			seen[key] = struct{}{}
			if err := consumeUniqueJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return fmt.Errorf("unterminated JSON object")
		}
	case '[':
		for decoder.More() {
			if err := consumeUniqueJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return fmt.Errorf("unterminated JSON array")
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
	return nil
}
