package content

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

const (
	csvProcessorID      = "xmysql.csv-parser"
	csvProcessorVersion = "1"
)

// ParseCSVFromStore parses an explicitly requested CSV CONTENT value into a
// source-bound typed table block. Field values remain strings because CSV
// carries no SQL type declarations; callers must not infer numeric/date types
// from lexical appearance alone.
func ParseCSVFromStore(ctx context.Context, store *SegmentStore, value Value, source SourceRef) (Block, error) {
	if err := value.Validate(); err != nil {
		return Block{}, err
	}
	if source.Revision != value.Revision || source.Digest != value.Digest {
		return Block{}, fmt.Errorf("%w: CSV source does not match the CONTENT revision and digest", ErrInvalidBlock)
	}
	if err := source.Validate(); err != nil {
		return Block{}, err
	}
	data, err := value.ReadAllFromStore(ctx, store, MaxInlineTextBytes)
	if err != nil {
		return Block{}, err
	}
	inline := value.Clone()
	inline.Bytes = data
	inline.ExternalRef = nil
	inline.TransientRef = false
	text, err := ExtractText(inline)
	if err != nil {
		return Block{}, err
	}
	baseOffset := 0
	if len(data) >= 3 && bytes.Equal(data[:3], []byte{0xef, 0xbb, 0xbf}) {
		baseOffset = 3
	}

	reader := csv.NewReader(strings.NewReader(text))
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = false
	header, err := reader.Read()
	if err != nil {
		return Block{}, fmt.Errorf("parse CSV header: %w", err)
	}
	if len(header) == 0 || len(header) > MaxStructuredTableColumns {
		return Block{}, fmt.Errorf("CSV must contain between 1 and %d columns", MaxStructuredTableColumns)
	}
	columns := make([]TableColumn, len(header))
	seen := make(map[string]struct{}, len(header))
	for index, name := range header {
		name = strings.TrimSpace(name)
		if name == "" {
			name = fmt.Sprintf("column_%d", index+1)
		}
		if len(name) > 256 {
			return Block{}, fmt.Errorf("CSV header column %d exceeds 256 bytes", index+1)
		}
		key := strings.ToLower(name)
		if _, exists := seen[key]; exists {
			return Block{}, fmt.Errorf("CSV contains duplicate header %q", name)
		}
		seen[key] = struct{}{}
		columns[index] = TableColumn{Name: name, SQLType: "VARCHAR", Meaning: "CSV field; source text preserved"}
	}

	rows := make([]TableRow, 0)
	previousOffset := reader.InputOffset()
	for ordinal := uint64(1); ; ordinal++ {
		record, readErr := reader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return Block{}, fmt.Errorf("parse CSV row %d: %w", ordinal, readErr)
		}
		if ordinal > MaxStructuredTableRows {
			return Block{}, fmt.Errorf("CSV exceeds %d data rows", MaxStructuredTableRows)
		}
		if len(record) != len(columns) {
			return Block{}, fmt.Errorf("CSV row %d has %d fields; header has %d", ordinal, len(record), len(columns))
		}
		if int64(len(columns))*int64(len(rows)+1) > MaxStructuredTableCells {
			return Block{}, fmt.Errorf("CSV exceeds %d table cells", MaxStructuredTableCells)
		}
		endOffset := reader.InputOffset()
		start := uint64(baseOffset) + uint64(previousOffset)
		end := uint64(baseOffset) + uint64(endOffset)
		rowLocator := Locator{ByteStart: &start, ByteEnd: &end}
		cells := make([]TableCell, len(record))
		for columnIndex, field := range record {
			if len(field) > 1<<20 {
				return Block{}, fmt.Errorf("CSV row %d column %d exceeds the 1 MiB cell source limit", ordinal, columnIndex+1)
			}
			encoded, marshalErr := json.Marshal(field)
			if marshalErr != nil {
				return Block{}, fmt.Errorf("encode CSV row %d column %d: %w", ordinal, columnIndex+1, marshalErr)
			}
			cells[columnIndex] = TableCell{State: TableCellPresent, Value: encoded, SourceText: field}
		}
		rows = append(rows, TableRow{Ordinal: ordinal, Cells: cells, Locator: rowLocator})
		previousOffset = endOffset
	}

	table := StructuredTable{TableID: hex.EncodeToString(source.Digest[:]), Title: "CSV", Columns: columns, Rows: rows}
	payload, err := NewStructuredTablePayload(table)
	if err != nil {
		return Block{}, err
	}
	start, end := uint64(0), value.Size()
	block, err := NewBlock(source, 1, BlockTable, "", Locator{ByteStart: &start, ByteEnd: &end}, csvProcessorID, csvProcessorVersion, "", "")
	if err != nil {
		return Block{}, err
	}
	block.Payload = &payload
	block.Attributes = map[string]string{"format": "csv", "header": "true", "value_type": "string"}
	block.ID = block.Identity()
	if err := block.Validate(); err != nil {
		return Block{}, err
	}
	return block, nil
}
