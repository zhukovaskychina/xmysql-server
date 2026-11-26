package content

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"math/big"
	"strconv"
	"strings"
)

const (
	StructuredTablePayloadSchemaID      = "xmysql.table"
	StructuredTablePayloadSchemaVersion = 1
	StructuredTablePayloadMediaType     = "application/vnd.xmysql.table+json"
	MaxStructuredTableColumns           = 16384
	MaxStructuredTableRows              = 100000
	MaxStructuredTableCells             = 200000
)

type StructuredTable struct {
	TableID string        `json:"table_id,omitempty"`
	Title   string        `json:"title,omitempty"`
	Columns []TableColumn `json:"columns"`
	Rows    []TableRow    `json:"rows"`
}

type TableColumn struct {
	Name     string `json:"name"`
	SQLType  string `json:"sql_type"`
	Unit     string `json:"unit,omitempty"`
	Meaning  string `json:"meaning,omitempty"`
	Nullable *bool  `json:"nullable,omitempty"`
}

type TableRow struct {
	Ordinal uint64      `json:"ordinal"`
	Cells   []TableCell `json:"cells"`
	Locator Locator     `json:"locator,omitempty"`
}

type TableCellState string

const (
	TableCellPresent          TableCellState = "PRESENT"
	TableCellSourceNull       TableCellState = "SOURCE_NULL"
	TableCellSourceMissing    TableCellState = "SOURCE_MISSING"
	TableCellNotProcessed     TableCellState = "NOT_PROCESSED"
	TableCellConversionFailed TableCellState = "CONVERSION_FAILED"
)

type TableCell struct {
	State      TableCellState  `json:"state"`
	Value      json.RawMessage `json:"value,omitempty"`
	SourceText string          `json:"source_text,omitempty"`
	Error      string          `json:"error,omitempty"`
	Locator    Locator         `json:"locator,omitempty"`
}

func NewStructuredTablePayload(table StructuredTable) (TypedPayload, error) {
	encoded, err := json.Marshal(table)
	if err != nil {
		return TypedPayload{}, fmt.Errorf("encode structured table: %w", err)
	}
	if err := ValidateStructuredTable(table); err != nil {
		return TypedPayload{}, err
	}
	return NewTypedPayload(StructuredTablePayloadSchemaID, StructuredTablePayloadSchemaVersion, StructuredTablePayloadMediaType, encoded)
}

func DecodeStructuredTable(payload TypedPayload) (StructuredTable, error) {
	if payload.SchemaID != StructuredTablePayloadSchemaID || payload.SchemaVersion != StructuredTablePayloadSchemaVersion {
		return StructuredTable{}, fmt.Errorf("%w: unsupported structured table schema %s@%d", ErrInvalidBlock, payload.SchemaID, payload.SchemaVersion)
	}
	if err := payload.Validate(); err != nil {
		return StructuredTable{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload.JSON))
	decoder.DisallowUnknownFields()
	var table StructuredTable
	if err := decoder.Decode(&table); err != nil {
		return StructuredTable{}, fmt.Errorf("%w: decode structured table: %v", ErrInvalidBlock, err)
	}
	if err := ValidateStructuredTable(table); err != nil {
		return StructuredTable{}, err
	}
	return table, nil
}

// StructuredTableRetrievalText builds a deterministic Markdown view for
// lexical retrieval and model context. The typed payload remains authoritative;
// this view preserves each column's SQL type/unit and each row's locator, and
// labels missing or failed cells instead of flattening them to empty strings.
func StructuredTableRetrievalText(payload TypedPayload) (string, error) {
	table, err := DecodeStructuredTable(payload)
	if err != nil {
		return "", err
	}
	var output strings.Builder
	if table.Title != "" {
		output.WriteString("# ")
		output.WriteString(markdownTableCell(table.Title))
		output.WriteString("\n\n")
	}
	output.WriteByte('|')
	for _, column := range table.Columns {
		output.WriteByte(' ')
		output.WriteString(markdownTableCell(tableColumnRetrievalLabel(column)))
		output.WriteString(" |")
	}
	output.WriteString(" _row_locator_ |\n|")
	for range table.Columns {
		output.WriteString(" --- |")
	}
	output.WriteString(" --- |\n")
	for _, row := range table.Rows {
		output.WriteByte('|')
		for _, cell := range row.Cells {
			output.WriteByte(' ')
			output.WriteString(markdownTableCell(tableCellRetrievalValue(cell)))
			output.WriteString(" |")
		}
		locator, marshalErr := json.Marshal(row.Locator)
		if marshalErr != nil {
			return "", fmt.Errorf("encode table row locator: %w", marshalErr)
		}
		output.WriteByte(' ')
		output.WriteString(markdownTableCell(fmt.Sprintf("row=%d %s", row.Ordinal, locator)))
		output.WriteString(" |\n")
		if uint64(output.Len()) > MaxInlineTextBytes {
			return "", fmt.Errorf("structured table retrieval text exceeds %d bytes", MaxInlineTextBytes)
		}
	}
	if uint64(output.Len()) > MaxInlineTextBytes {
		return "", fmt.Errorf("structured table retrieval text exceeds %d bytes", MaxInlineTextBytes)
	}
	return output.String(), nil
}

func tableColumnRetrievalLabel(column TableColumn) string {
	label := column.Name + " (" + column.SQLType
	if column.Unit != "" {
		label += ", unit=" + column.Unit
	}
	label += ")"
	if column.Meaning != "" {
		label += " — " + column.Meaning
	}
	return label
}

func tableCellRetrievalValue(cell TableCell) string {
	switch cell.State {
	case TableCellPresent:
		var text string
		if json.Unmarshal(cell.Value, &text) == nil {
			return text
		}
		return string(cell.Value)
	case TableCellSourceNull:
		return "[SOURCE_NULL]"
	case TableCellSourceMissing:
		return "[SOURCE_MISSING]"
	case TableCellNotProcessed:
		return "[NOT_PROCESSED]"
	case TableCellConversionFailed:
		return "[CONVERSION_FAILED: " + cell.Error + "]"
	default:
		return "[UNKNOWN_CELL_STATE]"
	}
}

func markdownTableCell(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "|", "\\|")
	value = html.EscapeString(value)
	value = strings.ReplaceAll(value, "`", "&#96;")
	value = strings.ReplaceAll(value, "\r\n", "<br>")
	value = strings.ReplaceAll(value, "\r", "<br>")
	value = strings.ReplaceAll(value, "\n", "<br>")
	return value
}

func validateStructuredTableJSON(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var table StructuredTable
	if err := decoder.Decode(&table); err != nil {
		return fmt.Errorf("%w: decode structured table payload: %v", ErrInvalidBlock, err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("%w: structured table payload contains trailing JSON", ErrInvalidBlock)
	}
	return ValidateStructuredTable(table)
}

func ValidateStructuredTable(table StructuredTable) error {
	if len(table.Columns) == 0 || len(table.Columns) > MaxStructuredTableColumns || len(table.Rows) > MaxStructuredTableRows {
		return fmt.Errorf("%w: structured table must contain 1..%d columns and at most %d rows", ErrInvalidBlock, MaxStructuredTableColumns, MaxStructuredTableRows)
	}
	if int64(len(table.Columns))*int64(len(table.Rows)) > MaxStructuredTableCells {
		return fmt.Errorf("%w: structured table exceeds %d cells", ErrInvalidBlock, MaxStructuredTableCells)
	}
	if len(table.TableID) > 256 || len(table.Title) > 4096 {
		return fmt.Errorf("%w: structured table identity or title exceeds the limit", ErrInvalidBlock)
	}
	columnNames := make(map[string]struct{}, len(table.Columns))
	for index, column := range table.Columns {
		name := strings.TrimSpace(column.Name)
		if name == "" || len(name) > 256 || len(column.Unit) > 256 || len(column.Meaning) > 4096 {
			return fmt.Errorf("%w: structured table column %d has invalid metadata", ErrInvalidBlock, index)
		}
		key := strings.ToLower(name)
		if _, duplicate := columnNames[key]; duplicate {
			return fmt.Errorf("%w: duplicate structured table column %q", ErrInvalidBlock, name)
		}
		columnNames[key] = struct{}{}
		if _, ok := normalizeTableSQLType(column.SQLType); !ok {
			return fmt.Errorf("%w: unsupported SQL type %q for table column %q", ErrInvalidBlock, column.SQLType, name)
		}
	}
	rowOrdinals := make(map[uint64]struct{}, len(table.Rows))
	for rowIndex, row := range table.Rows {
		if row.Ordinal == 0 || len(row.Cells) != len(table.Columns) {
			return fmt.Errorf("%w: row %d requires a positive ordinal and one cell per column", ErrInvalidBlock, rowIndex)
		}
		if _, duplicate := rowOrdinals[row.Ordinal]; duplicate {
			return fmt.Errorf("%w: duplicate structured table row ordinal %d", ErrInvalidBlock, row.Ordinal)
		}
		rowOrdinals[row.Ordinal] = struct{}{}
		if err := row.Locator.Validate(); err != nil {
			return fmt.Errorf("%w: row %d locator: %v", ErrInvalidBlock, row.Ordinal, err)
		}
		for columnIndex, cell := range row.Cells {
			if err := validateTableCell(cell, table.Columns[columnIndex].SQLType); err != nil {
				return fmt.Errorf("%w: row %d column %q: %v", ErrInvalidBlock, row.Ordinal, table.Columns[columnIndex].Name, err)
			}
		}
	}
	return nil
}

func validateTableCell(cell TableCell, sqlType string) error {
	if err := cell.Locator.Validate(); err != nil {
		return fmt.Errorf("cell locator: %v", err)
	}
	switch cell.State {
	case TableCellPresent:
		if len(cell.Value) == 0 || !json.Valid(cell.Value) || cell.Error != "" {
			return fmt.Errorf("PRESENT cell requires a valid JSON value and no error")
		}
		if err := validateTypedSQLValue(sqlType, cell.Value); err != nil {
			return err
		}
	case TableCellSourceNull:
		if len(cell.Value) != 0 && !bytes.Equal(bytes.TrimSpace(cell.Value), []byte("null")) {
			return fmt.Errorf("SOURCE_NULL cell cannot carry a non-NULL value")
		}
		if cell.Error != "" {
			return fmt.Errorf("SOURCE_NULL cell cannot carry an error")
		}
	case TableCellSourceMissing:
		if len(cell.Value) != 0 || cell.Error != "" {
			return fmt.Errorf("SOURCE_MISSING cell cannot carry a value or error")
		}
	case TableCellNotProcessed:
		if len(cell.Value) != 0 || cell.Error != "" {
			return fmt.Errorf("NOT_PROCESSED cell cannot carry a value or error")
		}
	case TableCellConversionFailed:
		if len(cell.Value) != 0 || strings.TrimSpace(cell.Error) == "" {
			return fmt.Errorf("CONVERSION_FAILED cell requires an error and cannot carry a typed value")
		}
	default:
		return fmt.Errorf("unknown table cell state %q", cell.State)
	}
	if len(cell.SourceText) > 1<<20 || len(cell.Error) > 4096 {
		return fmt.Errorf("cell source text or error exceeds the limit")
	}
	return nil
}

func normalizeTableSQLType(sqlType string) (string, bool) {
	value := strings.ToUpper(strings.TrimSpace(sqlType))
	if index := strings.IndexByte(value, '('); index >= 0 {
		value = value[:index]
	}
	parts := strings.Fields(value)
	if len(parts) == 0 {
		return "", false
	}
	base := parts[0]
	switch base {
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "YEAR",
		"DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL", "BOOL", "BOOLEAN", "BIT",
		"CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "ENUM", "SET",
		"BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB",
		"DATE", "TIME", "DATETIME", "TIMESTAMP", "JSON":
		return base, true
	default:
		return "", false
	}
}

func validateTypedSQLValue(sqlType string, encoded json.RawMessage) error {
	baseType, ok := normalizeTableSQLType(sqlType)
	if !ok {
		return fmt.Errorf("unsupported SQL type %q", sqlType)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("invalid typed JSON value: %v", err)
	}
	if value == nil {
		return fmt.Errorf("PRESENT cell cannot contain JSON null; use SOURCE_NULL")
	}
	switch baseType {
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "YEAR":
		number, ok := value.(json.Number)
		if !ok {
			return fmt.Errorf("%s requires an integer JSON number", baseType)
		}
		integer, ok := new(big.Int).SetString(number.String(), 10)
		if !ok {
			return fmt.Errorf("%s requires an exact integer JSON number", baseType)
		}
		if baseType != "YEAR" {
			bits := map[string]uint{"TINYINT": 8, "SMALLINT": 16, "MEDIUMINT": 24, "INT": 32, "INTEGER": 32, "BIGINT": 64}[baseType]
			if err := validateIntegerRange(sqlType, integer, bits); err != nil {
				return err
			}
		} else if integer.Sign() < 0 || integer.Cmp(big.NewInt(2155)) > 0 {
			return fmt.Errorf("YEAR value is outside the supported range 0..2155")
		}
	case "DECIMAL", "NUMERIC":
		number, ok := value.(json.Number)
		if !ok {
			return fmt.Errorf("%s requires a JSON number", baseType)
		}
		if _, ok := new(big.Rat).SetString(number.String()); !ok {
			return fmt.Errorf("%s requires an exact decimal JSON number", baseType)
		}
	case "FLOAT", "DOUBLE", "REAL":
		number, ok := value.(json.Number)
		if !ok {
			return fmt.Errorf("%s requires a JSON number", baseType)
		}
		if _, err := strconv.ParseFloat(number.String(), 64); err != nil {
			return fmt.Errorf("%s requires a finite JSON number", baseType)
		}
	case "BOOL", "BOOLEAN":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("%s requires a JSON boolean", baseType)
		}
	case "BIT":
		switch typed := value.(type) {
		case bool:
		case json.Number:
			if typed.String() != "0" && typed.String() != "1" {
				return fmt.Errorf("BIT accepts only 0 or 1")
			}
		default:
			return fmt.Errorf("BIT requires a JSON boolean or 0/1")
		}
	case "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "ENUM", "SET", "DATE", "TIME", "DATETIME", "TIMESTAMP":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("%s requires a JSON string", baseType)
		}
	case "BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB":
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("%s requires a base64 JSON string", baseType)
		}
		if _, err := base64.StdEncoding.DecodeString(text); err != nil {
			return fmt.Errorf("%s value is not valid base64", baseType)
		}
	case "JSON":
		// json.Decoder already validated the payload while preserving the raw
		// cell bytes in TableCell.Value.
	}
	return nil
}

func validateIntegerRange(sqlType string, integer *big.Int, bits uint) error {
	unsigned := strings.Contains(strings.ToUpper(sqlType), "UNSIGNED")
	if unsigned {
		max := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), bits), big.NewInt(1))
		if integer.Sign() < 0 || integer.Cmp(max) > 0 {
			return fmt.Errorf("unsigned %s value is out of range", sqlType)
		}
		return nil
	}
	limit := new(big.Int).Lsh(big.NewInt(1), bits-1)
	min := new(big.Int).Neg(new(big.Int).Set(limit))
	max := new(big.Int).Sub(new(big.Int).Set(limit), big.NewInt(1))
	if integer.Cmp(min) < 0 || integer.Cmp(max) > 0 {
		return fmt.Errorf("signed %s value is out of range", sqlType)
	}
	return nil
}
