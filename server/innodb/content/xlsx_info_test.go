package content

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func TestParseXLSXRowsPreservesSheetCellsAndFormulaEvidence(t *testing.T) {
	value, source := testXLSXValue(t)
	rows, err := ParseXLSXRows(context.Background(), nil, value, source, XLSXOptions{})
	if err != nil {
		t.Fatalf("ParseXLSXRows: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("got %d worksheet rows, want 3: %#v", len(rows), rows)
	}
	first := rows[0]
	if first.Attributes["sheet_name"] != "Summary" || first.Attributes["row_number"] != "1" || first.Text == "" || first.Kind != BlockTable || !first.Source.Matches(source) {
		t.Fatalf("first row lost structure or source binding: %#v", first)
	}
	if !strings.Contains(first.Text, "A1") || !strings.Contains(first.Text, "Revenue") {
		t.Fatalf("first row text missing cell address or resolved shared string: %q", first.Text)
	}
	var payload map[string]any
	if err := json.Unmarshal(first.Payload.JSON, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["sheet_name"] != "Summary" || payload["row_number"] != float64(1) {
		t.Fatalf("first row payload = %#v", payload)
	}
	second := rows[1]
	if second.Attributes["row_number"] != "2" || !strings.Contains(second.Text, "formula=SUM(B1:B1)") || !strings.Contains(second.Text, "42") {
		t.Fatalf("formula row text lost formula or cached value: %q", second.Text)
	}
	if rows[2].Attributes["sheet_name"] != "Data" || rows[2].Attributes["row_number"] != "1" || !strings.Contains(rows[2].Text, "north region") {
		t.Fatalf("second worksheet row = %#v", rows[2])
	}
	for index, row := range rows {
		if err := row.Validate(); err != nil {
			t.Fatalf("row block %d invalid: %v", index, err)
		}
	}
}

func TestParseXLSXRowsEnforcesPartAndSourceLimits(t *testing.T) {
	value, source := testXLSXValue(t)
	if rows, err := ParseXLSXRows(context.Background(), nil, value, source, XLSXOptions{MaxXMLPartBytes: 16}); err == nil || rows != nil {
		t.Fatalf("oversized XML part returned rows=%v err=%v", rows, err)
	}
	if rows, err := ParseXLSXRows(context.Background(), nil, value, source, XLSXOptions{MaxCells: 1}); err == nil || rows != nil {
		t.Fatalf("cell limit returned rows=%v err=%v", rows, err)
	}
	if rows, err := ParseXLSXRows(context.Background(), nil, value, source, XLSXOptions{MaxRows: 1}); err == nil || rows != nil {
		t.Fatalf("workbook row limit returned rows=%v err=%v", rows, err)
	}
	wrongValue, err := NewValue("text/plain", 1, value.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	wrongSource, err := NewSourceRef("app", "books", "body", []byte("pk-xlsx"), wrongValue)
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := ParseXLSXRows(context.Background(), nil, wrongValue, wrongSource, XLSXOptions{}); err == nil || rows != nil {
		t.Fatalf("wrong MIME returned rows=%v err=%v", rows, err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if rows, err := ParseXLSXRows(cancelled, nil, value, source, XLSXOptions{}); !errors.Is(err, context.Canceled) || rows != nil {
		t.Fatalf("cancelled parse returned rows=%v err=%v", rows, err)
	}
}

func TestParseXLSXRowsRejectsPackagePathTraversal(t *testing.T) {
	parts := testXLSXParts()
	parts["xl/_rels/workbook.xml.rels"] = `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="../../escape.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/><Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/></Relationships>`
	value, err := NewValue(xlsxMIMEType, 1, zipXLSX(t, parts))
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewSourceRef("app", "books", "body", []byte("pk-xlsx-path"), value)
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := ParseXLSXRows(context.Background(), nil, value, source, XLSXOptions{}); err == nil || rows != nil || !strings.Contains(err.Error(), "escapes") {
		t.Fatalf("path traversal returned rows=%v err=%v", rows, err)
	}
}

func TestIsolatedXLSXRowParserRunsWorkerAndHonorsCancellation(t *testing.T) {
	value, source := testXLSXValue(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	parser := IsolatedXLSXRowParser{Executable: executable, Args: []string{"-test.run=^TestXLSXWorkerProcessHelper$"}, Env: []string{"XMYSQL_INTERNAL_XLSX_WORKER_TEST=1"}}
	rows, err := parser.ParseXLSXRows(context.Background(), nil, value, source, XLSXOptions{})
	if err != nil || len(rows) != 3 || rows[0].Attributes["sheet_name"] != "Summary" {
		t.Fatalf("isolated XLSX parse rows=%#v err=%v", rows, err)
	}
	t.Setenv("XMYSQL_TEST_XLSX_WORKER_MODE", "sleep")
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	rows, err = parser.ParseXLSXRows(ctx, nil, value, source, XLSXOptions{})
	if !errors.Is(err, context.DeadlineExceeded) || rows != nil {
		t.Fatalf("cancelled worker returned rows=%v err=%v", rows, err)
	}
}

func TestXLSXWorkerProcessHelper(t *testing.T) {
	if os.Getenv("XMYSQL_INTERNAL_XLSX_WORKER_TEST") != "1" {
		return
	}
	if os.Getenv("XMYSQL_TEST_XLSX_WORKER_MODE") == "sleep" {
		time.Sleep(30 * time.Second)
		os.Exit(4)
	}
	rows, err := runXLSXWorker(os.Stdin)
	if err != nil {
		_, _ = os.Stderr.WriteString(err.Error())
		os.Exit(2)
	}
	if err := json.NewEncoder(os.Stdout).Encode(rows); err != nil {
		os.Exit(3)
	}
	os.Exit(0)
}

func testXLSXValue(t *testing.T) (Value, SourceRef) {
	t.Helper()
	value, err := NewValue(xlsxMIMEType, 4, zipXLSX(t, testXLSXParts()))
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewSourceRef("app", "books", "body", []byte("pk-xlsx"), value)
	if err != nil {
		t.Fatal(err)
	}
	return value, source
}

func testXLSXParts() map[string]string {
	return map[string]string{
		"xl/workbook.xml":            `<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Summary" sheetId="1" r:id="rId1"/><sheet name="Data" sheetId="2" r:id="rId2"/></sheets></workbook>`,
		"xl/_rels/workbook.xml.rels": `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/><Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/></Relationships>`,
		"xl/sharedStrings.xml":       `<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><si><t>Revenue</t></si><si><r><t>north</t></r><r><t xml:space="preserve"> region</t></r></si></sst>`,
		"xl/worksheets/sheet1.xml":   `<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="inlineStr"><is><t>Amount</t></is></c></row><row r="2"><c r="A2" t="s"><v>1</v></c><c r="B2"><v>42</v></c><c r="C2"><f>SUM(B1:B1)</f><v>42</v></c></row></sheetData></worksheet>`,
		"xl/worksheets/sheet2.xml":   `<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>north region</t></is></c></row></sheetData></worksheet>`,
	}
}

func zipXLSX(t *testing.T, parts map[string]string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for name, text := range parts {
		file, err := writer.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte(text)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}
