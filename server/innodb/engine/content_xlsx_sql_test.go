package engine

import (
	"archive/zip"
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
)

func TestContentParseXLSXTableFunctionReturnsStructuredRows(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	engine.QueryExecutor.SetContentXLSXRowParser(content.InProcessXLSXRowParser{})
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table books (id int primary key, body CONTENT)")
	xlsx := testSQLXLSX()
	insert := "insert into books values (1, CONTENT_FROM_BYTES(X'" + hex.EncodeToString(xlsx) + "','application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))"
	mustExecSQL(t, engine, "app", insert)
	rows := mustQuerySQL(t, engine, "app", "select b.sheet_name,b.row_number,b.text,b.structure_path,CONTENT_BLOCK_SOURCE_TABLE(b.block) from books d, table(CONTENT_PARSE_XLSX(d.body)) b order by b.ordinal")
	if len(rows) != 2 {
		t.Fatalf("got %d worksheet row blocks, want 2: %#v", len(rows), rows)
	}
	if rows[0][0] != "Summary" || fmt.Sprint(rows[0][1]) != "1" || !bytes.Contains([]byte(rows[0][2].(string)), []byte("A1")) || rows[0][3] == "" || rows[0][4] != "books" {
		t.Fatalf("first XLSX row lost structure or source binding: %#v", rows[0])
	}
	if rows[1][0] != "Summary" || fmt.Sprint(rows[1][1]) != "2" || !bytes.Contains([]byte(rows[1][2].(string)), []byte("formula=SUM(A1:A1)")) {
		t.Fatalf("second XLSX row lost formula evidence: %#v", rows[1])
	}
}

func testSQLXLSX() []byte {
	parts := map[string]string{
		"xl/workbook.xml":            `<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Summary" sheetId="1" r:id="rId1"/></sheets></workbook>`,
		"xl/_rels/workbook.xml.rels": `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>`,
		"xl/worksheets/sheet1.xml":   `<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>Title</t></is></c></row><row r="2"><c r="A2"><v>42</v></c><c r="B2"><f>SUM(A1:A1)</f><v>42</v></c></row></sheetData></worksheet>`,
	}
	var out bytes.Buffer
	writer := zip.NewWriter(&out)
	for name, text := range parts {
		file, err := writer.Create(name)
		if err != nil {
			panic(err)
		}
		if _, err := file.Write([]byte(text)); err != nil {
			panic(err)
		}
	}
	if err := writer.Close(); err != nil {
		panic(err)
	}
	return out.Bytes()
}
