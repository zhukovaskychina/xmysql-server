package engine

import (
	"archive/zip"
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
)

func TestContentParseDOCXTableFunctionReturnsSourceBoundParagraphs(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	engine.QueryExecutor.SetContentDOCXBlockParser(content.InProcessDOCXBlockParser{})
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table docs (id int primary key, body CONTENT)")
	docx := testSQLDOCX(`<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p><w:r><w:t>paragraph alpha</w:t></w:r></w:p><w:tbl><w:tr><w:tc><w:p><w:r><w:t>cell beta</w:t></w:r></w:p></w:tc></w:tr></w:tbl></w:body></w:document>`)
	insert := "insert into docs values (1, CONTENT_FROM_BYTES(X'" + hex.EncodeToString(docx) + "','application/vnd.openxmlformats-officedocument.wordprocessingml.document'))"
	mustExecSQL(t, engine, "app", insert)
	rows := mustQuerySQL(t, engine, "app", "select d.id,b.ordinal,b.text,b.structure_path,CONTENT_BLOCK_SOURCE_TABLE(b.block) from docs d, table(CONTENT_PARSE_DOCX(d.body)) b order by b.ordinal")
	if len(rows) != 2 {
		t.Fatalf("got %d DOCX paragraph rows, want 2: %#v", len(rows), rows)
	}
	if rows[0][2] != "paragraph alpha" || rows[1][2] != "cell beta" || rows[0][3] == "" || rows[1][3] == "" || rows[0][4] != "docs" || rows[1][4] != "docs" {
		t.Fatalf("DOCX SQL rows lost text, structure, or provenance: %#v", rows)
	}
}

func testSQLDOCX(documentXML string) []byte {
	var out bytes.Buffer
	writer := zip.NewWriter(&out)
	file, err := writer.Create("word/document.xml")
	if err != nil {
		panic(err)
	}
	if _, err := file.Write([]byte(documentXML)); err != nil {
		panic(err)
	}
	if err := writer.Close(); err != nil {
		panic(err)
	}
	return out.Bytes()
}
