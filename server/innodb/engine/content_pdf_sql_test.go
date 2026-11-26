package engine

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestContentParsePDFTableFunctionReturnsPageTextAndOCRStatus(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	engine.QueryExecutor.SetContentPDFPageParser(content.InProcessPDFPageParser{})
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table pdf_docs (id int primary key, body CONTENT)")
	insert := fmt.Sprintf("insert into pdf_docs values (1, CONTENT_FROM_BYTES(X'%s','application/pdf'))", hex.EncodeToString(testSQLPDF([]string{"page one words", ""})))
	mustExecSQL(t, engine, "app", insert)

	rows := mustQuerySQL(t, engine, "app", "select p.page,p.text,p.extraction_status,CONTENT_BLOCK_PAGE(p.block),CONTENT_BLOCK_SOURCE_TABLE(p.block) from pdf_docs d, table(CONTENT_PARSE_PDF(d.body)) p order by p.page")
	if len(rows) != 2 {
		t.Fatalf("got %d PDF page rows, want 2", len(rows))
	}
	if got := fmt.Sprint(rows[0][0]); got != "1" {
		t.Fatalf("first page number = %s, want 1", got)
	}
	if got := fmt.Sprint(rows[0][1]); got != "page one words" {
		t.Fatalf("first page text = %q", got)
	}
	if got := fmt.Sprint(rows[0][2]); got != "TEXT_EXTRACTED" {
		t.Fatalf("first page status = %q", got)
	}
	if got := fmt.Sprint(rows[0][3]); got != "1" {
		t.Fatalf("first page block locator = %q, want 1", got)
	}
	if got := fmt.Sprint(rows[0][4]); got != "pdf_docs" {
		t.Fatalf("first page source table = %q, want pdf_docs", got)
	}
	if got := fmt.Sprint(rows[1][2]); got != "OCR_REQUIRED" {
		t.Fatalf("blank page status = %q, want OCR_REQUIRED", got)
	}

	if _, err := sqlparser.Parse("select p.page from pdf_docs d, table(CONTENT_PARSE_PDF(d.body)) p"); err != nil {
		t.Fatalf("parse supported SQL call: %v", err)
	}
}

func testSQLPDF(pages []string) []byte {
	objects := []string{"<< /Type /Catalog /Pages 2 0 R >>", fmt.Sprintf("<< /Type /Pages /Kids [%s] /Count %d >>", sqlPDFPageRefs(len(pages)), len(pages))}
	fontID := 3 + len(pages)*3
	for i, text := range pages {
		pageID := 3 + i*3
		contentID := pageID + 2
		stream := fmt.Sprintf("BT /F1 12 Tf 72 720 Td (%s) Tj ET", strings.NewReplacer("\\", "\\\\", "(", "\\(", ")", "\\)").Replace(text))
		objects = append(objects,
			fmt.Sprintf("<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 %d 0 R >> >> /Contents %d 0 R >>", fontID, contentID),
			"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
			fmt.Sprintf("<< /Length %d >>\nstream\n%s\nendstream", len(stream), stream),
		)
	}
	objects = append(objects, "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
	var pdf bytes.Buffer
	pdf.WriteString("%PDF-1.4\n")
	offsets := make([]int, len(objects)+1)
	for index, object := range objects {
		offsets[index+1] = pdf.Len()
		fmt.Fprintf(&pdf, "%d 0 obj\n%s\nendobj\n", index+1, object)
	}
	xref := pdf.Len()
	fmt.Fprintf(&pdf, "xref\n0 %d\n0000000000 65535 f \n", len(objects)+1)
	for _, offset := range offsets[1:] {
		fmt.Fprintf(&pdf, "%010d 00000 n \n", offset)
	}
	fmt.Fprintf(&pdf, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(objects)+1, xref)
	return pdf.Bytes()
}

func sqlPDFPageRefs(count int) string {
	refs := make([]string, count)
	for i := range refs {
		refs[i] = fmt.Sprintf("%d 0 R", 3+i*3)
	}
	return strings.Join(refs, " ")
}
