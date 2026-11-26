package content

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestIsolatedDOCXBlockParserRunsWorkerAndReturnsBoundBlocks(t *testing.T) {
	value, err := NewValue(docxMIMEType, 2, testDOCX(`<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p><w:r><w:t>isolated paragraph</w:t></w:r></w:p></w:body></w:document>`))
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewSourceRef("app", "documents", "body", []byte("pk-isolated-docx"), value)
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	parser := IsolatedDOCXBlockParser{Executable: executable, Args: []string{"-test.run=^TestDOCXWorkerProcessHelper$"}, Env: []string{"XMYSQL_INTERNAL_DOCX_WORKER_TEST=1"}}
	blocks, err := parser.ParseDOCXBlocks(context.Background(), nil, value, source, DOCXOptions{})
	if err != nil {
		t.Fatalf("isolated ParseDOCXBlocks: %v", err)
	}
	if len(blocks) != 1 || blocks[0].Text != "isolated paragraph" || !blocks[0].Source.Matches(source) {
		t.Fatalf("isolated worker returned unexpected blocks: %#v", blocks)
	}
}

func TestIsolatedDOCXBlockParserKillsWorkerWhenContextExpires(t *testing.T) {
	value, err := NewValue(docxMIMEType, 1, testDOCX(`<w:document/>`))
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewSourceRef("app", "documents", "body", []byte("pk-docx-cancel"), value)
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	parser := IsolatedDOCXBlockParser{Executable: executable, Args: []string{"-test.run=^TestDOCXWorkerProcessHelper$"}, Env: []string{"XMYSQL_INTERNAL_DOCX_WORKER_TEST=1"}}
	t.Setenv("XMYSQL_TEST_DOCX_WORKER_MODE", "sleep")
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	blocks, err := parser.ParseDOCXBlocks(ctx, nil, value, source, DOCXOptions{})
	if !errors.Is(err, context.DeadlineExceeded) || blocks != nil {
		t.Fatalf("expired worker returned blocks=%v err=%v", blocks, err)
	}
}

func TestDOCXWorkerProcessHelper(t *testing.T) {
	if os.Getenv("XMYSQL_INTERNAL_DOCX_WORKER_TEST") != "1" {
		return
	}
	if os.Getenv("XMYSQL_TEST_DOCX_WORKER_MODE") == "sleep" {
		time.Sleep(30 * time.Second)
		os.Exit(4)
	}
	blocks, err := runDOCXWorker(os.Stdin)
	if err != nil {
		_, _ = os.Stderr.WriteString(err.Error())
		os.Exit(2)
	}
	if err := json.NewEncoder(os.Stdout).Encode(blocks); err != nil {
		os.Exit(3)
	}
	os.Exit(0)
}

func TestParseDOCXBlocksPreservesParagraphAndTableStructure(t *testing.T) {
	docx := testDOCX(`<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p><w:r><w:t>first paragraph</w:t></w:r></w:p><w:tbl><w:tr><w:tc><w:p><w:r><w:t>table cell</w:t></w:r></w:p></w:tc></w:tr></w:tbl><w:p><w:r><w:t>last paragraph</w:t></w:r></w:p></w:body></w:document>`)
	value, err := NewValue(docxMIMEType, 3, docx)
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewSourceRef("app", "documents", "body", []byte("pk-docx"), value)
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := ParseDOCXBlocks(context.Background(), nil, value, source, DOCXOptions{})
	if err != nil {
		t.Fatalf("ParseDOCXBlocks: %v", err)
	}
	if len(blocks) != 3 {
		t.Fatalf("got %d blocks, want 3: %#v", len(blocks), blocks)
	}
	wantText := []string{"first paragraph", "table cell", "last paragraph"}
	for i, block := range blocks {
		if block.Text != wantText[i] || block.Ordinal != uint32(i+1) || !block.Source.Matches(source) {
			t.Fatalf("block %d = %#v", i, block)
		}
		if block.Attributes["structure_path"] == "" {
			t.Fatalf("block %d has no structure path: %#v", i, block.Attributes)
		}
		if i == 1 && block.Kind != BlockTable {
			t.Fatalf("table paragraph kind = %q, want %q", block.Kind, BlockTable)
		}
		if i != 1 && block.Kind != BlockText {
			t.Fatalf("body paragraph kind = %q, want %q", block.Kind, BlockText)
		}
		if err := block.Validate(); err != nil {
			t.Fatalf("block %d invalid: %v", i, err)
		}
	}
}

func TestParseDOCXBlocksBoundsXMLAndRejectsMalformedOrForeignInput(t *testing.T) {
	docx := testDOCX(`<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p><w:r><w:t>bounded text</w:t></w:r></w:p></w:body></w:document>`)
	value, err := NewValue(docxMIMEType, 1, docx)
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewSourceRef("app", "documents", "body", []byte("pk-docx"), value)
	if err != nil {
		t.Fatal(err)
	}
	if blocks, err := ParseDOCXBlocks(context.Background(), nil, value, source, DOCXOptions{MaxDocumentXMLBytes: 16}); err == nil || blocks != nil {
		t.Fatalf("oversized XML returned blocks=%v err=%v", blocks, err)
	}
	if blocks, err := ParseDOCXBlocks(context.Background(), nil, value, source, DOCXOptions{MaxParagraphs: 0}); err != nil || len(blocks) != 1 {
		t.Fatalf("zero-valued options should use defaults: blocks=%v err=%v", blocks, err)
	}
	badValue, err := NewValue("text/plain", 1, docx)
	if err != nil {
		t.Fatal(err)
	}
	badSource, err := NewSourceRef("app", "documents", "body", []byte("pk-docx"), badValue)
	if err != nil {
		t.Fatal(err)
	}
	if blocks, err := ParseDOCXBlocks(context.Background(), nil, badValue, badSource, DOCXOptions{}); err == nil || blocks != nil {
		t.Fatalf("foreign MIME returned blocks=%v err=%v", blocks, err)
	}
	malformed, err := NewValue(docxMIMEType, 1, testDOCX(`<w:document>`))
	if err != nil {
		t.Fatal(err)
	}
	malformedSource, err := NewSourceRef("app", "documents", "body", []byte("pk-docx"), malformed)
	if err != nil {
		t.Fatal(err)
	}
	if blocks, err := ParseDOCXBlocks(context.Background(), nil, malformed, malformedSource, DOCXOptions{}); err == nil || blocks != nil {
		t.Fatalf("malformed document returned blocks=%v err=%v", blocks, err)
	}
	wrongRoot, err := NewValue(docxMIMEType, 1, testDOCX(`<document><body><p>not WordprocessingML</p></body></document>`))
	if err != nil {
		t.Fatal(err)
	}
	wrongRootSource, err := NewSourceRef("app", "documents", "body", []byte("pk-docx"), wrongRoot)
	if err != nil {
		t.Fatal(err)
	}
	if blocks, err := ParseDOCXBlocks(context.Background(), nil, wrongRoot, wrongRootSource, DOCXOptions{}); err == nil || blocks != nil {
		t.Fatalf("wrong XML root returned blocks=%v err=%v", blocks, err)
	}
}

func testDOCX(documentXML string) []byte {
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

func TestParseDOCXBlocksRejectsDuplicateDocumentParts(t *testing.T) {
	var out bytes.Buffer
	writer := zip.NewWriter(&out)
	for i := 0; i < 2; i++ {
		file, err := writer.Create("word/document.xml")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = file.Write([]byte(fmt.Sprintf("<document>%d</document>", i)))
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	value, err := NewValue(docxMIMEType, 1, out.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewSourceRef("app", "documents", "body", []byte("pk-docx"), value)
	if err != nil {
		t.Fatal(err)
	}
	if blocks, err := ParseDOCXBlocks(context.Background(), nil, value, source, DOCXOptions{}); err == nil || blocks != nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate part returned blocks=%v err=%v", blocks, err)
	}
}
