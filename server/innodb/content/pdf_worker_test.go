package content

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"
)

func TestIsolatedPDFPageParserRunsWorkerAndReturnsSourceBoundBlocks(t *testing.T) {
	pdf := testPDF([]string{"isolated worker text"})
	value, err := NewValue("application/pdf", 4, pdf)
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewSourceRef("app", "docs", "body", []byte("pk-worker"), value)
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	parser := IsolatedPDFPageParser{Executable: executable, Args: []string{"-test.run=^TestPDFWorkerProcessHelper$"}, Env: []string{"XMYSQL_INTERNAL_PDF_WORKER_TEST=1"}}
	blocks, err := parser.ParsePDFPages(context.Background(), nil, value, source, PDFOptions{})
	if err != nil {
		t.Fatalf("isolated ParsePDFPages: %v", err)
	}
	if len(blocks) != 1 || blocks[0].Text != "isolated worker text" || !blocks[0].Source.Matches(source) {
		t.Fatalf("isolated worker returned unexpected blocks: %#v", blocks)
	}
}

func TestIsolatedPDFPageParserKillsWorkerWhenContextExpires(t *testing.T) {
	pdf := testPDF([]string{"cancel worker"})
	value, err := NewValue("application/pdf", 1, pdf)
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewSourceRef("app", "docs", "body", []byte("pk-cancel"), value)
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	parser := IsolatedPDFPageParser{Executable: executable, Args: []string{"-test.run=^TestPDFWorkerProcessHelper$"}, Env: []string{"XMYSQL_INTERNAL_PDF_WORKER_TEST=1"}}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	t.Setenv("XMYSQL_TEST_PDF_WORKER_MODE", "sleep")
	blocks, err := parser.ParsePDFPages(ctx, nil, value, source, PDFOptions{})
	if !errors.Is(err, context.DeadlineExceeded) || blocks != nil {
		t.Fatalf("expired worker returned blocks=%v err=%v", blocks, err)
	}
}

func TestPDFWorkerProcessHelper(t *testing.T) {
	if os.Getenv("XMYSQL_INTERNAL_PDF_WORKER_TEST") != "1" {
		return
	}
	if os.Getenv("XMYSQL_TEST_PDF_WORKER_MODE") == "sleep" {
		time.Sleep(30 * time.Second)
		os.Exit(4)
	}
	blocks, err := runPDFWorker(os.Stdin, os.Stdout)
	if err != nil {
		_, _ = os.Stderr.WriteString(err.Error())
		os.Exit(2)
	}
	if err := json.NewEncoder(os.Stdout).Encode(blocks); err != nil {
		os.Exit(3)
	}
	os.Exit(0)
}
