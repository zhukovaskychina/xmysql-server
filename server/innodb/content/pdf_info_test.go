package content

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestParsePDFPagesPreservesPageLocatorsAndMarksOCRRequiredPages(t *testing.T) {
	pdf := testPDF([]string{"first page text", ""})
	value, err := NewValue("application/pdf", 1, pdf)
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewSourceRef("app", "documents", "body", []byte("pk-1"), value)
	if err != nil {
		t.Fatal(err)
	}

	blocks, err := ParsePDFPages(context.Background(), nil, value, source, PDFOptions{})
	if err != nil {
		t.Fatalf("ParsePDFPages: %v", err)
	}
	if len(blocks) != 2 {
		t.Fatalf("got %d page blocks, want 2", len(blocks))
	}
	if blocks[0].Locator.Page == nil || *blocks[0].Locator.Page != 1 || blocks[0].Text != "first page text" {
		t.Fatalf("first page block = %#v", blocks[0])
	}
	if blocks[1].Locator.Page == nil || *blocks[1].Locator.Page != 2 || blocks[1].Text != "" || blocks[1].Attributes["extraction_status"] != "OCR_REQUIRED" {
		t.Fatalf("empty text page must remain visible as OCR_REQUIRED: %#v", blocks[1])
	}
	for _, block := range blocks {
		if !block.Source.Matches(source) {
			t.Fatalf("page block is not bound to its source revision: %#v", block.Source)
		}
		if err := block.Validate(); err != nil {
			t.Fatalf("invalid page block: %v", err)
		}
	}
}

func TestParsePDFPagesRejectsMIMEAndPageLimitBeforePublishingBlocks(t *testing.T) {
	pdf := testPDF([]string{"one", "two"})
	value, err := NewValue("text/plain", 1, pdf)
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewSourceRef("app", "documents", "body", []byte("pk-1"), value)
	if err != nil {
		t.Fatal(err)
	}
	if blocks, err := ParsePDFPages(context.Background(), nil, value, source, PDFOptions{}); err == nil || blocks != nil {
		t.Fatalf("MIME mismatch returned blocks=%v err=%v", blocks, err)
	}

	pdfValue, err := NewValue("application/pdf", 1, pdf)
	if err != nil {
		t.Fatal(err)
	}
	pdfSource, err := NewSourceRef("app", "documents", "body", []byte("pk-1"), pdfValue)
	if err != nil {
		t.Fatal(err)
	}
	if blocks, err := ParsePDFPages(context.Background(), nil, pdfValue, pdfSource, PDFOptions{MaxPages: 1}); err == nil || blocks != nil {
		t.Fatalf("page limit returned blocks=%v err=%v", blocks, err)
	}
}

func TestParsePDFPagesReadsExternalObjectsAndHonorsCancellation(t *testing.T) {
	pdf := testPDF([]string{"large-object page"})
	store, err := NewSegmentStore(t.TempDir(), SegmentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := store.Put(context.Background(), "application/pdf", 1, bytes.NewReader(pdf))
	if err != nil {
		t.Fatal(err)
	}
	value, err := NewSegmentedValue(ref)
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewSourceRef("app", "documents", "body", []byte("pk-1"), value)
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := ParsePDFPages(context.Background(), store, value, source, PDFOptions{})
	if err != nil {
		t.Fatalf("ParsePDFPages external object: %v", err)
	}
	if len(blocks) != 1 || blocks[0].Text != "large-object page" {
		t.Fatalf("external PDF blocks = %#v", blocks)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if blocks, err := ParsePDFPages(cancelled, store, value, source, PDFOptions{}); !errors.Is(err, context.Canceled) || blocks != nil {
		t.Fatalf("cancelled extraction returned blocks=%v err=%v", blocks, err)
	}
}

func TestParsePDFPagesRejectsOversizedConfiguredLimits(t *testing.T) {
	value, err := NewValue("application/pdf", 1, testPDF([]string{"bounded"}))
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewSourceRef("app", "documents", "body", []byte("pk-1"), value)
	if err != nil {
		t.Fatal(err)
	}
	if blocks, err := ParsePDFPages(context.Background(), nil, value, source, PDFOptions{MaxMemoryBytes: defaultPDFMemory + 1}); err == nil || blocks != nil {
		t.Fatalf("oversized configured limits returned blocks=%v err=%v", blocks, err)
	}
}

func testPDF(pages []string) []byte {
	objects := []string{
		"<< /Type /Catalog /Pages 2 0 R >>",
		fmt.Sprintf("<< /Type /Pages /Kids [%s] /Count %d >>", pageRefs(len(pages)), len(pages)),
	}
	for i, text := range pages {
		pageObjectID := 3 + i*3
		contentObjectID := pageObjectID + 2
		objects = append(objects,
			fmt.Sprintf("<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 %d 0 R >> >> /Contents %d 0 R >>", 3+len(pages)*3, contentObjectID),
			"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
			fmt.Sprintf("<< /Length %d >>\nstream\nBT /F1 12 Tf 72 720 Td (%s) Tj ET\nendstream", len(fmt.Sprintf("BT /F1 12 Tf 72 720 Td (%s) Tj ET", escapePDFText(text))), escapePDFText(text)),
		)
	}
	objects = append(objects, "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
	var out bytes.Buffer
	out.WriteString("%PDF-1.4\n")
	offsets := make([]int, len(objects)+1)
	for i, object := range objects {
		offsets[i+1] = out.Len()
		fmt.Fprintf(&out, "%d 0 obj\n%s\nendobj\n", i+1, object)
	}
	xrefOffset := out.Len()
	fmt.Fprintf(&out, "xref\n0 %d\n0000000000 65535 f \n", len(objects)+1)
	for _, offset := range offsets[1:] {
		fmt.Fprintf(&out, "%010d 00000 n \n", offset)
	}
	fmt.Fprintf(&out, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(objects)+1, xrefOffset)
	return out.Bytes()
}

func pageRefs(pageCount int) string {
	refs := make([]string, pageCount)
	for i := range refs {
		refs[i] = fmt.Sprintf("%d 0 R", 3+i*3)
	}
	return strings.Join(refs, " ")
}

func escapePDFText(value string) string {
	return strings.NewReplacer("\\", "\\\\", "(", "\\(", ")", "\\)").Replace(value)
}
