package content

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

var (
	ErrUnsupportedTextMIME = errors.New("unsupported CONTENT text MIME type")
	ErrInvalidTextEncoding = errors.New("CONTENT text is not valid UTF-8")
)

// MaxInlineTextBytes bounds the amount of text one SQL scalar call may return.
// Larger content values should be read in bounded ranges or parsed into blocks.
const MaxInlineTextBytes uint64 = 4 << 20

// ExtractText decodes explicitly textual CONTENT values without changing the
// stored source bytes. It does not OCR or infer text from binary documents.
func ExtractText(value Value) (string, error) {
	if err := value.Validate(); err != nil {
		return "", err
	}
	if value.ExternalRef != nil {
		return "", ErrExternalContentRequiresStore
	}
	if !isTextMIME(value.MIMEType) {
		return "", fmt.Errorf("%w: %q", ErrUnsupportedTextMIME, value.MIMEType)
	}
	if uint64(len(value.Bytes)) > MaxInlineTextBytes {
		return "", fmt.Errorf("%w: CONTENT has %d bytes; inline text limit is %d bytes", ErrInvalidValue, len(value.Bytes), MaxInlineTextBytes)
	}
	text := value.Bytes
	if len(text) >= 3 && text[0] == 0xef && text[1] == 0xbb && text[2] == 0xbf {
		text = text[3:]
	}
	if !utf8.Valid(text) {
		return "", ErrInvalidTextEncoding
	}
	return string(text), nil
}

// ExtractTextFromStore decodes explicitly textual CONTENT while bounding the
// materialized text size, including when the source is externally segmented.
func ExtractTextFromStore(ctx context.Context, store *SegmentStore, value Value) (string, error) {
	if err := value.Validate(); err != nil {
		return "", err
	}
	if value.Size() > MaxInlineTextBytes {
		return "", fmt.Errorf("%w: CONTENT has %d bytes; inline text limit is %d bytes", ErrInvalidValue, value.Size(), MaxInlineTextBytes)
	}
	if value.ExternalRef == nil {
		return ExtractText(value)
	}
	textBytes, err := value.ReadAllFromStore(ctx, store, MaxInlineTextBytes)
	if err != nil {
		return "", err
	}
	inline, err := NewValue(value.MIMEType, value.Revision, textBytes)
	if err != nil {
		return "", err
	}
	return ExtractText(inline)
}

func isTextMIME(mimeType string) bool {
	mediaType := strings.ToLower(strings.TrimSpace(strings.SplitN(mimeType, ";", 2)[0]))
	return strings.HasPrefix(mediaType, "text/") || mediaType == "application/json" ||
		mediaType == "application/xml" || mediaType == "application/x-ndjson" ||
		mediaType == "application/ld+json" || mediaType == "application/x-www-form-urlencoded"
}

// IsTextMIMEType reports whether CONTENT is explicitly declared as textual.
// It does not infer text from arbitrary binary documents.
func IsTextMIMEType(mimeType string) bool { return isTextMIME(mimeType) }
