package content

import (
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

// BlockRetrievalText returns a deterministic text projection when a block has
// a text representation or a supported typed JSON payload. It refuses stale,
// unverified, invalid UTF-8 or over-budget values rather than silently
// truncating evidence used for lexical ranking or model context.
func BlockRetrievalText(block Block) (string, bool, error) {
	if err := block.Validate(); err != nil {
		return "", false, err
	}
	if block.State != BlockStateReady {
		return "", false, nil
	}
	if block.Text != "" {
		if !utf8.ValidString(block.Text) {
			return "", false, fmt.Errorf("content block text is not valid UTF-8")
		}
		if uint64(len(block.Text)) > MaxInlineTextBytes {
			return "", false, fmt.Errorf("content block retrieval text exceeds %d bytes", MaxInlineTextBytes)
		}
		return block.Text, true, nil
	}
	if block.Payload == nil {
		return "", false, nil
	}
	if block.Payload.SchemaID == StructuredTablePayloadSchemaID {
		text, err := StructuredTableRetrievalText(*block.Payload)
		return text, err == nil, err
	}
	if block.Payload.SchemaID == ImageInfoPayloadSchemaID || block.Payload.SchemaID == AudioInfoPayloadSchemaID {
		return "", false, nil
	}
	if err := block.Payload.Validate(); err != nil {
		return "", false, err
	}
	if !json.Valid(block.Payload.JSON) || uint64(len(block.Payload.JSON)) > MaxInlineTextBytes {
		return "", false, fmt.Errorf("content block JSON retrieval text is invalid or exceeds %d bytes", MaxInlineTextBytes)
	}
	return string(block.Payload.JSON), true, nil
}
