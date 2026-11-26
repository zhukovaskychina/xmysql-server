package content

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"mime"
	"strings"
)

const MaxTypedPayloadBytes = 32 << 20

// TypedPayload carries a schema-versioned JSON representation alongside a
// source block. The schema ID defines domain-neutral value types and shape;
// the digest binds the exact serialized payload bytes.
type TypedPayload struct {
	SchemaID      string          `json:"schema_id"`
	SchemaVersion uint32          `json:"schema_version"`
	MediaType     string          `json:"media_type"`
	JSON          json.RawMessage `json:"json"`
	SHA256        string          `json:"sha256"`
}

func NewTypedPayload(schemaID string, schemaVersion uint32, mediaType string, encodedJSON []byte) (TypedPayload, error) {
	payload := TypedPayload{
		SchemaID: strings.TrimSpace(schemaID), SchemaVersion: schemaVersion,
		MediaType: strings.TrimSpace(strings.ToLower(mediaType)),
		JSON:      append(json.RawMessage(nil), encodedJSON...),
	}
	payload.SHA256 = payload.DigestHex()
	if err := payload.Validate(); err != nil {
		return TypedPayload{}, err
	}
	return payload, nil
}

func (p TypedPayload) Validate() error {
	if p.SchemaID == "" || len(p.SchemaID) > 256 || p.SchemaVersion == 0 || p.MediaType == "" || strings.ContainsAny(p.MediaType, "\r\n") {
		return fmt.Errorf("%w: typed payload schema and media type are required", ErrInvalidBlock)
	}
	mediaType, _, err := mime.ParseMediaType(p.MediaType)
	if err != nil || !(strings.HasSuffix(strings.ToLower(mediaType), "/json") || strings.HasSuffix(strings.ToLower(mediaType), "+json")) {
		return fmt.Errorf("%w: typed payload media type must identify JSON", ErrInvalidBlock)
	}
	if len(p.JSON) == 0 || len(p.JSON) > MaxTypedPayloadBytes || !json.Valid(p.JSON) {
		return fmt.Errorf("%w: typed payload JSON must contain 1 to %d valid bytes", ErrInvalidBlock, MaxTypedPayloadBytes)
	}
	if !strings.EqualFold(p.SHA256, p.DigestHex()) {
		return fmt.Errorf("%w: typed payload digest mismatch", ErrInvalidBlock)
	}
	if p.SchemaID == StructuredTablePayloadSchemaID {
		if p.SchemaVersion != StructuredTablePayloadSchemaVersion {
			return fmt.Errorf("%w: unsupported structured table schema version %d", ErrInvalidBlock, p.SchemaVersion)
		}
		if err := validateStructuredTableJSON(p.JSON); err != nil {
			return err
		}
	}
	if p.SchemaID == ImageInfoPayloadSchemaID {
		if p.SchemaVersion != ImageInfoPayloadSchemaVersion || !strings.EqualFold(strings.TrimSpace(p.MediaType), ImageInfoPayloadMediaType) {
			return fmt.Errorf("%w: unsupported image info payload schema or media type", ErrInvalidBlock)
		}
		if err := validateImageInfoJSON(p.JSON); err != nil {
			return err
		}
	}
	if p.SchemaID == AudioInfoPayloadSchemaID {
		if p.SchemaVersion != AudioInfoPayloadSchemaVersion || !strings.EqualFold(strings.TrimSpace(p.MediaType), AudioInfoPayloadMediaType) {
			return fmt.Errorf("%w: unsupported audio info payload schema or media type", ErrInvalidBlock)
		}
		if err := validateAudioInfoJSON(p.JSON); err != nil {
			return err
		}
	}
	return nil
}

func (p TypedPayload) DigestHex() string {
	digest := sha256.Sum256(p.JSON)
	return hex.EncodeToString(digest[:])
}

func (p TypedPayload) Clone() TypedPayload {
	p.JSON = append(json.RawMessage(nil), p.JSON...)
	return p
}
