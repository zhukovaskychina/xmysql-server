package content

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

var ErrInvalidBlockEncoding = errors.New("invalid encoded derived content block")

const (
	blockEncodingVersion       = 2
	legacyBlockEncodingVersion = 1
	blockMaxEncodedSize        = 64 << 20
)

var blockEncodingMagic = [4]byte{'X', 'M', 'B', 'L'}

const BlockMIMEType = "application/vnd.xmysql.content-block"

// MarshalBinary serializes the immutable derivation record for storage in an
// ordinary database BLOB/CONTENT value. JSON supplies an extensible payload;
// the fixed header versions it and the checksum detects partial or corrupt
// publication. encoding/json sorts map keys, so identical blocks have stable
// bytes regardless of Go map iteration order.
func (b Block) MarshalBinary() ([]byte, error) {
	if err := b.Validate(); err != nil {
		return nil, err
	}
	if b.ID == "" || b.ID != b.Identity() {
		return nil, fmt.Errorf("%w: block ID does not match its source and derivation identity", ErrInvalidBlockEncoding)
	}
	encodedBlock := b
	if b.IdentityVersion <= 1 {
		// Identity version 1 is implicit in the original wire format. Keep it
		// implicit when re-encoding a legacy block so older readers do not see
		// a newly introduced JSON field they reject as unknown.
		encodedBlock.IdentityVersion = 0
	}
	payload, err := json.Marshal(encodedBlock)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidBlockEncoding, err)
	}
	if len(payload) > blockMaxEncodedSize {
		return nil, fmt.Errorf("%w: payload exceeds %d bytes", ErrInvalidBlockEncoding, blockMaxEncodedSize)
	}
	encoded := make([]byte, 9+len(payload)+sha256.Size)
	copy(encoded[:4], blockEncodingMagic[:])
	encoded[4] = blockEncodingVersion
	if b.IdentityVersion <= 1 {
		encoded[4] = legacyBlockEncodingVersion
	}
	binary.BigEndian.PutUint32(encoded[5:9], uint32(len(payload)))
	copy(encoded[9:9+len(payload)], payload)
	digest := sha256.Sum256(encoded[:9+len(payload)])
	copy(encoded[9+len(payload):], digest[:])
	return encoded, nil
}

// IsEncodedBlock recognizes the versioned derived-block envelope.
func IsEncodedBlock(encoded []byte) bool {
	return len(encoded) >= len(blockEncodingMagic) && bytes.Equal(encoded[:len(blockEncodingMagic)], blockEncodingMagic[:])
}

// BlockValue wraps a derived block in the database's ordinary CONTENT value.
// The outer CONTENT revision belongs to the stored block row; the immutable
// source revision remains inside the block's SourceRef.
func BlockValue(block Block) (Value, error) {
	encoded, err := block.MarshalBinary()
	if err != nil {
		return Value{}, err
	}
	return NewValue(BlockMIMEType, 1, encoded)
}

// BlockFromValue restores a derived block from a CONTENT value after checking
// the wrapper's MIME and the block's embedded source identity.
func BlockFromValue(value Value) (Block, error) {
	if err := value.Validate(); err != nil {
		return Block{}, err
	}
	if value.MIMEType != BlockMIMEType {
		return Block{}, fmt.Errorf("%w: MIME type %q is not a derived content block", ErrInvalidBlockEncoding, value.MIMEType)
	}
	var block Block
	if err := block.UnmarshalBinary(value.Bytes); err != nil {
		return Block{}, err
	}
	return block, nil
}

// BlockFromJSON builds a typed derived block from a client-friendly JSON
// document. The source primary key and SHA-256 are lowercase or uppercase hex;
// ID is optional and, when supplied, must match the computed derivation ID.
func BlockFromJSON(input []byte) (Block, error) {
	if len(input) == 0 || len(input) > blockMaxEncodedSize {
		return Block{}, fmt.Errorf("%w: JSON input size is invalid", ErrInvalidBlockEncoding)
	}
	var wire struct {
		ID               string            `json:"id,omitempty"`
		Schema           string            `json:"schema"`
		Table            string            `json:"table"`
		Column           string            `json:"column"`
		PrimaryKeyHex    string            `json:"primary_key_hex"`
		SourceRevision   uint64            `json:"source_revision"`
		SourceSHA256     string            `json:"source_sha256"`
		Ordinal          uint32            `json:"ordinal"`
		Kind             BlockKind         `json:"kind"`
		Text             string            `json:"text,omitempty"`
		Payload          *TypedPayload     `json:"payload,omitempty"`
		Locator          Locator           `json:"locator"`
		ParentID         string            `json:"parent_id,omitempty"`
		Relation         string            `json:"relation,omitempty"`
		Attributes       map[string]string `json:"attributes,omitempty"`
		ProcessorID      string            `json:"processor_id"`
		ProcessorVersion string            `json:"processor_version"`
		ModelID          string            `json:"model_id,omitempty"`
		ModelVersion     string            `json:"model_version,omitempty"`
	}
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return Block{}, fmt.Errorf("%w: %v", ErrInvalidBlockEncoding, err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return Block{}, fmt.Errorf("%w: trailing JSON payload", ErrInvalidBlockEncoding)
	}
	primaryKey, err := hex.DecodeString(wire.PrimaryKeyHex)
	if err != nil {
		return Block{}, fmt.Errorf("%w: primary_key_hex must be hexadecimal", ErrInvalidBlockEncoding)
	}
	digestBytes, err := hex.DecodeString(wire.SourceSHA256)
	if err != nil || len(digestBytes) != sha256.Size {
		return Block{}, fmt.Errorf("%w: source_sha256 must contain exactly %d hexadecimal bytes", ErrInvalidBlockEncoding, sha256.Size)
	}
	var digest [sha256.Size]byte
	copy(digest[:], digestBytes)
	source := SourceRef{
		Schema: wire.Schema, Table: wire.Table, Column: wire.Column,
		PrimaryKey: primaryKey, Revision: wire.SourceRevision, Digest: digest,
	}
	block, err := NewBlock(source, wire.Ordinal, wire.Kind, wire.Text, wire.Locator,
		wire.ProcessorID, wire.ProcessorVersion, wire.ModelID, wire.ModelVersion)
	if err != nil {
		return Block{}, err
	}
	// The JSON form contains caller-asserted source fields. A matching digest
	// shape and block identity prove integrity of the claim, not that the claim
	// refers to the current database row. Keep it unusable as verified evidence
	// until a database-bound constructor publishes a READY block.
	block.State = BlockStateUnverified
	block.Attributes = wire.Attributes
	block.Payload = wire.Payload
	if block.Payload != nil && block.Payload.SHA256 == "" {
		block.Payload.SHA256 = block.Payload.DigestHex()
	}
	block.ParentID = wire.ParentID
	block.Relation = wire.Relation
	if err := block.Validate(); err != nil {
		return Block{}, err
	}
	block.ID = block.Identity()
	if wire.ID != "" && wire.ID != block.ID {
		return Block{}, fmt.Errorf("%w: supplied block ID does not match source and derivation identity", ErrInvalidBlockEncoding)
	}
	return block, nil
}

// BlockFromSourceJSON creates a block using source identity read from the
// current database row by the SQL executor. Source fields are deliberately
// absent from this JSON format so a caller cannot override the row binding.
func BlockFromSourceJSON(input []byte, source SourceRef) (Block, error) {
	if len(input) == 0 || len(input) > blockMaxEncodedSize {
		return Block{}, fmt.Errorf("%w: JSON input size is invalid", ErrInvalidBlockEncoding)
	}
	var wire struct {
		Ordinal          uint32            `json:"ordinal"`
		Kind             BlockKind         `json:"kind"`
		Text             string            `json:"text,omitempty"`
		Payload          *TypedPayload     `json:"payload,omitempty"`
		Locator          Locator           `json:"locator"`
		ParentID         string            `json:"parent_id,omitempty"`
		Relation         string            `json:"relation,omitempty"`
		Attributes       map[string]string `json:"attributes,omitempty"`
		ProcessorID      string            `json:"processor_id"`
		ProcessorVersion string            `json:"processor_version"`
		ModelID          string            `json:"model_id,omitempty"`
		ModelVersion     string            `json:"model_version,omitempty"`
	}
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return Block{}, fmt.Errorf("%w: %v", ErrInvalidBlockEncoding, err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return Block{}, fmt.Errorf("%w: trailing JSON payload", ErrInvalidBlockEncoding)
	}
	block, err := NewBlock(source, wire.Ordinal, wire.Kind, wire.Text, wire.Locator,
		wire.ProcessorID, wire.ProcessorVersion, wire.ModelID, wire.ModelVersion)
	if err != nil {
		return Block{}, err
	}
	block.Attributes = wire.Attributes
	block.Payload = wire.Payload
	if block.Payload != nil && block.Payload.SHA256 == "" {
		block.Payload.SHA256 = block.Payload.DigestHex()
	}
	block.ParentID = wire.ParentID
	block.Relation = wire.Relation
	if err := block.Validate(); err != nil {
		return Block{}, err
	}
	block.ID = block.Identity()
	return block, nil
}

// UnmarshalBinary rejects unsupported versions, oversized/truncated values,
// unknown fields, checksum failures and IDs that do not match the decoded
// source revision and processor/model identity.
func (b *Block) UnmarshalBinary(encoded []byte) error {
	if b == nil || len(encoded) < 9+sha256.Size || !IsEncodedBlock(encoded) ||
		(encoded[4] != legacyBlockEncodingVersion && encoded[4] != blockEncodingVersion) {
		return ErrInvalidBlockEncoding
	}
	encodingVersion := encoded[4]
	payloadLength := uint64(binary.BigEndian.Uint32(encoded[5:9]))
	if payloadLength > blockMaxEncodedSize || payloadLength != uint64(len(encoded)-(9+sha256.Size)) {
		return ErrInvalidBlockEncoding
	}
	contentEnd := 9 + int(payloadLength)
	digest := sha256.Sum256(encoded[:contentEnd])
	if !bytes.Equal(digest[:], encoded[contentEnd:]) {
		return fmt.Errorf("%w: checksum mismatch", ErrInvalidBlockEncoding)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded[9:contentEnd]))
	decoder.DisallowUnknownFields()
	var decoded Block
	if err := decoder.Decode(&decoded); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidBlockEncoding, err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("%w: trailing JSON payload", ErrInvalidBlockEncoding)
	}
	if encodingVersion == legacyBlockEncodingVersion {
		if decoded.IdentityVersion != 0 && decoded.IdentityVersion != 1 {
			return fmt.Errorf("%w: legacy envelope carries a non-legacy identity", ErrInvalidBlockEncoding)
		}
		decoded.IdentityVersion = 1
	} else if decoded.IdentityVersion != 2 {
		return fmt.Errorf("%w: version 2 envelope requires identity version 2", ErrInvalidBlockEncoding)
	}
	if decoded.State == "" {
		// Older encodings did not distinguish database-bound sources from
		// caller-asserted ones. Fail closed when upgrading those values.
		decoded.State = BlockStateUnverified
	}
	if err := decoded.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidBlockEncoding, err)
	}
	if decoded.ID == "" || decoded.ID != decoded.Identity() {
		return fmt.Errorf("%w: block ID does not match its source and derivation identity", ErrInvalidBlockEncoding)
	}
	decoded.Source.PrimaryKey = append([]byte(nil), decoded.Source.PrimaryKey...)
	decoded.Locator = cloneContentLocator(decoded.Locator)
	if decoded.Attributes != nil {
		attributes := make(map[string]string, len(decoded.Attributes))
		for key, value := range decoded.Attributes {
			attributes[key] = value
		}
		decoded.Attributes = attributes
	}
	if decoded.Payload != nil {
		payload := decoded.Payload.Clone()
		decoded.Payload = &payload
	}
	*b = decoded
	return nil
}

func cloneContentLocator(locator Locator) Locator {
	clone := locator
	if locator.ByteStart != nil {
		value := *locator.ByteStart
		clone.ByteStart = &value
	}
	if locator.ByteEnd != nil {
		value := *locator.ByteEnd
		clone.ByteEnd = &value
	}
	if locator.Page != nil {
		value := *locator.Page
		clone.Page = &value
	}
	if locator.Region != nil {
		value := *locator.Region
		clone.Region = &value
	}
	if locator.StartMS != nil {
		value := *locator.StartMS
		clone.StartMS = &value
	}
	if locator.EndMS != nil {
		value := *locator.EndMS
		clone.EndMS = &value
	}
	return clone
}
