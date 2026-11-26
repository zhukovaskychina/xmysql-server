package content

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
)

var ErrInvalidBlock = errors.New("invalid derived content block")

// SourceRef identifies the exact relational value from which a derived block
// was produced. PrimaryKey is a canonical, schema-aware binary key encoding;
// it is not a display string and must not be reconstructed from query output.
type SourceRef struct {
	Schema     string            `json:"schema"`
	Table      string            `json:"table"`
	Column     string            `json:"column"`
	PrimaryKey []byte            `json:"primary_key"`
	Revision   uint64            `json:"revision"`
	Digest     [sha256.Size]byte `json:"digest"`
}

func NewSourceRef(schema, table, column string, primaryKey []byte, value Value) (SourceRef, error) {
	if err := value.Validate(); err != nil {
		return SourceRef{}, fmt.Errorf("%w: invalid source content: %v", ErrInvalidBlock, err)
	}
	source := SourceRef{
		Schema: schema, Table: table, Column: column,
		PrimaryKey: append([]byte(nil), primaryKey...), Revision: value.Revision, Digest: value.Digest,
	}
	if err := source.Validate(); err != nil {
		return SourceRef{}, err
	}
	return source, nil
}

func (s SourceRef) Validate() error {
	if strings.TrimSpace(s.Schema) == "" || strings.TrimSpace(s.Table) == "" ||
		strings.TrimSpace(s.Column) == "" || len(s.PrimaryKey) == 0 || s.Revision == 0 || s.Digest == ([sha256.Size]byte{}) {
		return ErrInvalidBlock
	}
	return nil
}

// Matches reports whether two references identify the same exact relational
// value version, including its canonical primary key and content digest.
func (s SourceRef) Matches(other SourceRef) bool {
	return strings.EqualFold(strings.TrimSpace(s.Schema), strings.TrimSpace(other.Schema)) &&
		strings.EqualFold(strings.TrimSpace(s.Table), strings.TrimSpace(other.Table)) &&
		strings.EqualFold(strings.TrimSpace(s.Column), strings.TrimSpace(other.Column)) &&
		bytes.Equal(s.PrimaryKey, other.PrimaryKey) && s.Revision == other.Revision && s.Digest == other.Digest
}

// BlockKind describes a generic content unit. Locator carries the page,
// region, byte, or media-time coordinates; the kind never encodes a business
// domain.
type BlockKind string

const (
	BlockText         BlockKind = "text"
	BlockTable        BlockKind = "table"
	BlockImageRegion  BlockKind = "image_region"
	BlockAudioSegment BlockKind = "audio_segment"
	BlockVideoFrame   BlockKind = "video_frame"
	BlockVideoSegment BlockKind = "video_segment"
	BlockStructured   BlockKind = "structured"
)

type BlockState string

const (
	BlockStateReady BlockState = "READY"
	BlockStateStale BlockState = "STALE"
	// BlockStateUnverified marks a block whose source identity was supplied by
	// a caller rather than bound to a database row. Such a block is readable as
	// data, but must never be treated as verified evidence.
	BlockStateUnverified BlockState = "UNVERIFIED"
)

// Block is an immutable, versioned projection of source content. A block may
// carry text, a typed structured payload, or both; original bytes remain in
// the source CONTENT value and are not duplicated here.
type Block struct {
	ID               string            `json:"id"`
	State            BlockState        `json:"state,omitempty"`
	IdentityVersion  uint8             `json:"identity_version,omitempty"`
	Source           SourceRef         `json:"source"`
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

// NewBlock binds a parsed unit to the source revision and processor versions.
// Its ID is stable across retries of the same derivation and changes when the
// source, processor/model version, ordinal, kind, or location changes.
func NewBlock(source SourceRef, ordinal uint32, kind BlockKind, text string, locator Locator, processorID, processorVersion, modelID, modelVersion string) (Block, error) {
	block := Block{
		State: BlockStateReady, IdentityVersion: 2, Source: source, Ordinal: ordinal, Kind: kind, Text: text, Locator: locator,
		ProcessorID: processorID, ProcessorVersion: processorVersion,
		ModelID: modelID, ModelVersion: modelVersion,
	}
	if err := block.Validate(); err != nil {
		return Block{}, err
	}
	block.Source.PrimaryKey = append([]byte(nil), source.PrimaryKey...)
	block.Locator = cloneContentLocator(locator)
	block.ID = block.Identity()
	return block, nil
}

func (b Block) Validate() error {
	if err := b.Source.Validate(); err != nil {
		return fmt.Errorf("%w: source reference: %v", ErrInvalidBlock, err)
	}
	if b.State != BlockStateReady && b.State != BlockStateStale && b.State != BlockStateUnverified {
		return fmt.Errorf("%w: unknown state %q", ErrInvalidBlock, b.State)
	}
	switch b.Kind {
	case BlockText, BlockTable, BlockImageRegion, BlockAudioSegment, BlockVideoFrame, BlockVideoSegment, BlockStructured:
	default:
		return fmt.Errorf("%w: unknown kind %q", ErrInvalidBlock, b.Kind)
	}
	if strings.TrimSpace(b.ProcessorID) == "" || strings.TrimSpace(b.ProcessorVersion) == "" {
		return fmt.Errorf("%w: processor identity is required", ErrInvalidBlock)
	}
	if (strings.TrimSpace(b.ModelID) == "") != (strings.TrimSpace(b.ModelVersion) == "") {
		return fmt.Errorf("%w: model ID and version must be provided together", ErrInvalidBlock)
	}
	identityVersion := b.IdentityVersion
	if identityVersion == 0 {
		identityVersion = 1
	}
	if identityVersion != 1 && identityVersion != 2 {
		return fmt.Errorf("%w: unsupported identity version %d", ErrInvalidBlock, identityVersion)
	}
	if identityVersion == 1 && (b.Payload != nil || b.ParentID != "" || b.Relation != "") {
		return fmt.Errorf("%w: version 1 identity cannot carry typed payload or parent relationship", ErrInvalidBlock)
	}
	if len(b.Text) > blockMaxEncodedSize {
		return fmt.Errorf("%w: text exceeds %d bytes", ErrInvalidBlock, blockMaxEncodedSize)
	}
	if b.Payload != nil {
		if identityVersion < 2 {
			return fmt.Errorf("%w: typed payload requires identity version 2", ErrInvalidBlock)
		}
		if err := b.Payload.Validate(); err != nil {
			return err
		}
	}
	if (b.ParentID == "") != (b.Relation == "") {
		return fmt.Errorf("%w: parent_id and relation must be supplied together", ErrInvalidBlock)
	}
	if b.ParentID != "" {
		parent, err := hex.DecodeString(b.ParentID)
		if err != nil || len(parent) != sha256.Size {
			return fmt.Errorf("%w: parent_id must be a 32-byte hexadecimal block ID", ErrInvalidBlock)
		}
		switch strings.ToLower(strings.TrimSpace(b.Relation)) {
		case "contains", "continues", "derived_from", "references":
		default:
			return fmt.Errorf("%w: unsupported block relation %q", ErrInvalidBlock, b.Relation)
		}
	}
	if len(b.Attributes) > 1024 {
		return fmt.Errorf("%w: attributes exceed 1024 entries", ErrInvalidBlock)
	}
	attributeBytes := 0
	for key, value := range b.Attributes {
		if strings.TrimSpace(key) == "" || len(key) > 1024 || len(value) > 1<<20 {
			return fmt.Errorf("%w: block attribute key or value exceeds the limit", ErrInvalidBlock)
		}
		attributeBytes += len(key) + len(value)
		if attributeBytes > blockMaxEncodedSize {
			return fmt.Errorf("%w: aggregate attributes exceed %d bytes", ErrInvalidBlock, blockMaxEncodedSize)
		}
	}
	representationBytes := len(b.Text) + attributeBytes
	if b.Payload != nil {
		representationBytes += len(b.Payload.JSON)
	}
	if representationBytes > blockMaxEncodedSize {
		return fmt.Errorf("%w: text, payload and attributes exceed %d bytes", ErrInvalidBlock, blockMaxEncodedSize)
	}
	if err := b.Locator.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidBlock, err)
	}
	return nil
}

// Identity returns a deterministic identifier for the derivation unit.
func (b Block) Identity() string {
	if b.IdentityVersion <= 1 {
		return b.identityV1()
	}
	h := sha256.New()
	writeIdentityPart(h, []byte("xmysql-content-block-identity-v2"))
	writeIdentityPart(h, []byte(strings.ToLower(strings.TrimSpace(b.Source.Schema))))
	writeIdentityPart(h, []byte(strings.ToLower(strings.TrimSpace(b.Source.Table))))
	writeIdentityPart(h, []byte(strings.ToLower(strings.TrimSpace(b.Source.Column))))
	writeIdentityPart(h, b.Source.PrimaryKey)
	writeIdentityPart(h, binary.BigEndian.AppendUint64(nil, b.Source.Revision))
	writeIdentityPart(h, b.Source.Digest[:])
	writeIdentityPart(h, binary.BigEndian.AppendUint32(nil, b.Ordinal))
	writeIdentityPart(h, []byte(b.Kind))
	writeIdentityPart(h, []byte(b.ProcessorID))
	writeIdentityPart(h, []byte(b.ProcessorVersion))
	writeIdentityPart(h, []byte(b.ModelID))
	writeIdentityPart(h, []byte(b.ModelVersion))
	writeIdentityPart(h, locatorIdentity(b.Locator))
	writeIdentityPart(h, []byte(b.Text))
	attributes, _ := json.Marshal(b.Attributes)
	writeIdentityPart(h, attributes)
	if b.Payload == nil {
		writeIdentityPart(h, nil)
	} else {
		writeIdentityPart(h, []byte(b.Payload.SchemaID))
		writeIdentityPart(h, binary.BigEndian.AppendUint32(nil, b.Payload.SchemaVersion))
		writeIdentityPart(h, []byte(b.Payload.MediaType))
		writeIdentityPart(h, []byte(strings.ToLower(strings.TrimSpace(b.Payload.SHA256))))
	}
	writeIdentityPart(h, []byte(strings.ToLower(strings.TrimSpace(b.ParentID))))
	writeIdentityPart(h, []byte(strings.ToLower(strings.TrimSpace(b.Relation))))
	return hex.EncodeToString(h.Sum(nil))
}

func (b Block) identityV1() string {
	h := sha256.New()
	writeIdentityPart(h, []byte(strings.ToLower(strings.TrimSpace(b.Source.Schema))))
	writeIdentityPart(h, []byte(strings.ToLower(strings.TrimSpace(b.Source.Table))))
	writeIdentityPart(h, []byte(strings.ToLower(strings.TrimSpace(b.Source.Column))))
	writeIdentityPart(h, b.Source.PrimaryKey)
	writeIdentityPart(h, binary.BigEndian.AppendUint64(nil, b.Source.Revision))
	writeIdentityPart(h, b.Source.Digest[:])
	writeIdentityPart(h, binary.BigEndian.AppendUint32(nil, b.Ordinal))
	writeIdentityPart(h, []byte(b.Kind))
	writeIdentityPart(h, []byte(b.ProcessorID))
	writeIdentityPart(h, []byte(b.ProcessorVersion))
	writeIdentityPart(h, []byte(b.ModelID))
	writeIdentityPart(h, []byte(b.ModelVersion))
	writeIdentityPart(h, locatorIdentity(b.Locator))
	return hex.EncodeToString(h.Sum(nil))
}

type identityWriter interface {
	Write([]byte) (int, error)
}

func writeIdentityPart(writer identityWriter, value []byte) {
	_, _ = writer.Write(binary.BigEndian.AppendUint64(nil, uint64(len(value))))
	_, _ = writer.Write(value)
}

func locatorIdentity(locator Locator) []byte {
	result := make([]byte, 0, 64)
	appendOptionalUint64 := func(value *uint64) {
		if value == nil {
			result = append(result, 0)
			return
		}
		result = append(result, 1)
		result = binary.BigEndian.AppendUint64(result, *value)
	}
	appendOptionalUint32 := func(value *uint32) {
		if value == nil {
			result = append(result, 0)
			return
		}
		result = append(result, 1)
		result = binary.BigEndian.AppendUint32(result, *value)
	}
	appendOptionalUint64(locator.ByteStart)
	appendOptionalUint64(locator.ByteEnd)
	appendOptionalUint32(locator.Page)
	if locator.Region == nil {
		result = append(result, 0)
	} else {
		result = append(result, 1)
		for _, value := range []float64{locator.Region.X, locator.Region.Y, locator.Region.Width, locator.Region.Height} {
			result = binary.BigEndian.AppendUint64(result, math.Float64bits(value))
		}
	}
	appendOptionalUint64(locator.StartMS)
	appendOptionalUint64(locator.EndMS)
	result = append(result, binary.BigEndian.AppendUint64(nil, uint64(len(locator.Track)))...)
	result = append(result, locator.Track...)
	return result
}
