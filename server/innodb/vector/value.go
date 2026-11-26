// Package vector defines exact, typed embedding values used by SQL retrieval.
// It carries model-space identity so vectors from unrelated models or
// modalities cannot be compared just because their dimensions match.
package vector

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
)

const maxDimension = 65535
const maxJSONBytes = 64 << 20

var (
	ErrInvalidValue  = errors.New("invalid VECTOR value")
	ErrSpaceMismatch = errors.New("VECTOR model space mismatch")
	ErrInvalidMetric = errors.New("unsupported VECTOR distance metric")
	ErrStaleValue    = errors.New("stale VECTOR value")
)

type Metric string

const (
	MetricL2           Metric = "L2"
	MetricCosine       Metric = "COSINE"
	MetricInnerProduct Metric = "INNER_PRODUCT"
)

// Space records the encoder provenance and an administrator/model-declared
// compatibility ID. Distinct text and image encoders can be compared when
// they explicitly emit vectors into the same compatibility space.
type Space struct {
	ModelID      string
	ModelVersion string
	ID           string
}

type State string

const (
	StateReady      State = "READY"
	StateStale      State = "STALE"
	StateUnverified State = "UNVERIFIED"
)

type Value struct {
	Space          Space
	SourceModality string
	Elements       []float32
	Source         *content.SourceRef
	SourceBlockID  string
	State          State
}

func NewValue(space Space, sourceModality string, elements []float32) (Value, error) {
	value := Value{Space: Space{
		ModelID: strings.TrimSpace(space.ModelID), ModelVersion: strings.TrimSpace(space.ModelVersion),
		ID: strings.TrimSpace(space.ID),
	}, SourceModality: strings.ToLower(strings.TrimSpace(sourceModality)), Elements: append([]float32(nil), elements...), State: StateUnverified}
	if err := value.Validate(); err != nil {
		return Value{}, err
	}
	return value, nil
}

func NewBoundValue(space Space, sourceModality string, elements []float32, source content.SourceRef) (Value, error) {
	value, err := NewValue(space, sourceModality, elements)
	if err != nil {
		return Value{}, err
	}
	copySource := source
	copySource.PrimaryKey = append([]byte(nil), source.PrimaryKey...)
	value.Source = &copySource
	value.State = StateReady
	if err := value.Validate(); err != nil {
		return Value{}, err
	}
	return value, nil
}

// NewBlockBoundValue additionally binds an embedding to the immutable
// identity of the exact parsed content block used as model input.
func NewBlockBoundValue(space Space, sourceModality string, elements []float32, source content.SourceRef, blockID string) (Value, error) {
	value, err := NewBoundValue(space, sourceModality, elements, source)
	if err != nil {
		return Value{}, err
	}
	value.SourceBlockID = strings.ToLower(strings.TrimSpace(blockID))
	if err := value.Validate(); err != nil {
		return Value{}, err
	}
	return value, nil
}

// FromSourceJSON parses a strict client vector document and binds it to the
// exact CONTENT source reference supplied by the database row evaluator.
func FromSourceJSON(input []byte, source content.SourceRef) (Value, error) {
	if len(input) == 0 || len(input) > maxJSONBytes {
		return Value{}, fmt.Errorf("%w: vector JSON size is invalid", ErrInvalidValue)
	}
	var wire struct {
		ModelID      string    `json:"model_id"`
		ModelVersion string    `json:"model_version"`
		SpaceID      string    `json:"space_id"`
		Modality     string    `json:"source_modality"`
		Elements     []float32 `json:"elements"`
	}
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return Value{}, fmt.Errorf("%w: %v", ErrInvalidValue, err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return Value{}, fmt.Errorf("%w: trailing vector JSON payload", ErrInvalidValue)
	}
	return NewBoundValue(Space{ModelID: wire.ModelID, ModelVersion: wire.ModelVersion, ID: wire.SpaceID}, wire.Modality, wire.Elements, source)
}

// FromBlockJSON parses the regular vector JSON contract and binds it to both
// the exact source row revision and the READY content block identity.
func FromBlockJSON(input []byte, source content.SourceRef, block content.Block) (Value, error) {
	if err := block.Validate(); err != nil {
		return Value{}, fmt.Errorf("%w: invalid source block: %v", ErrInvalidValue, err)
	}
	if block.State != content.BlockStateReady || !block.Source.Matches(source) {
		return Value{}, fmt.Errorf("%w: source block is not READY for the current content revision", ErrInvalidValue)
	}
	value, err := FromSourceJSON(input, source)
	if err != nil {
		return Value{}, err
	}
	value.SourceBlockID = strings.ToLower(strings.TrimSpace(block.ID))
	if err := value.Validate(); err != nil {
		return Value{}, err
	}
	return value, nil
}

func (v Value) Validate() error {
	if strings.TrimSpace(v.Space.ModelID) == "" || strings.TrimSpace(v.Space.ModelVersion) == "" ||
		strings.TrimSpace(v.Space.ID) == "" || strings.TrimSpace(v.SourceModality) == "" || len(v.Elements) == 0 || len(v.Elements) > maxDimension {
		return ErrInvalidValue
	}
	switch v.State {
	case StateUnverified, StateReady, StateStale:
	default:
		return fmt.Errorf("%w: unknown vector state %q", ErrInvalidValue, v.State)
	}
	if v.Source == nil {
		if v.State != StateUnverified {
			return fmt.Errorf("%w: a bound vector state requires a source reference", ErrInvalidValue)
		}
	} else {
		if err := v.Source.Validate(); err != nil {
			return fmt.Errorf("%w: invalid vector source reference", ErrInvalidValue)
		}
		if v.State == StateUnverified {
			return fmt.Errorf("%w: a source-bound vector cannot be unverified", ErrInvalidValue)
		}
	}
	if v.SourceBlockID != "" {
		blockID, err := hex.DecodeString(v.SourceBlockID)
		if err != nil || len(blockID) != sha256.Size || hex.EncodeToString(blockID) != v.SourceBlockID || v.Source == nil {
			return fmt.Errorf("%w: source block identity requires a source-bound vector and a SHA-256 block ID", ErrInvalidValue)
		}
	}
	for index, element := range v.Elements {
		if math.IsNaN(float64(element)) || math.IsInf(float64(element), 0) {
			return fmt.Errorf("%w: non-finite element at index %d", ErrInvalidValue, index)
		}
	}
	return nil
}

func (v Value) Dimension() int { return len(v.Elements) }

func (v Value) Clone() Value {
	clone := v
	clone.Elements = append([]float32(nil), v.Elements...)
	if v.Source != nil {
		source := *v.Source
		source.PrimaryKey = append([]byte(nil), v.Source.PrimaryKey...)
		clone.Source = &source
	}
	return clone
}

// MarshalBinary emits a versioned, checksummed representation for row storage.
func (v Value) MarshalBinary() ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	for _, field := range []string{v.Space.ModelID, v.Space.ModelVersion, v.Space.ID, v.SourceModality} {
		if len(field) > int(^uint16(0)) {
			return nil, fmt.Errorf("%w: model space identifier is too long", ErrInvalidValue)
		}
	}
	buffer := make([]byte, 0, 128+len(v.Elements)*4+len(v.Space.ModelID)+len(v.Space.ModelVersion)+len(v.Space.ID)+len(v.SourceModality)+sha256.Size)
	version := byte(2)
	if v.SourceBlockID != "" {
		version = 3
	}
	buffer = append(buffer, 'X', 'M', 'V', version)
	buffer = binary.BigEndian.AppendUint16(buffer, uint16(len(v.Space.ModelID)))
	buffer = append(buffer, v.Space.ModelID...)
	buffer = binary.BigEndian.AppendUint16(buffer, uint16(len(v.Space.ModelVersion)))
	buffer = append(buffer, v.Space.ModelVersion...)
	buffer = binary.BigEndian.AppendUint16(buffer, uint16(len(v.Space.ID)))
	buffer = append(buffer, v.Space.ID...)
	buffer = binary.BigEndian.AppendUint16(buffer, uint16(len(v.SourceModality)))
	buffer = append(buffer, v.SourceModality...)
	stateCode := byte(0)
	switch v.State {
	case StateReady:
		stateCode = 1
	case StateStale:
		stateCode = 2
	case StateUnverified:
		stateCode = 3
	}
	buffer = append(buffer, stateCode)
	if v.Source == nil {
		buffer = append(buffer, 0)
	} else {
		buffer = append(buffer, 1)
		for _, field := range []string{v.Source.Schema, v.Source.Table, v.Source.Column} {
			if len(field) > int(^uint16(0)) {
				return nil, fmt.Errorf("%w: source identity field is too long", ErrInvalidValue)
			}
			buffer = binary.BigEndian.AppendUint16(buffer, uint16(len(field)))
			buffer = append(buffer, field...)
		}
		if uint64(len(v.Source.PrimaryKey)) > uint64(^uint32(0)) {
			return nil, fmt.Errorf("%w: source primary key is too long", ErrInvalidValue)
		}
		buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(v.Source.PrimaryKey)))
		buffer = append(buffer, v.Source.PrimaryKey...)
		buffer = binary.BigEndian.AppendUint64(buffer, v.Source.Revision)
		buffer = append(buffer, v.Source.Digest[:]...)
	}
	if version >= 3 {
		buffer = binary.BigEndian.AppendUint16(buffer, uint16(len(v.SourceBlockID)))
		buffer = append(buffer, v.SourceBlockID...)
	}
	buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(v.Elements)))
	for _, element := range v.Elements {
		buffer = binary.BigEndian.AppendUint32(buffer, math.Float32bits(element))
	}
	digest := sha256.Sum256(buffer)
	return append(buffer, digest[:]...), nil
}

func IsEncoded(data []byte) bool {
	return len(data) >= 4 && data[0] == 'X' && data[1] == 'M' && data[2] == 'V'
}

func (v *Value) UnmarshalBinary(data []byte) error {
	if v == nil || len(data) < 4+2+2+2+4+sha256.Size || string(data[:3]) != "XMV" || (data[3] != 1 && data[3] != 2 && data[3] != 3) {
		return ErrInvalidValue
	}
	version := data[3]
	contentEnd := len(data) - sha256.Size
	wantDigest := sha256.Sum256(data[:contentEnd])
	if string(wantDigest[:]) != string(data[contentEnd:]) {
		return fmt.Errorf("%w: checksum mismatch", ErrInvalidValue)
	}
	offset := 4
	readString := func() (string, error) {
		if offset+2 > contentEnd {
			return "", ErrInvalidValue
		}
		length := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if offset+length > contentEnd {
			return "", ErrInvalidValue
		}
		value := string(data[offset : offset+length])
		offset += length
		return value, nil
	}
	modelID, err := readString()
	if err != nil {
		return err
	}
	modelVersion, err := readString()
	if err != nil {
		return err
	}
	spaceID, err := readString()
	if err != nil {
		return err
	}
	modality, err := readString()
	if err != nil {
		return err
	}
	state := StateUnverified
	var source *content.SourceRef
	sourceBlockID := ""
	if version >= 2 {
		if offset+2 > contentEnd {
			return ErrInvalidValue
		}
		switch data[offset] {
		case 1:
			state = StateReady
		case 2:
			state = StateStale
		case 3:
			state = StateUnverified
		default:
			return ErrInvalidValue
		}
		offset++
		hasSource := data[offset]
		offset++
		if hasSource > 1 {
			return ErrInvalidValue
		}
		if hasSource == 1 {
			fields := make([]string, 3)
			for index := range fields {
				fields[index], err = readString()
				if err != nil {
					return err
				}
			}
			if offset+4 > contentEnd {
				return ErrInvalidValue
			}
			keyLength := uint64(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4
			if keyLength == 0 || keyLength > uint64(contentEnd-offset) || offset+int(keyLength)+8+sha256.Size > contentEnd {
				return ErrInvalidValue
			}
			key := append([]byte(nil), data[offset:offset+int(keyLength)]...)
			offset += int(keyLength)
			revision := binary.BigEndian.Uint64(data[offset : offset+8])
			offset += 8
			var sourceDigest [sha256.Size]byte
			copy(sourceDigest[:], data[offset:offset+sha256.Size])
			offset += sha256.Size
			source = &content.SourceRef{Schema: fields[0], Table: fields[1], Column: fields[2], PrimaryKey: key, Revision: revision, Digest: sourceDigest}
		}
		if version >= 3 {
			sourceBlockID, err = readString()
			if err != nil || sourceBlockID == "" || source == nil {
				return ErrInvalidValue
			}
		}
	} else {
		// Version 1 vectors had no row-level source binding and are therefore
		// retained as UNVERIFIED values when read by the newer engine.
		state = StateUnverified
	}
	if offset+4 > contentEnd {
		return ErrInvalidValue
	}
	dimension := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4
	if dimension == 0 || dimension > maxDimension || offset+dimension*4 != contentEnd {
		return ErrInvalidValue
	}
	elements := make([]float32, dimension)
	for index := range elements {
		elements[index] = math.Float32frombits(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
	}
	decoded := Value{
		Space:          Space{ModelID: modelID, ModelVersion: modelVersion, ID: spaceID},
		SourceModality: modality, Elements: elements, Source: source, SourceBlockID: sourceBlockID, State: state,
	}
	if err := decoded.Validate(); err != nil {
		return err
	}
	*v = decoded
	return nil
}

func Distance(left, right Value, metric Metric) (float64, error) {
	if err := left.Validate(); err != nil {
		return 0, fmt.Errorf("left vector: %w", err)
	}
	if err := right.Validate(); err != nil {
		return 0, fmt.Errorf("right vector: %w", err)
	}
	if left.State == StateStale || right.State == StateStale {
		return 0, ErrStaleValue
	}
	if left.Space.ID != right.Space.ID || len(left.Elements) != len(right.Elements) {
		return 0, ErrSpaceMismatch
	}
	switch Metric(strings.ToUpper(strings.TrimSpace(string(metric)))) {
	case MetricL2:
		var sum float64
		for index, a := range left.Elements {
			difference := float64(a) - float64(right.Elements[index])
			sum += difference * difference
		}
		return math.Sqrt(sum), nil
	case MetricCosine:
		var dot, leftNorm, rightNorm float64
		for index, a := range left.Elements {
			b := float64(right.Elements[index])
			af := float64(a)
			dot += af * b
			leftNorm += af * af
			rightNorm += b * b
		}
		if leftNorm == 0 || rightNorm == 0 {
			return 0, fmt.Errorf("%w: cosine distance is undefined for a zero vector", ErrInvalidValue)
		}
		return 1 - dot/(math.Sqrt(leftNorm)*math.Sqrt(rightNorm)), nil
	case MetricInnerProduct:
		var dot float64
		for index, a := range left.Elements {
			dot += float64(a) * float64(right.Elements[index])
		}
		return -dot, nil
	default:
		return 0, ErrInvalidMetric
	}
}
