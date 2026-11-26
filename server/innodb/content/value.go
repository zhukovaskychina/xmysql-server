// Package content defines the database's canonical, typed representation of
// opaque source content and the locations of derived evidence.
//
// Storage and SQL execution remain owned by the InnoDB engine. This package
// deliberately contains no external object-store, parser, or model client.
package content

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
)

// Kind identifies the source media family without changing the original bytes.
type Kind string

const (
	KindDocument Kind = "document"
	KindImage    Kind = "image"
	KindAudio    Kind = "audio"
	KindVideo    Kind = "video"
	KindOther    Kind = "other"
)

var (
	ErrInvalidValue                 = errors.New("invalid content value")
	ErrInvalidLocator               = errors.New("invalid content locator")
	ErrExternalContentRequiresStore = errors.New("external CONTENT value requires its segment store")
)

const MaxReadRangeBytes uint64 = 4 << 20

// Value is the logical value of a CONTENT column. Exactly one representation
// is present: inline Bytes or an immutable ExternalRef into a database-owned
// segment store. Revision is assigned by the owning row/version layer, not by
// the parser. ExternalRef.Revision belongs to the immutable object manifest;
// assigning that object to a newer row version does not change the manifest.
type Value struct {
	MIMEType    string
	Kind        Kind
	Revision    uint64
	Digest      [sha256.Size]byte
	Bytes       []byte
	ExternalRef *ObjectRef
	// TransientRef marks an object just published from this executing SQL
	// parameter. DML copies it into the row and records the object for rollback
	// cleanup; it is deliberately not serialized into the durable value.
	TransientRef bool
}

// NewValue makes an owned copy so a caller cannot mutate a value after it has
// been handed to a storage or derivation layer.
func NewValue(mimeType string, revision uint64, source []byte) (Value, error) {
	mimeType = strings.TrimSpace(strings.ToLower(mimeType))
	if mimeType == "" || strings.ContainsAny(mimeType, "\r\n") || revision == 0 {
		return Value{}, ErrInvalidValue
	}
	owned := append([]byte(nil), source...)
	return Value{
		MIMEType: mimeType,
		Kind:     KindFromMIME(mimeType),
		Revision: revision,
		Digest:   sha256.Sum256(owned),
		Bytes:    owned,
	}, nil
}

// Validate detects incomplete metadata and changes to bytes after construction.
func (v Value) Validate() error {
	if v.MIMEType == "" || v.Revision == 0 || v.Kind != KindFromMIME(v.MIMEType) {
		return ErrInvalidValue
	}
	if v.ExternalRef != nil {
		ref := v.ExternalRef
		if len(v.Bytes) != 0 || !validObjectRef(*ref) || ref.MIMEType != v.MIMEType ||
			ref.Size == 0 && v.Digest != sha256.Sum256(nil) || ref.Digest != v.Digest {
			return fmt.Errorf("%w: invalid external object reference", ErrInvalidValue)
		}
		return nil
	}
	if got := sha256.Sum256(v.Bytes); got != v.Digest {
		return fmt.Errorf("%w: source digest mismatch", ErrInvalidValue)
	}
	return nil
}

// NewSegmentedValue binds a logical CONTENT value to a committed immutable
// segment object. The caller owns publication and transaction semantics.
func NewSegmentedValue(ref ObjectRef) (Value, error) {
	value := Value{
		MIMEType: ref.MIMEType, Kind: KindFromMIME(ref.MIMEType), Revision: ref.Revision,
		Digest: ref.Digest, ExternalRef: &ref,
	}
	if err := value.Validate(); err != nil {
		return Value{}, err
	}
	return value, nil
}

// NewTransientSegmentedValue binds a newly published parameter object to a
// CONTENT value whose ownership must be recorded by the first DML write.
func NewTransientSegmentedValue(ref ObjectRef) (Value, error) {
	value, err := NewSegmentedValue(ref)
	if err != nil {
		return Value{}, err
	}
	value.TransientRef = true
	return value, nil
}

// Clone returns an owned copy of the logical value and its storage identity.
func (v Value) Clone() Value {
	v.Bytes = append([]byte(nil), v.Bytes...)
	if v.ExternalRef != nil {
		ref := *v.ExternalRef
		v.ExternalRef = &ref
	}
	return v
}

// Size reports the exact source payload size regardless of its physical form.
func (v Value) Size() uint64 {
	if v.ExternalRef != nil {
		return v.ExternalRef.Size
	}
	return uint64(len(v.Bytes))
}

// DigestHex returns the stable lowercase identity of the exact source bytes.
func (v Value) DigestHex() string { return hex.EncodeToString(v.Digest[:]) }

// ReadRange returns an owned, bounded byte slice from the exact source value.
// Offsets are zero-based; reads beyond EOF return an empty slice.
func (v Value) ReadRange(offset, length uint64) ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	if v.ExternalRef != nil {
		return nil, ErrExternalContentRequiresStore
	}
	if length > MaxReadRangeBytes {
		return nil, fmt.Errorf("%w: CONTENT range length %d exceeds the %d byte limit", ErrInvalidValue, length, MaxReadRangeBytes)
	}
	if length == 0 || offset >= uint64(len(v.Bytes)) {
		return []byte{}, nil
	}
	available := uint64(len(v.Bytes)) - offset
	if length > available {
		length = available
	}
	start := int(offset)
	end := start + int(length)
	return append([]byte(nil), v.Bytes[start:end]...), nil
}

// ReadRangeFromStore reads a bounded range from either representation.
func (v Value) ReadRangeFromStore(ctx context.Context, store *SegmentStore, offset, length uint64) ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	if v.ExternalRef == nil {
		return v.ReadRange(offset, length)
	}
	if store == nil {
		return nil, ErrExternalContentRequiresStore
	}
	return store.ReadRange(ctx, *v.ExternalRef, offset, length)
}

// ReadAllFromStore materializes a value only when it stays within maxBytes.
// Large values should be consumed with SegmentStore.CopyRange instead.
func (v Value) ReadAllFromStore(ctx context.Context, store *SegmentStore, maxBytes uint64) ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	if v.Size() > maxBytes {
		return nil, fmt.Errorf("%w: CONTENT size %d exceeds materialization limit %d", ErrInvalidValue, v.Size(), maxBytes)
	}
	if v.Size() > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("%w: CONTENT size %d cannot fit in memory on this platform", ErrInvalidValue, v.Size())
	}
	if v.ExternalRef == nil {
		return append([]byte(nil), v.Bytes...), nil
	}
	if store == nil {
		return nil, ErrExternalContentRequiresStore
	}
	var buffer bytes.Buffer
	buffer.Grow(int(v.Size()))
	if _, err := store.CopyRange(ctx, *v.ExternalRef, 0, v.Size(), &buffer); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// KindFromMIME provides a conservative family classification. Unknown types
// remain valid content and are not guessed to be a supported document format.
func KindFromMIME(mimeType string) Kind {
	mediaType := strings.ToLower(strings.TrimSpace(strings.SplitN(mimeType, ";", 2)[0]))
	switch {
	case strings.HasPrefix(mediaType, "image/"):
		return KindImage
	case strings.HasPrefix(mediaType, "audio/"):
		return KindAudio
	case strings.HasPrefix(mediaType, "video/"):
		return KindVideo
	case strings.HasPrefix(mediaType, "text/"), mediaType == "application/pdf",
		mediaType == "application/msword", mediaType == "application/rtf",
		strings.Contains(mediaType, "wordprocessingml"),
		strings.Contains(mediaType, "presentationml"),
		strings.Contains(mediaType, "spreadsheetml"):
		return KindDocument
	default:
		return KindOther
	}
}

// Locator points into the immutable source revision. Exactly one coordinate
// family is used: byte offsets, page/region, or a half-open media time range.
type Locator struct {
	ByteStart *uint64 `json:"byte_start,omitempty"`
	ByteEnd   *uint64 `json:"byte_end,omitempty"`
	Page      *uint32 `json:"page,omitempty"`
	Region    *Region `json:"region,omitempty"`
	StartMS   *uint64 `json:"start_ms,omitempty"`
	EndMS     *uint64 `json:"end_ms,omitempty"`
	Track     string  `json:"track,omitempty"`
}

// Region uses normalized coordinates in [0,1] relative to the recorded page
// or image dimensions.
type Region struct {
	X      float64 `json:"x"`
	Y      float64 `json:"y"`
	Width  float64 `json:"width"`
	Height float64 `json:"height"`
}

// Validate rejects mixed or reversed coordinate systems. A locator may be
// empty for content-level evidence with no finer-grained source position.
func (l Locator) Validate() error {
	byteRange := l.ByteStart != nil || l.ByteEnd != nil
	timeRange := l.StartMS != nil || l.EndMS != nil || l.Track != ""
	pageRegion := l.Page != nil || l.Region != nil
	families := 0
	for _, present := range []bool{byteRange, timeRange, pageRegion} {
		if present {
			families++
		}
	}
	if families > 1 {
		return ErrInvalidLocator
	}
	if byteRange && (l.ByteStart == nil || l.ByteEnd == nil || *l.ByteEnd < *l.ByteStart) {
		return ErrInvalidLocator
	}
	if timeRange && (l.StartMS == nil || l.EndMS == nil || *l.EndMS <= *l.StartMS) {
		return ErrInvalidLocator
	}
	if pageRegion && (l.Page == nil || *l.Page == 0) {
		return ErrInvalidLocator
	}
	if l.Region != nil {
		r := l.Region
		if math.IsNaN(r.X) || math.IsNaN(r.Y) || math.IsNaN(r.Width) || math.IsNaN(r.Height) ||
			math.IsInf(r.X, 0) || math.IsInf(r.Y, 0) || math.IsInf(r.Width, 0) || math.IsInf(r.Height, 0) ||
			r.X < 0 || r.Y < 0 || r.Width <= 0 || r.Height <= 0 ||
			r.X+r.Width > 1 || r.Y+r.Height > 1 {
			return ErrInvalidLocator
		}
	}
	return nil
}
