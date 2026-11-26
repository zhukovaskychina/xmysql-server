package content

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
)

var ErrInvalidEncoding = errors.New("invalid encoded content value")

const (
	contentEncodingVersionInline      = 1
	contentEncodingVersionSegmented   = 2
	contentEncodingVersionRowRevision = 3
	contentHeaderSize                 = 4 + 1 + 8 + 2 + 8 + sha256.Size
	contentSegmentedHeaderSize        = contentHeaderSize + 16 + sha256.Size
)

var contentEncodingMagic = [4]byte{'X', 'M', 'C', 'N'}

// IsEncoded reports whether data begins with the versioned CONTENT framing.
func IsEncoded(data []byte) bool {
	return len(data) >= len(contentEncodingMagic) && string(data[:len(contentEncodingMagic)]) == string(contentEncodingMagic[:])
}

// MarshalBinary encodes MIME type, source revision and digest together with
// inline bytes. It is the client-visible value representation; callers must
// explicitly read from a segment store before requesting external bytes.
func (v Value) MarshalBinary() ([]byte, error) {
	if v.ExternalRef != nil {
		return nil, ErrExternalContentRequiresStore
	}
	return v.MarshalStorageBinary()
}

// MarshalStorageBinary encodes either inline bytes or an immutable segmented
// object reference for the clustered-record storage codec.
func (v Value) MarshalStorageBinary() ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	if len(v.MIMEType) > int(^uint16(0)) {
		return nil, fmt.Errorf("%w: MIME type is too long", ErrInvalidEncoding)
	}
	if v.ExternalRef != nil {
		headerSize := contentSegmentedHeaderSize
		version := byte(contentEncodingVersionSegmented)
		if v.Revision != v.ExternalRef.Revision {
			version = contentEncodingVersionRowRevision
			headerSize += 8
		}
		encoded := make([]byte, headerSize+len(v.MIMEType))
		copy(encoded[:4], contentEncodingMagic[:])
		encoded[4] = version
		binary.BigEndian.PutUint64(encoded[5:13], v.Revision)
		binary.BigEndian.PutUint16(encoded[13:15], uint16(len(v.MIMEType)))
		binary.BigEndian.PutUint64(encoded[15:23], v.ExternalRef.Size)
		copy(encoded[23:23+sha256.Size], v.Digest[:])
		objectID, err := hex.DecodeString(v.ExternalRef.ID)
		if err != nil || len(objectID) != 16 {
			return nil, fmt.Errorf("%w: invalid segmented object ID", ErrInvalidEncoding)
		}
		copy(encoded[contentHeaderSize:contentHeaderSize+16], objectID)
		copy(encoded[contentHeaderSize+16:contentSegmentedHeaderSize], v.ExternalRef.ManifestDigest[:])
		if version == contentEncodingVersionRowRevision {
			binary.BigEndian.PutUint64(encoded[contentSegmentedHeaderSize:headerSize], v.ExternalRef.Revision)
		}
		copy(encoded[headerSize:], v.MIMEType)
		return encoded, nil
	}
	encoded := make([]byte, contentHeaderSize+len(v.MIMEType)+len(v.Bytes))
	copy(encoded[:4], contentEncodingMagic[:])
	encoded[4] = contentEncodingVersionInline
	binary.BigEndian.PutUint64(encoded[5:13], v.Revision)
	binary.BigEndian.PutUint16(encoded[13:15], uint16(len(v.MIMEType)))
	binary.BigEndian.PutUint64(encoded[15:23], uint64(len(v.Bytes)))
	copy(encoded[23:23+sha256.Size], v.Digest[:])
	offset := contentHeaderSize
	copy(encoded[offset:], v.MIMEType)
	copy(encoded[offset+len(v.MIMEType):], v.Bytes)
	return encoded, nil
}

// UnmarshalBinary validates framing and payload integrity before returning an
// owned value. It rejects truncated and trailing bytes rather than guessing.
func (v *Value) UnmarshalBinary(encoded []byte) error {
	if v == nil || len(encoded) < contentHeaderSize || string(encoded[:4]) != string(contentEncodingMagic[:]) {
		return ErrInvalidEncoding
	}
	version := encoded[4]
	revision := binary.BigEndian.Uint64(encoded[5:13])
	mimeLength := uint64(binary.BigEndian.Uint16(encoded[13:15]))
	payloadLength := binary.BigEndian.Uint64(encoded[15:23])
	if mimeLength > uint64(len(encoded)) {
		return ErrInvalidEncoding
	}
	var digest [sha256.Size]byte
	copy(digest[:], encoded[23:23+sha256.Size])
	var decoded Value
	switch version {
	case contentEncodingVersionInline:
		if payloadLength > uint64(len(encoded)) {
			return ErrInvalidEncoding
		}
		contentOffset := uint64(contentHeaderSize) + mimeLength
		if contentOffset > uint64(len(encoded)) || payloadLength != uint64(len(encoded))-contentOffset {
			return ErrInvalidEncoding
		}
		mimeType := string(encoded[contentHeaderSize:int(contentOffset)])
		decoded = Value{
			MIMEType: mimeType, Kind: KindFromMIME(mimeType), Revision: revision,
			Digest: digest, Bytes: append([]byte(nil), encoded[int(contentOffset):]...),
		}
	case contentEncodingVersionSegmented, contentEncodingVersionRowRevision:
		headerSize := contentSegmentedHeaderSize
		if version == contentEncodingVersionRowRevision {
			headerSize += 8
		}
		contentOffset := uint64(headerSize) + mimeLength
		if len(encoded) < headerSize || contentOffset != uint64(len(encoded)) {
			return ErrInvalidEncoding
		}
		objectRevision := revision
		if version == contentEncodingVersionRowRevision {
			objectRevision = binary.BigEndian.Uint64(encoded[contentSegmentedHeaderSize:headerSize])
		}
		objectID := hex.EncodeToString(encoded[contentHeaderSize : contentHeaderSize+16])
		var manifestDigest [sha256.Size]byte
		copy(manifestDigest[:], encoded[contentHeaderSize+16:contentSegmentedHeaderSize])
		mimeType := string(encoded[headerSize:int(contentOffset)])
		ref := ObjectRef{
			ID: objectID, MIMEType: mimeType, Revision: objectRevision, Size: payloadLength,
			Digest: digest, ManifestDigest: manifestDigest,
		}
		decoded = Value{
			MIMEType: mimeType, Kind: KindFromMIME(mimeType), Revision: revision,
			Digest: digest, ExternalRef: &ref,
		}
	default:
		return fmt.Errorf("%w: unsupported CONTENT encoding version %d", ErrInvalidEncoding, version)
	}
	if err := decoded.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidEncoding, err)
	}
	*v = decoded
	return nil
}
