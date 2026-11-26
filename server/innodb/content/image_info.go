package content

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"mime"
	"strings"
)

const (
	ImageInfoPayloadSchemaID             = "xmysql.image"
	ImageInfoPayloadSchemaVersion        = 1
	ImageInfoPayloadMediaType            = "application/vnd.xmysql.image+json"
	MaxImageDimension                    = 100000
	MaxImagePixels                uint64 = 100000000
	imageProbeLimit               uint64 = 4 << 20
	imageProcessorID                     = "xmysql.image-header-parser"
	imageProcessorVersion                = "1"
)

type ImageInfo struct {
	Format           string `json:"format"`
	MIMEType         string `json:"mime_type"`
	Width            uint32 `json:"width"`
	Height           uint32 `json:"height"`
	HeaderValidated  bool   `json:"header_validated"`
	RegionConvention string `json:"region_convention"`
}

// ParseImageFromStore reads at most 4 MiB of the source header and recognizes
// PNG, JPEG and GIF image configurations. It does not decode pixels, inspect
// all animation frames, OCR text, or validate the complete image object.
func ParseImageFromStore(ctx context.Context, store *SegmentStore, value Value, source SourceRef) (Block, error) {
	if err := value.Validate(); err != nil {
		return Block{}, err
	}
	if source.Revision != value.Revision || source.Digest != value.Digest {
		return Block{}, fmt.Errorf("%w: image source does not match the CONTENT revision and digest", ErrInvalidBlock)
	}
	if err := source.Validate(); err != nil {
		return Block{}, err
	}
	mimeType, _, err := mime.ParseMediaType(value.MIMEType)
	if err != nil || !strings.HasPrefix(strings.ToLower(mimeType), "image/") {
		return Block{}, fmt.Errorf("image parser requires an image/* MIME type")
	}
	if value.Size() == 0 {
		return Block{}, fmt.Errorf("image source is empty")
	}
	probeLength := value.Size()
	if probeLength > imageProbeLimit {
		probeLength = imageProbeLimit
	}
	probe, err := value.ReadRangeFromStore(ctx, store, 0, probeLength)
	if err != nil {
		return Block{}, fmt.Errorf("read image header: %w", err)
	}
	configuration, format, err := image.DecodeConfig(bytes.NewReader(probe))
	if err != nil {
		return Block{}, fmt.Errorf("decode image header: %w", err)
	}
	if !imageMIMEFormatMatches(mimeType, format) {
		return Block{}, fmt.Errorf("image MIME type %q does not match detected format %q", mimeType, format)
	}
	if configuration.Width <= 0 || configuration.Height <= 0 || configuration.Width > MaxImageDimension || configuration.Height > MaxImageDimension {
		return Block{}, fmt.Errorf("image dimensions exceed supported bounds")
	}
	pixels := uint64(configuration.Width) * uint64(configuration.Height)
	if pixels > MaxImagePixels {
		return Block{}, fmt.Errorf("image contains %d pixels; maximum is %d", pixels, MaxImagePixels)
	}
	info := ImageInfo{
		Format: format, MIMEType: mimeType, Width: uint32(configuration.Width), Height: uint32(configuration.Height),
		HeaderValidated: true, RegionConvention: "FULL_SOURCE_IMAGE",
	}
	payload, err := NewImageInfoPayload(info)
	if err != nil {
		return Block{}, err
	}
	block, err := NewBlock(source, 1, BlockImageRegion, "", Locator{}, imageProcessorID, imageProcessorVersion, "", "")
	if err != nil {
		return Block{}, err
	}
	block.Payload = &payload
	block.Attributes = map[string]string{"region_scope": "full_image", "validation_scope": "header_only", "pixels_decoded": "false"}
	block.ID = block.Identity()
	if err := block.Validate(); err != nil {
		return Block{}, err
	}
	return block, nil
}

func NewImageInfoPayload(info ImageInfo) (TypedPayload, error) {
	encoded, err := json.Marshal(info)
	if err != nil {
		return TypedPayload{}, fmt.Errorf("encode image info: %w", err)
	}
	if err := ValidateImageInfo(info); err != nil {
		return TypedPayload{}, err
	}
	return NewTypedPayload(ImageInfoPayloadSchemaID, ImageInfoPayloadSchemaVersion, ImageInfoPayloadMediaType, encoded)
}

func DecodeImageInfo(payload TypedPayload) (ImageInfo, error) {
	if payload.SchemaID != ImageInfoPayloadSchemaID || payload.SchemaVersion != ImageInfoPayloadSchemaVersion {
		return ImageInfo{}, fmt.Errorf("%w: unsupported image info schema %s@%d", ErrInvalidBlock, payload.SchemaID, payload.SchemaVersion)
	}
	if err := payload.Validate(); err != nil {
		return ImageInfo{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload.JSON))
	decoder.DisallowUnknownFields()
	var info ImageInfo
	if err := decoder.Decode(&info); err != nil {
		return ImageInfo{}, fmt.Errorf("%w: decode image info: %v", ErrInvalidBlock, err)
	}
	if err := ValidateImageInfo(info); err != nil {
		return ImageInfo{}, err
	}
	return info, nil
}

func validateImageInfoJSON(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var info ImageInfo
	if err := decoder.Decode(&info); err != nil {
		return fmt.Errorf("%w: decode image info payload: %v", ErrInvalidBlock, err)
	}
	if err := ValidateImageInfo(info); err != nil {
		return err
	}
	return nil
}

func ValidateImageInfo(info ImageInfo) error {
	if info.Format != "png" && info.Format != "jpeg" && info.Format != "gif" {
		return fmt.Errorf("%w: unsupported image format %q", ErrInvalidBlock, info.Format)
	}
	mimeType, _, err := mime.ParseMediaType(info.MIMEType)
	if err != nil || !imageMIMEFormatMatches(mimeType, info.Format) {
		return fmt.Errorf("%w: image info MIME type and format do not match", ErrInvalidBlock)
	}
	if info.Width == 0 || info.Height == 0 || info.Width > MaxImageDimension || info.Height > MaxImageDimension ||
		uint64(info.Width)*uint64(info.Height) > MaxImagePixels || !info.HeaderValidated || info.RegionConvention != "FULL_SOURCE_IMAGE" {
		return fmt.Errorf("%w: image dimensions or validation scope are invalid", ErrInvalidBlock)
	}
	return nil
}

func imageMIMEFormatMatches(mimeType, format string) bool {
	switch strings.ToLower(strings.TrimSpace(mimeType)) {
	case "image/png":
		return format == "png"
	case "image/jpeg":
		return format == "jpeg"
	case "image/gif":
		return format == "gif"
	default:
		return false
	}
}
