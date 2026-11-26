package content

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

const (
	videoContainerProcessorID      = "xmysql.iso-bmff-metadata"
	videoContainerProcessorVersion = "1"
	maxISOBoxHeaders               = 4096
)

// ParseISOBaseMediaVideoFromStore reads only ISO-BMFF box headers and the mvhd
// payload. Large mdat payloads are skipped by their declared lengths. The
// result is one whole-video segment with an exact half-open time locator; no
// decoded frame or audio/video transcript is implied.
func ParseISOBaseMediaVideoFromStore(ctx context.Context, store *SegmentStore, value Value, source SourceRef) (Block, error) {
	if err := value.Validate(); err != nil {
		return Block{}, err
	}
	if err := source.Validate(); err != nil {
		return Block{}, err
	}
	if source.Revision != value.Revision || source.Digest != value.Digest {
		return Block{}, fmt.Errorf("%w: video source does not match the CONTENT revision and digest", ErrInvalidBlock)
	}
	mimeType := strings.ToLower(strings.TrimSpace(strings.Split(value.MIMEType, ";")[0]))
	if value.Kind != KindVideo || (mimeType != "video/mp4" && mimeType != "video/quicktime" && mimeType != "video/x-m4v") {
		return Block{}, fmt.Errorf("video container parser supports video/mp4, video/quicktime and video/x-m4v")
	}
	fileSize := value.Size()
	if fileSize < 16 {
		return Block{}, fmt.Errorf("ISO-BMFF video source is too small")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	topLevelBoxes := 0
	foundFileType := false
	var durationMS uint64
	for offset := uint64(0); offset < fileSize; {
		if err := ctx.Err(); err != nil {
			return Block{}, err
		}
		topLevelBoxes++
		if topLevelBoxes > maxISOBoxHeaders {
			return Block{}, fmt.Errorf("ISO-BMFF video exceeds the %d top-level box limit", maxISOBoxHeaders)
		}
		box, err := readISOBoxHeader(ctx, store, value, offset, fileSize)
		if err != nil {
			return Block{}, fmt.Errorf("read ISO-BMFF box at %d: %w", offset, err)
		}
		switch box.kind {
		case "ftyp":
			foundFileType = box.payloadSize >= 8
		case "moov":
			durationMS, err = readISOMovieDurationMS(ctx, store, value, box)
			if err != nil {
				return Block{}, err
			}
		}
		offset += box.size
	}
	if !foundFileType {
		return Block{}, fmt.Errorf("ISO-BMFF video is missing a valid ftyp box")
	}
	if durationMS == 0 {
		return Block{}, fmt.Errorf("ISO-BMFF video has no bounded mvhd duration")
	}
	startMS, endMS := uint64(0), durationMS
	block, err := NewBlock(source, 1, BlockVideoSegment, "", Locator{StartMS: &startMS, EndMS: &endMS}, videoContainerProcessorID, videoContainerProcessorVersion, "", "")
	if err != nil {
		return Block{}, err
	}
	block.Attributes = map[string]string{
		"container": "iso-bmff", "duration_ms": strconv.FormatUint(durationMS, 10),
		"decode_scope": "container_metadata_only", "frames_decoded": "false",
	}
	block.ID = block.Identity()
	if err := block.Validate(); err != nil {
		return Block{}, err
	}
	return block, nil
}

type isoBoxHeader struct {
	kind        string
	offset      uint64
	size        uint64
	headerSize  uint64
	payloadSize uint64
}

func readISOBoxHeader(ctx context.Context, store *SegmentStore, value Value, offset, limit uint64) (isoBoxHeader, error) {
	if offset > limit || limit-offset < 8 {
		return isoBoxHeader{}, fmt.Errorf("truncated box header")
	}
	header, err := value.ReadRangeFromStore(ctx, store, offset, 8)
	if err != nil {
		return isoBoxHeader{}, err
	}
	size32 := binary.BigEndian.Uint32(header[:4])
	kind := string(header[4:8])
	headerSize := uint64(8)
	boxSize := uint64(size32)
	if size32 == 1 {
		if limit-offset < 16 {
			return isoBoxHeader{}, fmt.Errorf("truncated extended box header")
		}
		extended, err := value.ReadRangeFromStore(ctx, store, offset+8, 8)
		if err != nil {
			return isoBoxHeader{}, err
		}
		boxSize = binary.BigEndian.Uint64(extended)
		headerSize = 16
	} else if size32 == 0 {
		boxSize = limit - offset
	}
	if boxSize < headerSize || boxSize > limit-offset {
		return isoBoxHeader{}, fmt.Errorf("invalid %q box size %d", kind, boxSize)
	}
	return isoBoxHeader{kind: kind, offset: offset, size: boxSize, headerSize: headerSize, payloadSize: boxSize - headerSize}, nil
}

func readISOMovieDurationMS(ctx context.Context, store *SegmentStore, value Value, movie isoBoxHeader) (uint64, error) {
	position := movie.offset + movie.headerSize
	end := movie.offset + movie.size
	for boxes := 0; position < end; boxes++ {
		if boxes >= maxISOBoxHeaders {
			return 0, fmt.Errorf("ISO-BMFF moov box exceeds the %d child-box limit", maxISOBoxHeaders)
		}
		box, err := readISOBoxHeader(ctx, store, value, position, end)
		if err != nil {
			return 0, fmt.Errorf("read ISO-BMFF moov child: %w", err)
		}
		if box.kind == "mvhd" {
			return parseISOMovieHeader(ctx, store, value, box)
		}
		position += box.size
	}
	return 0, fmt.Errorf("ISO-BMFF moov box is missing mvhd")
}

func parseISOMovieHeader(ctx context.Context, store *SegmentStore, value Value, box isoBoxHeader) (uint64, error) {
	if box.payloadSize < 20 {
		return 0, fmt.Errorf("ISO-BMFF mvhd payload is truncated")
	}
	readLength := box.payloadSize
	if readLength > 32 {
		readLength = 32
	}
	payload, err := value.ReadRangeFromStore(ctx, store, box.offset+box.headerSize, readLength)
	if err != nil {
		return 0, fmt.Errorf("read ISO-BMFF mvhd payload: %w", err)
	}
	version := payload[0]
	var timescale uint32
	var duration uint64
	switch version {
	case 0:
		if len(payload) < 20 {
			return 0, fmt.Errorf("ISO-BMFF version 0 mvhd is truncated")
		}
		timescale = binary.BigEndian.Uint32(payload[12:16])
		duration = uint64(binary.BigEndian.Uint32(payload[16:20]))
		if duration == uint64(^uint32(0)) {
			return 0, fmt.Errorf("ISO-BMFF mvhd duration is unspecified")
		}
	case 1:
		if len(payload) < 32 {
			return 0, fmt.Errorf("ISO-BMFF version 1 mvhd is truncated")
		}
		timescale = binary.BigEndian.Uint32(payload[20:24])
		duration = binary.BigEndian.Uint64(payload[24:32])
		if duration == ^uint64(0) {
			return 0, fmt.Errorf("ISO-BMFF mvhd duration is unspecified")
		}
	default:
		return 0, fmt.Errorf("unsupported ISO-BMFF mvhd version %d", version)
	}
	if timescale == 0 || duration == 0 {
		return 0, fmt.Errorf("ISO-BMFF mvhd has an empty duration or time scale")
	}
	wholeSeconds := duration / uint64(timescale)
	remainder := duration % uint64(timescale)
	if wholeSeconds > ^uint64(0)/1000 {
		return 0, fmt.Errorf("ISO-BMFF video duration overflows milliseconds")
	}
	fractionalMS := remainder * 1000 / uint64(timescale)
	wholeMS := wholeSeconds * 1000
	if fractionalMS > ^uint64(0)-wholeMS {
		return 0, fmt.Errorf("ISO-BMFF video duration overflows milliseconds")
	}
	durationMS := wholeMS + fractionalMS
	if durationMS == 0 {
		return 0, fmt.Errorf("ISO-BMFF video duration is shorter than one millisecond")
	}
	return durationMS, nil
}
