package content

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"mime"
	"strings"
)

const (
	AudioInfoPayloadSchemaID      = "xmysql.audio"
	AudioInfoPayloadSchemaVersion = 1
	AudioInfoPayloadMediaType     = "application/vnd.xmysql.audio+json"
	maxWaveChunks                 = 1024
	audioProcessorID              = "xmysql.wave-header-parser"
	audioProcessorVersion         = "1"
)

// AudioInfo describes a bounded, deterministic inspection of a RIFF/WAVE
// container. It does not describe transcription, speech, or semantic content.
type AudioInfo struct {
	Format          string `json:"format"`
	MIMEType        string `json:"mime_type"`
	Codec           string `json:"codec"`
	Channels        uint16 `json:"channels"`
	SampleRateHz    uint32 `json:"sample_rate_hz"`
	BitsPerSample   uint16 `json:"bits_per_sample"`
	BlockAlignBytes uint16 `json:"block_align_bytes"`
	DataBytes       uint64 `json:"data_bytes"`
	SampleFrames    uint64 `json:"sample_frames"`
	DurationMS      uint64 `json:"duration_ms"`
	HeaderValidated bool   `json:"header_validated"`
}

// ParseWaveFromStore reads only RIFF/WAVE chunk headers and the small fmt
// chunk. Audio samples remain in CONTENT and are never loaded or decoded.
// Only integer PCM and IEEE float WAVE payloads are accepted because their
// duration can be derived from block alignment and sample rate.
func ParseWaveFromStore(ctx context.Context, store *SegmentStore, value Value, source SourceRef) (Block, error) {
	if err := value.Validate(); err != nil {
		return Block{}, err
	}
	if source.Revision != value.Revision || source.Digest != value.Digest {
		return Block{}, fmt.Errorf("%w: audio source does not match the CONTENT revision and digest", ErrInvalidBlock)
	}
	if err := source.Validate(); err != nil {
		return Block{}, err
	}
	mimeType, _, err := mime.ParseMediaType(value.MIMEType)
	if err != nil || !isWaveMIME(mimeType) {
		return Block{}, fmt.Errorf("WAVE parser requires audio/wav or audio/wave MIME type")
	}
	if value.Size() < 12 {
		return Block{}, fmt.Errorf("WAVE source is shorter than its RIFF header")
	}
	read := func(offset, length uint64) ([]byte, error) {
		if length > MaxReadRangeBytes || offset > value.Size() || length > value.Size()-offset {
			return nil, fmt.Errorf("WAVE range is outside the source or exceeds the read limit")
		}
		return value.ReadRangeFromStore(ctx, store, offset, length)
	}
	header, err := read(0, 12)
	if err != nil {
		return Block{}, fmt.Errorf("read WAVE header: %w", err)
	}
	if string(header[:4]) != "RIFF" || string(header[8:12]) != "WAVE" {
		return Block{}, fmt.Errorf("source is not a RIFF/WAVE container")
	}
	riffEnd := uint64(binary.LittleEndian.Uint32(header[4:8])) + 8
	if riffEnd < 12 || riffEnd > value.Size() {
		return Block{}, fmt.Errorf("RIFF size exceeds the CONTENT source")
	}
	var format []byte
	var dataBytes uint64
	hasData := false
	position := uint64(12)
	for chunks := 0; position < riffEnd; chunks++ {
		if chunks >= maxWaveChunks || riffEnd-position < 8 {
			return Block{}, fmt.Errorf("WAVE chunk table is malformed or exceeds %d chunks", maxWaveChunks)
		}
		chunkHeader, readErr := read(position, 8)
		if readErr != nil {
			return Block{}, fmt.Errorf("read WAVE chunk header: %w", readErr)
		}
		chunkID := string(chunkHeader[:4])
		chunkSize := uint64(binary.LittleEndian.Uint32(chunkHeader[4:8]))
		chunkStart := position + 8
		if chunkSize > riffEnd-chunkStart {
			return Block{}, fmt.Errorf("WAVE %q chunk exceeds RIFF bounds", chunkID)
		}
		switch chunkID {
		case "fmt ":
			if format != nil || chunkSize < 16 || chunkSize > 64<<10 {
				return Block{}, fmt.Errorf("WAVE fmt chunk is duplicate or outside the supported size")
			}
			format, err = read(chunkStart, chunkSize)
			if err != nil {
				return Block{}, fmt.Errorf("read WAVE fmt chunk: %w", err)
			}
		case "data":
			if hasData {
				return Block{}, fmt.Errorf("multiple WAVE data chunks are not supported")
			}
			dataBytes, hasData = chunkSize, true
		}
		advance := uint64(8) + chunkSize + chunkSize%2
		if advance > riffEnd-position {
			return Block{}, fmt.Errorf("WAVE chunk padding exceeds RIFF bounds")
		}
		position += advance
	}
	if position != riffEnd || format == nil || !hasData {
		return Block{}, fmt.Errorf("WAVE source must contain exactly one fmt and one data chunk")
	}
	info, err := parseWaveFormat(mimeType, format, dataBytes)
	if err != nil {
		return Block{}, err
	}
	payload, err := NewAudioInfoPayload(info)
	if err != nil {
		return Block{}, err
	}
	locator := Locator{}
	if info.DurationMS > 0 {
		start, end := uint64(0), info.DurationMS
		locator.StartMS, locator.EndMS = &start, &end
	}
	block, err := NewBlock(source, 1, BlockAudioSegment, "", locator, audioProcessorID, audioProcessorVersion, "", "")
	if err != nil {
		return Block{}, err
	}
	block.Payload = &payload
	block.Attributes = map[string]string{"region_scope": "full_audio", "validation_scope": "container_header_only", "samples_decoded": "false", "transcription_available": "false"}
	block.ID = block.Identity()
	if err := block.Validate(); err != nil {
		return Block{}, err
	}
	return block, nil
}

// ReadWaveSegmentFromStore returns a self-contained RIFF/WAVE value containing
// only the sample frames in the half-open [startMS, endMS) interval. It copies
// the validated format chunk and reads the corresponding aligned data bytes in
// bounded ranges, so callers can send the located audio evidence to a model
// without loading unrelated portions of the source recording.
func ReadWaveSegmentFromStore(ctx context.Context, store *SegmentStore, value Value, startMS, endMS, maxBytes uint64) ([]byte, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	if value.Kind != KindAudio {
		return nil, fmt.Errorf("WAVE segment reader requires an audio CONTENT value")
	}
	if maxBytes == 0 || maxBytes > MaxReadRangeBytes*16 {
		return nil, fmt.Errorf("WAVE segment byte limit must be between 1 and %d", MaxReadRangeBytes*16)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	mimeType, _, err := mime.ParseMediaType(value.MIMEType)
	if err != nil || !isWaveMIME(mimeType) {
		return nil, fmt.Errorf("WAVE segment reader requires audio/wav or audio/wave MIME type")
	}
	if value.Size() < 12 {
		return nil, fmt.Errorf("WAVE source is shorter than its RIFF header")
	}
	read := func(offset, length uint64) ([]byte, error) {
		if length > MaxReadRangeBytes || offset > value.Size() || length > value.Size()-offset {
			return nil, fmt.Errorf("WAVE range is outside the source or exceeds the read limit")
		}
		return value.ReadRangeFromStore(ctx, store, offset, length)
	}
	header, err := read(0, 12)
	if err != nil {
		return nil, fmt.Errorf("read WAVE header: %w", err)
	}
	if string(header[:4]) != "RIFF" || string(header[8:12]) != "WAVE" {
		return nil, fmt.Errorf("source is not a RIFF/WAVE container")
	}
	riffEnd := uint64(binary.LittleEndian.Uint32(header[4:8])) + 8
	if riffEnd < 12 || riffEnd > value.Size() {
		return nil, fmt.Errorf("RIFF size exceeds the CONTENT source")
	}
	var format []byte
	var dataOffset, dataBytes uint64
	hasData := false
	position := uint64(12)
	for chunks := 0; position < riffEnd; chunks++ {
		if chunks >= maxWaveChunks || riffEnd-position < 8 {
			return nil, fmt.Errorf("WAVE chunk table is malformed or exceeds %d chunks", maxWaveChunks)
		}
		chunkHeader, readErr := read(position, 8)
		if readErr != nil {
			return nil, fmt.Errorf("read WAVE chunk header: %w", readErr)
		}
		chunkID := string(chunkHeader[:4])
		chunkSize := uint64(binary.LittleEndian.Uint32(chunkHeader[4:8]))
		chunkStart := position + 8
		if chunkSize > riffEnd-chunkStart {
			return nil, fmt.Errorf("WAVE %q chunk exceeds RIFF bounds", chunkID)
		}
		switch chunkID {
		case "fmt ":
			if format != nil || chunkSize < 16 || chunkSize > 64<<10 {
				return nil, fmt.Errorf("WAVE fmt chunk is duplicate or outside the supported size")
			}
			format, err = read(chunkStart, chunkSize)
			if err != nil {
				return nil, fmt.Errorf("read WAVE fmt chunk: %w", err)
			}
		case "data":
			if hasData {
				return nil, fmt.Errorf("multiple WAVE data chunks are not supported")
			}
			dataOffset, dataBytes, hasData = chunkStart, chunkSize, true
		}
		advance := uint64(8) + chunkSize + chunkSize%2
		if advance > riffEnd-position {
			return nil, fmt.Errorf("WAVE chunk padding exceeds RIFF bounds")
		}
		position += advance
	}
	if position != riffEnd || format == nil || !hasData {
		return nil, fmt.Errorf("WAVE source must contain exactly one fmt and one data chunk")
	}
	info, err := parseWaveFormat(mimeType, format, dataBytes)
	if err != nil {
		return nil, err
	}
	if startMS >= endMS || endMS > info.DurationMS {
		return nil, fmt.Errorf("WAVE segment interval [%d,%d) ms is outside duration %d ms", startMS, endMS, info.DurationMS)
	}
	frameForMS := func(milliseconds uint64) (uint64, error) {
		rate := uint64(info.SampleRateHz)
		seconds := milliseconds / 1000
		if seconds > ^uint64(0)/rate {
			return 0, fmt.Errorf("WAVE segment time overflows sample-frame coordinates")
		}
		whole := seconds * rate
		fraction := milliseconds % 1000 * rate
		fraction = (fraction + 999) / 1000
		if fraction > ^uint64(0)-whole {
			return 0, fmt.Errorf("WAVE segment time overflows sample-frame coordinates")
		}
		return whole + fraction, nil
	}
	firstFrame, err := frameForMS(startMS)
	if err != nil {
		return nil, err
	}
	endFrame, err := frameForMS(endMS)
	if err != nil {
		return nil, err
	}
	if endFrame > info.SampleFrames {
		endFrame = info.SampleFrames
	}
	if startMS == 0 && endMS == info.DurationMS {
		endFrame = info.SampleFrames
	}
	if firstFrame >= endFrame {
		return nil, fmt.Errorf("WAVE segment interval contains no complete sample frames")
	}
	frameBytes := uint64(info.BlockAlignBytes)
	segmentBytes := (endFrame - firstFrame) * frameBytes
	formatChunkBytes := uint64(8 + len(format) + len(format)%2)
	dataChunkBytes := uint64(8) + segmentBytes + segmentBytes%2
	riffSize := uint64(4) + formatChunkBytes + dataChunkBytes
	if segmentBytes > maxBytes || riffSize > uint64(^uint32(0)) || riffSize+8 > maxBytes {
		return nil, fmt.Errorf("WAVE segment exceeds the %d byte media limit", maxBytes)
	}
	output := make([]byte, int(riffSize+8))
	copy(output[:4], "RIFF")
	binary.LittleEndian.PutUint32(output[4:8], uint32(riffSize))
	copy(output[8:12], "WAVE")
	writeOffset := 12
	copy(output[writeOffset:writeOffset+4], "fmt ")
	binary.LittleEndian.PutUint32(output[writeOffset+4:writeOffset+8], uint32(len(format)))
	copy(output[writeOffset+8:], format)
	writeOffset += int(formatChunkBytes)
	copy(output[writeOffset:writeOffset+4], "data")
	binary.LittleEndian.PutUint32(output[writeOffset+4:writeOffset+8], uint32(segmentBytes))
	writeOffset += 8
	readOffset := dataOffset + firstFrame*frameBytes
	remaining := segmentBytes
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		length := remaining
		if length > MaxReadRangeBytes {
			length = MaxReadRangeBytes
		}
		chunk, readErr := read(readOffset, length)
		if readErr != nil {
			return nil, fmt.Errorf("read WAVE sample range: %w", readErr)
		}
		copy(output[writeOffset:], chunk)
		writeOffset += len(chunk)
		readOffset += length
		remaining -= length
	}
	return output, nil
}

func parseWaveFormat(mimeType string, format []byte, dataBytes uint64) (AudioInfo, error) {
	codecTag := binary.LittleEndian.Uint16(format[0:2])
	channels := binary.LittleEndian.Uint16(format[2:4])
	sampleRate := binary.LittleEndian.Uint32(format[4:8])
	averageBytesPerSecond := binary.LittleEndian.Uint32(format[8:12])
	blockAlign := binary.LittleEndian.Uint16(format[12:14])
	bitsPerSample := binary.LittleEndian.Uint16(format[14:16])
	codec := ""
	switch codecTag {
	case 1:
		codec = "PCM"
	case 3:
		codec = "IEEE_FLOAT"
	default:
		return AudioInfo{}, fmt.Errorf("unsupported WAVE codec tag 0x%04x", codecTag)
	}
	if channels == 0 || channels > 256 || sampleRate == 0 || sampleRate > 768000 ||
		bitsPerSample == 0 || bitsPerSample%8 != 0 || blockAlign == 0 ||
		uint32(channels)*uint32(bitsPerSample)/8 != uint32(blockAlign) ||
		uint64(sampleRate)*uint64(blockAlign) != uint64(averageBytesPerSecond) || dataBytes%uint64(blockAlign) != 0 {
		return AudioInfo{}, fmt.Errorf("invalid WAVE PCM/float format dimensions or alignment")
	}
	if codec == "IEEE_FLOAT" && bitsPerSample != 32 && bitsPerSample != 64 {
		return AudioInfo{}, fmt.Errorf("IEEE float WAVE samples must be 32 or 64 bits")
	}
	if codec == "PCM" && bitsPerSample != 8 && bitsPerSample != 16 && bitsPerSample != 24 && bitsPerSample != 32 {
		return AudioInfo{}, fmt.Errorf("PCM WAVE samples must be 8, 16, 24 or 32 bits")
	}
	frames := dataBytes / uint64(blockAlign)
	durationMS := frames * 1000 / uint64(sampleRate)
	return AudioInfo{
		Format: "wav", MIMEType: strings.ToLower(mimeType), Codec: codec,
		Channels: channels, SampleRateHz: sampleRate, BitsPerSample: bitsPerSample,
		BlockAlignBytes: blockAlign, DataBytes: dataBytes, SampleFrames: frames,
		DurationMS: durationMS, HeaderValidated: true,
	}, nil
}

func isWaveMIME(mimeType string) bool {
	switch strings.ToLower(strings.TrimSpace(mimeType)) {
	case "audio/wav", "audio/wave", "audio/x-wav":
		return true
	default:
		return false
	}
}

func NewAudioInfoPayload(info AudioInfo) (TypedPayload, error) {
	encoded, err := json.Marshal(info)
	if err != nil {
		return TypedPayload{}, fmt.Errorf("encode audio info: %w", err)
	}
	if err := ValidateAudioInfo(info); err != nil {
		return TypedPayload{}, err
	}
	return NewTypedPayload(AudioInfoPayloadSchemaID, AudioInfoPayloadSchemaVersion, AudioInfoPayloadMediaType, encoded)
}

func DecodeAudioInfo(payload TypedPayload) (AudioInfo, error) {
	if payload.SchemaID != AudioInfoPayloadSchemaID || payload.SchemaVersion != AudioInfoPayloadSchemaVersion || payload.MediaType != AudioInfoPayloadMediaType {
		return AudioInfo{}, fmt.Errorf("%w: unsupported audio info schema %s@%d", ErrInvalidBlock, payload.SchemaID, payload.SchemaVersion)
	}
	if err := payload.Validate(); err != nil {
		return AudioInfo{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload.JSON))
	decoder.DisallowUnknownFields()
	var info AudioInfo
	if err := decoder.Decode(&info); err != nil {
		return AudioInfo{}, fmt.Errorf("%w: decode audio info: %v", ErrInvalidBlock, err)
	}
	if err := ValidateAudioInfo(info); err != nil {
		return AudioInfo{}, err
	}
	return info, nil
}

func validateAudioInfoJSON(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var info AudioInfo
	if err := decoder.Decode(&info); err != nil {
		return fmt.Errorf("%w: decode audio info payload: %v", ErrInvalidBlock, err)
	}
	return ValidateAudioInfo(info)
}

func ValidateAudioInfo(info AudioInfo) error {
	if info.Format != "wav" || !isWaveMIME(info.MIMEType) ||
		(info.Codec != "PCM" && info.Codec != "IEEE_FLOAT") ||
		info.Channels == 0 || info.Channels > 256 || info.SampleRateHz == 0 || info.SampleRateHz > 768000 ||
		info.BitsPerSample == 0 || info.BitsPerSample%8 != 0 || info.BlockAlignBytes == 0 ||
		uint32(info.Channels)*uint32(info.BitsPerSample)/8 != uint32(info.BlockAlignBytes) ||
		info.DataBytes > uint64(^uint32(0)) || info.DataBytes%uint64(info.BlockAlignBytes) != 0 ||
		info.SampleFrames != info.DataBytes/uint64(info.BlockAlignBytes) || !info.HeaderValidated {
		return fmt.Errorf("%w: audio format fields or validation scope are invalid", ErrInvalidBlock)
	}
	if info.Codec == "IEEE_FLOAT" && info.BitsPerSample != 32 && info.BitsPerSample != 64 {
		return fmt.Errorf("%w: IEEE float sample depth is invalid", ErrInvalidBlock)
	}
	if info.Codec == "PCM" && info.BitsPerSample != 8 && info.BitsPerSample != 16 && info.BitsPerSample != 24 && info.BitsPerSample != 32 {
		return fmt.Errorf("%w: PCM sample depth is invalid", ErrInvalidBlock)
	}
	expectedDuration := info.SampleFrames / uint64(info.SampleRateHz) * 1000
	expectedDuration += info.SampleFrames % uint64(info.SampleRateHz) * 1000 / uint64(info.SampleRateHz)
	if info.DurationMS != expectedDuration {
		return fmt.Errorf("%w: audio duration does not match frame count and sample rate", ErrInvalidBlock)
	}
	return nil
}
