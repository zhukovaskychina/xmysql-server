package content

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

type waveTestChunk struct {
	id   string
	data []byte
}

func TestParseWaveFromStoreDerivesValidatedPCMMetadata(t *testing.T) {
	data := make([]byte, 16000) // 8 kHz, mono, 16-bit PCM: one second.
	value, source := waveTestValueAndSource(t, "audio/wav", waveTestFile(waveTestChunk{id: "fmt ", data: waveTestFormat(1, 1, 8000, 16)}, waveTestChunk{id: "data", data: data}))

	block, err := ParseWaveFromStore(context.Background(), nil, value, source)
	require.NoError(t, err)
	require.Equal(t, BlockStateReady, block.State)
	require.Equal(t, BlockAudioSegment, block.Kind)
	require.Equal(t, uint32(1), block.Ordinal)
	require.NotNil(t, block.Locator.StartMS)
	require.NotNil(t, block.Locator.EndMS)
	require.EqualValues(t, 0, *block.Locator.StartMS)
	require.EqualValues(t, 1000, *block.Locator.EndMS)
	require.Equal(t, "container_header_only", block.Attributes["validation_scope"])
	require.Equal(t, "false", block.Attributes["samples_decoded"])
	require.Equal(t, "false", block.Attributes["transcription_available"])

	info, err := DecodeAudioInfo(*block.Payload)
	require.NoError(t, err)
	require.Equal(t, AudioInfo{
		Format: "wav", MIMEType: "audio/wav", Codec: "PCM", Channels: 1,
		SampleRateHz: 8000, BitsPerSample: 16, BlockAlignBytes: 2,
		DataBytes: 16000, SampleFrames: 8000, DurationMS: 1000, HeaderValidated: true,
	}, info)
}

func TestParseWaveFromStoreRejectsForgedOrUnsupportedSources(t *testing.T) {
	validFormat := waveTestFormat(1, 1, 8000, 16)
	validData := []byte{0, 0}
	tooManyChunks := make([]waveTestChunk, maxWaveChunks+1)
	for index := range tooManyChunks {
		tooManyChunks[index] = waveTestChunk{id: "JUNK", data: []byte{}}
	}

	tests := []struct {
		name     string
		mimeType string
		chunks   []waveTestChunk
		mutate   func([]byte)
		want     string
	}{
		{name: "MIME mismatch", mimeType: "audio/mpeg", chunks: []waveTestChunk{{id: "fmt ", data: validFormat}, {id: "data", data: validData}}, want: "requires audio/wav"},
		{name: "missing fmt", mimeType: "audio/wav", chunks: []waveTestChunk{{id: "data", data: validData}}, want: "exactly one fmt and one data"},
		{name: "missing data", mimeType: "audio/wav", chunks: []waveTestChunk{{id: "fmt ", data: validFormat}}, want: "exactly one fmt and one data"},
		{name: "duplicate fmt", mimeType: "audio/wav", chunks: []waveTestChunk{{id: "fmt ", data: validFormat}, {id: "fmt ", data: validFormat}, {id: "data", data: validData}}, want: "fmt chunk is duplicate"},
		{name: "duplicate data", mimeType: "audio/wav", chunks: []waveTestChunk{{id: "fmt ", data: validFormat}, {id: "data", data: validData}, {id: "data", data: validData}}, want: "multiple WAVE data chunks"},
		{name: "unsupported compression", mimeType: "audio/wav", chunks: []waveTestChunk{{id: "fmt ", data: waveTestFormat(2, 1, 8000, 16)}, {id: "data", data: validData}}, want: "unsupported WAVE codec"},
		{name: "sample frame misalignment", mimeType: "audio/wav", chunks: []waveTestChunk{{id: "fmt ", data: validFormat}, {id: "data", data: []byte{0}}}, want: "invalid WAVE PCM/float format dimensions or alignment"},
		{name: "declared RIFF exceeds value", mimeType: "audio/wav", chunks: []waveTestChunk{{id: "fmt ", data: validFormat}, {id: "data", data: validData}}, mutate: func(encoded []byte) { binary.LittleEndian.PutUint32(encoded[4:8], uint32(len(encoded))) }, want: "RIFF size exceeds"},
		{name: "declared data exceeds RIFF", mimeType: "audio/wav", chunks: []waveTestChunk{{id: "fmt ", data: validFormat}, {id: "data", data: validData}}, mutate: func(encoded []byte) { binary.LittleEndian.PutUint32(encoded[40:44], 1000) }, want: "chunk exceeds RIFF bounds"},
		{name: "chunk limit", mimeType: "audio/wav", chunks: tooManyChunks, want: "chunk table is malformed or exceeds"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded := waveTestFile(test.chunks...)
			if test.mutate != nil {
				test.mutate(encoded)
			}
			value, source := waveTestValueAndSource(t, test.mimeType, encoded)
			_, err := ParseWaveFromStore(context.Background(), nil, value, source)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func waveTestValueAndSource(t *testing.T, mimeType string, encoded []byte) (Value, SourceRef) {
	t.Helper()
	value, err := NewValue(mimeType, 1, encoded)
	require.NoError(t, err)
	source, err := NewSourceRef("app", "audio_docs", "body", []byte{1}, value)
	require.NoError(t, err)
	return value, source
}

func waveTestFormat(codec uint16, channels uint16, sampleRate uint32, bitsPerSample uint16) []byte {
	format := make([]byte, 16)
	blockAlign := channels * bitsPerSample / 8
	binary.LittleEndian.PutUint16(format[0:2], codec)
	binary.LittleEndian.PutUint16(format[2:4], channels)
	binary.LittleEndian.PutUint32(format[4:8], sampleRate)
	binary.LittleEndian.PutUint32(format[8:12], sampleRate*uint32(blockAlign))
	binary.LittleEndian.PutUint16(format[12:14], blockAlign)
	binary.LittleEndian.PutUint16(format[14:16], bitsPerSample)
	return format
}

func waveTestFile(chunks ...waveTestChunk) []byte {
	encoded := make([]byte, 12)
	copy(encoded[:4], "RIFF")
	copy(encoded[8:12], "WAVE")
	for _, chunk := range chunks {
		header := make([]byte, 8)
		copy(header[:4], chunk.id)
		binary.LittleEndian.PutUint32(header[4:8], uint32(len(chunk.data)))
		encoded = append(encoded, header...)
		encoded = append(encoded, chunk.data...)
		if len(chunk.data)%2 != 0 {
			encoded = append(encoded, 0)
		}
	}
	binary.LittleEndian.PutUint32(encoded[4:8], uint32(len(encoded)-8))
	return encoded
}
