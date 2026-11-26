package engine

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContentParseAudioRunsThroughSQLAndRejectsForgedWaveHeaders(t *testing.T) {
	engine := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, engine, "", "create database app")
	mustExecSQL(t, engine, "app", "create table audio_docs (id int primary key, media content)")
	mustExecSQL(t, engine, "app", "create table audio_blocks (id int primary key, audio_block content)")

	validWave := engineTestWave(1, 2, 16000)
	mustExecSQL(t, engine, "app", fmt.Sprintf("insert into audio_docs values (1, CONTENT_FROM_BYTES(X'%s', 'audio/wav'))", hex.EncodeToString(validWave)))
	mustExecSQL(t, engine, "app", "insert into audio_blocks select id, CONTENT_PARSE_AUDIO(media) from audio_docs where id = 1")
	rows := mustQuerySQL(t, engine, "app", "select CONTENT_BLOCK_STATE(b.audio_block), CONTENT_BLOCK_START_MS(b.audio_block), CONTENT_BLOCK_END_MS(b.audio_block), CONTENT_BLOCK_CONTEXT(b.audio_block, d.media, 'ALLOW_DECLARED_DERIVATION') from audio_docs d join audio_blocks b on d.id = b.id where d.id = 1")
	require.Len(t, rows, 1)
	require.Equal(t, []interface{}{"READY", "0", "1000"}, rows[0][:3])
	var contextItem struct {
		ValueState                  string `json:"value_state"`
		DerivationState             string `json:"derivation_state"`
		RequiresSourceModalityInput bool   `json:"requires_source_modality_input"`
		Kind                        string `json:"kind"`
		Source                      struct {
			MediaType   string `json:"media_type"`
			MediaFamily string `json:"media_family"`
		} `json:"source"`
		Attributes map[string]string `json:"attributes"`
	}
	require.NoError(t, json.Unmarshal([]byte(rows[0][3].(string)), &contextItem))
	require.Equal(t, "SOURCE_VERSION_MATCHED", contextItem.ValueState)
	require.Equal(t, "DECLARED_TRANSFORMATION_NOT_VERIFIED", contextItem.DerivationState)
	require.True(t, contextItem.RequiresSourceModalityInput)
	require.Equal(t, "audio_segment", contextItem.Kind)
	require.Equal(t, "audio/wav", contextItem.Source.MediaType)
	require.Equal(t, "audio", contextItem.Source.MediaFamily)
	require.Equal(t, "false", contextItem.Attributes["transcription_available"])

	var request GenerationRequest
	engine.QueryExecutor.SetGenerationRuntime(cancellationGenerationRuntime(func(_ context.Context, captured GenerationRequest) (GenerationResult, error) {
		request = captured
		return GenerationResult{ModelID: "audio-model", ModelVersion: "3", Text: "speech present", UsageKnown: true, InputTokens: 10, OutputTokens: 2}, nil
	}))
	descriptions := mustQuerySQL(t, engine, "app", "select AI_DESCRIBE_BLOCK_RESULT(b.audio_block, d.media, 'audio-model') from audio_docs d join audio_blocks b on d.id = b.id where d.id = 1")
	require.Len(t, descriptions, 1)
	require.Equal(t, "DESCRIBE", request.Task)
	require.Equal(t, "audio", request.Modality)
	require.Equal(t, "audio/wav", request.MIMEType)
	require.Equal(t, validWave, request.Media)
	require.EqualValues(t, 0, *request.Locator.StartMS)
	require.EqualValues(t, 1000, *request.Locator.EndMS)
	require.Equal(t, "app", request.Source.Schema)
	require.Equal(t, "audio_docs", request.Source.Table)
	require.Equal(t, "media", request.Source.Column)
	require.NotEmpty(t, request.Source.PrimaryKey)

	var descriptionResult struct {
		Text         string `json:"text"`
		ModelID      string `json:"model_id"`
		ModelVersion string `json:"model_version"`
		Modality     string `json:"modality"`
		MIMEType     string `json:"mime_type"`
		Source       struct {
			SHA256 string `json:"sha256"`
		} `json:"source"`
		BlockID string `json:"block_id"`
	}
	require.NoError(t, json.Unmarshal([]byte(descriptions[0][0].(string)), &descriptionResult))
	require.Equal(t, "speech present", descriptionResult.Text)
	require.Equal(t, "audio-model", descriptionResult.ModelID)
	require.Equal(t, "3", descriptionResult.ModelVersion)
	require.Equal(t, "audio", descriptionResult.Modality)
	require.Equal(t, "audio/wav", descriptionResult.MIMEType)
	require.NotEmpty(t, descriptionResult.Source.SHA256)
	require.NotEmpty(t, descriptionResult.BlockID)

	invalidWave := append([]byte(nil), validWave...)
	binary.LittleEndian.PutUint16(invalidWave[32:34], 1) // impossible block alignment for mono 16-bit PCM
	mustExecSQL(t, engine, "app", fmt.Sprintf("insert into audio_docs values (2, CONTENT_FROM_BYTES(X'%s', 'audio/wav'))", hex.EncodeToString(invalidWave)))
	insertResult := <-engine.ExecuteQuery(nil, "insert into audio_blocks select id, CONTENT_PARSE_AUDIO(media) from audio_docs where id = 2", "app")
	require.ErrorContains(t, insertResult.Err, "invalid WAVE PCM/float format dimensions or alignment")
	require.Empty(t, mustQuerySQL(t, engine, "app", "select id from audio_blocks where id = 2"), "failed parsing must not publish a derived row")
}

func engineTestWave(codec, channels uint16, frames uint32) []byte {
	const sampleRate = uint32(16000)
	const bitsPerSample = uint16(16)
	blockAlign := channels * bitsPerSample / 8
	format := make([]byte, 16)
	binary.LittleEndian.PutUint16(format[0:2], codec)
	binary.LittleEndian.PutUint16(format[2:4], channels)
	binary.LittleEndian.PutUint32(format[4:8], sampleRate)
	binary.LittleEndian.PutUint32(format[8:12], sampleRate*uint32(blockAlign))
	binary.LittleEndian.PutUint16(format[12:14], blockAlign)
	binary.LittleEndian.PutUint16(format[14:16], bitsPerSample)
	data := make([]byte, int(frames)*int(blockAlign))
	encoded := make([]byte, 12)
	copy(encoded[:4], "RIFF")
	copy(encoded[8:12], "WAVE")
	for _, chunk := range []struct {
		id   string
		data []byte
	}{{"fmt ", format}, {"data", data}} {
		header := make([]byte, 8)
		copy(header[:4], chunk.id)
		binary.LittleEndian.PutUint32(header[4:8], uint32(len(chunk.data)))
		encoded = append(encoded, header...)
		encoded = append(encoded, chunk.data...)
	}
	binary.LittleEndian.PutUint32(encoded[4:8], uint32(len(encoded)-8))
	return encoded
}
