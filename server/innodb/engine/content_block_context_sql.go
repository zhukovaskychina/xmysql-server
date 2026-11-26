package engine

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

// CONTENT_BLOCK_CONTEXT(block_column, source_content_column[, mode]) serializes
// one bounded evidence item. The default mode requires exact source byte-range
// equality. ALLOW_DECLARED_DERIVATION includes a located derived representation
// while explicitly marking that its contents were not verified against the
// original bytes and that modality-aware processing still needs the source.
func evaluateContentBlockContext(function *sqlparser.FuncExpr, values map[string]interface{}) (interface{}, error) {
	if function == nil || len(function.Exprs) < 2 || len(function.Exprs) > 3 {
		return nil, fmt.Errorf("CONTENT_BLOCK_CONTEXT requires a CONTENT_BLOCK, a source CONTENT column, and an optional mode")
	}
	blockFunction := *function
	blockFunction.Exprs = function.Exprs[:1]
	block, isNull, err := evaluateContentBlockArgument(&blockFunction, values)
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}
	sourceArgument, ok := function.Exprs[1].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("CONTENT_BLOCK_CONTEXT source must be a CONTENT column")
	}
	sourceColumn, ok := sourceArgument.Expr.(*sqlparser.ColName)
	if !ok {
		return nil, fmt.Errorf("CONTENT_BLOCK_CONTEXT source must be a CONTENT column")
	}
	sourceValue, err := evaluateExpressionWithRow(sourceColumn, values)
	if err != nil {
		return nil, err
	}
	if sourceValue == nil || block.State != content.BlockStateReady {
		return nil, nil
	}
	mode := "EXACT_SOURCE_RANGE"
	if len(function.Exprs) == 3 {
		modeArgument, ok := function.Exprs[2].(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("CONTENT_BLOCK_CONTEXT mode must be a string literal")
		}
		modeValue, ok := modeArgument.Expr.(*sqlparser.SQLVal)
		if !ok || modeValue.Type != sqlparser.StrVal {
			return nil, fmt.Errorf("CONTENT_BLOCK_CONTEXT mode must be a string literal")
		}
		mode = strings.ToUpper(strings.TrimSpace(string(modeValue.Val)))
		if mode != "EXACT_SOURCE_RANGE" && mode != "ALLOW_DECLARED_DERIVATION" {
			return nil, fmt.Errorf("CONTENT_BLOCK_CONTEXT mode must be EXACT_SOURCE_RANGE or ALLOW_DECLARED_DERIVATION")
		}
	}
	textEvidence := block.Kind == content.BlockText && block.Payload == nil
	structuredEvidence := (block.Kind == content.BlockTable || block.Kind == content.BlockStructured) && block.Payload != nil
	imageEvidence := block.Kind == content.BlockImageRegion && block.Payload != nil && block.Payload.SchemaID == content.ImageInfoPayloadSchemaID
	audioEvidence := block.Kind == content.BlockAudioSegment && block.Payload != nil && block.Payload.SchemaID == content.AudioInfoPayloadSchemaID
	videoEvidence := block.Kind == content.BlockVideoFrame || block.Kind == content.BlockVideoSegment
	if !textEvidence && !structuredEvidence && !imageEvidence && !audioEvidence && !videoEvidence {
		return nil, nil
	}
	var typedSource content.Value
	switch typed := sourceValue.(type) {
	case content.Value:
		typedSource = typed
	case *content.Value:
		if typed == nil {
			return nil, nil
		}
		typedSource = *typed
	default:
		return nil, fmt.Errorf("CONTENT_BLOCK_CONTEXT requires a typed CONTENT source; got %T", sourceValue)
	}
	if imageEvidence && typedSource.Kind != content.KindImage || audioEvidence && typedSource.Kind != content.KindAudio ||
		videoEvidence && (typedSource.Kind != content.KindVideo || block.Locator.StartMS == nil || block.Locator.EndMS == nil) {
		return nil, nil
	}
	rowContext, ok := contentSourceContextForColumn(values, sourceColumn)
	if !ok || len(rowContext.PrimaryKey) == 0 {
		return nil, fmt.Errorf("CONTENT_BLOCK_CONTEXT requires stored-row context for the source column")
	}
	sourceColumnName := contentSourcePhysicalColumnName(rowContext, sourceColumn.Name.String())
	actualSource, err := content.NewSourceRef(rowContext.Schema, rowContext.Table, sourceColumnName, rowContext.PrimaryKey, typedSource)
	if err != nil {
		return nil, fmt.Errorf("CONTENT_BLOCK_CONTEXT current source is invalid: %w", err)
	}
	if !block.Source.Matches(actualSource) {
		return nil, nil
	}
	if textEvidence && !utf8.ValidString(block.Text) {
		return nil, nil
	}
	exactSourceRange := false
	if block.Locator.ByteStart != nil && block.Locator.ByteEnd != nil && *block.Locator.ByteEnd >= *block.Locator.ByteStart &&
		*block.Locator.ByteEnd-*block.Locator.ByteStart <= content.MaxReadRangeBytes {
		store, storeErr := contentSegmentStoreForValue(values, typedSource)
		if storeErr != nil {
			return nil, storeErr
		}
		sourceRange, readErr := typedSource.ReadRangeFromStore(storedFunctionContext(values), store, *block.Locator.ByteStart, *block.Locator.ByteEnd-*block.Locator.ByteStart)
		if readErr != nil {
			return nil, fmt.Errorf("CONTENT_BLOCK_CONTEXT source range read failed: %w", readErr)
		}
		exactSourceRange = textEvidence && bytes.Equal(sourceRange, []byte(block.Text)) ||
			structuredEvidence && bytes.Equal(sourceRange, block.Payload.JSON)
	}
	if !exactSourceRange && mode != "ALLOW_DECLARED_DERIVATION" {
		return nil, nil
	}
	fullImage := imageEvidence && strings.EqualFold(block.Attributes["region_scope"], "full_image")
	fullAudio := audioEvidence && strings.EqualFold(block.Attributes["region_scope"], "full_audio")
	if !exactSourceRange && !contentLocatorHasPosition(block.Locator) && !fullImage && !fullAudio {
		return nil, nil
	}
	valueState, derivationState := "SOURCE_MATCHED", "EXACT_SOURCE_RANGE_MATCH"
	if !exactSourceRange {
		valueState, derivationState = "SOURCE_VERSION_MATCHED", "DECLARED_TRANSFORMATION_NOT_VERIFIED"
	}
	sourceContext := contentBlockContextSource{
		Schema: block.Source.Schema, Table: block.Source.Table, Column: block.Source.Column,
		PrimaryKey: hex.EncodeToString(block.Source.PrimaryKey), Revision: block.Source.Revision,
		SHA256: hex.EncodeToString(block.Source.Digest[:]),
	}
	var semanticDefinition *metadata.ColumnSemanticDefinition
	if definition, exists := rowContext.ColumnSemantics[strings.ToLower(strings.TrimSpace(sourceColumnName))]; exists {
		confirmed := definition
		semanticDefinition = &confirmed
	}
	if !exactSourceRange {
		size := typedSource.Size()
		sourceContext.MediaType = typedSource.MIMEType
		sourceContext.MediaFamily = string(typedSource.Kind)
		sourceContext.SizeBytes = &size
	}
	contextItem := struct {
		SchemaVersion               int                                `json:"schema_version"`
		EvidenceID                  string                             `json:"evidence_id"`
		ValueState                  string                             `json:"value_state"`
		DerivationState             string                             `json:"derivation_state"`
		RequiresSourceModalityInput bool                               `json:"requires_source_modality_input,omitempty"`
		Kind                        content.BlockKind                  `json:"kind"`
		Text                        string                             `json:"text,omitempty"`
		Payload                     *content.TypedPayload              `json:"payload,omitempty"`
		Source                      contentBlockContextSource          `json:"source"`
		Locator                     content.Locator                    `json:"locator"`
		ParentID                    string                             `json:"parent_id,omitempty"`
		Relation                    string                             `json:"relation,omitempty"`
		ProcessorID                 string                             `json:"processor_id"`
		ProcessorVersion            string                             `json:"processor_version"`
		ModelID                     string                             `json:"model_id,omitempty"`
		ModelVersion                string                             `json:"model_version,omitempty"`
		Attributes                  map[string]string                  `json:"attributes,omitempty"`
		SemanticDefinition          *metadata.ColumnSemanticDefinition `json:"confirmed_column_semantics,omitempty"`
		TableSemantics              *metadata.TableSemanticDefinition  `json:"confirmed_table_semantics,omitempty"`
	}{
		SchemaVersion:               1,
		EvidenceID:                  block.ID,
		ValueState:                  valueState,
		DerivationState:             derivationState,
		RequiresSourceModalityInput: !exactSourceRange,
		Kind:                        block.Kind,
		Text:                        block.Text,
		Payload:                     block.Payload,
		Source:                      sourceContext,
		Locator:                     block.Locator,
		ParentID:                    block.ParentID,
		Relation:                    block.Relation,
		ProcessorID:                 block.ProcessorID,
		ProcessorVersion:            block.ProcessorVersion,
		ModelID:                     block.ModelID,
		ModelVersion:                block.ModelVersion,
		Attributes:                  block.Attributes,
		SemanticDefinition:          semanticDefinition,
		TableSemantics:              rowContext.TableSemantics,
	}
	if !exactSourceRange {
		contextItem.SchemaVersion = 2
	}
	encoded, err := json.Marshal(contextItem)
	if err != nil {
		return nil, fmt.Errorf("CONTENT_BLOCK_CONTEXT encoding failed: %w", err)
	}
	if uint64(len(encoded)) > content.MaxReadRangeBytes {
		return nil, fmt.Errorf("CONTENT_BLOCK_CONTEXT item exceeds the %d byte limit", content.MaxReadRangeBytes)
	}
	return string(encoded), nil
}

// CONTENT_BLOCK_SOURCE_SLICE(block_column, source_content_column, offset, length)
// reads actual bytes from the exact source revision named by a located block.
// The range is bounded and is not interpreted as a cropped page or media clip.
func evaluateContentBlockSourceSlice(function *sqlparser.FuncExpr, values map[string]interface{}) (interface{}, error) {
	if function == nil || len(function.Exprs) != 4 {
		return nil, fmt.Errorf("CONTENT_BLOCK_SOURCE_SLICE requires a CONTENT_BLOCK, source CONTENT column, zero-based byte offset and length")
	}
	blockFunction := *function
	blockFunction.Exprs = function.Exprs[:1]
	block, isNull, err := evaluateContentBlockArgument(&blockFunction, values)
	if err != nil {
		return nil, err
	}
	if isNull || block.State != content.BlockStateReady {
		return nil, nil
	}
	sourceArgument, ok := function.Exprs[1].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("CONTENT_BLOCK_SOURCE_SLICE source must be a CONTENT column")
	}
	sourceColumn, ok := sourceArgument.Expr.(*sqlparser.ColName)
	if !ok {
		return nil, fmt.Errorf("CONTENT_BLOCK_SOURCE_SLICE source must be a CONTENT column")
	}
	sourceValue, err := evaluateExpressionWithRow(sourceColumn, values)
	if err != nil {
		return nil, err
	}
	if sourceValue == nil {
		return nil, nil
	}
	var typedSource content.Value
	switch typed := sourceValue.(type) {
	case content.Value:
		typedSource = typed
	case *content.Value:
		if typed == nil {
			return nil, nil
		}
		typedSource = *typed
	default:
		return nil, fmt.Errorf("CONTENT_BLOCK_SOURCE_SLICE requires a typed CONTENT source; got %T", sourceValue)
	}
	rowContext, ok := contentSourceContextForColumn(values, sourceColumn)
	if !ok || len(rowContext.PrimaryKey) == 0 {
		return nil, fmt.Errorf("CONTENT_BLOCK_SOURCE_SLICE requires stored-row context for the source column")
	}
	sourceColumnName := contentSourcePhysicalColumnName(rowContext, sourceColumn.Name.String())
	actualSource, err := content.NewSourceRef(rowContext.Schema, rowContext.Table, sourceColumnName, rowContext.PrimaryKey, typedSource)
	if err != nil {
		return nil, fmt.Errorf("CONTENT_BLOCK_SOURCE_SLICE current source is invalid: %w", err)
	}
	if !block.Source.Matches(actualSource) {
		return nil, nil
	}
	arguments := make([]interface{}, 2)
	for index := range arguments {
		argument, ok := function.Exprs[index+2].(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("CONTENT_BLOCK_SOURCE_SLICE offset and length must be scalar expressions")
		}
		arguments[index], err = evaluateExpressionWithRow(argument.Expr, values)
		if err != nil {
			return nil, err
		}
		if arguments[index] == nil {
			return nil, nil
		}
	}
	offset, err := contentRangeArgument(arguments[0], "offset")
	if err != nil {
		return nil, err
	}
	length, err := contentRangeArgument(arguments[1], "length")
	if err != nil {
		return nil, err
	}
	if length > content.MaxReadRangeBytes {
		return nil, fmt.Errorf("CONTENT_BLOCK_SOURCE_SLICE length exceeds %d bytes", content.MaxReadRangeBytes)
	}
	store, err := contentSegmentStoreForValue(values, typedSource)
	if err != nil {
		return nil, err
	}
	return typedSource.ReadRangeFromStore(storedFunctionContext(values), store, offset, length)
}

func contentLocatorHasPosition(locator content.Locator) bool {
	return locator.ByteStart != nil && locator.ByteEnd != nil || locator.Page != nil || locator.StartMS != nil && locator.EndMS != nil
}

type contentBlockContextSource struct {
	Schema      string  `json:"schema"`
	Table       string  `json:"table"`
	Column      string  `json:"column"`
	PrimaryKey  string  `json:"primary_key_hex"`
	Revision    uint64  `json:"revision"`
	SHA256      string  `json:"sha256"`
	MediaType   string  `json:"media_type,omitempty"`
	MediaFamily string  `json:"media_family,omitempty"`
	SizeBytes   *uint64 `json:"size_bytes,omitempty"`
}
