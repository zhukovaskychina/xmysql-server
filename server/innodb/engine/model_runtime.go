package engine

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/vector"
)

const MaxAIEmbeddingSourceBytes uint64 = 16 << 20
const MaxAIGenerationPromptBytes uint64 = 1 << 20
const MaxAIMediaSourceBytes uint64 = 16 << 20

// Model function result types are part of the SQL contract. Inferring them
// must never issue an inference request or consume a statement reservation.
func modelProjectionResultType(expression sqlparser.Expr) (metadata.DataType, bool) {
	function, ok := expression.(*sqlparser.FuncExpr)
	if !ok {
		return "", false
	}
	switch strings.ToUpper(strings.Trim(function.Name.String(), "` ")) {
	case "AI_EMBED", "AI_EMBED_BLOCK":
		return metadata.TypeVector, true
	case "AI_GENERATE", "AI_GENERATE_RESULT", "AI_SUMMARIZE", "AI_DESCRIBE_BLOCK", "AI_DESCRIBE_BLOCK_RESULT":
		return metadata.TypeVarchar, true
	default:
		return "", false
	}
}

// Constant SELECTs share the explicit model entry points used by table
// projections. Other expressions retain their existing evaluator.
func (e *XMySQLExecutor) executeConstantSelectWithModels(ctx *ExecutionContext, stmt *sqlparser.Select, sessionValues map[string]interface{}) (*SelectResult, error) {
	se := &SelectExecutor{embeddingRuntime: e.embeddingRuntimeSnapshot(), generationRuntime: e.generationRuntimeSnapshot()}
	if ctx != nil {
		se.modelContext = ctx.Context
	}
	return executeConstantSelectWithEvaluator(stmt, func(expr sqlparser.Expr, row map[string]interface{}) (interface{}, error) {
		if function, ok := expr.(*sqlparser.FuncExpr); ok {
			switch strings.ToUpper(strings.Trim(function.Name.String(), "` ")) {
			case "AI_EMBED", "AI_GENERATE", "AI_GENERATE_RESULT", "AI_SUMMARIZE":
				// The existing constant SELECT evaluator does not execute these
				// clauses. Do not issue a model call under silently ignored filters.
				if stmt.Where != nil || stmt.Having != nil || len(stmt.GroupBy) != 0 || len(stmt.OrderBy) != 0 || stmt.Limit != nil {
					return nil, fmt.Errorf("constant model SELECT with filtering, grouping, ordering or LIMIT is not supported")
				}
			}
			switch strings.ToUpper(strings.Trim(function.Name.String(), "` ")) {
			case "AI_EMBED":
				return evaluateAIEmbedSQL(se.embeddingRuntime, se.modelContext, function, row)
			case "AI_GENERATE", "AI_GENERATE_RESULT", "AI_SUMMARIZE":
				return se.evaluateAIGenerateSQL(function, row)
			}
		}
		return evaluateExpressionWithRow(expr, row)
	}, sessionValues)
}

type GenerationRequest struct {
	ModelAlias string
	Task       string
	Prompt     string
	Modality   string
	MIMEType   string
	Media      []byte
	Locator    content.Locator
	Source     content.SourceRef
	BlockID    string
	// SemanticDefinition is populated only from a validated CONFIRMED source
	// column definition; model suggestions are never promoted into this field.
	SemanticDefinition *metadata.ColumnSemanticDefinition
	// TableSemantics contains only confirmed source-table meaning and
	// relationships whose targets are visible to the current SQL session.
	TableSemantics *metadata.TableSemanticDefinition
}

type GenerationResult struct {
	ModelID      string
	ModelVersion string
	Text         string
	UsageKnown   bool
	InputTokens  uint64
	OutputTokens uint64
	RequestID    string
}

type GenerationRuntime interface {
	// Generate may receive text in Prompt or actual source bytes in Media.
	// For multimodal requests it must honor Modality, MIMEType and Locator,
	// consume the authorized input before returning, and honor cancellation.
	Generate(context.Context, GenerationRequest) (GenerationResult, error)
}

// EmbeddingRequest contains the actual authorized model input for one query or
// READY content block. Media is supplied as bytes, never as a database path or
// vector-only placeholder. Provider implementations must consume it before
// returning and honor context cancellation.
type EmbeddingRequest struct {
	ModelAlias string
	Modality   string
	Text       string
	MIMEType   string
	Media      []byte
	Locator    content.Locator
	Source     content.SourceRef
	BlockID    string
}

// EmbeddingResult records the provider's concrete model/version and its
// governed compatibility-space identity alongside the returned coordinates.
type EmbeddingResult struct {
	ModelID      string
	ModelVersion string
	SpaceID      string
	Elements     []float32
}

// EmbeddingRuntime is the replaceable database-internal inference adapter.
// Applications install one server-side runtime; regular SQL clients invoke it
// only through explicit AI_EMBED and AI_EMBED_BLOCK functions.
type EmbeddingRuntime interface {
	Embed(context.Context, EmbeddingRequest) (EmbeddingResult, error)
}

func (e *XMySQLExecutor) SetEmbeddingRuntime(runtime EmbeddingRuntime) {
	if e == nil {
		return
	}
	e.embeddingRuntimeMu.Lock()
	e.embeddingRuntime = runtime
	e.embeddingRuntimeMu.Unlock()
}

func (e *XMySQLExecutor) embeddingRuntimeSnapshot() EmbeddingRuntime {
	if e == nil {
		return nil
	}
	e.embeddingRuntimeMu.RLock()
	defer e.embeddingRuntimeMu.RUnlock()
	return e.embeddingRuntime
}

// evaluateAIEmbedSQL evaluates AI_EMBED(model_alias, input) for a query or
// projection vector. Text is sent as text; typed CONTENT is sent as explicit
// text or as bounded original media bytes with its declared modality and MIME.
// The result is intentionally unbound to a stored row and remains UNVERIFIED.
func evaluateAIEmbedSQL(runtime EmbeddingRuntime, modelContext context.Context, function *sqlparser.FuncExpr, values map[string]interface{}) (interface{}, error) {
	if function == nil || len(function.Exprs) != 2 {
		return nil, fmt.Errorf("AI_EMBED requires model alias and text")
	}
	if runtime == nil {
		return nil, fmt.Errorf("AI_EMBED is unavailable: no embedding runtime is configured")
	}
	arguments := make([]interface{}, 2)
	for index, expression := range function.Exprs {
		aliased, ok := expression.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("AI_EMBED arguments must be scalar expressions")
		}
		value, err := evaluateExpressionWithRow(aliased.Expr, values)
		if err != nil {
			return nil, err
		}
		arguments[index] = value
	}
	modelAlias, err := vectorStringArgument(arguments[0], "model alias")
	if err != nil || strings.TrimSpace(modelAlias) == "" {
		if err == nil {
			err = fmt.Errorf("model alias must not be empty")
		}
		return nil, fmt.Errorf("AI_EMBED: %w", err)
	}
	request := EmbeddingRequest{ModelAlias: strings.TrimSpace(modelAlias), Modality: "text"}
	switch input := arguments[1].(type) {
	case string, []byte:
		request.Text, err = vectorStringArgument(input, "text")
		if err != nil {
			return nil, fmt.Errorf("AI_EMBED: %w", err)
		}
		if uint64(len(request.Text)) > MaxAIEmbeddingSourceBytes {
			return nil, fmt.Errorf("AI_EMBED text size %d exceeds the %d byte limit", len(request.Text), MaxAIEmbeddingSourceBytes)
		}
		if !utf8.ValidString(request.Text) {
			return nil, fmt.Errorf("AI_EMBED text is not valid UTF-8")
		}
	case content.Value:
		request, err = embeddingRequestForContent(request.ModelAlias, input, values, ctxOrBackground(modelContext))
		if err != nil {
			return nil, fmt.Errorf("AI_EMBED CONTENT input: %w", err)
		}
	case *content.Value:
		if input == nil {
			return nil, fmt.Errorf("AI_EMBED CONTENT input must not be NULL")
		}
		request, err = embeddingRequestForContent(request.ModelAlias, *input, values, ctxOrBackground(modelContext))
		if err != nil {
			return nil, fmt.Errorf("AI_EMBED CONTENT input: %w", err)
		}
	case nil:
		return nil, fmt.Errorf("AI_EMBED input must not be NULL")
	default:
		return nil, fmt.Errorf("AI_EMBED input must be text or typed CONTENT; got %T", arguments[1])
	}
	ctx := ctxOrBackground(modelContext)
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("AI_EMBED cancelled before provider call: %w", err)
	}
	result, err := invokeEmbedding(ctx, runtime, request)
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, fmt.Errorf("AI_EMBED cancelled during provider call: %w", contextErr)
	}
	if err != nil {
		return nil, fmt.Errorf("AI_EMBED provider call failed: %w", err)
	}
	value, err := vector.NewValue(vector.Space{ModelID: result.ModelID, ModelVersion: result.ModelVersion, ID: result.SpaceID}, request.Modality, result.Elements)
	if err != nil {
		return nil, fmt.Errorf("AI_EMBED provider returned an invalid vector: %w", err)
	}
	return value, nil
}

func embeddingRequestForContent(modelAlias string, input content.Value, values map[string]interface{}, ctx context.Context) (EmbeddingRequest, error) {
	if err := input.Validate(); err != nil {
		return EmbeddingRequest{}, fmt.Errorf("invalid CONTENT value: %w", err)
	}
	if input.Size() == 0 || input.Size() > MaxAIEmbeddingSourceBytes {
		return EmbeddingRequest{}, fmt.Errorf("CONTENT size must be between 1 and %d bytes; got %d", MaxAIEmbeddingSourceBytes, input.Size())
	}
	request := EmbeddingRequest{ModelAlias: strings.TrimSpace(modelAlias), MIMEType: input.MIMEType}
	if content.IsTextMIMEType(input.MIMEType) {
		store, err := contentSegmentStoreForValue(values, input)
		if err != nil {
			return EmbeddingRequest{}, err
		}
		text, err := content.ExtractTextFromStore(ctx, store, input)
		if err != nil {
			return EmbeddingRequest{}, err
		}
		request.Modality, request.Text = "text", text
		return request, nil
	}
	switch input.Kind {
	case content.KindImage:
		request.Modality = "image"
	case content.KindAudio:
		request.Modality = "audio"
	case content.KindVideo:
		request.Modality = "video"
	case content.KindDocument:
		request.Modality = "document"
	default:
		return EmbeddingRequest{}, fmt.Errorf("unsupported source modality %q", input.Kind)
	}
	store, err := contentSegmentStoreForValue(values, input)
	if err != nil {
		return EmbeddingRequest{}, err
	}
	media, err := input.ReadAllFromStore(ctx, store, MaxAIEmbeddingSourceBytes)
	if err != nil {
		return EmbeddingRequest{}, fmt.Errorf("read actual CONTENT bytes: %w", err)
	}
	request.Media = media
	return request, nil
}

func (e *XMySQLExecutor) SetGenerationRuntime(runtime GenerationRuntime) {
	if e == nil {
		return
	}
	e.embeddingRuntimeMu.Lock()
	e.generationRuntime = runtime
	e.embeddingRuntimeMu.Unlock()
}

func (e *XMySQLExecutor) generationRuntimeSnapshot() GenerationRuntime {
	if e == nil {
		return nil
	}
	e.embeddingRuntimeMu.RLock()
	defer e.embeddingRuntimeMu.RUnlock()
	return e.generationRuntime
}

func (se *SelectExecutor) evaluateAIGenerateSQL(function *sqlparser.FuncExpr, values map[string]interface{}) (interface{}, error) {
	if function == nil || len(function.Exprs) != 2 {
		return nil, fmt.Errorf("%s requires prompt and model alias", function.Name.String())
	}
	if se == nil || se.generationRuntime == nil {
		return nil, fmt.Errorf("%s is unavailable: no generation runtime is configured", function.Name.String())
	}
	arguments := make([]interface{}, 2)
	for index, expression := range function.Exprs {
		aliased, ok := expression.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("%s arguments must be scalar expressions", function.Name.String())
		}
		value, err := evaluateExpressionWithRow(aliased.Expr, values)
		if err != nil {
			return nil, err
		}
		arguments[index] = value
	}
	prompt, err := se.aiPromptText(arguments[0], values)
	if err != nil {
		return nil, fmt.Errorf("%s prompt: %w", function.Name.String(), err)
	}
	modelAlias, err := vectorStringArgument(arguments[1], "model alias")
	if err != nil {
		return nil, fmt.Errorf("%s: %w", function.Name.String(), err)
	}
	modelAlias = strings.TrimSpace(modelAlias)
	if modelAlias == "" {
		return nil, fmt.Errorf("%s model alias must not be empty", function.Name.String())
	}
	task := "GENERATE"
	functionName := strings.ToUpper(strings.Trim(function.Name.String(), "` "))
	if functionName == "AI_SUMMARIZE" {
		task = "SUMMARIZE"
	} else if functionName == "AI_DESCRIBE_BLOCK" || functionName == "AI_DESCRIBE_BLOCK_RESULT" {
		task = "DESCRIBE"
	}
	ctx := ctxOrBackground(se.modelContext)
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%s cancelled before provider call: %w", function.Name.String(), err)
	}
	result, err := invokeGeneration(ctx, se.generationRuntime, GenerationRequest{
		ModelAlias: strings.TrimSpace(modelAlias), Task: task, Prompt: prompt,
	})
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, fmt.Errorf("%s cancelled during provider call: %w", function.Name.String(), contextErr)
	}
	if err != nil {
		return nil, fmt.Errorf("%s provider call failed: %w", function.Name.String(), err)
	}
	if strings.TrimSpace(result.ModelID) == "" || strings.TrimSpace(result.ModelVersion) == "" {
		return nil, fmt.Errorf("%s provider did not identify its concrete model and version", function.Name.String())
	}
	if !utf8.ValidString(result.Text) {
		return nil, fmt.Errorf("%s provider returned invalid UTF-8 text", function.Name.String())
	}
	if functionName == "AI_GENERATE_RESULT" {
		return encodeAIResultJSON(function.Name.String(), result)
	}
	return result.Text, nil
}

func (se *SelectExecutor) evaluateAIDescribeBlockSQL(function *sqlparser.FuncExpr, values map[string]interface{}) (interface{}, error) {
	if function == nil || len(function.Exprs) != 3 {
		return nil, fmt.Errorf("%s requires a CONTENT_BLOCK column, a source CONTENT column, and a model alias", function.Name.String())
	}
	if se == nil || se.generationRuntime == nil {
		return nil, fmt.Errorf("%s is unavailable: no generation runtime is configured", function.Name.String())
	}
	blockArgument, blockOK := function.Exprs[0].(*sqlparser.AliasedExpr)
	sourceArgument, sourceOK := function.Exprs[1].(*sqlparser.AliasedExpr)
	modelArgument, modelOK := function.Exprs[2].(*sqlparser.AliasedExpr)
	if !blockOK || !sourceOK || !modelOK {
		return nil, fmt.Errorf("%s arguments must be scalar expressions", function.Name.String())
	}
	if _, ok := blockArgument.Expr.(*sqlparser.ColName); !ok {
		return nil, fmt.Errorf("%s block argument must be a column", function.Name.String())
	}
	sourceColumn, ok := sourceArgument.Expr.(*sqlparser.ColName)
	if !ok {
		return nil, fmt.Errorf("%s source argument must be a column", function.Name.String())
	}
	block, isNull, err := evaluateContentBlockArgument(&sqlparser.FuncExpr{
		Name: sqlparser.NewColIdent(function.Name.String()), Exprs: sqlparser.SelectExprs{blockArgument},
	}, values)
	if err != nil || isNull {
		return nil, err
	}
	if block.State != content.BlockStateReady {
		return nil, fmt.Errorf("%s requires a READY content block", function.Name.String())
	}
	sourceValue, err := evaluateExpressionWithRow(sourceColumn, values)
	if err != nil {
		return nil, err
	}
	var source content.Value
	switch typed := sourceValue.(type) {
	case content.Value:
		source = typed
	case *content.Value:
		if typed == nil {
			return nil, fmt.Errorf("%s source CONTENT must not be NULL", function.Name.String())
		}
		source = *typed
	default:
		return nil, fmt.Errorf("%s source must be typed CONTENT; got %T", function.Name.String(), sourceValue)
	}
	rowContext, ok := contentSourceContextForColumn(values, sourceColumn)
	if !ok || len(rowContext.PrimaryKey) == 0 {
		return nil, fmt.Errorf("%s requires stored-row context for the source column", function.Name.String())
	}
	actualSource, err := content.NewSourceRef(rowContext.Schema, rowContext.Table, sourceColumn.Name.String(), rowContext.PrimaryKey, source)
	if err != nil {
		return nil, fmt.Errorf("%s source verification failed: %w", function.Name.String(), err)
	}
	if !block.Source.Matches(actualSource) {
		return nil, fmt.Errorf("%s block does not match the current source revision", function.Name.String())
	}
	modelValue, err := evaluateExpressionWithRow(modelArgument.Expr, values)
	if err != nil {
		return nil, err
	}
	modelAlias, err := vectorStringArgument(modelValue, "model alias")
	if err != nil || strings.TrimSpace(modelAlias) == "" {
		if err == nil {
			err = fmt.Errorf("model alias must not be empty")
		}
		return nil, fmt.Errorf("%s: %w", function.Name.String(), err)
	}
	request := GenerationRequest{
		ModelAlias: strings.TrimSpace(modelAlias), Task: "DESCRIBE", MIMEType: source.MIMEType,
		Locator: block.Locator, Source: actualSource, BlockID: block.ID,
	}
	if definition, exists := rowContext.ColumnSemantics[strings.ToLower(strings.TrimSpace(sourceColumn.Name.String()))]; exists {
		confirmed := definition
		request.SemanticDefinition = &confirmed
	}
	request.TableSemantics = rowContext.TableSemantics
	switch block.Kind {
	case content.BlockText, content.BlockTable, content.BlockStructured:
		text, available, err := content.BlockRetrievalText(block)
		if err != nil {
			return nil, fmt.Errorf("%s cannot prepare text input: %w", function.Name.String(), err)
		}
		if !available {
			return nil, fmt.Errorf("%s block has no text representation", function.Name.String())
		}
		request.Modality, request.Prompt = "text", text
	case content.BlockImageRegion:
		if source.Kind != content.KindImage {
			return nil, fmt.Errorf("%s image block is bound to non-image CONTENT (%s)", function.Name.String(), source.Kind)
		}
		if block.Locator.Region == nil && !strings.EqualFold(block.Attributes["region_scope"], "full_image") {
			return nil, fmt.Errorf("%s image block requires a region locator or full_image scope", function.Name.String())
		}
		request.Modality = "image"
	case content.BlockAudioSegment:
		if source.Kind != content.KindAudio {
			return nil, fmt.Errorf("%s audio block is bound to non-audio CONTENT (%s)", function.Name.String(), source.Kind)
		}
		if block.Locator.StartMS == nil || block.Locator.EndMS == nil {
			return nil, fmt.Errorf("%s audio block requires a time-range locator", function.Name.String())
		}
		request.Modality = "audio"
	case content.BlockVideoFrame, content.BlockVideoSegment:
		if source.Kind != content.KindVideo {
			return nil, fmt.Errorf("%s video frame is bound to non-video CONTENT (%s)", function.Name.String(), source.Kind)
		}
		if block.Locator.StartMS == nil || block.Locator.EndMS == nil {
			return nil, fmt.Errorf("%s video frame requires a time-range locator", function.Name.String())
		}
		request.Modality = "video"
	default:
		return nil, fmt.Errorf("%s does not support block kind %q", function.Name.String(), block.Kind)
	}
	if request.Modality != "text" {
		ctx := ctxOrBackground(se.modelContext)
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("%s cancelled before media read: %w", function.Name.String(), err)
		}
		if source.Size() == 0 || source.Size() > MaxAIMediaSourceBytes {
			return nil, fmt.Errorf("%s media source must be between 1 and %d bytes; got %d", function.Name.String(), MaxAIMediaSourceBytes, source.Size())
		}
		store, err := contentSegmentStoreForValue(values, source)
		if err != nil {
			return nil, fmt.Errorf("%s cannot open source media: %w", function.Name.String(), err)
		}
		request.Media, err = source.ReadAllFromStore(ctx, store, MaxAIMediaSourceBytes)
		if err != nil {
			return nil, fmt.Errorf("%s cannot read bounded source media: %w", function.Name.String(), err)
		}
	}
	ctx := ctxOrBackground(se.modelContext)
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%s cancelled before provider call: %w", function.Name.String(), err)
	}
	result, err := invokeGeneration(ctx, se.generationRuntime, request)
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, fmt.Errorf("%s cancelled during provider call: %w", function.Name.String(), contextErr)
	}
	if err != nil {
		return nil, fmt.Errorf("%s provider call failed: %w", function.Name.String(), err)
	}
	if strings.TrimSpace(result.ModelID) == "" || strings.TrimSpace(result.ModelVersion) == "" || !utf8.ValidString(result.Text) {
		return nil, fmt.Errorf("%s provider returned an invalid model identity or non-UTF-8 output", function.Name.String())
	}
	if strings.EqualFold(strings.Trim(function.Name.String(), "` "), "AI_DESCRIBE_BLOCK_RESULT") {
		return encodeAIDescribeResultJSON(function.Name.String(), result, request)
	}
	return result.Text, nil
}

func encodeAIDescribeResultJSON(functionName string, result GenerationResult, request GenerationRequest) (interface{}, error) {
	encoded, err := json.Marshal(struct {
		Text          string                             `json:"text"`
		ModelID       string                             `json:"model_id"`
		ModelVersion  string                             `json:"model_version"`
		UsageKnown    bool                               `json:"usage_known"`
		InputTokens   uint64                             `json:"input_tokens"`
		OutputTokens  uint64                             `json:"output_tokens"`
		RequestID     string                             `json:"request_id,omitempty"`
		Source        contentBlockContextSource          `json:"source"`
		BlockID       string                             `json:"block_id"`
		Modality      string                             `json:"modality"`
		MIMEType      string                             `json:"mime_type"`
		Locator       content.Locator                    `json:"locator"`
		Semantic      *metadata.ColumnSemanticDefinition `json:"confirmed_column_semantics,omitempty"`
		TableSemantic *metadata.TableSemanticDefinition  `json:"confirmed_table_semantics,omitempty"`
	}{
		Text: result.Text, ModelID: result.ModelID, ModelVersion: result.ModelVersion,
		UsageKnown: result.UsageKnown, InputTokens: result.InputTokens,
		OutputTokens: result.OutputTokens, RequestID: result.RequestID,
		Source: contentBlockContextSource{
			Schema: request.Source.Schema, Table: request.Source.Table, Column: request.Source.Column,
			PrimaryKey: hex.EncodeToString(request.Source.PrimaryKey), Revision: request.Source.Revision,
			SHA256: hex.EncodeToString(request.Source.Digest[:]), MediaType: request.MIMEType,
			MediaFamily: request.Modality,
		},
		BlockID: request.BlockID, Modality: request.Modality,
		MIMEType: request.MIMEType, Locator: request.Locator,
		Semantic:      request.SemanticDefinition,
		TableSemantic: request.TableSemantics,
	})
	if err != nil {
		return nil, fmt.Errorf("%s result encoding failed: %w", functionName, err)
	}
	return string(encoded), nil
}

func encodeAIResultJSON(functionName string, result GenerationResult) (interface{}, error) {
	encoded, err := json.Marshal(struct {
		Text         string `json:"text"`
		ModelID      string `json:"model_id"`
		ModelVersion string `json:"model_version"`
		UsageKnown   bool   `json:"usage_known"`
		InputTokens  uint64 `json:"input_tokens"`
		OutputTokens uint64 `json:"output_tokens"`
		RequestID    string `json:"request_id,omitempty"`
	}{
		Text: result.Text, ModelID: result.ModelID, ModelVersion: result.ModelVersion, UsageKnown: result.UsageKnown,
		InputTokens: result.InputTokens, OutputTokens: result.OutputTokens, RequestID: result.RequestID,
	})
	if err != nil {
		return nil, fmt.Errorf("%s result encoding failed: %w", functionName, err)
	}
	return string(encoded), nil
}

func (se *SelectExecutor) aiPromptText(value interface{}, values map[string]interface{}) (string, error) {
	var prompt string
	switch typed := value.(type) {
	case string:
		prompt = typed
	case []byte:
		prompt = string(typed)
	case content.Value:
		text, err := se.extractAIPromptContent(typed, values)
		if err != nil {
			return "", err
		}
		prompt = text
	case *content.Value:
		if typed == nil {
			return "", fmt.Errorf("value is NULL")
		}
		text, err := se.extractAIPromptContent(*typed, values)
		if err != nil {
			return "", err
		}
		prompt = text
	default:
		return "", fmt.Errorf("expected text or textual CONTENT, got %T", value)
	}
	if uint64(len(prompt)) > MaxAIGenerationPromptBytes {
		return "", fmt.Errorf("prompt size %d exceeds the %d byte limit", len(prompt), MaxAIGenerationPromptBytes)
	}
	if !utf8.ValidString(prompt) {
		return "", fmt.Errorf("prompt is not valid UTF-8")
	}
	return prompt, nil
}

func (se *SelectExecutor) extractAIPromptContent(value content.Value, values map[string]interface{}) (string, error) {
	if value.Size() > MaxAIGenerationPromptBytes {
		return "", fmt.Errorf("CONTENT prompt size %d exceeds the %d byte limit", value.Size(), MaxAIGenerationPromptBytes)
	}
	if value.ExternalRef == nil {
		return content.ExtractText(value)
	}
	store, err := contentSegmentStoreForValue(values, value)
	if err != nil {
		return "", err
	}
	return content.ExtractTextFromStore(ctxOrBackground(se.modelContext), store, value)
}

func (se *SelectExecutor) evaluateAIEmbedBlock(functionName, modelAlias string, block content.Block, source content.Value, actualSource content.SourceRef, values map[string]interface{}) (vector.Value, error) {
	if se == nil || se.embeddingRuntime == nil {
		return vector.Value{}, fmt.Errorf("%s is unavailable: no embedding runtime is configured", functionName)
	}
	if block.State != content.BlockStateReady || !block.Source.Matches(actualSource) {
		return vector.Value{}, fmt.Errorf("%s requires a READY content block for the current source revision", functionName)
	}
	modelAlias = strings.TrimSpace(modelAlias)
	if modelAlias == "" {
		return vector.Value{}, fmt.Errorf("%s model alias must not be empty", functionName)
	}
	request := EmbeddingRequest{
		ModelAlias: modelAlias, Locator: block.Locator, Source: actualSource,
		BlockID: block.ID, MIMEType: source.MIMEType,
	}
	switch block.Kind {
	case content.BlockText, content.BlockTable, content.BlockStructured:
		text, available, err := content.BlockRetrievalText(block)
		if err != nil {
			return vector.Value{}, fmt.Errorf("%s cannot prepare text input: %w", functionName, err)
		}
		if !available {
			return vector.Value{}, fmt.Errorf("%s block has no text representation for embedding", functionName)
		}
		request.Modality = "text"
		request.Text = text
	case content.BlockImageRegion:
		request.Modality = "image"
	case content.BlockAudioSegment:
		request.Modality = "audio"
	case content.BlockVideoFrame, content.BlockVideoSegment:
		request.Modality = "video"
	default:
		return vector.Value{}, fmt.Errorf("%s does not support block kind %q", functionName, block.Kind)
	}
	if request.Modality != "text" {
		if source.Size() == 0 || source.Size() > MaxAIEmbeddingSourceBytes {
			return vector.Value{}, fmt.Errorf("%s media source must be between 1 and %d bytes; got %d", functionName, MaxAIEmbeddingSourceBytes, source.Size())
		}
		store, err := contentSegmentStoreForValue(values, source)
		if err != nil {
			return vector.Value{}, fmt.Errorf("%s cannot open source media: %w", functionName, err)
		}
		media, err := source.ReadAllFromStore(ctxOrBackground(se.modelContext), store, MaxAIEmbeddingSourceBytes)
		if err != nil {
			return vector.Value{}, fmt.Errorf("%s cannot read bounded source media: %w", functionName, err)
		}
		request.Media = media
	}
	ctx := ctxOrBackground(se.modelContext)
	if err := ctx.Err(); err != nil {
		return vector.Value{}, fmt.Errorf("%s cancelled before provider call: %w", functionName, err)
	}
	result, err := invokeEmbedding(ctx, se.embeddingRuntime, request)
	if contextErr := ctx.Err(); contextErr != nil {
		return vector.Value{}, fmt.Errorf("%s cancelled during provider call: %w", functionName, contextErr)
	}
	if err != nil {
		return vector.Value{}, fmt.Errorf("%s provider call failed: %w", functionName, err)
	}
	return vector.NewBlockBoundValue(vector.Space{
		ModelID: result.ModelID, ModelVersion: result.ModelVersion, ID: result.SpaceID,
	}, request.Modality, result.Elements, actualSource, block.ID)
}

func (se *SelectExecutor) evaluateAIEmbedBlockSQL(function *sqlparser.FuncExpr, values map[string]interface{}) (interface{}, error) {
	if function == nil || len(function.Exprs) != 3 {
		return nil, fmt.Errorf("AI_EMBED_BLOCK requires a CONTENT_BLOCK column, a source CONTENT column, and a model alias")
	}
	blockArgument, blockOK := function.Exprs[0].(*sqlparser.AliasedExpr)
	sourceArgument, sourceOK := function.Exprs[1].(*sqlparser.AliasedExpr)
	modelArgument, modelOK := function.Exprs[2].(*sqlparser.AliasedExpr)
	if !blockOK || !sourceOK || !modelOK {
		return nil, fmt.Errorf("AI_EMBED_BLOCK arguments must be scalar expressions")
	}
	_, blockColumnOK := blockArgument.Expr.(*sqlparser.ColName)
	sourceColumn, sourceColumnOK := sourceArgument.Expr.(*sqlparser.ColName)
	if !blockColumnOK || !sourceColumnOK {
		return nil, fmt.Errorf("AI_EMBED_BLOCK block and source arguments must be columns")
	}
	block, isNull, err := evaluateContentBlockArgument(&sqlparser.FuncExpr{
		Name: sqlparser.NewColIdent(function.Name.String()), Exprs: sqlparser.SelectExprs{blockArgument},
	}, values)
	if err != nil || isNull {
		return nil, err
	}
	if block.State != content.BlockStateReady {
		return nil, fmt.Errorf("AI_EMBED_BLOCK requires a READY content block")
	}
	sourceValue, err := evaluateExpressionWithRow(sourceColumn, values)
	if err != nil {
		return nil, err
	}
	var source content.Value
	switch typed := sourceValue.(type) {
	case content.Value:
		source = typed
	case *content.Value:
		if typed == nil {
			return nil, fmt.Errorf("AI_EMBED_BLOCK source CONTENT must not be NULL")
		}
		source = *typed
	default:
		return nil, fmt.Errorf("AI_EMBED_BLOCK source must be typed CONTENT; got %T", sourceValue)
	}
	rowContext, ok := contentSourceContextForColumn(values, sourceColumn)
	if !ok || len(rowContext.PrimaryKey) == 0 {
		return nil, fmt.Errorf("AI_EMBED_BLOCK requires stored-row context for the source column")
	}
	actualSource, err := content.NewSourceRef(rowContext.Schema, rowContext.Table, sourceColumn.Name.String(), rowContext.PrimaryKey, source)
	if err != nil {
		return nil, fmt.Errorf("AI_EMBED_BLOCK source verification failed: %w", err)
	}
	if !block.Source.Matches(actualSource) {
		return nil, fmt.Errorf("AI_EMBED_BLOCK block does not match the current source revision")
	}
	modelValue, err := evaluateExpressionWithRow(modelArgument.Expr, values)
	if err != nil {
		return nil, err
	}
	modelAlias, err := vectorStringArgument(modelValue, "model alias")
	if err != nil {
		return nil, fmt.Errorf("AI_EMBED_BLOCK: %w", err)
	}
	return se.evaluateAIEmbedBlock(function.Name.String(), modelAlias, block, source, actualSource, values)
}

func ctxOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
