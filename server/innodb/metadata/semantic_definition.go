package metadata

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

const ColumnSemanticCommentPrefix = "xmysql-semantic:v1:"

// ColumnSemanticDefinition is an opt-in, machine-readable interpretation of
// a column. It is stored inside the ordinary column COMMENT so schema changes
// and metadata persistence continue to use the database's existing DDL path.
// Suggested model output is explicitly distinct from confirmed definitions.
type ColumnSemanticDefinition struct {
	Revision     uint64            `json:"revision"`
	Status       string            `json:"status"`
	Source       string            `json:"source"`
	ConfirmedBy  string            `json:"confirmed_by,omitempty"`
	BusinessName string            `json:"business_name,omitempty"`
	Description  string            `json:"description,omitempty"`
	Unit         string            `json:"unit,omitempty"`
	NullMeaning  string            `json:"null_meaning,omitempty"`
	Scope        string            `json:"scope,omitempty"`
	EnumMeanings map[string]string `json:"enum_meanings,omitempty"`
	Confidence   *float64          `json:"confidence,omitempty"`
}

type TableSemanticDefinition struct {
	Revision      uint64                 `json:"revision"`
	Status        string                 `json:"status"`
	Source        string                 `json:"source"`
	ConfirmedBy   string                 `json:"confirmed_by,omitempty"`
	BusinessName  string                 `json:"business_name,omitempty"`
	Description   string                 `json:"description,omitempty"`
	Relationships []SemanticRelationship `json:"relationships,omitempty"`
}

type SemanticRelationship struct {
	Name          string   `json:"name"`
	TargetSchema  string   `json:"target_schema,omitempty"`
	TargetTable   string   `json:"target_table"`
	SourceColumns []string `json:"source_columns"`
	TargetColumns []string `json:"target_columns"`
	Cardinality   string   `json:"cardinality"`
	Meaning       string   `json:"meaning,omitempty"`
	Status        string   `json:"status"`
	Source        string   `json:"source"`
	ConfirmedBy   string   `json:"confirmed_by,omitempty"`
}

// ParseColumnSemanticComment parses an opt-in semantic definition. Plain
// MySQL comments remain ordinary comments and are never treated as facts.
func ParseColumnSemanticComment(comment string) (*ColumnSemanticDefinition, bool, error) {
	if !strings.HasPrefix(comment, ColumnSemanticCommentPrefix) {
		return nil, false, nil
	}
	payload := strings.TrimSpace(strings.TrimPrefix(comment, ColumnSemanticCommentPrefix))
	if payload == "" || len(payload) > 8192 {
		return nil, true, fmt.Errorf("semantic definition payload must contain 1 to 8192 bytes")
	}
	decoder := json.NewDecoder(bytes.NewBufferString(payload))
	decoder.DisallowUnknownFields()
	var definition ColumnSemanticDefinition
	if err := decoder.Decode(&definition); err != nil {
		return nil, true, fmt.Errorf("invalid semantic definition JSON: %w", err)
	}
	if err := decoder.Decode(new(interface{})); err != io.EOF {
		if err == nil {
			return nil, true, fmt.Errorf("semantic definition contains trailing JSON data")
		}
		return nil, true, fmt.Errorf("invalid semantic definition trailing data: %w", err)
	}
	definition.Status = strings.ToUpper(strings.TrimSpace(definition.Status))
	definition.Source = strings.ToUpper(strings.TrimSpace(definition.Source))
	if err := definition.Validate(); err != nil {
		return nil, true, err
	}
	return &definition, true, nil
}

func FormatColumnSemanticComment(definition ColumnSemanticDefinition) (string, error) {
	definition.Status = strings.ToUpper(strings.TrimSpace(definition.Status))
	definition.Source = strings.ToUpper(strings.TrimSpace(definition.Source))
	if err := definition.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(definition)
	if err != nil {
		return "", fmt.Errorf("encode semantic definition: %w", err)
	}
	if len(payload) > 8192 {
		return "", fmt.Errorf("semantic definition payload exceeds 8192 bytes")
	}
	return ColumnSemanticCommentPrefix + string(payload), nil
}

func ParseTableSemanticComment(comment string) (*TableSemanticDefinition, bool, error) {
	if !strings.HasPrefix(comment, ColumnSemanticCommentPrefix) {
		return nil, false, nil
	}
	payload := strings.TrimSpace(strings.TrimPrefix(comment, ColumnSemanticCommentPrefix))
	if payload == "" || len(payload) > 8192 {
		return nil, true, fmt.Errorf("semantic definition payload must contain 1 to 8192 bytes")
	}
	decoder := json.NewDecoder(bytes.NewBufferString(payload))
	decoder.DisallowUnknownFields()
	var definition TableSemanticDefinition
	if err := decoder.Decode(&definition); err != nil {
		return nil, true, fmt.Errorf("invalid table semantic definition JSON: %w", err)
	}
	if err := decoder.Decode(new(interface{})); err != io.EOF {
		if err == nil {
			return nil, true, fmt.Errorf("table semantic definition contains trailing JSON data")
		}
		return nil, true, fmt.Errorf("invalid table semantic definition trailing data: %w", err)
	}
	definition.Status = strings.ToUpper(strings.TrimSpace(definition.Status))
	definition.Source = strings.ToUpper(strings.TrimSpace(definition.Source))
	if err := definition.Validate(); err != nil {
		return nil, true, err
	}
	return &definition, true, nil
}

func FormatTableSemanticComment(definition TableSemanticDefinition) (string, error) {
	definition.Status = strings.ToUpper(strings.TrimSpace(definition.Status))
	definition.Source = strings.ToUpper(strings.TrimSpace(definition.Source))
	definition.Relationships = append([]SemanticRelationship(nil), definition.Relationships...)
	for index := range definition.Relationships {
		definition.Relationships[index].Status = strings.ToUpper(strings.TrimSpace(definition.Relationships[index].Status))
		definition.Relationships[index].Source = strings.ToUpper(strings.TrimSpace(definition.Relationships[index].Source))
		definition.Relationships[index].Cardinality = strings.ToUpper(strings.TrimSpace(definition.Relationships[index].Cardinality))
	}
	if err := definition.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(definition)
	if err != nil {
		return "", fmt.Errorf("encode table semantic definition: %w", err)
	}
	if len(payload) > 8192 {
		return "", fmt.Errorf("table semantic definition payload exceeds 8192 bytes")
	}
	return ColumnSemanticCommentPrefix + string(payload), nil
}

func (d TableSemanticDefinition) Validate() error {
	if d.Revision == 0 || d.Revision > uint64(^uint64(0)>>1) {
		return fmt.Errorf("table semantic revision must fit a positive BIGINT")
	}
	if err := validateSemanticAttestation(d.Status, d.Source, d.ConfirmedBy); err != nil {
		return err
	}
	if strings.TrimSpace(d.BusinessName) == "" && strings.TrimSpace(d.Description) == "" {
		return fmt.Errorf("table semantic definition requires business_name or description")
	}
	if len(d.Relationships) > 256 {
		return fmt.Errorf("table semantic definition exceeds 256 relationships")
	}
	seenNames := make(map[string]struct{}, len(d.Relationships))
	for index := range d.Relationships {
		relationship := &d.Relationships[index]
		relationship.Status = strings.ToUpper(strings.TrimSpace(relationship.Status))
		relationship.Source = strings.ToUpper(strings.TrimSpace(relationship.Source))
		relationship.Cardinality = strings.ToUpper(strings.TrimSpace(relationship.Cardinality))
		if err := relationship.Validate(); err != nil {
			return fmt.Errorf("relationship %d: %w", index, err)
		}
		name := strings.ToLower(strings.TrimSpace(relationship.Name))
		if _, exists := seenNames[name]; exists {
			return fmt.Errorf("table semantic relationship name %q is duplicated", relationship.Name)
		}
		seenNames[name] = struct{}{}
	}
	return nil
}

func (r SemanticRelationship) Validate() error {
	if strings.TrimSpace(r.Name) == "" || strings.TrimSpace(r.TargetTable) == "" {
		return fmt.Errorf("relationship name and target_table are required")
	}
	if len(r.SourceColumns) == 0 || len(r.SourceColumns) != len(r.TargetColumns) {
		return fmt.Errorf("source_columns and target_columns must have the same nonzero length")
	}
	switch strings.ToUpper(strings.TrimSpace(r.Cardinality)) {
	case "ONE_TO_ONE", "ONE_TO_MANY", "MANY_TO_ONE", "MANY_TO_MANY":
	default:
		return fmt.Errorf("relationship cardinality %q is unsupported", r.Cardinality)
	}
	if err := validateSemanticAttestation(r.Status, r.Source, r.ConfirmedBy); err != nil {
		return err
	}
	for _, column := range append(append([]string(nil), r.SourceColumns...), r.TargetColumns...) {
		if strings.TrimSpace(column) == "" || len(column) > 256 {
			return fmt.Errorf("relationship columns must be nonempty and at most 256 bytes")
		}
	}
	for name, value := range map[string]string{"meaning": r.Meaning, "target_schema": r.TargetSchema} {
		if len(value) > 4096 {
			return fmt.Errorf("relationship %s exceeds 4096 bytes", name)
		}
	}
	return nil
}

func validateSemanticAttestation(status, source, confirmedBy string) error {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "DRAFT", "SUGGESTED", "CONFIRMED", "REJECTED":
	default:
		return fmt.Errorf("semantic definition status %q is unsupported", status)
	}
	switch strings.ToUpper(strings.TrimSpace(source)) {
	case "MANUAL", "IMPORT", "MODEL":
	default:
		return fmt.Errorf("semantic definition source %q is unsupported", source)
	}
	if strings.EqualFold(strings.TrimSpace(status), "CONFIRMED") && (strings.EqualFold(strings.TrimSpace(source), "MODEL") || strings.TrimSpace(confirmedBy) == "") {
		return fmt.Errorf("confirmed semantic definitions require a non-model source and confirmed_by")
	}
	if strings.EqualFold(strings.TrimSpace(status), "SUGGESTED") && !strings.EqualFold(strings.TrimSpace(source), "MODEL") {
		return fmt.Errorf("suggested semantic definitions must identify MODEL as their source")
	}
	if len(confirmedBy) > 4096 {
		return fmt.Errorf("semantic definition confirmed_by exceeds 4096 bytes")
	}
	return nil
}

func (d ColumnSemanticDefinition) Validate() error {
	if d.Revision == 0 || d.Revision > uint64(^uint64(0)>>1) {
		return fmt.Errorf("semantic definition revision must fit a positive BIGINT")
	}
	status := strings.ToUpper(strings.TrimSpace(d.Status))
	source := strings.ToUpper(strings.TrimSpace(d.Source))
	switch status {
	case "DRAFT", "SUGGESTED", "CONFIRMED", "REJECTED":
	default:
		return fmt.Errorf("semantic definition status %q is unsupported", d.Status)
	}
	switch source {
	case "MANUAL", "IMPORT", "MODEL":
	default:
		return fmt.Errorf("semantic definition source %q is unsupported", d.Source)
	}
	if status == "CONFIRMED" && (source == "MODEL" || strings.TrimSpace(d.ConfirmedBy) == "") {
		return fmt.Errorf("confirmed semantic definitions require a non-model source and confirmed_by")
	}
	if status == "SUGGESTED" && source != "MODEL" {
		return fmt.Errorf("suggested semantic definitions must identify MODEL as their source")
	}
	if strings.TrimSpace(d.BusinessName) == "" && strings.TrimSpace(d.Description) == "" {
		return fmt.Errorf("semantic definition requires business_name or description")
	}
	for name, value := range map[string]string{
		"confirmed_by": d.ConfirmedBy, "business_name": d.BusinessName,
		"description": d.Description, "unit": d.Unit, "null_meaning": d.NullMeaning,
		"scope": d.Scope,
	} {
		if len(value) > 4096 {
			return fmt.Errorf("semantic definition %s exceeds 4096 bytes", name)
		}
	}
	if d.Confidence != nil && (*d.Confidence < 0 || *d.Confidence > 1) {
		return fmt.Errorf("semantic definition confidence must be between zero and one")
	}
	for key, value := range d.EnumMeanings {
		if strings.TrimSpace(key) == "" || len(key) > 256 || len(value) > 4096 {
			return fmt.Errorf("semantic enum meanings contain an empty or oversized entry")
		}
	}
	return nil
}
