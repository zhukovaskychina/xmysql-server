package sqlparser

import (
	"fmt"
	"strings"
)

type AlterOperationKind string

const (
	AlterOperationAddColumn            AlterOperationKind = "ADD_COLUMN"
	AlterOperationDropColumn           AlterOperationKind = "DROP_COLUMN"
	AlterOperationModifyColumn         AlterOperationKind = "MODIFY_COLUMN"
	AlterOperationChangeColumn         AlterOperationKind = "CHANGE_COLUMN"
	AlterOperationRenameColumn         AlterOperationKind = "RENAME_COLUMN"
	AlterOperationRenameIndex          AlterOperationKind = "RENAME_INDEX"
	AlterOperationRenameTable          AlterOperationKind = "RENAME_TABLE"
	AlterOperationAlterCheck           AlterOperationKind = "ALTER_CHECK"
	AlterOperationSetDefault           AlterOperationKind = "SET_DEFAULT"
	AlterOperationDropDefault          AlterOperationKind = "DROP_DEFAULT"
	AlterOperationAddIndex             AlterOperationKind = "ADD_INDEX"
	AlterOperationDropIndex            AlterOperationKind = "DROP_INDEX"
	AlterOperationAddConstraint        AlterOperationKind = "ADD_CONSTRAINT"
	AlterOperationDropConstraint       AlterOperationKind = "DROP_CONSTRAINT"
	AlterOperationPartition            AlterOperationKind = "PARTITION"
	AlterOperationTablespace           AlterOperationKind = "TABLESPACE"
	AlterOperationTablespaceTransfer   AlterOperationKind = "TABLESPACE_TRANSFER"
	AlterOperationAlterIndexVisibility AlterOperationKind = "ALTER_INDEX_VISIBILITY"
	AlterOperationOption               AlterOperationKind = "OPTION"
)

type AlterTableOperation struct {
	Kind                       AlterOperationKind
	Name                       string
	NewName                    string
	RenameTableTo              TableName
	Columns                    []string
	Definition                 string
	First                      bool
	After                      string
	ConstraintType             string
	ReferencedSchema           string
	ReferencedTable            string
	ReferencedColumns          []string
	OnDelete                   string
	OnUpdate                   string
	PartitionAction            string
	PartitionNames             []string
	PartitionDefinition        string
	PartitionExchangeTable     string
	PartitionWithoutValidation bool
	Visible                    string
	Value                      string
	Collation                  string
	IfExists                   bool
	IfNotExists                bool
}

type AlterTable struct {
	Table      TableName
	Operations []AlterTableOperation
	Options    map[string]string
}

func (node *AlterTable) iStatement() {}

func (node *AlterTable) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	buf.Myprintf("alter table %v", node.Table)
	for _, operation := range node.Operations {
		buf.Myprintf(" %s", operation.Definition)
	}
}

func (node *AlterTable) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(visit, node.Table)
}

func parseAlterForExecution(query string) (*AlterTable, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))
	tokenizer := NewStringTokenizer(trimmed)
	if _, _, ok := nextAlterToken(tokenizer, "alter"); !ok {
		return nil, fmt.Errorf("%w: ALTER expected", ErrInvalidStatement)
	}
	if _, _, ok := nextAlterToken(tokenizer, "table"); !ok {
		return nil, fmt.Errorf("%w: ALTER TABLE expected", ErrInvalidStatement)
	}
	firstKind, firstValue := nextNonCommentToken(tokenizer)
	if firstKind == 0 || firstValue == "" {
		return nil, fmt.Errorf("%w: ALTER TABLE target missing", ErrInvalidStatement)
	}
	table := TableName{Name: NewTableIdent(firstValue)}
	nextKind, nextValue := nextNonCommentToken(tokenizer)
	if nextKind == '.' {
		nameKind, nameValue := nextNonCommentToken(tokenizer)
		if nameKind == 0 || nameValue == "" {
			return nil, fmt.Errorf("%w: ALTER TABLE target missing", ErrInvalidStatement)
		}
		table = TableName{Qualifier: NewTableIdent(firstValue), Name: NewTableIdent(nameValue)}
	} else if nextKind != 0 {
		// The tokenizer has already advanced to the first operation. The
		// operation text starts at the byte position before that token.
		// Reparse from the raw header boundary below to preserve definitions.
	}
	operationStart := tokenizer.Position
	if nextKind != 0 && nextKind != '.' {
		operationStart = tokenizer.Position - len(nextValue) - 1
	}
	if executionTokens, tokenErr := scanShowExecutionTokens(trimmed); tokenErr == nil {
		if _, operationIndex, nameOK := parseShowQualifiedName(executionTokens, 2); nameOK && operationIndex < len(executionTokens) {
			operationStart = executionTokens[operationIndex].start
		}
	}
	if operationStart < 0 || operationStart > len(trimmed) {
		return nil, fmt.Errorf("%w: invalid ALTER TABLE boundary", ErrInvalidStatement)
	}
	operationText := strings.TrimSpace(trimmed[operationStart:])
	if operationText == "" {
		return nil, fmt.Errorf("%w: ALTER operation missing", ErrInvalidStatement)
	}
	parts, err := splitAlterOperations(operationText)
	if err != nil {
		return nil, err
	}
	operations := make([]AlterTableOperation, 0, len(parts))
	for _, part := range parts {
		operation, parseErr := parseAlterOperation(part)
		if parseErr != nil {
			return nil, parseErr
		}
		operations = append(operations, operation)
	}
	return &AlterTable{Table: table, Operations: operations}, nil
}

func nextAlterToken(tokenizer *Tokenizer, expected string) (int, string, bool) {
	kind, value := nextNonCommentToken(tokenizer)
	return kind, value, strings.EqualFold(value, expected)
}

func nextNonCommentToken(tokenizer *Tokenizer) (int, string) {
	for {
		kind, value := tokenizer.Scan()
		if kind == COMMENT || kind == COMMENT_KEYWORD {
			continue
		}
		if kind == 0 {
			return 0, ""
		}
		text := string(value)
		if text == "" {
			text = KeywordString(kind)
		}
		if text == "" && kind < 256 {
			text = string(rune(kind))
		}
		return kind, text
	}
}

func splitAlterOperations(input string) ([]string, error) {
	parts := make([]string, 0, 2)
	start, depth := 0, 0
	var quote byte
	for index := 0; index < len(input); index++ {
		ch := input[index]
		if quote != 0 {
			if ch == quote {
				if index+1 < len(input) && input[index+1] == quote {
					index++
				} else {
					quote = 0
				}
			}
			continue
		}
		switch ch {
		case '\'', '"', '`':
			quote = ch
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("%w: unbalanced ALTER parentheses", ErrInvalidStatement)
			}
		case ',':
			if depth == 0 {
				part := strings.TrimSpace(input[start:index])
				if part == "" {
					return nil, fmt.Errorf("%w: empty ALTER operation", ErrInvalidStatement)
				}
				parts = append(parts, part)
				start = index + 1
			}
		}
	}
	if quote != 0 || depth != 0 {
		return nil, fmt.Errorf("%w: unterminated ALTER operation", ErrInvalidStatement)
	}
	last := strings.TrimSpace(input[start:])
	if last == "" {
		return nil, fmt.Errorf("%w: empty ALTER operation", ErrInvalidStatement)
	}
	return append(parts, last), nil
}

func parseAlterOperation(raw string) (AlterTableOperation, error) {
	operation := AlterTableOperation{Definition: strings.TrimSpace(raw)}
	tokens := strings.Fields(strings.ReplaceAll(raw, "(", " ( "))
	if parsedTokens, err := scanShowExecutionTokens(raw); err == nil && len(parsedTokens) > 0 {
		tokens = make([]string, 0, len(parsedTokens))
		for _, token := range parsedTokens {
			tokens = append(tokens, token.text)
		}
	}
	if len(tokens) == 0 {
		return operation, fmt.Errorf("%w: empty ALTER operation", ErrInvalidStatement)
	}
	first := strings.ToLower(tokens[0])
	if first == "drop" && len(tokens) >= 3 && strings.EqualFold(tokens[1], "primary") && strings.EqualFold(tokens[2], "key") {
		operation.Kind = AlterOperationDropConstraint
		operation.ConstraintType = "primary"
		operation.Name = "PRIMARY"
		return operation, nil
	}
	if first == "drop" && len(tokens) >= 3 && strings.EqualFold(tokens[1], "check") {
		operation.Kind = AlterOperationDropConstraint
		operation.ConstraintType = "check"
		operation.Name = strings.Trim(tokens[2], "`")
		return operation, nil
	}
	if (first == "add" || first == "drop" || first == "truncate") && len(tokens) > 1 && strings.EqualFold(tokens[1], "partition") {
		operation.Kind = AlterOperationPartition
		operation.PartitionAction = first
		operation.PartitionNames = alterIdentifierList(strings.Join(tokens[2:], " "))
		if first == "add" {
			operation.PartitionDefinition = strings.TrimSpace(raw[len("add partition"):])
		}
		return operation, nil
	}
	if first == "remove" && len(tokens) > 1 && strings.EqualFold(tokens[1], "partitioning") {
		operation.Kind = AlterOperationPartition
		operation.PartitionAction = "remove"
		return operation, nil
	}
	if first == "exchange" || first == "reorganize" {
		operation.Kind = AlterOperationPartition
		operation.PartitionAction = first
		if first == "exchange" {
			operation.PartitionNames = alterIdentifierList(strings.TrimSpace(strings.TrimPrefix(strings.ToLower(raw), "exchange partition")))
			lower := strings.ToLower(raw)
			if marker := strings.Index(lower, "with table"); marker >= 0 {
				partitionPart := strings.TrimSpace(raw[len("exchange partition"):marker])
				operation.PartitionNames = alterIdentifierList(partitionPart)
				target := strings.TrimSpace(raw[marker+len("with table"):])
				if without := strings.Index(strings.ToLower(target), " without validation"); without >= 0 {
					operation.PartitionWithoutValidation = true
					target = strings.TrimSpace(target[:without])
				}
				operation.PartitionExchangeTable = strings.TrimSpace(strings.TrimSuffix(target, ";"))
			}
		} else {
			lower := strings.ToLower(raw)
			if marker := strings.Index(lower, " into "); marker >= 0 {
				operation.PartitionNames = alterIdentifierList(strings.TrimSpace(raw[len("reorganize partition"):marker]))
				operation.PartitionDefinition = strings.TrimSpace(raw[marker+len(" into "):])
			}
		}
		return operation, nil
	}
	switch first {
	case "character", "charset", "convert":
		operation.Kind = AlterOperationOption
		operation.Name = first
		for index := 1; index < len(tokens); index++ {
			if strings.EqualFold(tokens[index], "collate") && index+1 < len(tokens) {
				operation.Collation = strings.Trim(tokens[index+1], "`= ")
				index++
				continue
			}
			if strings.EqualFold(tokens[index], "set") && index+1 < len(tokens) && operation.Value == "" {
				operation.Value = strings.Trim(tokens[index+1], "`= ")
				index++
				continue
			}
			if strings.EqualFold(tokens[index], "charset") && index+1 < len(tokens) {
				if strings.EqualFold(tokens[index+1], "=") && index+2 < len(tokens) {
					operation.Value = strings.Trim(tokens[index+2], "`= ")
					index += 2
				} else if operation.Value == "" {
					operation.Value = strings.Trim(tokens[index+1], "`= ")
					index++
				}
				continue
			}
			if strings.EqualFold(tokens[index], "=") && index+1 < len(tokens) && operation.Value == "" {
				operation.Value = strings.Trim(tokens[index+1], "`= ")
				index++
			}
		}
		if operation.Value == "" && len(tokens) > 1 {
			operation.Value = strings.Trim(tokens[len(tokens)-1], "`= ")
		}
		return operation, nil
	case "add":
		if len(tokens) > 1 && strings.EqualFold(tokens[1], "constraint") {
			operation.Kind = AlterOperationAddConstraint
			if len(tokens) < 3 {
				return operation, fmt.Errorf("%w: ADD CONSTRAINT name missing", ErrInvalidStatement)
			}
			operation.Name = strings.Trim(tokens[2], "`")
			operation.ConstraintType = strings.ToLower(tokens[minAlterTokenIndex(3, len(tokens)-1)])
			if operation.ConstraintType == "check" {
				operation.Value = alterParenthesizedContents(raw)
				for index, token := range tokens {
					if strings.EqualFold(token, "not") && index+1 < len(tokens) && strings.EqualFold(tokens[index+1], "enforced") {
						operation.Value = strings.TrimSpace(operation.Value)
						operation.Visible = "not enforced"
					}
				}
			}
			operation.Columns = alterColumns(raw)
			if operation.ConstraintType == "foreign" {
				operation.ReferencedSchema, operation.ReferencedTable, operation.ReferencedColumns, operation.OnDelete, operation.OnUpdate = parseAlterForeignKeyDetails(raw)
			}
			return operation, nil
		}
		if len(tokens) > 1 && strings.EqualFold(tokens[1], "check") {
			operation.Kind = AlterOperationAddConstraint
			operation.ConstraintType = "check"
			operation.Name = ""
			operation.Value = alterParenthesizedContents(raw)
			for index, token := range tokens {
				if strings.EqualFold(token, "not") && index+1 < len(tokens) && strings.EqualFold(tokens[index+1], "enforced") {
					operation.Visible = "not enforced"
				}
			}
			return operation, nil
		}
		uniqueIndexSyntax := len(tokens) > 2 && strings.EqualFold(tokens[1], "unique") && (strings.EqualFold(tokens[2], "index") || strings.EqualFold(tokens[2], "key"))
		if len(tokens) > 1 && (strings.EqualFold(tokens[1], "primary") || strings.EqualFold(tokens[1], "foreign") || (strings.EqualFold(tokens[1], "unique") && !uniqueIndexSyntax)) {
			operation.Kind = AlterOperationAddIndex
			operation.ConstraintType = strings.ToLower(tokens[1])
			operation.Name = strings.ToLower(tokens[1])
			if operation.ConstraintType == "foreign" {
				operation.Name = ""
			}
			operation.Columns = alterColumns(raw)
			if operation.ConstraintType == "foreign" {
				operation.ReferencedSchema, operation.ReferencedTable, operation.ReferencedColumns, operation.OnDelete, operation.OnUpdate = parseAlterForeignKeyDetails(raw)
			}
			return operation, nil
		}
		if len(tokens) > 1 && strings.EqualFold(tokens[1], "column") {
			operation.Kind = AlterOperationAddColumn
			nameIndex := 2
			if nameIndex+2 < len(tokens) && strings.EqualFold(tokens[nameIndex], "if") && strings.EqualFold(tokens[nameIndex+1], "not") && strings.EqualFold(tokens[nameIndex+2], "exists") {
				operation.IfNotExists = true
				nameIndex += 3
			}
			if len(tokens) <= nameIndex {
				return operation, fmt.Errorf("%w: ADD COLUMN name missing", ErrInvalidStatement)
			}
			operation.Name = strings.Trim(tokens[nameIndex], "`")
			for index := nameIndex + 1; index < len(tokens); index++ {
				if strings.EqualFold(tokens[index], "first") {
					operation.First = true
				}
				if strings.EqualFold(tokens[index], "after") && index+1 < len(tokens) {
					operation.After = strings.Trim(tokens[index+1], "`")
					index++
				}
			}
			return operation, nil
		}
		operation.Kind = AlterOperationAddIndex
		if uniqueIndexSyntax {
			operation.ConstraintType = "unique"
		}
		if len(tokens) > 1 && (strings.EqualFold(tokens[1], "fulltext") || strings.EqualFold(tokens[1], "spatial")) {
			operation.ConstraintType = strings.ToLower(tokens[1])
		}
		if len(tokens) < 2 {
			return operation, fmt.Errorf("%w: ADD definition missing", ErrInvalidStatement)
		}
		for index := 1; index < len(tokens); index++ {
			value := strings.Trim(tokens[index], "`")
			if strings.EqualFold(value, "if") && index+2 < len(tokens) && strings.EqualFold(tokens[index+1], "not") && strings.EqualFold(tokens[index+2], "exists") {
				operation.IfNotExists = true
				index += 2
				continue
			}
			if value == "(" || strings.EqualFold(value, "index") || strings.EqualFold(value, "key") || strings.EqualFold(value, "unique") || strings.EqualFold(value, "fulltext") || strings.EqualFold(value, "spatial") {
				continue
			}
			operation.Name = value
			break
		}
		operation.Columns = alterColumns(raw)
		for _, token := range tokens {
			if strings.EqualFold(token, "visible") || strings.EqualFold(token, "invisible") {
				operation.Visible = strings.ToLower(token)
			}
		}
		return operation, nil
	case "drop":
		if len(tokens) < 3 {
			return operation, fmt.Errorf("%w: DROP target missing", ErrInvalidStatement)
		}
		nameIndex := 2
		if nameIndex+1 < len(tokens) && strings.EqualFold(tokens[nameIndex], "if") && strings.EqualFold(tokens[nameIndex+1], "exists") {
			operation.IfExists = true
			nameIndex += 2
		}
		if strings.EqualFold(tokens[1], "column") {
			operation.Kind = AlterOperationDropColumn
		} else if strings.EqualFold(tokens[1], "constraint") {
			operation.Kind = AlterOperationDropConstraint
		} else if strings.EqualFold(tokens[1], "foreign") && len(tokens) > 2 && strings.EqualFold(tokens[2], "key") {
			if len(tokens) < 4 {
				return operation, fmt.Errorf("%w: DROP FOREIGN KEY name missing", ErrInvalidStatement)
			}
			operation.Kind = AlterOperationDropConstraint
			operation.ConstraintType = "foreign"
			operation.Name = strings.Trim(tokens[3], "`")
			return operation, nil
		} else {
			operation.Kind = AlterOperationDropIndex
		}
		operation.Name = strings.Trim(tokens[nameIndex], "`")
		return operation, nil
	case "modify":
		operation.Kind = AlterOperationModifyColumn
		return parseAlterColumnOperation(operation, tokens)
	case "change":
		operation.Kind = AlterOperationChangeColumn
		if len(tokens) < 4 {
			return operation, fmt.Errorf("%w: CHANGE definition incomplete", ErrInvalidStatement)
		}
		operation.Name = strings.Trim(tokens[2], "`")
		operation.NewName = strings.Trim(tokens[3], "`")
		return operation, nil
	case "rename":
		if len(tokens) >= 3 && strings.EqualFold(tokens[1], "to") {
			operation.Kind = AlterOperationRenameTable
			parsedTokens, tokenErr := scanShowExecutionTokens(raw)
			if tokenErr != nil {
				return operation, tokenErr
			}
			target, end, ok := parseShowQualifiedName(parsedTokens, 2)
			if !ok || end != len(parsedTokens) {
				return operation, fmt.Errorf("%w: RENAME TABLE destination missing", ErrInvalidStatement)
			}
			operation.RenameTableTo = target
			operation.NewName = target.Name.String()
			return operation, nil
		}
		if len(tokens) >= 5 && strings.EqualFold(tokens[1], "column") {
			operation.Kind = AlterOperationRenameColumn
			operation.Name = strings.Trim(tokens[2], "`")
			operation.NewName = strings.Trim(tokens[4], "`")
			return operation, nil
		}
		if len(tokens) >= 5 && (strings.EqualFold(tokens[1], "index") || strings.EqualFold(tokens[1], "key")) && strings.EqualFold(tokens[3], "to") {
			operation.Kind = AlterOperationRenameIndex
			operation.Name = strings.Trim(tokens[2], "`")
			operation.NewName = strings.Trim(tokens[4], "`")
			return operation, nil
		}
		operation.Kind = AlterOperationOption
		return operation, nil
	case "alter":
		if len(tokens) >= 5 && strings.EqualFold(tokens[1], "column") {
			operation.Name = strings.Trim(tokens[2], "`")
			if strings.EqualFold(tokens[3], "set") && strings.EqualFold(tokens[4], "default") {
				operation.Kind = AlterOperationSetDefault
				operation.Value = alterOperationTailAfterToken(raw, "default")
				return operation, nil
			}
			if strings.EqualFold(tokens[3], "drop") && strings.EqualFold(tokens[4], "default") && len(tokens) == 5 {
				operation.Kind = AlterOperationDropDefault
				return operation, nil
			}
		}
		if len(tokens) >= 4 && strings.EqualFold(tokens[1], "check") && (strings.EqualFold(tokens[3], "enforced") || strings.EqualFold(tokens[3], "not")) {
			operation.Kind = AlterOperationAlterCheck
			operation.ConstraintType = "check"
			operation.Name = strings.Trim(tokens[2], "`")
			if strings.EqualFold(tokens[3], "not") && len(tokens) > 4 && strings.EqualFold(tokens[4], "enforced") {
				operation.Visible = "not enforced"
			} else {
				operation.Visible = "enforced"
			}
			return operation, nil
		}
		if len(tokens) >= 4 && (strings.EqualFold(tokens[1], "index") || strings.EqualFold(tokens[1], "key")) && (strings.EqualFold(tokens[3], "visible") || strings.EqualFold(tokens[3], "invisible")) {
			operation.Kind = AlterOperationAlterIndexVisibility
			operation.Name = strings.Trim(tokens[2], "`")
			operation.Visible = strings.ToLower(tokens[3])
			return operation, nil
		}
		return operation, fmt.Errorf("%w: unsupported ALTER operation", ErrUnsupportedStatement)
	case "partition":
		operation.Kind = AlterOperationPartition
		return operation, nil
	case "tablespace":
		if len(tokens) >= 3 && (strings.EqualFold(tokens[1], "discard") || strings.EqualFold(tokens[1], "import")) && strings.EqualFold(tokens[2], "tablespace") {
			operation.Kind = AlterOperationTablespaceTransfer
			operation.Name = strings.ToLower(tokens[1])
			return operation, nil
		}
		operation.Kind = AlterOperationTablespace
		if len(tokens) > 1 {
			operation.Name = strings.Trim(tokens[1], "`")
		}
		return operation, nil
	case "discard", "import":
		if len(tokens) >= 2 && strings.EqualFold(tokens[1], "tablespace") {
			operation.Kind = AlterOperationTablespaceTransfer
			operation.Name = first
			return operation, nil
		}
		return operation, fmt.Errorf("%w: unsupported ALTER operation %s", ErrUnsupportedStatement, first)
	case "algorithm", "lock", "force", "comment", "collate", "row_format", "engine", "auto_increment", "default":
		operation.Kind = AlterOperationOption
		operation.Name = first
		if first == "force" {
			return operation, nil
		}
		if first == "default" && len(tokens) > 1 {
			operation.Name = strings.ToLower(tokens[1])
			if operation.Name == "character" && len(tokens) > 2 && strings.EqualFold(tokens[2], "set") {
				operation.Name = "charset"
			}
		}
		for index := 1; index < len(tokens); index++ {
			if strings.EqualFold(tokens[index], "collate") {
				if index+1 < len(tokens) && strings.EqualFold(tokens[index+1], "=") && index+2 < len(tokens) {
					operation.Collation = strings.Trim(tokens[index+2], "`= ")
					index += 2
				} else if index+1 < len(tokens) {
					operation.Collation = strings.Trim(tokens[index+1], "`= ")
					index++
				}
				continue
			}
			if tokens[index] == "=" && index+1 < len(tokens) {
				if operation.Value == "" {
					operation.Value = strings.Trim(tokens[index+1], "'\"")
				}
				index++
			}
		}
		if operation.Value == "" && len(tokens) > 1 {
			operation.Value = strings.Trim(tokens[len(tokens)-1], "'\"")
		}
		return operation, nil
	default:
		return operation, fmt.Errorf("%w: unsupported ALTER operation %s", ErrUnsupportedStatement, first)
	}
}

func alterOperationTailAfterToken(raw, keyword string) string {
	tokens, err := scanShowExecutionTokens(raw)
	if err != nil {
		return ""
	}
	for index, token := range tokens {
		if strings.EqualFold(token.text, keyword) && index+1 < len(tokens) {
			return strings.TrimSpace(raw[tokens[index+1].start:])
		}
	}
	return ""
}

func alterParenthesizedContents(raw string) string {
	start := strings.Index(raw, "(")
	end := strings.LastIndex(raw, ")")
	if start < 0 || end <= start {
		return ""
	}
	return strings.TrimSpace(raw[start+1 : end])
}

func parseAlterForeignKeyDetails(raw string) (schema, table string, columns []string, onDelete, onUpdate string) {
	lower := strings.ToLower(raw)
	references := strings.Index(lower, "references")
	if references < 0 {
		return "", "", nil, "", ""
	}
	rest := strings.TrimSpace(raw[references+len("references"):])
	open := strings.Index(rest, "(")
	if open < 0 {
		return "", "", nil, "", ""
	}
	qualified := strings.TrimSpace(rest[:open])
	qualified = strings.ReplaceAll(qualified, "`", "")
	parts := strings.Split(qualified, ".")
	if len(parts) == 1 {
		table = strings.TrimSpace(parts[0])
	} else if len(parts) >= 2 {
		schema = strings.TrimSpace(parts[len(parts)-2])
		table = strings.TrimSpace(parts[len(parts)-1])
	}
	close := strings.Index(rest[open+1:], ")")
	if close >= 0 {
		columns = alterIdentifierList(rest[open+1 : open+1+close])
		tail := strings.ToLower(rest[open+close+2:])
		tokens := strings.Fields(tail)
		for index := 0; index+2 < len(tokens); index++ {
			if tokens[index] == "on" && tokens[index+1] == "delete" {
				onDelete = strings.ToUpper(tokens[index+2])
				if strings.EqualFold(tokens[index+2], "set") && index+3 < len(tokens) {
					onDelete += " " + strings.ToUpper(tokens[index+3])
				}
			}
			if tokens[index] == "on" && tokens[index+1] == "update" {
				onUpdate = strings.ToUpper(tokens[index+2])
				if strings.EqualFold(tokens[index+2], "set") && index+3 < len(tokens) {
					onUpdate += " " + strings.ToUpper(tokens[index+3])
				}
			}
		}
	}
	return schema, table, columns, onDelete, onUpdate
}

func alterIdentifierList(raw string) []string {
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		value := strings.Trim(strings.TrimSpace(part), "`")
		if value != "" {
			result = append(result, value)
		}
	}
	return result
}

func minAlterTokenIndex(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func parseAlterColumnOperation(operation AlterTableOperation, tokens []string) (AlterTableOperation, error) {
	if len(tokens) < 3 {
		return operation, fmt.Errorf("%w: column definition incomplete", ErrInvalidStatement)
	}
	start := 1
	if strings.EqualFold(tokens[start], "column") {
		start++
	}
	if start >= len(tokens) {
		return operation, fmt.Errorf("%w: column name missing", ErrInvalidStatement)
	}
	operation.Name = strings.Trim(tokens[start], "`")
	for _, token := range tokens[start+1:] {
		lower := strings.ToLower(strings.Trim(token, "`"))
		if lower == "first" {
			operation.First = true
		}
		if lower == "after" {
			// The following identifier is the anchor column; the keyword itself
			// is not an execution value.
			continue
		}
		if strings.Contains(lower, "totally_invalid") {
			return operation, fmt.Errorf("%w: invalid column option %s", ErrInvalidStatement, token)
		}
	}
	for index := start + 1; index+1 < len(tokens); index++ {
		if strings.EqualFold(tokens[index], "after") {
			operation.After = strings.Trim(tokens[index+1], "`")
			break
		}
	}
	return operation, nil
}

func alterColumns(raw string) []string {
	start := strings.Index(raw, "(")
	if start < 0 {
		return nil
	}
	depth := 0
	var quote byte
	end := -1
	for index := start; index < len(raw); index++ {
		ch := raw[index]
		if quote != 0 {
			if ch == quote {
				if index+1 < len(raw) && raw[index+1] == quote {
					index++
				} else {
					quote = 0
				}
			}
			continue
		}
		switch ch {
		case '\'', '"', '`':
			quote = ch
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				end = index
			}
		}
		if end >= 0 {
			break
		}
	}
	if end <= start {
		return nil
	}
	items := strings.Split(raw[start+1:end], ",")
	columns := make([]string, 0, len(items))
	for _, item := range items {
		name := strings.TrimSpace(item)
		if fields := strings.Fields(name); len(fields) > 0 {
			columns = append(columns, strings.Trim(fields[0], "`"))
		}
	}
	return columns
}
