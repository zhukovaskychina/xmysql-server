package sqlparser

import (
	"fmt"
	"sort"
	"strings"
)

// ObjectKind identifies the catalog object referenced by a statement.
type ObjectKind string

const (
	ObjectKindTable    ObjectKind = "table"
	ObjectKindView     ObjectKind = "view"
	ObjectKindRoutine  ObjectKind = "routine"
	ObjectKindTrigger  ObjectKind = "trigger"
	ObjectKindEvent    ObjectKind = "event"
	ObjectKindDatabase ObjectKind = "database"
)

// AccessKind describes how a statement uses an object. A single object may be
// read and written by different parts of one DML statement.
type AccessKind string

const (
	AccessKindRead      AccessKind = "read"
	AccessKindWrite     AccessKind = "write"
	AccessKindReadWrite AccessKind = "read_write"
)

// ObjectReference is the normalized catalog identity of an object in an AST.
type ObjectReference struct {
	Schema string
	Name   string
	Kind   ObjectKind
	Access AccessKind
}

// CollectObjectReferences returns physical table references from the
// statement. CTE names are lexical query names, so they are excluded from the
// physical catalog result. The function does not authorize or lock anything.
func CollectObjectReferences(stmt Statement, defaultSchema string) ([]ObjectReference, error) {
	if stmt == nil {
		return nil, fmt.Errorf("%w: nil statement", ErrInvalidStatement)
	}
	if !referenceCollectionSupported(stmt) {
		return nil, fmt.Errorf("%w: AST %T", ErrUnsupportedStatement, stmt)
	}

	defaultSchema = strings.TrimSpace(defaultSchema)
	cteNames := make(map[string]struct{})
	references := make(map[string]ObjectReference)
	if err := Walk(func(node SQLNode) (bool, error) {
		switch current := node.(type) {
		case *CTEDefinition:
			if current != nil {
				cteNames[strings.ToLower(strings.TrimSpace(current.Name.String()))] = struct{}{}
			}
		case TableName:
			if current.IsEmpty() {
				return true, nil
			}
			schema := strings.TrimSpace(current.Qualifier.String())
			name := strings.TrimSpace(current.Name.String())
			if schema == "" {
				schema = defaultSchema
			}
			if schema == "" {
				schema = ""
			}
			if _, isCTE := cteNames[strings.ToLower(name)]; isCTE && schema == defaultSchema {
				return true, nil
			}
			key := referenceKey(schema, name, ObjectKindTable)
			if _, exists := references[key]; !exists {
				references[key] = ObjectReference{Schema: schema, Name: name, Kind: ObjectKindTable, Access: AccessKindRead}
			}
		}
		return true, nil
	}, stmt); err != nil {
		return nil, err
	}
	appendReference := func(schema, name string, kind ObjectKind, access AccessKind) {
		if strings.TrimSpace(name) == "" {
			return
		}
		if strings.TrimSpace(schema) == "" {
			schema = defaultSchema
		}
		key := referenceKey(schema, name, kind)
		if current, exists := references[key]; exists {
			current.Access = mergeAccess(current.Access, access)
			references[key] = current
		} else {
			references[key] = ObjectReference{Schema: schema, Name: name, Kind: kind, Access: access}
		}
	}
	switch current := stmt.(type) {
	case *AlterTable:
		appendReference(current.Table.Qualifier.String(), current.Table.Name.String(), ObjectKindTable, AccessKindWrite)
	case *TableDDLStatement:
		for _, target := range current.Targets {
			appendReference(target.Qualifier.String(), target.Name.String(), ObjectKindTable, AccessKindWrite)
		}
		if current.Like.Name.String() != "" {
			appendReference(current.Like.Qualifier.String(), current.Like.Name.String(), ObjectKindTable, AccessKindRead)
		}
	case *Show:
		if current.OnTable.Name.String() != "" {
			appendReference(current.OnTable.Qualifier.String(), current.OnTable.Name.String(), ObjectKindTable, AccessKindRead)
		}
	case *ViewStatement:
		appendReference(current.Name.Qualifier.String(), current.Name.Name.String(), ObjectKindView, AccessKindWrite)
	case *StoredObjectStatement:
		kind := ObjectKindRoutine
		switch current.ObjectType {
		case "trigger":
			kind = ObjectKindTrigger
		case "event":
			kind = ObjectKindEvent
		}
		appendReference(current.Name.Qualifier.String(), current.Name.Name.String(), kind, AccessKindWrite)
		if current.TargetTable.Name.String() != "" {
			appendReference(current.TargetTable.Qualifier.String(), current.TargetTable.Name.String(), ObjectKindTable, AccessKindReadWrite)
		}
	case *AdminStatement:
		for _, target := range current.Targets {
			appendReference(target.Qualifier.String(), target.Name.String(), ObjectKindTable, AccessKindWrite)
		}
	}

	for _, target := range writeTargets(stmt) {
		if target.IsEmpty() {
			continue
		}
		schema := strings.TrimSpace(target.Qualifier.String())
		name := strings.TrimSpace(target.Name.String())
		if schema == "" {
			schema = defaultSchema
		}
		key := referenceKey(schema, name, ObjectKindTable)
		current, exists := references[key]
		if !exists {
			current = ObjectReference{Schema: schema, Name: name, Kind: ObjectKindTable}
		}
		if current.Access == AccessKindRead {
			// The first occurrence is the DML target itself. A nested read of
			// the same table is represented as read_write by the caller that
			// explicitly records both uses; the simple target case is write.
			current.Access = AccessKindWrite
		} else {
			current.Access = mergeAccess(current.Access, AccessKindWrite)
		}
		references[key] = current
	}

	result := make([]ObjectReference, 0, len(references))
	for _, reference := range references {
		result = append(result, reference)
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].Schema != result[j].Schema {
			return result[i].Schema < result[j].Schema
		}
		return result[i].Name < result[j].Name
	})
	return result, nil
}

func referenceCollectionSupported(stmt Statement) bool {
	switch stmt.(type) {
	case *With, *Select, *Union, *ParenSelect, *Insert, *Update, *Delete, *Set, *Use, *Begin, *Commit, *Rollback,
		*Show, *ExplainStatement, *AlterTable, *AdminStatement, *PreparedStatement, *HandlerStatement, *ComponentStatement,
		*AccountStatement, *ViewStatement, *StoredObjectStatement, *TableDDLStatement:
		return true
	default:
		return false
	}
}

func writeTargets(stmt Statement) []TableName {
	switch current := stmt.(type) {
	case *With:
		if current != nil && current.Body != nil {
			return writeTargets(current.Body)
		}
	case *Insert:
		return []TableName{current.Table}
	case *Update:
		if table, ok := firstTableName(current.TableExprs); ok {
			return []TableName{table}
		}
	case *Delete:
		if len(current.Targets) > 0 {
			return []TableName{current.Targets[0]}
		}
		if table, ok := firstTableName(current.TableExprs); ok {
			return []TableName{table}
		}
	}
	return nil
}

func firstTableName(expressions TableExprs) (TableName, bool) {
	for _, expression := range expressions {
		switch current := expression.(type) {
		case *AliasedTableExpr:
			if table, ok := current.Expr.(TableName); ok {
				return table, true
			}
		case *ParenTableExpr:
			if table, ok := firstTableName(current.Exprs); ok {
				return table, true
			}
		case *JoinTableExpr:
			if table, ok := firstTableName(TableExprs{current.LeftExpr}); ok {
				return table, true
			}
		}
	}
	return TableName{}, false
}

func referenceKey(schema, name string, kind ObjectKind) string {
	return strings.ToLower(schema) + "\x00" + strings.ToLower(name) + "\x00" + string(kind)
}

func mergeAccess(left, right AccessKind) AccessKind {
	if left == "" {
		return right
	}
	if left == right {
		return left
	}
	return AccessKindReadWrite
}
