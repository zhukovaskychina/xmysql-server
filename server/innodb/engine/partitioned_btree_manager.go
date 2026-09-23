package engine

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
)

// partitionedBTreeManager presents multiple physical clustered indexes as one
// table-level access path. Writes are routed from the decoded row payload;
// deletes and scans fan out because a primary key alone does not identify a
// partition for RANGE/LIST partitioning.
type partitionedBTreeManager struct {
	partitions  []partitionedBTree
	tableMeta   *metadata.TableMeta
	method      string
	expression  string
	definitions []interface{}
}

type partitionedBTree struct {
	name  string
	info  manager.PartitionStorageInfo
	btree basic.BPlusTreeManager
}

func newPartitionedBTreeManager(
	ctx context.Context,
	tableStorageManager *manager.TableStorageManager,
	schemaName, tableName string,
	tableMeta *metadata.TableMeta,
) (basic.BPlusTreeManager, bool, error) {
	if tableStorageManager == nil {
		return nil, false, nil
	}
	storageInfo, err := tableStorageManager.GetTableStorageInfo(schemaName, tableName)
	if err != nil {
		return nil, false, err
	}
	if len(storageInfo.Partitions) == 0 {
		return nil, false, nil
	}
	definitions := partitionDefinitionsForRouter(storageInfo.Partitioning)
	if len(definitions) != len(storageInfo.Partitions) {
		return nil, false, fmt.Errorf("partition storage mapping for %s.%s is incomplete", schemaName, tableName)
	}
	router := &partitionedBTreeManager{
		partitions:  make([]partitionedBTree, 0, len(storageInfo.Partitions)),
		tableMeta:   tableMeta,
		method:      strings.ToUpper(strings.TrimSpace(fmt.Sprint(storageInfo.Partitioning["method"]))),
		expression:  strings.Trim(strings.TrimSpace(fmt.Sprint(storageInfo.Partitioning["expression"])), "` "),
		definitions: definitions,
	}
	for _, partition := range storageInfo.Partitions {
		btree, createErr := tableStorageManager.CreateBTreeManagerForPartition(ctx, schemaName, tableName, partition.Name)
		if createErr != nil {
			return nil, false, createErr
		}
		router.partitions = append(router.partitions, partitionedBTree{name: partition.Name, info: partition, btree: btree})
	}
	if router.expression == "" || router.method == "" {
		return nil, false, fmt.Errorf("partition metadata for %s.%s is incomplete", schemaName, tableName)
	}
	return router, true, nil
}

// RestrictToWherePartitions applies the planner's conservative constant
// predicate pruning to the physical fan-out. Unsupported predicates leave the
// complete partition set intact, preserving correctness over selectivity.
func (p *partitionedBTreeManager) RestrictToWherePartitions(conditions []string) {
	if p == nil || len(conditions) == 0 || len(p.partitions) == 0 {
		return
	}
	descriptor := map[string]interface{}{
		"method":     p.method,
		"expression": p.expression,
		"partitions": p.definitions,
	}
	rules := partitionPlannerRules(descriptor)
	if len(rules) != len(p.partitions) {
		return
	}
	allowedSet := make(map[int]struct{}, len(p.partitions))
	for ordinal := range p.partitions {
		allowedSet[ordinal] = struct{}{}
	}
	for _, condition := range conditions {
		conditionSet, ok := partitionAllowedOrdinals(condition, p.method, p.expression, rules)
		if !ok {
			return
		}
		for ordinal := range allowedSet {
			if _, keep := conditionSet[ordinal]; !keep {
				delete(allowedSet, ordinal)
			}
		}
	}
	selected := make([]partitionedBTree, 0, len(allowedSet))
	for ordinal, partition := range p.partitions {
		if _, keep := allowedSet[ordinal]; keep {
			selected = append(selected, partition)
		}
	}
	p.partitions = selected
}

func partitionAllowedOrdinals(condition, method, expression string, rules []plan.PartitionRule) (map[int]struct{}, bool) {
	condition = strings.TrimSpace(condition)
	for strings.HasPrefix(condition, "(") && strings.HasSuffix(condition, ")") {
		close := matchingParenIndex(condition, 0)
		if close != len(condition)-1 {
			break
		}
		condition = strings.TrimSpace(condition[1 : len(condition)-1])
	}
	if operator, value, ok := partitionTupleConstantPredicate(condition, expression); ok {
		allowed, err := plan.PrunePartitions(method, operator, value, rules)
		if err != nil {
			return nil, false
		}
		return partitionOrdinalSet(allowed), true
	}
	if values, ok := partitionTupleInPredicate(condition, expression); ok {
		union := make(map[int]struct{})
		for _, value := range values {
			allowed, err := plan.PrunePartitions(method, "=", value, rules)
			if err != nil {
				return nil, false
			}
			for _, ordinal := range allowed {
				union[ordinal] = struct{}{}
			}
		}
		return union, true
	}
	if strings.EqualFold(method, "LIST") {
		if excluded, ok := partitionTupleNotInPredicate(condition, expression); ok {
			allowed := allPartitionOrdinalSet(len(rules))
			for ordinal, rule := range rules {
				if len(rule.ListTuple) == 0 {
					continue
				}
				allExcluded := true
				for _, item := range rule.ListTuple {
					matched := false
					for _, value := range excluded {
						comparison, err := comparePartitionTuples(item, value)
						if err == nil && comparison == 0 {
							matched = true
							break
						}
					}
					if !matched {
						allExcluded = false
						break
					}
				}
				if allExcluded {
					delete(allowed, ordinal)
				}
			}
			return allowed, true
		}
	}
	if strings.EqualFold(method, "RANGE") {
		if lower, upper, ok := partitionTupleBetweenPredicate(condition, expression, false); ok {
			lowerSet, lowerErr := plan.PrunePartitions(method, ">=", lower, rules)
			upperSet, upperErr := plan.PrunePartitions(method, "<=", upper, rules)
			if lowerErr != nil || upperErr != nil {
				return nil, false
			}
			return intersectPartitionOrdinalSlices(lowerSet, upperSet), true
		}
		if lower, upper, ok := partitionTupleBetweenPredicate(condition, expression, true); ok {
			lowerSet, lowerErr := plan.PrunePartitions(method, "<", lower, rules)
			upperSet, upperErr := plan.PrunePartitions(method, ">", upper, rules)
			if lowerErr != nil || upperErr != nil {
				return nil, false
			}
			return unionPartitionOrdinalSlices(lowerSet, upperSet), true
		}
	}
	if strings.EqualFold(method, "RANGE") {
		if lower, upper, ok := partitionConstantTextBetweenPredicate(condition, expression); ok {
			lowerSet, lowerErr := plan.PrunePartitions(method, ">=", lower, rules)
			upperSet, upperErr := plan.PrunePartitions(method, "<=", upper, rules)
			if lowerErr != nil || upperErr != nil {
				return nil, false
			}
			return intersectPartitionOrdinalSlices(lowerSet, upperSet), true
		}
		if lower, upper, ok := partitionConstantTextNotBetweenPredicate(condition, expression); ok {
			lowerSet, lowerErr := plan.PrunePartitions(method, "<", lower, rules)
			upperSet, upperErr := plan.PrunePartitions(method, ">", upper, rules)
			if lowerErr != nil || upperErr != nil {
				return nil, false
			}
			return unionPartitionOrdinalSlices(lowerSet, upperSet), true
		}
	}
	if lower, upper, ok := partitionConstantBetweenPredicate(condition, expression); ok {
		lowerSet, lowerOK := partitionAllowedOrdinals(fmt.Sprintf("%s >= %d", expression, lower), method, expression, rules)
		upperSet, upperOK := partitionAllowedOrdinals(fmt.Sprintf("%s <= %d", expression, upper), method, expression, rules)
		if !lowerOK || !upperOK {
			return nil, false
		}
		intersection := make(map[int]struct{})
		for ordinal := range lowerSet {
			if _, exists := upperSet[ordinal]; exists {
				intersection[ordinal] = struct{}{}
			}
		}
		return intersection, true
	}
	if lower, upper, ok := partitionConstantNotBetweenPredicate(condition, expression); ok {
		lowerSet, lowerOK := partitionAllowedOrdinals(fmt.Sprintf("%s < %d", expression, lower), method, expression, rules)
		upperSet, upperOK := partitionAllowedOrdinals(fmt.Sprintf("%s > %d", expression, upper), method, expression, rules)
		if !lowerOK || !upperOK {
			return nil, false
		}
		union := make(map[int]struct{}, len(lowerSet)+len(upperSet))
		for ordinal := range lowerSet {
			union[ordinal] = struct{}{}
		}
		for ordinal := range upperSet {
			union[ordinal] = struct{}{}
		}
		return union, true
	}
	if values, ok := partitionConstantNotInPredicate(condition, expression); ok {
		if !strings.EqualFold(method, "LIST") {
			return allPartitionOrdinalSet(len(rules)), true
		}
		excluded := make(map[string]struct{}, len(values))
		for _, value := range values {
			if identity, identityOK := partitionLiteralIdentity(value); identityOK {
				excluded[identity] = struct{}{}
			}
		}
		allowed := allPartitionOrdinalSet(len(rules))
		for ordinal, rule := range rules {
			allValuesExcluded := len(rule.ListValue) > 0 || len(rule.ListText) > 0
			for _, value := range rule.ListValue {
				if _, exists := excluded[partitionLiteralIdentityForInt(value)]; !exists {
					allValuesExcluded = false
					break
				}
			}
			if allValuesExcluded {
				for _, value := range rule.ListText {
					if _, exists := excluded["text:"+value]; !exists {
						allValuesExcluded = false
						break
					}
				}
			}
			if allValuesExcluded {
				delete(allowed, ordinal)
			}
		}
		return allowed, true
	}
	if parts := splitTopLevelKeyword(condition, "or"); len(parts) == 2 {
		left, leftOK := partitionAllowedOrdinals(parts[0], method, expression, rules)
		right, rightOK := partitionAllowedOrdinals(parts[1], method, expression, rules)
		if !leftOK || !rightOK {
			return nil, false
		}
		union := make(map[int]struct{}, len(left)+len(right))
		for ordinal := range left {
			union[ordinal] = struct{}{}
		}
		for ordinal := range right {
			union[ordinal] = struct{}{}
		}
		return union, true
	}
	if parts := splitTopLevelKeyword(condition, "and"); len(parts) == 2 {
		left, leftOK := partitionAllowedOrdinals(parts[0], method, expression, rules)
		right, rightOK := partitionAllowedOrdinals(parts[1], method, expression, rules)
		if !leftOK || !rightOK {
			return nil, false
		}
		intersection := make(map[int]struct{}, len(left))
		for ordinal := range left {
			if _, exists := right[ordinal]; exists {
				intersection[ordinal] = struct{}{}
			}
		}
		return intersection, true
	}
	if values, ok := partitionConstantInPredicate(condition, expression); ok {
		union := make(map[int]struct{})
		for _, value := range values {
			allowed, err := plan.PrunePartitions(method, "=", value, rules)
			if err != nil {
				return nil, false
			}
			for _, ordinal := range allowed {
				union[ordinal] = struct{}{}
			}
		}
		return union, true
	}
	operator, value, ok := partitionConstantPredicate(condition, expression)
	if !ok {
		return nil, false
	}
	allowed, err := plan.PrunePartitions(method, operator, value, rules)
	if err != nil {
		return nil, false
	}
	result := make(map[int]struct{}, len(allowed))
	for _, ordinal := range allowed {
		result[ordinal] = struct{}{}
	}
	return result, true
}

func partitionOrdinalSet(ordinals []int) map[int]struct{} {
	result := make(map[int]struct{}, len(ordinals))
	for _, ordinal := range ordinals {
		result[ordinal] = struct{}{}
	}
	return result
}

func partitionTupleConstantPredicate(condition, expression string) (string, []interface{}, bool) {
	columns, rest, ok := partitionTupleConditionPrefix(condition)
	if !ok || !samePartitionColumns(columns, expression) {
		return "", nil, false
	}
	operator := ""
	for _, candidate := range []string{"<=", ">=", "=", "<", ">"} {
		if strings.HasPrefix(rest, candidate) {
			operator = candidate
			rest = strings.TrimSpace(strings.TrimPrefix(rest, candidate))
			break
		}
	}
	if operator == "" {
		return "", nil, false
	}
	value, ok := partitionTupleLiteral(rest)
	return operator, value, ok
}

func partitionTupleInPredicate(condition, expression string) ([][]interface{}, bool) {
	columns, rest, ok := partitionTupleConditionPrefix(condition)
	if !ok || !samePartitionColumns(columns, expression) || !strings.HasPrefix(strings.ToLower(rest), "in") {
		return nil, false
	}
	list := strings.TrimSpace(rest[2:])
	if len(list) < 2 || list[0] != '(' || matchingParenIndex(list, 0) != len(list)-1 {
		return nil, false
	}
	contents := list[1 : len(list)-1]
	values := make([][]interface{}, 0)
	for _, raw := range splitTopLevelComma(contents) {
		tuple, tupleOK := partitionTupleLiteral(strings.TrimSpace(raw))
		if !tupleOK {
			return nil, false
		}
		values = append(values, tuple)
	}
	return values, len(values) > 0
}

func partitionTupleNotInPredicate(condition, expression string) ([][]interface{}, bool) {
	columns, rest, ok := partitionTupleConditionPrefix(condition)
	if !ok || !samePartitionColumns(columns, expression) || !strings.HasPrefix(strings.ToLower(rest), "not in") {
		return nil, false
	}
	list := strings.TrimSpace(rest[len("not in"):])
	if len(list) < 2 || list[0] != '(' || matchingParenIndex(list, 0) != len(list)-1 {
		return nil, false
	}
	contents := list[1 : len(list)-1]
	values := make([][]interface{}, 0)
	for _, raw := range splitTopLevelComma(contents) {
		tuple, tupleOK := partitionTupleLiteral(strings.TrimSpace(raw))
		if !tupleOK {
			return nil, false
		}
		values = append(values, tuple)
	}
	return values, len(values) > 0
}

func partitionTupleBetweenPredicate(condition, expression string, negated bool) ([]interface{}, []interface{}, bool) {
	columns, rest, ok := partitionTupleConditionPrefix(condition)
	if !ok || !samePartitionColumns(columns, expression) {
		return nil, nil, false
	}
	keyword := "between"
	if negated {
		keyword = "not between"
	}
	lowerRest := strings.ToLower(rest)
	if !strings.HasPrefix(lowerRest, keyword) {
		return nil, nil, false
	}
	rest = strings.TrimSpace(rest[len(keyword):])
	if len(rest) == 0 || rest[0] != '(' {
		return nil, nil, false
	}
	lowerClose := matchingParenIndex(rest, 0)
	if lowerClose < 0 {
		return nil, nil, false
	}
	lower, lowerOK := partitionTupleLiteral(rest[:lowerClose+1])
	if !lowerOK {
		return nil, nil, false
	}
	rest = strings.TrimSpace(rest[lowerClose+1:])
	if len(rest) < 3 || !strings.EqualFold(rest[:3], "and") {
		return nil, nil, false
	}
	rest = strings.TrimSpace(rest[3:])
	upper, upperOK := partitionTupleLiteral(rest)
	if !upperOK {
		return nil, nil, false
	}
	return lower, upper, true
}

func partitionTupleConditionPrefix(condition string) ([]string, string, bool) {
	condition = strings.TrimSpace(condition)
	if len(condition) < 2 || condition[0] != '(' {
		return nil, "", false
	}
	close := matchingParenIndex(condition, 0)
	if close < 0 {
		return nil, "", false
	}
	columns := splitTopLevelComma(condition[1:close])
	return columns, strings.TrimSpace(condition[close+1:]), true
}

func samePartitionColumns(columns []string, expression string) bool {
	expected := partitionExpressionColumns(expression)
	if len(columns) != len(expected) || len(expected) < 2 {
		return false
	}
	for index := range columns {
		if !strings.EqualFold(strings.Trim(strings.TrimSpace(columns[index]), "` "), strings.Trim(expected[index], "` ")) {
			return false
		}
	}
	return true
}

func partitionTupleLiteral(raw string) ([]interface{}, bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 2 || raw[0] != '(' || matchingParenIndex(raw, 0) != len(raw)-1 {
		return nil, false
	}
	parts := splitTopLevelComma(raw[1 : len(raw)-1])
	values := make([]interface{}, 0, len(parts))
	for _, part := range parts {
		value, ok := partitionPredicateLiteral(strings.TrimSpace(part))
		if !ok {
			return nil, false
		}
		values = append(values, value)
	}
	return values, len(values) > 0
}

func intersectPartitionOrdinalSlices(left, right []int) map[int]struct{} {
	rightSet := make(map[int]struct{}, len(right))
	for _, ordinal := range right {
		rightSet[ordinal] = struct{}{}
	}
	result := make(map[int]struct{}, len(left))
	for _, ordinal := range left {
		if _, ok := rightSet[ordinal]; ok {
			result[ordinal] = struct{}{}
		}
	}
	return result
}

func unionPartitionOrdinalSlices(left, right []int) map[int]struct{} {
	result := make(map[int]struct{}, len(left)+len(right))
	for _, ordinal := range left {
		result[ordinal] = struct{}{}
	}
	for _, ordinal := range right {
		result[ordinal] = struct{}{}
	}
	return result
}

func partitionConstantBetweenPredicate(condition, expression string) (int64, int64, bool) {
	identifier := strings.Trim(strings.TrimSpace(expression), "`")
	pattern := regexp.MustCompile(`(?is)^\s*(?:[a-zA-Z0-9_$]+\s*\.\s*)?` + regexp.QuoteMeta(identifier) + `\s+between\s+(-?[0-9]+)\s+and\s+(-?[0-9]+)\s*$`)
	match := pattern.FindStringSubmatch(strings.TrimSpace(condition))
	if len(match) != 3 {
		return 0, 0, false
	}
	lower, lowerErr := strconv.ParseInt(match[1], 10, 64)
	upper, upperErr := strconv.ParseInt(match[2], 10, 64)
	if lowerErr != nil || upperErr != nil || lower > upper {
		return 0, 0, false
	}
	return lower, upper, true
}

func partitionConstantNotBetweenPredicate(condition, expression string) (int64, int64, bool) {
	identifier := strings.Trim(strings.TrimSpace(expression), "`")
	pattern := regexp.MustCompile(`(?is)^\s*(?:[a-zA-Z0-9_$]+\s*\.\s*)?` + regexp.QuoteMeta(identifier) + `\s+not\s+between\s+(-?[0-9]+)\s+and\s+(-?[0-9]+)\s*$`)
	match := pattern.FindStringSubmatch(strings.TrimSpace(condition))
	if len(match) != 3 {
		return 0, 0, false
	}
	lower, lowerErr := strconv.ParseInt(match[1], 10, 64)
	upper, upperErr := strconv.ParseInt(match[2], 10, 64)
	if lowerErr != nil || upperErr != nil || lower > upper {
		return 0, 0, false
	}
	return lower, upper, true
}

func partitionConstantTextBetweenPredicate(condition, expression string) (string, string, bool) {
	identifier := strings.Trim(strings.TrimSpace(expression), "`")
	pattern := regexp.MustCompile(`(?is)^\s*(?:[a-zA-Z0-9_$]+\s*\.\s*)?` + regexp.QuoteMeta(identifier) + `\s+between\s+('(?:''|[^'])*'|"(?:""|[^"])*")\s+and\s+('(?:''|[^'])*'|"(?:""|[^"])*")\s*$`)
	match := pattern.FindStringSubmatch(strings.TrimSpace(condition))
	if len(match) != 3 {
		return "", "", false
	}
	lowerValue, lowerOK := partitionPredicateLiteral(match[1])
	upperValue, upperOK := partitionPredicateLiteral(match[2])
	lower, lowerText := lowerValue.(string)
	upper, upperText := upperValue.(string)
	if !lowerOK || !upperOK || !lowerText || !upperText || lower > upper {
		return "", "", false
	}
	return lower, upper, true
}

func partitionConstantTextNotBetweenPredicate(condition, expression string) (string, string, bool) {
	identifier := strings.Trim(strings.TrimSpace(expression), "`")
	pattern := regexp.MustCompile(`(?is)^\s*(?:[a-zA-Z0-9_$]+\s*\.\s*)?` + regexp.QuoteMeta(identifier) + `\s+not\s+between\s+('(?:''|[^'])*'|"(?:""|[^"])*")\s+and\s+('(?:''|[^'])*'|"(?:""|[^"])*")\s*$`)
	match := pattern.FindStringSubmatch(strings.TrimSpace(condition))
	if len(match) != 3 {
		return "", "", false
	}
	lowerValue, lowerOK := partitionPredicateLiteral(match[1])
	upperValue, upperOK := partitionPredicateLiteral(match[2])
	lower, lowerText := lowerValue.(string)
	upper, upperText := upperValue.(string)
	if !lowerOK || !upperOK || !lowerText || !upperText || lower > upper {
		return "", "", false
	}
	return lower, upper, true
}

func partitionConstantNotInPredicate(condition, expression string) ([]interface{}, bool) {
	identifier := strings.Trim(strings.TrimSpace(expression), "`")
	pattern := regexp.MustCompile(`(?is)^\s*(?:[a-zA-Z0-9_$]+\s*\.\s*)?` + regexp.QuoteMeta(identifier) + `\s+not\s+in\s*\(([^)]*)\)\s*$`)
	match := pattern.FindStringSubmatch(strings.TrimSpace(condition))
	if len(match) != 2 || strings.TrimSpace(match[1]) == "" {
		return nil, false
	}
	parts := strings.Split(match[1], ",")
	values := make([]interface{}, 0, len(parts))
	for _, part := range parts {
		value, ok := partitionPredicateLiteral(part)
		if !ok {
			return nil, false
		}
		values = append(values, value)
	}
	return values, len(values) > 0
}

func partitionLiteralIdentity(value interface{}) (string, bool) {
	if text, ok := value.(string); ok {
		return "text:" + text, true
	}
	if number, ok := partitionNumericLiteralValue(value); ok {
		return partitionLiteralIdentityForInt(number), true
	}
	return "", false
}

func partitionNumericLiteralValue(value interface{}) (int64, bool) {
	switch number := value.(type) {
	case int:
		return int64(number), true
	case int8:
		return int64(number), true
	case int16:
		return int64(number), true
	case int32:
		return int64(number), true
	case int64:
		return number, true
	case uint:
		return int64(number), true
	case uint8:
		return int64(number), true
	case uint16:
		return int64(number), true
	case uint32:
		return int64(number), true
	case uint64:
		return int64(number), true
	default:
		return 0, false
	}
}

func partitionLiteralIdentityForInt(value int64) string {
	return "number:" + strconv.FormatInt(value, 10)
}

func allPartitionOrdinalSet(count int) map[int]struct{} {
	result := make(map[int]struct{}, count)
	for ordinal := 0; ordinal < count; ordinal++ {
		result[ordinal] = struct{}{}
	}
	return result
}

func partitionConstantInPredicate(condition, expression string) ([]interface{}, bool) {
	identifier := strings.Trim(strings.TrimSpace(expression), "`")
	pattern := regexp.MustCompile(`(?is)^\s*(?:[a-zA-Z0-9_$]+\s*\.\s*)?` + regexp.QuoteMeta(identifier) + `\s+in\s*\(([^)]*)\)\s*$`)
	match := pattern.FindStringSubmatch(strings.TrimSpace(condition))
	if len(match) != 2 || strings.TrimSpace(match[1]) == "" {
		return nil, false
	}
	parts := strings.Split(match[1], ",")
	values := make([]interface{}, 0, len(parts))
	for _, part := range parts {
		value, ok := partitionPredicateLiteral(part)
		if !ok {
			return nil, false
		}
		values = append(values, value)
	}
	return values, len(values) > 0
}

func partitionPredicateLiteral(raw string) (interface{}, bool) {
	raw = strings.TrimSpace(raw)
	if value, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return value, true
	}
	if len(raw) < 2 || (raw[0] != '\'' && raw[0] != '"') || raw[len(raw)-1] != raw[0] {
		return nil, false
	}
	quote := raw[0]
	value := raw[1 : len(raw)-1]
	value = strings.ReplaceAll(value, string([]byte{quote, quote}), string(quote))
	return value, true
}

func (dml *StorageIntegratedDMLExecutor) createBTreeManagerForDML(ctx context.Context, schemaName, tableName string, tableMeta *metadata.TableMeta) (basic.BPlusTreeManager, error) {
	if dml == nil || dml.tableStorageManager == nil {
		return nil, fmt.Errorf("table storage manager is not initialized")
	}
	if router, partitioned, err := newPartitionedBTreeManager(ctx, dml.tableStorageManager, schemaName, tableName, tableMeta); err != nil {
		return nil, err
	} else if partitioned {
		if err := persistPartitionStorageRoots(dml.dataDir, schemaName, tableName, dml.tableStorageManager); err != nil {
			return nil, err
		}
		return router, nil
	}
	btree, err := dml.tableStorageManager.CreateBTreeManagerForTable(ctx, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	if info, infoErr := dml.tableStorageManager.GetTableStorageInfo(schemaName, tableName); infoErr == nil {
		if persistErr := persistTableStorageIdentityIfPresent(dml.dataDir, info); persistErr != nil {
			return nil, persistErr
		}
	}
	return btree, nil
}

func (p *partitionedBTreeManager) Init(context.Context, uint32, uint32) error {
	return fmt.Errorf("partitioned btree manager is initialized by its partition managers")
}

func (p *partitionedBTreeManager) GetAllLeafPages(ctx context.Context) ([]uint32, error) {
	pages := make([]uint32, 0)
	for _, partition := range p.partitions {
		partitionPages, err := partition.btree.GetAllLeafPages(ctx)
		if err != nil {
			return nil, err
		}
		pages = append(pages, partitionPages...)
	}
	return pages, nil
}

func (p *partitionedBTreeManager) Search(ctx context.Context, key interface{}) (uint32, int, error) {
	var lastErr error
	for _, partition := range p.partitions {
		page, slot, err := partition.btree.Search(ctx, key)
		if err == nil {
			return page, slot, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("partitioned table has no physical partitions")
	}
	return 0, 0, lastErr
}

func (p *partitionedBTreeManager) Insert(ctx context.Context, key interface{}, value []byte) error {
	if p == nil || p.tableMeta == nil {
		return fmt.Errorf("partitioned btree manager requires table metadata")
	}
	row, err := DecodeClusteredRecord(value, p.tableMeta)
	if err != nil {
		return fmt.Errorf("decode row for partition routing: %w", err)
	}
	partitionIndex, err := p.partitionForRow(row)
	if err != nil {
		return err
	}
	return p.partitions[partitionIndex].btree.Insert(ctx, key, value)
}

func (p *partitionedBTreeManager) Delete(ctx context.Context, key interface{}) error {
	var lastErr error
	for _, partition := range p.partitions {
		if err := partition.btree.Delete(ctx, key); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	if lastErr == nil {
		return fmt.Errorf("partitioned table has no physical partitions")
	}
	return lastErr
}

func (p *partitionedBTreeManager) RangeSearch(ctx context.Context, startKey, endKey interface{}) ([]basic.Row, error) {
	rows := make([]basic.Row, 0)
	for _, partition := range p.partitions {
		partitionRows, err := partition.btree.RangeSearch(ctx, startKey, endKey)
		if err != nil {
			return nil, err
		}
		rows = append(rows, partitionRows...)
	}
	return rows, nil
}

// FullScan is used by ClusteredIndexScanner when present, avoiding an
// artificial key range that could exclude records in one partition.
func (p *partitionedBTreeManager) FullScan(ctx context.Context) ([]basic.Row, error) {
	rows := make([]basic.Row, 0)
	for _, partition := range p.partitions {
		if scanner, ok := partition.btree.(interface {
			FullScan(context.Context) ([]basic.Row, error)
		}); ok {
			partitionRows, err := scanner.FullScan(ctx)
			if err != nil {
				return nil, err
			}
			rows = append(rows, partitionRows...)
			continue
		}
		partitionRows, err := partition.btree.RangeSearch(ctx, partitionScanStart(p.tableMeta), partitionScanEnd(p.tableMeta))
		if err != nil {
			return nil, err
		}
		rows = append(rows, partitionRows...)
	}
	return rows, nil
}

func (p *partitionedBTreeManager) GetFirstLeafPage(ctx context.Context) (uint32, error) {
	if len(p.partitions) == 0 {
		return 0, fmt.Errorf("partitioned table has no physical partitions")
	}
	return p.partitions[0].btree.GetFirstLeafPage(ctx)
}

func (p *partitionedBTreeManager) partitionForRow(row *InsertRowData) (int, error) {
	if row == nil {
		return -1, fmt.Errorf("partition routing requires a row")
	}
	values, err := partitionExpressionValues(row.ColumnValues, p.expression)
	if err != nil {
		return -1, err
	}
	return partitionForValues(p.method, values, p.definitions)
}

func partitionDefinitionsForRouter(partitioning map[string]interface{}) []interface{} {
	if partitioning == nil {
		return nil
	}
	switch definitions := partitioning["partitions"].(type) {
	case []interface{}:
		return definitions
	case []map[string]interface{}:
		result := make([]interface{}, len(definitions))
		for index := range definitions {
			result[index] = definitions[index]
		}
		return result
	default:
		return nil
	}
}

func partitionScanStart(*metadata.TableMeta) interface{} { return int64(-1 << 63) }
func partitionScanEnd(*metadata.TableMeta) interface{}   { return int64(1<<63 - 1) }
