package plan

import "fmt"

// PartitionRule is the planner-facing subset of a persisted partition
// descriptor needed for constant predicate pruning.
type PartitionRule struct {
	Name          string
	LessThan      *int64
	LessThanText  *string
	LessThanTuple []interface{}
	MaxValue      bool
	ListValue     []int64
	ListText      []string
	ListTuple     [][]interface{}
}

// PrunePartitions returns the partition ordinals that can satisfy a constant
// predicate. An unknown/NULL value conservatively returns all partitions.
func PrunePartitions(method, operator string, value interface{}, rules []PartitionRule) ([]int, error) {
	if value == nil {
		return allPartitionOrdinals(len(rules)), nil
	}
	key, numericValue := numericPartitionValue(value)
	result := make([]int, 0, len(rules))
	switch method {
	case "LIST":
		if tuple, tupleValue := tuplePartitionValue(value); tupleValue {
			for i, rule := range rules {
				for _, item := range rule.ListTuple {
					comparison, err := compareTuplePartitionValues(tuple, item)
					if err == nil && comparison == 0 && operator == "=" {
						result = append(result, i)
						break
					}
				}
			}
			return uniquePartitionOrdinals(result), nil
		}
		if numeric, numericValue := numericPartitionValue(value); numericValue {
			for i, rule := range rules {
				for _, item := range rule.ListValue {
					if comparePartitionValue(operator, item, numeric) {
						result = append(result, i)
						break
					}
				}
			}
		} else if text, textValue := textPartitionValue(value); textValue {
			for i, rule := range rules {
				for _, item := range rule.ListText {
					if operator == "=" && item == text {
						result = append(result, i)
						break
					}
				}
			}
		} else {
			return nil, fmt.Errorf("partition pruning requires a numeric or text constant, got %T", value)
		}
	case "HASH", "KEY":
		if !numericValue {
			return nil, fmt.Errorf("partition pruning requires a numeric constant, got %T", value)
		}
		if operator == "=" && len(rules) > 0 {
			result = append(result, intPositiveMod(key, int64(len(rules))))
		} else {
			result = allPartitionOrdinals(len(rules))
		}
	case "RANGE":
		if tuple, tupleValue := tuplePartitionValue(value); tupleValue {
			for _, rule := range rules {
				if !rule.MaxValue && len(rule.LessThanTuple) == 0 {
					return allPartitionOrdinals(len(rules)), nil
				}
			}
			var lower []interface{}
			hasLower := false
			for i, rule := range rules {
				upperKnown := len(rule.LessThanTuple) > 0 && !rule.MaxValue
				upper := rule.LessThanTuple
				matches := false
				if operator == "=" {
					matches = tupleInHalfOpenInterval(tuple, lower, hasLower, upper, upperKnown)
				} else if operator == "<" || operator == "<=" {
					matches = !hasLower
					if hasLower {
						comparison, err := compareTuplePartitionValues(lower, tuple)
						if err != nil {
							return allPartitionOrdinals(len(rules)), nil
						}
						matches = comparison < 0 || (operator == "<=" && comparison == 0)
					}
				} else if operator == ">" || operator == ">=" {
					matches = !upperKnown
					if upperKnown {
						comparison, err := compareTuplePartitionValues(upper, tuple)
						if err != nil {
							return allPartitionOrdinals(len(rules)), nil
						}
						matches = comparison > 0
					}
				}
				if matches {
					result = append(result, i)
				}
				if upperKnown {
					lower = upper
					hasLower = true
				}
			}
			return uniquePartitionOrdinals(result), nil
		}
		if !numericValue {
			text, textValue := textPartitionValue(value)
			if !textValue {
				return nil, fmt.Errorf("partition pruning requires a numeric or text constant, got %T", value)
			}
			for _, rule := range rules {
				if !rule.MaxValue && rule.LessThanText == nil {
					return allPartitionOrdinals(len(rules)), nil
				}
			}
			var lower string
			hasLower := false
			for i, rule := range rules {
				upperKnown := rule.LessThanText != nil && !rule.MaxValue
				upper := ""
				if upperKnown {
					upper = *rule.LessThanText
				}
				matches := false
				switch operator {
				case "=":
					matches = (!hasLower || text >= lower) && (!upperKnown || text < upper)
				case "<":
					matches = !hasLower || lower < text
				case "<=":
					matches = !hasLower || lower <= text
				case ">", ">=":
					matches = !upperKnown || upper > text
				}
				if matches {
					result = append(result, i)
				}
				if upperKnown {
					lower = upper
					hasLower = true
				}
			}
			return uniquePartitionOrdinals(result), nil
		}
		for _, rule := range rules {
			if !rule.MaxValue && rule.LessThanText != nil {
				return allPartitionOrdinals(len(rules)), nil
			}
		}
		var lower int64
		hasLower := false
		for i, rule := range rules {
			upperKnown := rule.LessThan != nil && !rule.MaxValue
			upper := int64(0)
			if upperKnown {
				upper = *rule.LessThan
			}
			matches := false
			switch operator {
			case "=":
				matches = (!hasLower || key >= lower) && (!upperKnown || key < upper)
			case "<":
				// The half-open partition interval has a value below key iff
				// its lower bound is below key.
				matches = !hasLower || lower < key
			case "<=":
				matches = !hasLower || lower <= key
			case ">", ">=":
				// The interval has a value greater than or equal to key iff
				// its exclusive upper bound is greater than key (or infinite).
				matches = !upperKnown || upper > key
			}
			if matches {
				result = append(result, i)
			}
			if upperKnown {
				lower = upper
				hasLower = true
			}
		}
	default:
		return nil, fmt.Errorf("unsupported partition method %q", method)
	}
	return uniquePartitionOrdinals(result), nil
}

func tuplePartitionValue(value interface{}) ([]interface{}, bool) {
	tuple, ok := value.([]interface{})
	return tuple, ok && len(tuple) > 0
}

func tupleInHalfOpenInterval(value, lower []interface{}, hasLower bool, upper []interface{}, hasUpper bool) bool {
	if hasLower {
		comparison, err := compareTuplePartitionValues(value, lower)
		if err != nil || comparison < 0 {
			return false
		}
	}
	if hasUpper {
		comparison, err := compareTuplePartitionValues(value, upper)
		if err != nil || comparison >= 0 {
			return false
		}
	}
	return true
}

func compareTuplePartitionValues(left, right []interface{}) (int, error) {
	if len(left) != len(right) {
		return 0, fmt.Errorf("partition tuple arity mismatch")
	}
	for index := range left {
		leftNumber, leftNumeric := numericPartitionValue(left[index])
		rightNumber, rightNumeric := numericPartitionValue(right[index])
		if leftNumeric && rightNumeric {
			if leftNumber < rightNumber {
				return -1, nil
			}
			if leftNumber > rightNumber {
				return 1, nil
			}
			continue
		}
		leftText, leftTextOK := textPartitionValue(left[index])
		rightText, rightTextOK := textPartitionValue(right[index])
		if !leftTextOK || !rightTextOK {
			return 0, fmt.Errorf("partition tuple values have incompatible types")
		}
		if leftText < rightText {
			return -1, nil
		}
		if leftText > rightText {
			return 1, nil
		}
	}
	return 0, nil
}

func textPartitionValue(value interface{}) (string, bool) {
	switch text := value.(type) {
	case string:
		return text, true
	case []byte:
		return string(text), true
	default:
		return "", false
	}
}

func numericPartitionValue(value interface{}) (int64, bool) {
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

func comparePartitionValue(operator string, left, right int64) bool {
	switch operator {
	case "=":
		return left == right
	case "<":
		return left < right
	case "<=":
		return left <= right
	case ">":
		return left > right
	case ">=":
		return left >= right
	default:
		return false
	}
}

func intPositiveMod(value, divisor int64) int {
	result := value % divisor
	if result < 0 {
		result += divisor
	}
	return int(result)
}

func allPartitionOrdinals(count int) []int {
	result := make([]int, count)
	for i := range result {
		result[i] = i
	}
	return result
}

func uniquePartitionOrdinals(values []int) []int {
	seen := make(map[int]struct{}, len(values))
	result := make([]int, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
