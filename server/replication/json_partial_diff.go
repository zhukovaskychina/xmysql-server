package replication

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
)

// DeriveJSONPartialUpdates converts two JSON row images into a bounded set of
// object-member diffs suitable for a native PARTIAL_UPDATE_ROWS_EVENT. It
// deliberately falls back for values that are not valid JSON or for SQL NULL
// row images, so callers can retain the full-value event path.
func DeriveJSONPartialUpdates(before, after interface{}) ([]JSONPartialUpdate, bool) {
	beforeValue, beforeOK := decodeJSONPartialValue(before)
	afterValue, afterOK := decodeJSONPartialValue(after)
	if !beforeOK || !afterOK || jsonPartialValuesEqual(beforeValue, afterValue) {
		return nil, false
	}
	updates := make([]JSONPartialUpdate, 0)
	deriveJSONPartialUpdates("$", beforeValue, afterValue, &updates)
	return updates, len(updates) > 0
}

// ApplyJSONPartialUpdates applies the bounded object-path diff emitted by
// DeriveJSONPartialUpdates and returns a canonical JSON document. It is used
// by row-image appliers when a native event carries a before image plus a
// partial after-image instead of a complete after document.
func ApplyJSONPartialUpdates(before interface{}, updates []JSONPartialUpdate) (string, bool) {
	value, ok := decodeJSONPartialValue(before)
	if !ok {
		return "", false
	}
	for _, update := range updates {
		tokens, parsed := parseJSONPartialPath(update.Path)
		if !parsed {
			return "", false
		}
		if len(tokens) == 0 {
			switch update.Operation {
			case JSONPartialOperationReplace, JSONPartialOperationInsert:
				value = update.Value
			case JSONPartialOperationRemove:
				value = nil
			default:
				return "", false
			}
			continue
		}
		object, objectOK := value.(map[string]interface{})
		if !objectOK {
			return "", false
		}
		for _, token := range tokens[:len(tokens)-1] {
			child, childOK := object[token].(map[string]interface{})
			if !childOK {
				return "", false
			}
			object = child
		}
		key := tokens[len(tokens)-1]
		switch update.Operation {
		case JSONPartialOperationReplace, JSONPartialOperationInsert:
			object[key] = update.Value
		case JSONPartialOperationRemove:
			delete(object, key)
		default:
			return "", false
		}
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", false
	}
	return string(raw), true
}

func parseJSONPartialPath(path string) ([]string, bool) {
	if path == "$" {
		return nil, true
	}
	if !strings.HasPrefix(path, "$.") {
		return nil, false
	}
	path = path[2:]
	tokens := make([]string, 0, 2)
	for len(path) > 0 {
		if path[0] == '"' {
			end := 1
			escaped := false
			for end < len(path) {
				if path[end] == '"' && !escaped {
					break
				}
				if path[end] == '\\' && !escaped {
					escaped = true
				} else {
					escaped = false
				}
				end++
			}
			if end >= len(path) {
				return nil, false
			}
			var token string
			if err := json.Unmarshal([]byte(path[:end+1]), &token); err != nil {
				return nil, false
			}
			tokens = append(tokens, token)
			path = path[end+1:]
		} else {
			end := strings.IndexByte(path, '.')
			if end < 0 {
				end = len(path)
			}
			if end == 0 {
				return nil, false
			}
			tokens = append(tokens, path[:end])
			path = path[end:]
		}
		if len(path) == 0 {
			break
		}
		if path[0] != '.' {
			return nil, false
		}
		path = path[1:]
		if len(path) == 0 {
			return nil, false
		}
	}
	return tokens, true
}

func decodeJSONPartialValue(value interface{}) (interface{}, bool) {
	if value == nil {
		return nil, false
	}
	var raw []byte
	switch typed := value.(type) {
	case json.RawMessage:
		raw = []byte(typed)
	case string:
		raw = []byte(typed)
	case []byte:
		raw = typed
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, false
		}
		raw = encoded
	}
	var decoded interface{}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, false
	}
	return decoded, true
}

func jsonPartialValuesEqual(left, right interface{}) bool {
	leftRaw, leftErr := json.Marshal(left)
	rightRaw, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftRaw) == string(rightRaw)
}

func deriveJSONPartialUpdates(path string, before, after interface{}, updates *[]JSONPartialUpdate) {
	if jsonPartialValuesEqual(before, after) {
		return
	}
	beforeObject, beforeIsObject := before.(map[string]interface{})
	afterObject, afterIsObject := after.(map[string]interface{})
	if beforeIsObject && afterIsObject {
		keys := make([]string, 0, len(beforeObject)+len(afterObject))
		seen := make(map[string]struct{}, len(beforeObject)+len(afterObject))
		for key := range beforeObject {
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
		for key := range afterObject {
			if _, exists := seen[key]; !exists {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		for _, key := range keys {
			childPath := jsonPartialPath(path, key)
			beforeValue, beforeExists := beforeObject[key]
			afterValue, afterExists := afterObject[key]
			switch {
			case !beforeExists && afterExists:
				*updates = append(*updates, JSONPartialUpdate{Operation: JSONPartialOperationInsert, Path: childPath, Value: afterValue})
			case beforeExists && !afterExists:
				*updates = append(*updates, JSONPartialUpdate{Operation: JSONPartialOperationRemove, Path: childPath})
			case beforeExists && afterExists:
				deriveJSONPartialUpdates(childPath, beforeValue, afterValue, updates)
			}
		}
		return
	}
	*updates = append(*updates, JSONPartialUpdate{Operation: JSONPartialOperationReplace, Path: path, Value: after})
}

func jsonPartialPath(parent, key string) string {
	if key != "" {
		valid := true
		for index, character := range key {
			if !(character == '_' || character == '$' || character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || index > 0 && character >= '0' && character <= '9') {
				valid = false
				break
			}
		}
		if valid {
			return parent + "." + key
		}
	}
	return parent + "." + strconv.Quote(key)
}
