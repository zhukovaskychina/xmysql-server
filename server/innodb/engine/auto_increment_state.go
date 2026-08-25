package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var autoIncrementStateMu sync.Mutex

func allocateAutoIncrementValue(dataDir, schemaName, tableName, columnName string) (uint64, error) {
	autoIncrementStateMu.Lock()
	defer autoIncrementStateMu.Unlock()

	state, err := loadAutoIncrementState(dataDir)
	if err != nil {
		return 0, err
	}

	key := autoIncrementKey(schemaName, tableName, columnName)
	next := state[key]
	if next == 0 {
		next = 1
	}
	state[key] = next + 1
	if err := saveAutoIncrementState(dataDir, state); err != nil {
		return 0, err
	}
	return next, nil
}

func observeAutoIncrementValue(dataDir, schemaName, tableName, columnName string, value uint64) error {
	autoIncrementStateMu.Lock()
	defer autoIncrementStateMu.Unlock()

	state, err := loadAutoIncrementState(dataDir)
	if err != nil {
		return err
	}

	key := autoIncrementKey(schemaName, tableName, columnName)
	next := value + 1
	if next > state[key] {
		state[key] = next
		return saveAutoIncrementState(dataDir, state)
	}
	return nil
}

func clearAutoIncrementTable(dataDir, schemaName, tableName string) error {
	autoIncrementStateMu.Lock()
	defer autoIncrementStateMu.Unlock()

	state, err := loadAutoIncrementState(dataDir)
	if err != nil {
		return err
	}
	prefix := strings.ToLower(strings.TrimSpace(schemaName)) + "." + strings.ToLower(strings.TrimSpace(tableName)) + "."
	for key := range state {
		if strings.HasPrefix(key, prefix) {
			delete(state, key)
		}
	}
	return saveAutoIncrementState(dataDir, state)
}

func clearAutoIncrementDatabase(dataDir, schemaName string) error {
	autoIncrementStateMu.Lock()
	defer autoIncrementStateMu.Unlock()

	state, err := loadAutoIncrementState(dataDir)
	if err != nil {
		return err
	}
	prefix := strings.ToLower(strings.TrimSpace(schemaName)) + "."
	for key := range state {
		if strings.HasPrefix(key, prefix) {
			delete(state, key)
		}
	}
	return saveAutoIncrementState(dataDir, state)
}

func loadAutoIncrementState(dataDir string) (map[string]uint64, error) {
	path := autoIncrementStatePath(dataDir)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]uint64{}, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return map[string]uint64{}, nil
	}

	state := make(map[string]uint64)
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("load auto increment state: %v", err)
	}
	return state, nil
}

func saveAutoIncrementState(dataDir string, state map[string]uint64) error {
	path := autoIncrementStatePath(dataDir)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func autoIncrementStatePath(dataDir string) string {
	if strings.TrimSpace(dataDir) == "" {
		dataDir = "./data"
	}
	return filepath.Join(dataDir, "_xmysql_auto_increment.json")
}

func autoIncrementKey(schemaName, tableName, columnName string) string {
	return strings.ToLower(strings.TrimSpace(schemaName)) + "." +
		strings.ToLower(strings.TrimSpace(tableName)) + "." +
		strings.ToLower(strings.TrimSpace(columnName))
}
