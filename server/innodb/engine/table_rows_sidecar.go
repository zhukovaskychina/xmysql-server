package engine

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

type tableRowSidecarRecord struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Op    string `json:"op,omitempty"`
}

func tableRowsSidecarPath(dataDir, schemaName, tableName string) string {
	if dataDir == "" || schemaName == "" || tableName == "" {
		return ""
	}
	return filepath.Join(dataDir, schemaName, tableName+".xrows.json")
}

func loadTableRowsSidecar(dataDir, schemaName, tableName string) ([]tableRowSidecarRecord, error) {
	path := tableRowsSidecarPath(dataDir, schemaName, tableName)
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	var records []tableRowSidecarRecord
	if err := json.Unmarshal(data, &records); err == nil {
		return records, nil
	}

	byKey := make(map[string]tableRowSidecarRecord)
	order := make([]string, 0)
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var record tableRowSidecarRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return nil, err
		}
		if record.Op == "delete" {
			delete(byKey, record.Key)
			continue
		}
		if _, exists := byKey[record.Key]; !exists {
			order = append(order, record.Key)
		}
		byKey[record.Key] = record
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	records = make([]tableRowSidecarRecord, 0, len(byKey))
	for _, key := range order {
		record, exists := byKey[key]
		if !exists {
			continue
		}
		record.Op = ""
		records = append(records, record)
	}
	return records, nil
}

func writeTableRowsSidecar(dataDir, schemaName, tableName string, records []tableRowSidecarRecord) error {
	path := tableRowsSidecarPath(dataDir, schemaName, tableName)
	if path == "" {
		return fmt.Errorf("missing table rows sidecar path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.Marshal(records)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func appendTableRowSidecar(dataDir, schemaName, tableName string, key interface{}, value []byte) error {
	path := tableRowsSidecarPath(dataDir, schemaName, tableName)
	if path == "" {
		return fmt.Errorf("missing table rows sidecar path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	keyString := fmt.Sprintf("%v", key)
	record := tableRowSidecarRecord{
		Key:   keyString,
		Value: base64.StdEncoding.EncodeToString(value),
		Op:    "upsert",
	}
	line, err := json.Marshal(record)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write(append(line, '\n')); err != nil {
		return err
	}
	return nil
}

func deleteTableRowSidecar(dataDir, schemaName, tableName string, key interface{}) error {
	path := tableRowsSidecarPath(dataDir, schemaName, tableName)
	if path == "" {
		return fmt.Errorf("missing table rows sidecar path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	record := tableRowSidecarRecord{Key: fmt.Sprintf("%v", key), Op: "delete"}
	line, err := json.Marshal(record)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write(append(line, '\n')); err != nil {
		return err
	}
	return nil
}

func decodeTableRowsSidecar(dataDir, schemaName, tableName string, tableMeta *metadata.TableMeta) ([]*InsertRowData, error) {
	records, err := loadTableRowsSidecar(dataDir, schemaName, tableName)
	if err != nil || len(records) == 0 {
		return nil, err
	}
	rows := make([]*InsertRowData, 0, len(records))
	dml := &StorageIntegratedDMLExecutor{}
	for _, record := range records {
		value, err := base64.StdEncoding.DecodeString(record.Value)
		if err != nil {
			return nil, err
		}
		row, err := dml.deserializeRowData(value, tableMeta)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}
