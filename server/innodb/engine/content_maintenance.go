package engine

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
)

const contentMaintenanceCaptureErrorParam = "content_maintenance_capture_error"

func cloneMaintenanceRequests(requests []content.MaintenanceRequest) []content.MaintenanceRequest {
	if len(requests) == 0 {
		return nil
	}
	cloned := make([]content.MaintenanceRequest, len(requests))
	copy(cloned, requests)
	for index := range cloned {
		cloned[index].Source.PrimaryKey = append([]byte(nil), requests[index].Source.PrimaryKey...)
	}
	return cloned
}

func textMaintenanceRequestsForChange(change transactionDMLChange) ([]content.MaintenanceRequest, error) {
	if change.kind != "insert" && change.kind != "update" || len(change.after) == 0 {
		return nil, nil
	}
	schema, table, ok := strings.Cut(strings.TrimSpace(change.tableName), ".")
	if !ok || schema == "" || table == "" {
		return nil, fmt.Errorf("invalid CONTENT maintenance source table %q", change.tableName)
	}
	columns := make([]string, 0)
	for name, typeName := range change.columnTypes {
		if strings.EqualFold(strings.TrimSpace(typeName), "CONTENT") {
			columns = append(columns, name)
		}
	}
	sort.Slice(columns, func(i, j int) bool { return strings.ToLower(columns[i]) < strings.ToLower(columns[j]) })
	type sourceColumn struct {
		name  string
		value *content.Value
	}
	sources := make([]sourceColumn, 0, len(columns))
	for _, column := range columns {
		value := rowContentValue(change.after, column)
		if value != nil && content.IsTextMIMEType(value.MIMEType) {
			sources = append(sources, sourceColumn{name: column, value: value})
		}
	}
	if len(sources) == 0 {
		return nil, nil
	}
	primaryKey := change.storageKey
	if change.newStorageKey != nil {
		if _, ok := storageKeyToBytes(change.newStorageKey); ok {
			primaryKey = change.newStorageKey
		}
	}
	primaryKeyBytes, ok := storageKeyToBytes(primaryKey)
	if !ok {
		return nil, fmt.Errorf("CONTENT maintenance source %s.%s has no stable primary key", schema, table)
	}
	requests := make([]content.MaintenanceRequest, 0, len(columns))
	for _, sourceColumn := range sources {
		source, err := content.NewSourceRef(schema, table, sourceColumn.name, primaryKeyBytes, *sourceColumn.value)
		if err != nil {
			return nil, fmt.Errorf("build CONTENT maintenance source %s.%s.%s: %w", schema, table, sourceColumn.name, err)
		}
		request, err := content.NewTextChunkMaintenanceRequest(source, content.TextChunkOptions{MaxBytes: content.DefaultTextChunkBytes})
		if err != nil {
			return nil, fmt.Errorf("build CONTENT text maintenance request: %w", err)
		}
		requests = append(requests, request)
	}
	return requests, nil
}

func (e *XMySQLExecutor) attachContentMaintenanceRequests(changes []transactionDMLChange) error {
	for index := range changes {
		if !transactionChangeHasContentColumn(changes[index].columnTypes) {
			continue
		}
		requests, err := textMaintenanceRequestsForChange(changes[index])
		if err != nil {
			return fmt.Errorf("build durable CONTENT maintenance intent failed: %w", err)
		}
		changes[index].maintenanceRequests = requests
	}
	return nil
}

func (e *XMySQLExecutor) enqueueContentMaintenance(changes []transactionDMLChange) error {
	if e == nil || e.contentMaintenanceQueue == nil {
		return nil
	}
	now := time.Now().UTC()
	for _, change := range changes {
		for _, request := range change.maintenanceRequests {
			if _, _, err := e.contentMaintenanceQueue.Enqueue(request, now); err != nil {
				return fmt.Errorf("enqueue CONTENT maintenance for %s: %w", request.Source.Table, err)
			}
		}
	}
	return nil
}
