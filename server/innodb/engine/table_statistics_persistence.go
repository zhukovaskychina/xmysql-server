package engine

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

const (
	persistedTableStatisticsFormatVersion = 1
	persistedTableStatisticsSuffix        = ".stats.json"
)

// persistedTableStatistics is a versioned, checksummed optimizer-statistics
// sidecar. The .frm file remains the compatibility source for older tables;
// the sidecar makes ANALYZE output independently identifiable and replaceable.
type persistedTableStatistics struct {
	FormatVersion    int                      `json:"format_version"`
	SchemaName       string                   `json:"schema_name"`
	TableName        string                   `json:"table_name"`
	TableFingerprint string                   `json:"table_fingerprint"`
	CollectedAt      time.Time                `json:"collected_at"`
	Stats            *metadata.InfoTableStats `json:"stats"`
	Checksum         string                   `json:"checksum"`
}

type persistedTableStatisticsPayload struct {
	FormatVersion    int                      `json:"format_version"`
	SchemaName       string                   `json:"schema_name"`
	TableName        string                   `json:"table_name"`
	TableFingerprint string                   `json:"table_fingerprint"`
	CollectedAt      time.Time                `json:"collected_at"`
	Stats            *metadata.InfoTableStats `json:"stats"`
}

func tableStatisticsSidecarPath(dataDir, schemaName, tableName string) string {
	return filepath.Join(dataDir, schemaName, tableName+persistedTableStatisticsSuffix)
}

func persistedTableMetadataFingerprint(info *persistedTableInfo) (string, error) {
	if info == nil {
		return "", fmt.Errorf("table metadata is nil")
	}
	metadataCopy := *info
	metadataCopy.Stats = nil
	raw, err := json.Marshal(metadataCopy)
	if err != nil {
		return "", fmt.Errorf("marshal table metadata fingerprint: %w", err)
	}
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("%x", sum[:]), nil
}

func persistedTableStatisticsChecksum(payload persistedTableStatisticsPayload) (string, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal table statistics checksum payload: %w", err)
	}
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("%x", sum[:]), nil
}

func (e *XMySQLExecutor) persistTableStatisticsSidecar(schemaName, tableName string, info *persistedTableInfo, stats *metadata.InfoTableStats) error {
	fingerprint, err := persistedTableMetadataFingerprint(info)
	if err != nil {
		return err
	}
	payload := persistedTableStatisticsPayload{
		FormatVersion:    persistedTableStatisticsFormatVersion,
		SchemaName:       schemaName,
		TableName:        tableName,
		TableFingerprint: fingerprint,
		CollectedAt:      time.Now().UTC(),
		Stats:            stats,
	}
	checksum, err := persistedTableStatisticsChecksum(payload)
	if err != nil {
		return err
	}
	record := persistedTableStatistics{
		FormatVersion:    payload.FormatVersion,
		SchemaName:       payload.SchemaName,
		TableName:        payload.TableName,
		TableFingerprint: payload.TableFingerprint,
		CollectedAt:      payload.CollectedAt,
		Stats:            payload.Stats,
		Checksum:         checksum,
	}
	raw, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal table statistics sidecar: %w", err)
	}
	return writeMetadataFileAtomic(tableStatisticsSidecarPath(e.getDataDir(), schemaName, tableName), raw)
}

func (e *XMySQLExecutor) readPersistedTableStatistics(schemaName, tableName string, info *persistedTableInfo) (*metadata.InfoTableStats, error) {
	path := tableStatisticsSidecarPath(e.getDataDir(), schemaName, tableName)
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read table statistics sidecar: %w", err)
	}
	var record persistedTableStatistics
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, fmt.Errorf("parse table statistics sidecar: %w", err)
	}
	if record.FormatVersion != persistedTableStatisticsFormatVersion {
		return nil, fmt.Errorf("unsupported table statistics sidecar version %d", record.FormatVersion)
	}
	if record.SchemaName != schemaName || record.TableName != tableName || record.Stats == nil {
		return nil, fmt.Errorf("table statistics sidecar identity mismatch")
	}
	fingerprint, err := persistedTableMetadataFingerprint(info)
	if err != nil {
		return nil, err
	}
	if record.TableFingerprint != fingerprint {
		return nil, fmt.Errorf("table statistics sidecar metadata fingerprint mismatch")
	}
	payload := persistedTableStatisticsPayload{
		FormatVersion:    record.FormatVersion,
		SchemaName:       record.SchemaName,
		TableName:        record.TableName,
		TableFingerprint: record.TableFingerprint,
		CollectedAt:      record.CollectedAt,
		Stats:            record.Stats,
	}
	checksum, err := persistedTableStatisticsChecksum(payload)
	if err != nil {
		return nil, err
	}
	if record.Checksum == "" || record.Checksum != checksum {
		return nil, fmt.Errorf("table statistics sidecar checksum mismatch")
	}
	return record.Stats, nil
}
