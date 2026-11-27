package manager

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const compressionPolicyVersion = 1

type persistedCompressionPolicy struct {
	Version          int                  `json:"version"`
	DefaultSettings  *CompressionSettings `json:"default_settings,omitempty"`
	CompressedSpaces []uint32             `json:"compressed_spaces,omitempty"`
	ExcludedSpaces   []uint32             `json:"excluded_spaces,omitempty"`
}

func loadCompressionPolicy(path string) (compressionPolicySnapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return compressionPolicySnapshot{}, err
	}
	var persisted persistedCompressionPolicy
	if err := json.Unmarshal(data, &persisted); err != nil {
		return compressionPolicySnapshot{}, fmt.Errorf("decode compression policy: %w", err)
	}
	if persisted.Version != compressionPolicyVersion {
		return compressionPolicySnapshot{}, fmt.Errorf("unsupported compression policy version %d", persisted.Version)
	}
	return compressionPolicySnapshot{
		DefaultSettings:  persisted.DefaultSettings,
		CompressedSpaces: append([]uint32(nil), persisted.CompressedSpaces...),
		ExcludedSpaces:   append([]uint32(nil), persisted.ExcludedSpaces...),
	}, nil
}

func saveCompressionPolicy(path string, snapshot compressionPolicySnapshot) error {
	if path == "" {
		return nil
	}
	persisted := persistedCompressionPolicy{
		Version:          compressionPolicyVersion,
		DefaultSettings:  snapshot.DefaultSettings,
		CompressedSpaces: append([]uint32(nil), snapshot.CompressedSpaces...),
		ExcludedSpaces:   append([]uint32(nil), snapshot.ExcludedSpaces...),
	}
	data, err := json.MarshalIndent(persisted, "", "  ")
	if err != nil {
		return fmt.Errorf("encode compression policy: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create compression policy directory: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".compression-policy-*.tmp")
	if err != nil {
		return fmt.Errorf("create compression policy temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write compression policy: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync compression policy: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close compression policy: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("replace compression policy: %w", err)
	}
	return nil
}

func (sm *StorageManager) SetSpaceCompressed(spaceID uint32, compressed bool) error {
	if sm == nil || sm.compressedProvider == nil {
		return errors.New("compression is not enabled")
	}
	sm.compressedProvider.SetSpaceCompressed(spaceID, compressed)
	if err := sm.persistCompressionPolicy(); err != nil {
		return err
	}
	return nil
}

func (sm *StorageManager) persistCompressionPolicy() error {
	if sm == nil || sm.compressedProvider == nil || sm.compressionPolicyPath == "" {
		return nil
	}
	return saveCompressionPolicy(sm.compressionPolicyPath, sm.compressedProvider.compressionPolicySnapshot())
}
