package engine

import (
	"fmt"
	"os"
	"path/filepath"
)

func clearBTreeSidecarForSpace(dataDir string, spaceID uint32) error {
	if dataDir == "" {
		dataDir = "data"
	}
	pattern := filepath.Join(dataDir, "_xmysql_btree_records", fmt.Sprintf("space_%d_page_*.json", spaceID))
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	for _, path := range matches {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
