package engine

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/content"
)

const contentSegmentStoreDirectory = ".xmysql-content"

var contentSegmentStoreCache = struct {
	sync.Mutex
	stores map[string]*content.SegmentStore
}{stores: make(map[string]*content.SegmentStore)}

func contentSegmentStoreForDataDir(dataDir string) (*content.SegmentStore, error) {
	return openContentSegmentStore(dataDir, nil)
}

// OpenContentSegmentStore returns the database-owned segment store shared by
// DML and protocol upload paths. An explicit limit must match the limit used
// when this process first opened the store for the data directory.
func OpenContentSegmentStore(dataDir string, maxObjectBytes uint64) (*content.SegmentStore, error) {
	options := &content.SegmentStoreOptions{MaxObjectBytes: maxObjectBytes}
	return openContentSegmentStore(dataDir, options)
}

func openContentSegmentStore(dataDir string, requested *content.SegmentStoreOptions) (*content.SegmentStore, error) {
	dataDir = strings.TrimSpace(dataDir)
	if dataDir == "" {
		return nil, content.ErrExternalContentRequiresStore
	}
	root, err := filepath.Abs(filepath.Join(dataDir, contentSegmentStoreDirectory))
	if err != nil {
		return nil, fmt.Errorf("resolve database content store path: %w", err)
	}
	key := filepath.Clean(root)
	contentSegmentStoreCache.Lock()
	defer contentSegmentStoreCache.Unlock()
	if store := contentSegmentStoreCache.stores[key]; store != nil {
		if requested != nil && requested.MaxObjectBytes != 0 && store.MaxObjectBytes() != requested.MaxObjectBytes {
			return nil, fmt.Errorf("database CONTENT store already uses max_object_bytes=%d, requested %d", store.MaxObjectBytes(), requested.MaxObjectBytes)
		}
		return store, nil
	}
	options := content.SegmentStoreOptions{}
	if requested != nil {
		options = *requested
	}
	store, err := content.NewSegmentStore(root, options)
	if err != nil {
		return nil, err
	}
	contentSegmentStoreCache.stores[key] = store
	return store, nil
}

func contentSegmentStoreForValue(values map[string]interface{}, value content.Value) (*content.SegmentStore, error) {
	if value.ExternalRef == nil {
		return nil, nil
	}
	dataDir, _ := values[storedFunctionDataDirKey].(string)
	return contentSegmentStoreForDataDir(dataDir)
}
