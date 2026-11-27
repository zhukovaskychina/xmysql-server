package manager

import (
	"fmt"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// storagePageManagerAdapter exposes the durable StorageManager page path to
// older consumers such as the insert buffer. It deliberately keeps the
// legacy PageManager contract small: page bytes are read/written through the
// same StorageProviderAdapter used by the optimized buffer pool.
type storagePageManagerAdapter struct {
	storageManager *StorageManager
}

func (a *storagePageManagerAdapter) provider() (*StorageProviderAdapter, error) {
	if a == nil || a.storageManager == nil || a.storageManager.spaceMgr == nil {
		return nil, fmt.Errorf("storage manager is not initialized")
	}
	return &StorageProviderAdapter{
		spaceManager: a.storageManager.spaceMgr,
		sm:           a.storageManager,
	}, nil
}

func (a *storagePageManagerAdapter) AllocPage(spaceID uint32) (uint32, error) {
	provider, err := a.provider()
	if err != nil {
		return 0, err
	}
	return provider.AllocatePage(spaceID)
}

func (a *storagePageManagerAdapter) FreePage(spaceID, pageNo uint32) error {
	if a == nil || a.storageManager == nil {
		return fmt.Errorf("storage manager is not initialized")
	}
	if a.storageManager.bufferPoolMgr == nil {
		return fmt.Errorf("buffer pool manager is not initialized")
	}
	return a.storageManager.bufferPoolMgr.FreePage(spaceID, pageNo)
}

func (a *storagePageManagerAdapter) GetPage(spaceID, pageNo uint32) ([]byte, error) {
	provider, err := a.provider()
	if err != nil {
		return nil, err
	}
	return provider.ReadPage(spaceID, pageNo)
}

func (a *storagePageManagerAdapter) WritePage(spaceID, pageNo uint32, content []byte) error {
	provider, err := a.provider()
	if err != nil {
		return err
	}
	return provider.WritePage(spaceID, pageNo, content)
}

func (a *storagePageManagerAdapter) FlushPage(spaceID, pageNo uint32) error {
	if a == nil || a.storageManager == nil {
		return fmt.Errorf("storage manager is not initialized")
	}
	if a.storageManager.bufferPoolMgr == nil {
		return fmt.Errorf("buffer pool manager is not initialized")
	}
	return a.storageManager.bufferPoolMgr.FlushPage(spaceID, pageNo)
}

func (a *storagePageManagerAdapter) ScanLeaves(uint32) (interface{}, interface{}) {
	return nil, fmt.Errorf("page-manager leaf scan is not available through the legacy adapter")
}

func (a *storagePageManagerAdapter) InsertKey(uint32, []byte, basic.Record) interface{} {
	return fmt.Errorf("page-manager key insertion is not available through the legacy adapter")
}

func (a *storagePageManagerAdapter) AllocatePage(interface{}) (interface{}, interface{}) {
	return nil, fmt.Errorf("page-manager typed allocation requires a tablespace ID")
}

var _ basic.PageManager = (*storagePageManagerAdapter)(nil)
