package space

import (
	"fmt"
	"sync"
	"sync/atomic"
	"xmysql-server/server/innodb/basic"
	"xmysql-server/server/innodb/storage/store/ibd"
)

type spaceManagerImpl struct {
	sync.RWMutex
	spaces   map[uint32]*IBDSpace
	ibdFiles map[uint32]*ibd.IBD_File
	nameToID map[string]uint32
	nextID   uint32
	dataDir  string
	txID     uint64
}

func (sm *spaceManagerImpl) FlushToDisk(pageNo uint32, content []byte) {
	//TODO implement me
	panic("implement me")
}

func (sm *spaceManagerImpl) LoadPageByPageNumber(pageNo uint32) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (sm *spaceManagerImpl) GetSpaceId() uint32 {
	//TODO implement me
	panic("implement me")
}

func NewSpaceManager(dataDir string) basic.SpaceManager {
	return &spaceManagerImpl{
		spaces:   make(map[uint32]*IBDSpace),
		ibdFiles: make(map[uint32]*ibd.IBD_File),
		nameToID: make(map[string]uint32),
		nextID:   1,
		dataDir:  dataDir,
		txID:     1,
	}
}

func (sm *spaceManagerImpl) CreateSpace(spaceID uint32, name string, isSystem bool) (basic.Space, error) {
	sm.Lock()
	defer sm.Unlock()

	if _, exists := sm.nameToID[name]; exists {
		return nil, fmt.Errorf("tablespace %s already exists", name)
	}

	ibdFile := ibd.NewIBDFile(sm.dataDir, name, spaceID)
	err := ibdFile.Create()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize tablespace: %v", err)
	}

	space := NewIBDSpace(ibdFile, isSystem)
	sm.spaces[spaceID] = space
	sm.ibdFiles[spaceID] = ibdFile
	sm.nameToID[name] = spaceID

	return space, nil
}

func (sm *spaceManagerImpl) GetSpace(spaceID uint32) (basic.Space, error) {
	sm.RLock()
	defer sm.RUnlock()

	space, exists := sm.spaces[spaceID]
	if !exists {
		return nil, fmt.Errorf("tablespace %d not found", spaceID)
	}
	return space, nil
}

func (sm *spaceManagerImpl) DropSpace(spaceID uint32) error {
	sm.Lock()
	defer sm.Unlock()

	ibdFile, exists := sm.spaces[spaceID]
	if !exists {
		return fmt.Errorf("tablespace %d not found", spaceID)
	}
	err := ibdFile.DropTable()
	if err != nil {
		return fmt.Errorf("failed to drop tablespace: %v", err)
	}

	delete(sm.nameToID, ibdFile.GetTableName())
	delete(sm.spaces, spaceID)
	delete(sm.ibdFiles, spaceID)

	return nil
}

func (sm *spaceManagerImpl) AllocateExtent(spaceID uint32, purpose basic.ExtentPurpose) (basic.Extent, error) {
	sm.RLock()
	defer sm.RUnlock()

	space, exists := sm.spaces[spaceID]
	if !exists {
		return nil, fmt.Errorf("tablespace %d not found", spaceID)
	}

	return space.AllocateExtent(purpose)
}

func (sm *spaceManagerImpl) FreeExtent(spaceID, extentID uint32) error {
	sm.RLock()
	defer sm.RUnlock()

	space, exists := sm.spaces[spaceID]
	if !exists {
		return fmt.Errorf("tablespace %d not found", spaceID)
	}

	return space.FreeExtent(extentID)
}

func (sm *spaceManagerImpl) Begin() (basic.Tx, error) {
	txID := atomic.AddUint64(&sm.txID, 1)
	return newSpaceTx(txID), nil
}

func (sm *spaceManagerImpl) CreateNewTablespace(name string) uint32 {
	sm.Lock()
	spaceID := sm.nextID
	sm.nextID++
	sm.Unlock()

	_, err := sm.CreateSpace(spaceID, name, false)
	if err != nil {
		panic(err)
	}

	return spaceID
}

func (sm *spaceManagerImpl) CreateTableSpace(name string) (uint32, error) {
	sm.Lock()
	defer sm.Unlock()

	if _, exists := sm.nameToID[name]; exists {
		return 0, fmt.Errorf("tablespace %s already exists", name)
	}

	spaceID := sm.nextID
	sm.nextID++

	ibdFile := ibd.NewIBDFile(sm.dataDir, name, spaceID)
	err := ibdFile.Create()
	if err != nil {
		return 0, fmt.Errorf("failed to initialize tablespace: %v", err)
	}

	space := NewIBDSpace(ibdFile, false)
	sm.spaces[spaceID] = space
	sm.nameToID[name] = spaceID
	return spaceID, nil
}

func (sm *spaceManagerImpl) GetTableSpace(spaceID uint32) (basic.FileTableSpace, error) {
	sm.RLock()
	defer sm.RUnlock()

	space, exists := sm.spaces[spaceID]
	if !exists {
		return nil, fmt.Errorf("tablespace %d not found", spaceID)
	}
	return space.AsFileTableSpace(), nil
}

func (sm *spaceManagerImpl) GetTableSpaceByName(name string) (basic.FileTableSpace, error) {
	sm.RLock()
	defer sm.RUnlock()

	spaceID, exists := sm.nameToID[name]
	if !exists {
		return nil, fmt.Errorf("tablespace %s not found", name)
	}
	return sm.spaces[uint32(spaceID)].AsFileTableSpace(), nil
}

func (sm *spaceManagerImpl) GetTableSpaceInfo(spaceID uint32) (*basic.TableSpaceInfo, error) {
	sm.RLock()
	defer sm.RUnlock()

	space, exists := sm.spaces[spaceID]
	if !exists {
		return nil, fmt.Errorf("tablespace %d not found", spaceID)
	}

	info := &basic.TableSpaceInfo{
		SpaceID:      spaceID,
		Name:         space.Name(),
		FilePath:     space.GetFilePath(),
		Size:         0, // TODO: Implement size tracking
		FreeSpace:    0, // TODO: Implement free space tracking
		SegmentCount: 0, // TODO: Implement segment counting
	}
	return info, nil
}

func (sm *spaceManagerImpl) DropTableSpace(spaceID uint32) error {
	sm.Lock()
	defer sm.Unlock()

	space, exists := sm.spaces[spaceID]
	if !exists {
		return fmt.Errorf("tablespace %d not found", spaceID)
	}

	err := space.DropTable()
	if err != nil {
		return fmt.Errorf("failed to drop tablespace: %v", err)
	}

	delete(sm.nameToID, space.Name())
	delete(sm.spaces, spaceID)
	delete(sm.ibdFiles, spaceID)
	return nil
}

func (sm *spaceManagerImpl) Close() error {
	sm.Lock()
	defer sm.Unlock()

	var lastErr error
	for _, space := range sm.spaces {
		if err := space.Close(); err != nil {
			lastErr = err
		}
	}

	sm.spaces = make(map[uint32]*IBDSpace)
	sm.ibdFiles = make(map[uint32]*ibd.IBD_File)
	sm.nameToID = make(map[string]uint32)

	return lastErr
}
