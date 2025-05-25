package manager

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/store/ibd"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/space"
)

// SpaceManagerImpl implements the SpaceManager interface
type SpaceManagerImpl struct {
	sync.RWMutex
	spaces   map[uint32]*space.IBDSpace
	ibdFiles map[uint32]*ibd.IBD_File
	nameToID map[string]uint32
	nextID   uint32
	dataDir  string
	txID     uint64
}

func (sm *SpaceManagerImpl) FlushToDisk(pageNo uint32, content []byte) {
	//TODO implement me
	panic("implement me")
}

func (sm *SpaceManagerImpl) LoadPageByPageNumber(pageNo uint32) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (sm *SpaceManagerImpl) GetSpaceId() uint32 {
	//TODO implement me
	panic("implement me")
}

// NewSpaceManager creates a new space manager
func NewSpaceManager(dataDir string) basic.SpaceManager {
	return &SpaceManagerImpl{
		spaces:   make(map[uint32]*space.IBDSpace),
		ibdFiles: make(map[uint32]*ibd.IBD_File),
		nameToID: make(map[string]uint32),
		nextID:   1,
		dataDir:  dataDir,
		txID:     1,
	}
}

func (sm *SpaceManagerImpl) CreateSpace(spaceID uint32, name string, isSystem bool) (basic.Space, error) {
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

	ibdSpace := space.NewIBDSpace(ibdFile, isSystem)
	sm.spaces[spaceID] = ibdSpace
	sm.ibdFiles[spaceID] = ibdFile
	sm.nameToID[name] = spaceID

	return ibdSpace, nil
}

func (sm *SpaceManagerImpl) GetSpace(spaceID uint32) (basic.Space, error) {
	sm.RLock()
	defer sm.RUnlock()

	space, exists := sm.spaces[spaceID]
	if !exists {
		return nil, fmt.Errorf("tablespace %d not found", spaceID)
	}
	return space, nil
}

func (sm *SpaceManagerImpl) DropSpace(spaceID uint32) error {
	sm.Lock()
	defer sm.Unlock()

	ibdSpace, exists := sm.spaces[spaceID]
	if !exists {
		return fmt.Errorf("tablespace %d not found", spaceID)
	}
	err := ibdSpace.DropTable()
	if err != nil {
		return fmt.Errorf("failed to drop tablespace: %v", err)
	}

	delete(sm.nameToID, ibdSpace.GetTableName())
	delete(sm.spaces, spaceID)
	delete(sm.ibdFiles, spaceID)

	return nil
}

func (sm *SpaceManagerImpl) AllocateExtent(spaceID uint32, purpose basic.ExtentPurpose) (basic.Extent, error) {
	sm.RLock()
	defer sm.RUnlock()

	space, exists := sm.spaces[spaceID]
	if !exists {
		return nil, fmt.Errorf("tablespace %d not found", spaceID)
	}

	return space.AllocateExtent(purpose)
}

func (sm *SpaceManagerImpl) FreeExtent(spaceID, extentID uint32) error {
	sm.RLock()
	defer sm.RUnlock()

	space, exists := sm.spaces[spaceID]
	if !exists {
		return fmt.Errorf("tablespace %d not found", spaceID)
	}

	return space.FreeExtent(extentID)
}

func (sm *SpaceManagerImpl) Begin() (basic.Tx, error) {
	txID := atomic.AddUint64(&sm.txID, 1)
	return newSpaceTx(txID), nil
}

func (sm *SpaceManagerImpl) CreateNewTablespace(name string) uint32 {
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

func (sm *SpaceManagerImpl) CreateTableSpace(name string) (uint32, error) {
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

	ibdSpace := space.NewIBDSpace(ibdFile, false)
	sm.spaces[spaceID] = ibdSpace
	sm.nameToID[name] = spaceID
	return spaceID, nil
}

func (sm *SpaceManagerImpl) GetTableSpace(spaceID uint32) (basic.FileTableSpace, error) {
	sm.RLock()
	defer sm.RUnlock()

	space, exists := sm.spaces[spaceID]
	if !exists {
		return nil, fmt.Errorf("tablespace %d not found", spaceID)
	}
	return space.AsFileTableSpace(), nil
}

func (sm *SpaceManagerImpl) GetTableSpaceByName(name string) (basic.FileTableSpace, error) {
	sm.RLock()
	defer sm.RUnlock()

	spaceID, exists := sm.nameToID[name]
	if !exists {
		return nil, fmt.Errorf("tablespace %s not found", name)
	}
	return sm.spaces[uint32(spaceID)].AsFileTableSpace(), nil
}

func (sm *SpaceManagerImpl) GetTableSpaceInfo(spaceID uint32) (*basic.TableSpaceInfo, error) {
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

func (sm *SpaceManagerImpl) DropTableSpace(spaceID uint32) error {
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

func (sm *SpaceManagerImpl) Close() error {
	sm.Lock()
	defer sm.Unlock()

	var lastErr error
	for _, space := range sm.spaces {
		if err := space.Close(); err != nil {
			lastErr = err
		}
	}

	sm.spaces = make(map[uint32]*space.IBDSpace)
	sm.ibdFiles = make(map[uint32]*ibd.IBD_File)
	sm.nameToID = make(map[string]uint32)

	return lastErr
}

// spaceTx implements the Tx interface for space transactions
type spaceTx struct {
	sync.Mutex
	id        uint64
	committed bool
	writes    []func()
}

// newSpaceTx creates a new space transaction
func newSpaceTx(id uint64) *spaceTx {
	return &spaceTx{
		id:        id,
		committed: false,
		writes:    make([]func(), 0),
	}
}

// AddWrite adds a write operation to the transaction
func (tx *spaceTx) AddWrite(writeFn func()) {
	tx.Lock()
	defer tx.Unlock()

	tx.writes = append(tx.writes, writeFn)
}

// Commit commits all write operations in the transaction
func (tx *spaceTx) Commit() error {
	tx.Lock()
	defer tx.Unlock()

	if tx.committed {
		return nil
	}

	// Execute all write operations
	for _, write := range tx.writes {
		write()
	}

	tx.committed = true
	return nil
}

// Rollback discards all write operations
func (tx *spaceTx) Rollback() error {
	tx.Lock()
	defer tx.Unlock()

	if tx.committed {
		return nil
	}

	// Clear write operations
	tx.writes = tx.writes[:0]
	return nil
}
