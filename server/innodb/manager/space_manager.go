package manager

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	sm.RLock()
	defer sm.RUnlock()

	// 操作系统表空间 (Space ID 0)
	systemSpace, exists := sm.spaces[0]
	if !exists {
		// 如果系统表空间不存在，忽略操作（避免panic）
		return
	}

	// 委托给系统表空间处理
	_ = systemSpace.FlushToDisk(pageNo, content)
}

func (sm *SpaceManagerImpl) LoadPageByPageNumber(pageNo uint32) ([]byte, error) {
	sm.RLock()
	defer sm.RUnlock()

	// 操作系统表空间 (Space ID 0)
	systemSpace, exists := sm.spaces[0]
	if !exists {
		return nil, fmt.Errorf("system tablespace not found")
	}

	// 委托给系统表空间处理
	return systemSpace.LoadPageByPageNumber(pageNo)
}

func (sm *SpaceManagerImpl) GetSpaceId() uint32 {
	// 返回系统表空间的ID
	return 0
}

// NewSpaceManager creates a new space manager
func NewSpaceManager(dataDir string) basic.SpaceManager {
	sm := &SpaceManagerImpl{
		spaces:   make(map[uint32]*space.IBDSpace),
		ibdFiles: make(map[uint32]*ibd.IBD_File),
		nameToID: make(map[string]uint32),
		nextID:   1,
		dataDir:  dataDir,
		txID:     1,
	}

	// 尝试加载现有的表空间（如果目录存在）
	if _, err := os.Stat(dataDir); err == nil {
		if err := sm.LoadExistingTablespaces(); err != nil {
			fmt.Printf("Warning: failed to load existing tablespaces: %v\n", err)
		}
	}

	return sm
}

func (sm *SpaceManagerImpl) CreateSpace(spaceID uint32, name string, isSystem bool) (basic.Space, error) {
	sm.Lock()
	defer sm.Unlock()

	if _, exists := sm.nameToID[name]; exists {
		return nil, fmt.Errorf("tablespace %s already exists", name)
	}

	// 创建 IBD 文件实例
	ibdFile := ibd.NewIBDFile(sm.dataDir, name, spaceID)

	// 检查文件是否已存在
	fileExists := ibdFile.Exists()

	if fileExists {
		// 文件已存在，打开并读取
		fmt.Printf("IBD file already exists, opening: %s (Space ID: %d)\n", name, spaceID)
		err := ibdFile.Open()
		if err != nil {
			return nil, fmt.Errorf("failed to open existing tablespace %s: %v", name, err)
		}
	} else {
		// 文件不存在，创建新文件
		fmt.Printf("Creating new IBD file: %s (Space ID: %d)\n", name, spaceID)
		err := ibdFile.Create()
		if err != nil {
			return nil, fmt.Errorf("failed to create tablespace %s: %v", name, err)
		}
	}

	// 创建 IBDSpace 实例
	ibdSpace := space.NewIBDSpace(ibdFile, isSystem)

	// 设置为活动状态
	ibdSpace.SetActive(true)

	// 如果是新创建的文件，分配第一个extent用于系统页面
	if !fileExists {
		// 分配第一个extent用于系统页面
		extent, err := ibdSpace.AllocateExtent(basic.ExtentPurposeSystem)
		if err != nil {
			ibdFile.Close()
			return nil, fmt.Errorf("failed to allocate system extent for %s: %v", name, err)
		}

		// 标记前几个页面为已分配（FSP header, IBUF bitmap等）
		for i := 0; i < 2; i++ {
			// 这里应该通过IBDSpace的内部方法来标记页面为已分配
			// 但由于IBDSpace的pageAllocs是私有的，我们暂时跳过这个步骤
			_ = extent // 避免未使用变量警告
		}
	}

	// 注册到管理器
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

	// 创建 IBD 文件实例
	ibdFile := ibd.NewIBDFile(sm.dataDir, name, spaceID)

	// 检查文件是否已存在
	fileExists := ibdFile.Exists()

	if fileExists {
		// 文件已存在，打开并读取
		fmt.Printf("IBD file already exists, opening: %s (Space ID: %d)\n", name, spaceID)
		err := ibdFile.Open()
		if err != nil {
			return 0, fmt.Errorf("failed to open existing tablespace %s: %v", name, err)
		}
	} else {
		// 文件不存在，创建新文件
		fmt.Printf("Creating new IBD file: %s (Space ID: %d)\n", name, spaceID)
		err := ibdFile.Create()
		if err != nil {
			return 0, fmt.Errorf("failed to create tablespace %s: %v", name, err)
		}
	}

	// 创建 IBDSpace 实例
	ibdSpace := space.NewIBDSpace(ibdFile, false)

	// 设置为活动状态
	ibdSpace.SetActive(true)

	// 如果是新创建的文件，分配第一个extent用于系统页面
	if !fileExists {
		// 分配第一个extent用于系统页面
		extent, err := ibdSpace.AllocateExtent(basic.ExtentPurposeSystem)
		if err != nil {
			ibdFile.Close()
			return 0, fmt.Errorf("failed to allocate system extent for %s: %v", name, err)
		}

		// 标记前几个页面为已分配（FSP header, IBUF bitmap等）
		for i := 0; i < 2; i++ {
			// 这里应该通过IBDSpace的内部方法来标记页面为已分配
			// 但由于IBDSpace的pageAllocs是私有的，我们暂时跳过这个步骤
			_ = extent // 避免未使用变量警告
		}
	}

	// 注册到管理器
	sm.spaces[spaceID] = ibdSpace
	sm.ibdFiles[spaceID] = ibdFile
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

// LoadExistingTablespaces 扫描数据目录并加载所有现有的IBD文件
func (sm *SpaceManagerImpl) LoadExistingTablespaces() error {
	sm.Lock()
	defer sm.Unlock()

	fmt.Println("Scanning for existing IBD files...")

	// 扫描数据目录
	return sm.scanDirectory(sm.dataDir, "")
}

// scanDirectory 递归扫描目录查找IBD文件
func (sm *SpaceManagerImpl) scanDirectory(dirPath, relativePath string) error {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("failed to read directory %s: %v", dirPath, err)
	}

	for _, entry := range entries {
		fullPath := filepath.Join(dirPath, entry.Name())
		currentRelativePath := relativePath
		if currentRelativePath != "" {
			currentRelativePath = filepath.Join(currentRelativePath, entry.Name())
		} else {
			currentRelativePath = entry.Name()
		}

		if entry.IsDir() {
			// 递归扫描子目录
			if err := sm.scanDirectory(fullPath, currentRelativePath); err != nil {
				return err
			}
		} else if strings.HasSuffix(entry.Name(), ".ibd") {
			// 找到IBD文件，尝试加载
			tableName := strings.TrimSuffix(currentRelativePath, ".ibd")

			// 跳过已经加载的表空间
			if _, exists := sm.nameToID[tableName]; exists {
				continue
			}

			// 为系统表空间分配正确的Space ID
			var spaceID uint32
			if tableName == "ibdata1" {
				spaceID = 0 // 系统表空间固定为Space ID 0
			} else {
				spaceID = sm.getNextAvailableSpaceID()
			}

			fmt.Printf("Found existing IBD file: %s, assigning Space ID: %d\n", tableName, spaceID)

			// 创建IBD文件实例并打开
			ibdFile := ibd.NewIBDFile(sm.dataDir, tableName, spaceID)
			if err := ibdFile.Open(); err != nil {
				fmt.Printf("Warning: failed to open existing IBD file %s: %v\n", tableName, err)
				continue
			}

			// 创建IBDSpace实例
			isSystem := strings.HasPrefix(tableName, "mysql/") ||
				strings.HasPrefix(tableName, "information_schema/") ||
				strings.HasPrefix(tableName, "performance_schema/") ||
				tableName == "ibdata1"

			ibdSpace := space.NewIBDSpace(ibdFile, isSystem)

			// 注册到管理器
			sm.spaces[spaceID] = ibdSpace
			sm.ibdFiles[spaceID] = ibdFile
			sm.nameToID[tableName] = spaceID

			// 更新nextID（但不要影响系统表空间的ID分配）
			if spaceID != 0 && spaceID >= sm.nextID {
				sm.nextID = spaceID + 1
			}
		}
	}

	return nil
}

// getNextAvailableSpaceID 获取下一个可用的Space ID
func (sm *SpaceManagerImpl) getNextAvailableSpaceID() uint32 {
	for {
		if _, exists := sm.spaces[sm.nextID]; !exists {
			id := sm.nextID
			sm.nextID++
			return id
		}
		sm.nextID++
	}
}
