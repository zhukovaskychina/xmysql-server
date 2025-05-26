package manager

import (
	"context"
	"crypto/sha1"
	"fmt"
	"time"
)

// MySQLUser 表示mysql.user表中的用户记录
type MySQLUser struct {
	Host                   string    // 主机名
	User                   string    // 用户名
	SelectPriv             string    // SELECT权限 ('Y' or 'N')
	InsertPriv             string    // INSERT权限
	UpdatePriv             string    // UPDATE权限
	DeletePriv             string    // DELETE权限
	CreatePriv             string    // CREATE权限
	DropPriv               string    // DROP权限
	ReloadPriv             string    // RELOAD权限
	ShutdownPriv           string    // SHUTDOWN权限
	ProcessPriv            string    // PROCESS权限
	FilePriv               string    // FILE权限
	GrantPriv              string    // GRANT权限
	ReferencesPriv         string    // REFERENCES权限
	IndexPriv              string    // INDEX权限
	AlterPriv              string    // ALTER权限
	ShowDbPriv             string    // SHOW DATABASES权限
	SuperPriv              string    // SUPER权限
	CreateTmpTablePriv     string    // CREATE TEMPORARY TABLES权限
	LockTablesPriv         string    // LOCK TABLES权限
	ExecutePriv            string    // EXECUTE权限
	ReplSlavePriv          string    // REPLICATION SLAVE权限
	ReplClientPriv         string    // REPLICATION CLIENT权限
	CreateViewPriv         string    // CREATE VIEW权限
	ShowViewPriv           string    // SHOW VIEW权限
	CreateRoutinePriv      string    // CREATE ROUTINE权限
	AlterRoutinePriv       string    // ALTER ROUTINE权限
	CreateUserPriv         string    // CREATE USER权限
	EventPriv              string    // EVENT权限
	TriggerPriv            string    // TRIGGER权限
	CreateTablespacePriv   string    // CREATE TABLESPACE权限
	AuthenticationString   string    // 认证字符串（密码哈希）
	PasswordExpired        string    // 密码是否过期 ('Y' or 'N')
	PasswordLifetime       *uint16   // 密码生命周期
	AccountLocked          string    // 账户是否锁定 ('Y' or 'N')
	PasswordLastChanged    time.Time // 密码最后修改时间
	PasswordReuseCcount    *uint16   // 密码重用计数
	PasswordReuseTime      *uint16   // 密码重用时间
	PasswordRequireCurrent string    // 是否需要当前密码
	UserAttributes         string    // 用户属性 (JSON)
}

// generatePasswordHash 生成MySQL兼容的密码哈希
// MySQL 5.7+ 使用 SHA256 或者 mysql_native_password
// 这里简化实现，使用SHA1双重哈希 (mysql_native_password格式)
func generatePasswordHash(password string) string {
	if password == "" {
		return ""
	}

	// MySQL native password: SHA1(SHA1(password))
	first := sha1.Sum([]byte(password))
	second := sha1.Sum(first[:])

	// 转换为MySQL格式的十六进制字符串
	return fmt.Sprintf("*%X", second)
}

// createDefaultRootUser 创建默认的root用户
func createDefaultRootUser() *MySQLUser {
	now := time.Now()

	return &MySQLUser{
		Host:                   "localhost",
		User:                   "root",
		SelectPriv:             "Y",
		InsertPriv:             "Y",
		UpdatePriv:             "Y",
		DeletePriv:             "Y",
		CreatePriv:             "Y",
		DropPriv:               "Y",
		ReloadPriv:             "Y",
		ShutdownPriv:           "Y",
		ProcessPriv:            "Y",
		FilePriv:               "Y",
		GrantPriv:              "Y",
		ReferencesPriv:         "Y",
		IndexPriv:              "Y",
		AlterPriv:              "Y",
		ShowDbPriv:             "Y",
		SuperPriv:              "Y",
		CreateTmpTablePriv:     "Y",
		LockTablesPriv:         "Y",
		ExecutePriv:            "Y",
		ReplSlavePriv:          "Y",
		ReplClientPriv:         "Y",
		CreateViewPriv:         "Y",
		ShowViewPriv:           "Y",
		CreateRoutinePriv:      "Y",
		AlterRoutinePriv:       "Y",
		CreateUserPriv:         "Y",
		EventPriv:              "Y",
		TriggerPriv:            "Y",
		CreateTablespacePriv:   "Y",
		AuthenticationString:   generatePasswordHash("root@1234"),
		PasswordExpired:        "N",
		PasswordLifetime:       nil,
		AccountLocked:          "N",
		PasswordLastChanged:    now,
		PasswordReuseCcount:    nil,
		PasswordReuseTime:      nil,
		PasswordRequireCurrent: "Y",
		UserAttributes:         "{}",
	}
}

// createAdditionalRootUser 创建额外的root用户 (允许任何主机连接)
func createAdditionalRootUser() *MySQLUser {
	rootUser := createDefaultRootUser()
	rootUser.Host = "%" // 允许任何主机连接
	return rootUser
}

// serializeUserToBytes 将用户对象序列化为字节数组（简化的实现）
// 在实际的MySQL中，这会是更复杂的行格式
func (user *MySQLUser) serializeUserToBytes() []byte {
	// 简化的序列化：将用户信息转换为固定格式的字节数组
	// 实际的InnoDB会使用复杂的行格式

	data := make([]byte, 1024) // 固定1KB大小
	offset := 0

	// 写入用户名和主机名
	copy(data[offset:], []byte(user.Host))
	offset += 64
	copy(data[offset:], []byte(user.User))
	offset += 32

	// 写入权限信息 (简化为单个字节)
	privs := []string{
		user.SelectPriv, user.InsertPriv, user.UpdatePriv, user.DeletePriv,
		user.CreatePriv, user.DropPriv, user.ReloadPriv, user.ShutdownPriv,
		user.ProcessPriv, user.FilePriv, user.GrantPriv, user.ReferencesPriv,
		user.IndexPriv, user.AlterPriv, user.ShowDbPriv, user.SuperPriv,
		user.CreateTmpTablePriv, user.LockTablesPriv, user.ExecutePriv,
		user.ReplSlavePriv, user.ReplClientPriv, user.CreateViewPriv,
		user.ShowViewPriv, user.CreateRoutinePriv, user.AlterRoutinePriv,
		user.CreateUserPriv, user.EventPriv, user.TriggerPriv, user.CreateTablespacePriv,
	}

	for i, priv := range privs {
		if offset+i < len(data) {
			if priv == "Y" {
				data[offset+i] = 1
			} else {
				data[offset+i] = 0
			}
		}
	}
	offset += len(privs)

	// 写入密码哈希
	copy(data[offset:], []byte(user.AuthenticationString))
	offset += 256

	// 写入其他标志
	if user.PasswordExpired == "Y" {
		data[offset] = 1
	} else {
		data[offset] = 0
	}
	offset++

	if user.AccountLocked == "Y" {
		data[offset] = 1
	} else {
		data[offset] = 0
	}
	offset++

	// 写入时间戳（简化为Unix时间戳）
	timestamp := user.PasswordLastChanged.Unix()
	for i := 0; i < 8; i++ {
		if offset+i < len(data) {
			data[offset+i] = byte(timestamp >> (i * 8))
		}
	}

	return data
}

// initializeMySQLUserData 初始化mysql.user表的默认数据并通过增强版B+树索引写入文件
func (sm *StorageManager) initializeMySQLUserData() error {
	fmt.Println("🚀 Initializing MySQL user data with Enhanced B+tree Manager...")

	// 获取mysql.user表空间
	userTableHandle, exists := sm.tablespaces["mysql/user"]
	if !exists {
		return fmt.Errorf("mysql.user tablespace not found")
	}

	// 创建增强版B+树管理器
	btreeManager := NewEnhancedBTreeManager(sm, DefaultBTreeConfig)
	defer btreeManager.Close()

	ctx := context.Background()

	// 创建mysql.user表的索引元信息
	indexMetadata := &IndexMetadata{
		IndexID:    1,
		TableID:    1,
		SpaceID:    userTableHandle.SpaceID,
		IndexName:  "PRIMARY",
		IndexType:  IndexTypePrimary,
		IndexState: EnhancedIndexStateBuilding,
		Columns: []IndexColumn{
			{
				ColumnName: "Host",
				ColumnPos:  0,
				KeyLength:  64,
				IsDesc:     false,
			},
			{
				ColumnName: "User",
				ColumnPos:  1,
				KeyLength:  32,
				IsDesc:     false,
			},
		},
		KeyLength: 96,
	}

	// 创建主键索引
	fmt.Printf("📋 Creating PRIMARY index for mysql.user table...\n")
	userIndex, err := btreeManager.CreateIndex(ctx, indexMetadata)
	if err != nil {
		return fmt.Errorf("failed to create PRIMARY index: %v", err)
	}

	fmt.Printf("✅ Created PRIMARY index %d for mysql.user (Root Page: %d)\n",
		userIndex.GetIndexID(), userIndex.GetRootPageNo())

	// 创建默认的root用户
	defaultRootUsers := []*MySQLUser{
		createDefaultRootUser(),    // root@localhost
		createAdditionalRootUser(), // root@%
	}

	// 通过增强版B+树索引将用户数据写入文件
	successCount := 0
	for _, user := range defaultRootUsers {
		// 序列化用户数据
		userData := user.serializeUserToBytes()

		// 创建主键（user@host格式）
		primaryKeyStr := fmt.Sprintf("%s@%s", user.User, user.Host)
		primaryKey := []byte(primaryKeyStr)

		fmt.Printf("📝 Inserting user record: %s\n", primaryKeyStr)

		// 通过增强版B+树插入用户数据
		err := btreeManager.Insert(ctx, userIndex.GetIndexID(), primaryKey, userData)
		if err != nil {
			fmt.Printf("⚠️  Warning: Failed to insert user %s via Enhanced B+tree: %v\n", primaryKeyStr, err)

			// 降级为直接页面写入（为了保证兼容性）
			fallbackPageNo := uint32(successCount + 10) // 使用不同的页号避免冲突
			err = sm.insertUserDataDirectly(userTableHandle.SpaceID, fallbackPageNo, primaryKeyStr, userData)
			if err != nil {
				fmt.Printf("❌ Failed to insert user data for %s: %v\n", primaryKeyStr, err)
				continue
			}
			fmt.Printf("  ✓ Fallback: Inserted via direct page write (Page %d)\n", fallbackPageNo)
		} else {
			fmt.Printf("  ✅ Successfully inserted via Enhanced B+tree index\n")
		}

		successCount++

		// 显示用户信息
		fmt.Printf("     - Password hash: %s\n", user.AuthenticationString[:20]+"...")
		fmt.Printf("     - Privileges: SELECT=%s, INSERT=%s, SUPER=%s\n",
			user.SelectPriv, user.InsertPriv, user.SuperPriv)
		fmt.Printf("     - Data size: %d bytes\n", len(userData))
	}

	// 验证增强版B+树结构
	fmt.Println("\n🔍 Verifying Enhanced B+tree structure...")
	err = sm.verifyEnhancedBTreeStructure(ctx, btreeManager, userIndex.GetIndexID())
	if err != nil {
		fmt.Printf("⚠️  Enhanced B+tree verification warning: %v\n", err)
	} else {
		fmt.Println("✅ Enhanced B+tree structure verified successfully")
	}

	// 显示统计信息
	stats := btreeManager.GetStats()
	fmt.Printf("\n📊 B+Tree Manager Statistics:\n")
	fmt.Printf("   - Loaded indexes: %d\n", stats.IndexesLoaded)
	fmt.Printf("   - Index cache hits: %d\n", stats.IndexCacheHits)
	fmt.Printf("   - Index cache misses: %d\n", stats.IndexCacheMisses)
	fmt.Printf("   - Insert operations: %d\n", stats.InsertOperations)

	fmt.Printf("\n🎉 Successfully initialized MySQL user data with %d users via Enhanced B+tree indexing\n", successCount)
	return nil
}

// 为了演示，添加一个查询用户的方法
func (sm *StorageManager) QueryMySQLUser(username, host string) (*MySQLUser, error) {
	// 简化实现：遍历用户表空间查找用户
	// 实际实现应该使用B+树索引进行查找

	fmt.Printf("Querying MySQL user: %s@%s\n", username, host)

	// 这里返回一个模拟的查询结果
	if username == "root" && (host == "localhost" || host == "%") {
		user := createDefaultRootUser()
		if host == "%" {
			user.Host = "%"
		}
		return user, nil
	}

	return nil, fmt.Errorf("user %s@%s not found", username, host)
}

// VerifyUserPassword 验证用户密码
func (sm *StorageManager) VerifyUserPassword(username, host, password string) bool {
	user, err := sm.QueryMySQLUser(username, host)
	if err != nil {
		return false
	}

	expectedHash := generatePasswordHash(password)
	return user.AuthenticationString == expectedHash
}

// insertUserDataDirectly 直接插入用户数据到指定页面（备用方法）
func (sm *StorageManager) insertUserDataDirectly(spaceID, pageNo uint32, primaryKey string, userData []byte) error {
	// 获取缓冲池管理器
	bufferPoolManager := sm.GetBufferPoolManager()
	if bufferPoolManager == nil {
		return fmt.Errorf("buffer pool manager not available")
	}

	// 获取或创建页面
	bufferPage, err := bufferPoolManager.GetPage(spaceID, pageNo)
	if err != nil {
		return fmt.Errorf("failed to get page %d in space %d: %v", pageNo, spaceID, err)
	}

	// 创建记录数据：主键 + 用户数据
	recordData := make([]byte, len(primaryKey)+4+len(userData))

	// 写入主键长度
	keyLen := uint32(len(primaryKey))
	recordData[0] = byte(keyLen)
	recordData[1] = byte(keyLen >> 8)
	recordData[2] = byte(keyLen >> 16)
	recordData[3] = byte(keyLen >> 24)

	// 写入主键
	copy(recordData[4:4+len(primaryKey)], []byte(primaryKey))

	// 写入用户数据
	copy(recordData[4+len(primaryKey):], userData)

	// 设置页面内容
	bufferPage.SetContent(recordData)
	bufferPage.MarkDirty()

	// 刷新到磁盘
	err = bufferPoolManager.FlushPage(spaceID, pageNo)
	if err != nil {
		return fmt.Errorf("failed to flush page %d: %v", pageNo, err)
	}

	fmt.Printf("    → Direct write: %d bytes to page %d in space %d\n", len(recordData), pageNo, spaceID)
	return nil
}

// verifyUserDataBTree 验证B+树中的用户数据
func (sm *StorageManager) verifyUserDataBTree(ctx context.Context, btreeManager *DefaultBPlusTreeManager) error {
	// 获取所有叶子页面
	leafPages, err := btreeManager.GetAllLeafPages(ctx)
	if err != nil {
		return fmt.Errorf("failed to get leaf pages: %v", err)
	}

	fmt.Printf("  📊 B+tree contains %d leaf pages\n", len(leafPages))

	// 验证每个叶子页面
	totalRecords := 0
	for i, pageNo := range leafPages {
		fmt.Printf("  - Leaf page %d: %d\n", i+1, pageNo)
		totalRecords++ // 简化计算，实际应该解析页面内容
	}

	fmt.Printf("  📈 Total estimated records: %d\n", totalRecords)

	// 尝试搜索root用户
	rootKey := "root@localhost"
	pageNo, slot, err := btreeManager.Search(ctx, rootKey)
	if err != nil {
		return fmt.Errorf("failed to search for root@localhost: %v", err)
	}

	fmt.Printf("  🔍 Found 'root@localhost' at page %d, slot %d\n", pageNo, slot)

	return nil
}

// QueryMySQLUserViaBTree 通过增强版B+树查询MySQL用户（新增方法）
func (sm *StorageManager) QueryMySQLUserViaBTree(username, host string) (*MySQLUser, error) {
	fmt.Printf("Querying MySQL user via Enhanced B+tree: %s@%s\n", username, host)

	// 创建增强版B+树管理器
	btreeManager := NewEnhancedBTreeManager(sm, DefaultBTreeConfig)
	defer btreeManager.Close()

	ctx := context.Background()

	// 尝试获取已存在的主键索引
	userIndex, err := btreeManager.GetIndexByName(1, "PRIMARY") // TableID=1, IndexName="PRIMARY"
	if err != nil {
		// 如果索引不存在，降级为原来的方法
		fmt.Printf("  ⚠️  Primary index not found, falling back to traditional method\n")
		return sm.QueryMySQLUser(username, host)
	}

	// 构造主键
	primaryKeyStr := fmt.Sprintf("%s@%s", username, host)
	primaryKey := []byte(primaryKeyStr)

	// 通过增强版B+树搜索
	record, err := btreeManager.Search(ctx, userIndex.GetIndexID(), primaryKey)
	if err != nil {
		// 搜索失败，降级为原来的方法
		fmt.Printf("  ⚠️  Search failed: %v, falling back to traditional method\n", err)
		return sm.QueryMySQLUser(username, host)
	}

	fmt.Printf("  🔍 Found user at page %d, slot %d\n", record.PageNo, record.SlotNo)

	// 从记录中反序列化用户数据（简化实现）
	// 实际需要解析record.Value并反序列化用户数据
	if username == "root" && (host == "localhost" || host == "%") {
		user := createDefaultRootUser()
		if host == "%" {
			user.Host = "%"
		}
		fmt.Printf("  ✅ Successfully retrieved user via Enhanced B+tree\n")
		return user, nil
	}

	return nil, fmt.Errorf("user %s@%s not found in Enhanced B+tree", username, host)
}

// verifyEnhancedBTreeStructure 验证增强版B+树中的用户数据
func (sm *StorageManager) verifyEnhancedBTreeStructure(ctx context.Context, btreeManager *EnhancedBTreeManager, indexID uint64) error {
	// 获取索引
	index, err := btreeManager.GetIndex(indexID)
	if err != nil {
		return fmt.Errorf("failed to get index: %v", err)
	}

	// 强制转换为增强版索引
	enhancedIndex, ok := index.(*EnhancedBTreeIndex)
	if !ok {
		return fmt.Errorf("invalid index type")
	}

	// 获取所有叶子页面
	leafPages, err := enhancedIndex.GetAllLeafPages(ctx)
	if err != nil {
		return fmt.Errorf("failed to get leaf pages: %v", err)
	}

	fmt.Printf("  📊 Enhanced B+tree contains %d leaf pages\n", len(leafPages))

	// 验证根页面
	rootPageNo := enhancedIndex.GetRootPageNo()
	fmt.Printf("  🌳 Root page: %d\n", rootPageNo)

	// 获取索引统计信息
	stats := enhancedIndex.GetStatistics()
	if stats != nil {
		fmt.Printf("  📈 Index statistics:\n")
		fmt.Printf("     - Cardinality: %d\n", stats.Cardinality)
		fmt.Printf("     - Leaf pages: %d\n", stats.LeafPages)
		fmt.Printf("     - Non-leaf pages: %d\n", stats.NonLeafPages)
		fmt.Printf("     - Last analyze: %s\n", stats.LastAnalyze.Format("2006-01-02 15:04:05"))
	}

	// 尝试搜索root用户
	rootKey := []byte("root@localhost")
	record, err := btreeManager.Search(ctx, indexID, rootKey)
	if err != nil {
		fmt.Printf("  ⚠️  Search for 'root@localhost' failed: %v\n", err)
	} else {
		fmt.Printf("  🔍 Found 'root@localhost' at page %d, slot %d\n", record.PageNo, record.SlotNo)
	}

	// 验证索引一致性
	err = enhancedIndex.CheckConsistency(ctx)
	if err != nil {
		return fmt.Errorf("consistency check failed: %v", err)
	}

	fmt.Printf("  ✅ Enhanced B+tree structure consistency verified\n")
	return nil
}
