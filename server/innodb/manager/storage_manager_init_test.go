package manager

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/conf"
)

// TestStorageManagerSystemTablespaceInitialization 测试系统表空间初始化
func TestStorageManagerSystemTablespaceInitialization(t *testing.T) {
	// 创建临时测试目录
	tempDir, err := os.MkdirTemp("", "xmysql_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建测试配置
	cfg := &conf.Cfg{
		DataDir:                   tempDir,
		InnodbDataDir:             tempDir,
		InnodbDataFilePath:        "ibdata1:100M:autoextend",
		InnodbBufferPoolSize:      16777216, // 16MB for testing
		InnodbPageSize:            16384,    // 16KB
		InnodbLogFileSize:         10485760, // 10MB
		InnodbLogBufferSize:       1048576,  // 1MB
		InnodbFlushLogAtTrxCommit: 1,
		InnodbFileFormat:          "Barracuda",
		InnodbDefaultRowFormat:    "DYNAMIC",
		InnodbDoublewrite:         true,
		InnodbAdaptiveHashIndex:   true,
		InnodbRedoLogDir:          filepath.Join(tempDir, "redo"),
		InnodbUndoLogDir:          filepath.Join(tempDir, "undo"),
	}

	// 创建必要的子目录
	os.MkdirAll(cfg.InnodbRedoLogDir, 0755)
	os.MkdirAll(cfg.InnodbUndoLogDir, 0755)

	// 初始化StorageManager
	t.Log("Initializing StorageManager with system tablespaces...")
	sm := NewStorageManager(cfg)
	if sm == nil {
		t.Fatal("Failed to create StorageManager")
	}

	// 验证系统表空间是否创建成功
	t.Log("Verifying system tablespace creation...")

	// 1. 验证系统表空间 (ibdata1, Space ID = 0)
	systemSpace, err := sm.spaceMgr.GetSpace(0)
	if err != nil {
		t.Errorf("Failed to get system tablespace: %v", err)
	} else {
		if systemSpace.ID() != 0 {
			t.Errorf("Expected system space ID 0, got %d", systemSpace.ID())
		}
		if !systemSpace.IsSystem() {
			t.Error("System tablespace should be marked as system")
		}
		if !systemSpace.IsActive() {
			t.Error("System tablespace should be active")
		}
		t.Logf("✓ System tablespace created: ID=%d, Name=%s", systemSpace.ID(), systemSpace.Name())
	}

	// 2. 验证MySQL系统表 (Space ID 1-26)
	expectedSystemTables := []string{
		"mysql/user",
		"mysql/db",
		"mysql/tables_priv",
		"mysql/columns_priv",
		"mysql/procs_priv",
	}

	for i, tableName := range expectedSystemTables {
		spaceID := uint32(i + 1)
		space, err := sm.spaceMgr.GetSpace(spaceID)
		if err != nil {
			t.Errorf("Failed to get system table %s (Space ID %d): %v", tableName, spaceID, err)
		} else {
			if space.ID() != spaceID {
				t.Errorf("Expected space ID %d for %s, got %d", spaceID, tableName, space.ID())
			}
			t.Logf("✓ System table created: %s (Space ID: %d)", tableName, spaceID)
		}
	}

	// 3. 验证information_schema表 (Space ID 100+)
	infoSchemaSpace, err := sm.spaceMgr.GetSpace(100)
	if err != nil {
		t.Errorf("Failed to get information_schema table: %v", err)
	} else {
		t.Logf("✓ Information_schema table created: Space ID=%d", infoSchemaSpace.ID())
	}

	// 4. 验证performance_schema表 (Space ID 200+)
	perfSchemaSpace, err := sm.spaceMgr.GetSpace(200)
	if err != nil {
		t.Errorf("Failed to get performance_schema table: %v", err)
	} else {
		t.Logf("✓ Performance_schema table created: Space ID=%d", perfSchemaSpace.ID())
	}

	// 5. 验证表空间handles
	if len(sm.tablespaces) == 0 {
		t.Error("No tablespace handles created")
	} else {
		t.Logf("✓ Created %d tablespace handles", len(sm.tablespaces))
	}

	// 6. 验证文件是否实际创建
	ibdataPath := filepath.Join(tempDir, "ibdata1.ibd")
	if _, err := os.Stat(ibdataPath); os.IsNotExist(err) {
		t.Errorf("System tablespace file not created: %s", ibdataPath)
	} else {
		t.Logf("✓ System tablespace file created: %s", ibdataPath)
	}

	// 7. 测试表空间信息获取
	spaceInfo, err := sm.GetSpaceInfo(0)
	if err != nil {
		t.Errorf("Failed to get space info: %v", err)
	} else {
		t.Logf("✓ Space info: ID=%d, Name=%s, PageSize=%d, State=%s",
			spaceInfo.SpaceID, spaceInfo.Name, spaceInfo.PageSize, spaceInfo.State)
	}

	// 8. 测试列出所有表空间
	spaces, err := sm.ListSpaces()
	if err != nil {
		t.Errorf("Failed to list spaces: %v", err)
	} else {
		t.Logf("✓ Listed %d tablespaces", len(spaces))
	}

	// 清理资源
	err = sm.Close()
	if err != nil {
		t.Errorf("Failed to close StorageManager: %v", err)
	} else {
		t.Log("✓ StorageManager closed successfully")
	}
}

// TestStorageManagerConfigurationHandling 测试配置处理
func TestStorageManagerConfigurationHandling(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "xmysql_config_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 测试默认配置
	t.Run("DefaultConfiguration", func(t *testing.T) {
		cfg := &conf.Cfg{}
		sm := NewStorageManager(cfg)
		if sm == nil {
			t.Fatal("Failed to create StorageManager with default config")
		}
		sm.Close()
		t.Log("✓ Default configuration handled correctly")
	})

	// 测试自定义配置
	t.Run("CustomConfiguration", func(t *testing.T) {
		cfg := &conf.Cfg{
			InnodbDataDir:             tempDir,
			InnodbDataFilePath:        "custom_ibdata:200M:autoextend",
			InnodbBufferPoolSize:      33554432, // 32MB
			InnodbPageSize:            16384,
			InnodbFlushLogAtTrxCommit: 2,
		}

		sm := NewStorageManager(cfg)
		if sm == nil {
			t.Fatal("Failed to create StorageManager with custom config")
		}

		// 验证自定义配置是否生效
		systemSpace, err := sm.spaceMgr.GetSpace(0)
		if err != nil {
			t.Errorf("Failed to get system space: %v", err)
		} else {
			expectedName := "custom_ibdata"
			if systemSpace.Name() != expectedName {
				t.Errorf("Expected system space name %s, got %s", expectedName, systemSpace.Name())
			} else {
				t.Logf("✓ Custom data file name applied: %s", systemSpace.Name())
			}
		}

		sm.Close()
	})
}

// TestStorageManagerErrorHandling 测试错误处理
func TestStorageManagerErrorHandling(t *testing.T) {
	// 测试无效的数据文件路径格式
	t.Run("InvalidDataFilePath", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("✓ Correctly panicked on invalid data file path: %v", r)
			} else {
				t.Error("Expected panic on invalid data file path")
			}
		}()

		cfg := &conf.Cfg{
			InnodbDataFilePath: "invalid_format", // 缺少冒号分隔符
		}
		NewStorageManager(cfg)
	})

	// 测试只读目录
	t.Run("ReadOnlyDirectory", func(t *testing.T) {
		if os.Getuid() == 0 {
			t.Skip("Skipping read-only test when running as root")
		}

		tempDir, err := os.MkdirTemp("", "xmysql_readonly_test_*")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		defer os.RemoveAll(tempDir)

		// 设置目录为只读
		os.Chmod(tempDir, 0444)
		defer os.Chmod(tempDir, 0755) // 恢复权限以便清理

		defer func() {
			if r := recover(); r != nil {
				t.Logf("✓ Correctly handled read-only directory: %v", r)
			}
		}()

		cfg := &conf.Cfg{
			InnodbDataDir: tempDir,
		}
		sm := NewStorageManager(cfg)
		if sm != nil {
			sm.Close()
		}
	})
}
