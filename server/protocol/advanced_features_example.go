package protocol

import (
	"fmt"
)

// 本文件展示如何使用MySQL协议的高级特性

// ExamplePacketSplitter 大包分片示例
func ExamplePacketSplitter() {
	splitter := NewPacketSplitter()

	// 创建一个大payload（超过16MB）
	largePayload := make([]byte, 20*1024*1024) // 20MB

	// 分片
	packets := splitter.SplitPacket(largePayload, 0)
	fmt.Printf("分片数量: %d\n", len(packets))

	// 合并
	merged, err := splitter.MergePackets(packets)
	if err != nil {
		fmt.Printf("合并失败: %v\n", err)
		return
	}

	fmt.Printf("合并后大小: %d bytes\n", len(merged))
}

// ExampleCompression 压缩协议示例
func ExampleCompression() {
	handler := NewCompressionHandler(true)

	// 创建测试数据
	payload := []byte("SELECT * FROM users WHERE id = 1; " +
		"SELECT * FROM orders WHERE user_id = 1; " +
		"SELECT * FROM products WHERE category = 'electronics';")

	// 压缩
	compressed, err := handler.CompressPacket(payload, 1)
	if err != nil {
		fmt.Printf("压缩失败: %v\n", err)
		return
	}

	fmt.Printf("原始大小: %d bytes\n", len(payload))
	fmt.Printf("压缩后大小: %d bytes\n", len(compressed))
	fmt.Printf("压缩率: %.2f%%\n", handler.GetCompressionRatio(payload, compressed)*100)

	// 解压
	decompressed, sequenceId, err := handler.DecompressPacket(compressed)
	if err != nil {
		fmt.Printf("解压失败: %v\n", err)
		return
	}

	fmt.Printf("解压后大小: %d bytes, 序列号: %d\n", len(decompressed), sequenceId)
}

// ExampleCharsetManager 字符集管理示例
func ExampleCharsetManager() {
	manager := NewCharsetManager()

	// 获取UTF-8字符集信息
	utf8, err := manager.GetCharsetByName("utf8")
	if err != nil {
		fmt.Printf("获取字符集失败: %v\n", err)
		return
	}

	fmt.Printf("字符集: %s\n", utf8.Name)
	fmt.Printf("默认校对规则: %s\n", utf8.Collation)
	fmt.Printf("最大字节长度: %d\n", utf8.MaxLen)
	fmt.Printf("描述: %s\n", utf8.Description)

	// 获取UTF-8MB4字符集
	utf8mb4ID := manager.GetUTF8MB4CharsetID()
	utf8mb4, _ := manager.GetCharsetByID(utf8mb4ID)
	fmt.Printf("\nUTF-8MB4字符集ID: %d, 名称: %s\n", utf8mb4ID, utf8mb4.Name)

	// 列出所有UTF-8MB4的校对规则
	collations := manager.GetCollationsByCharset("utf8mb4")
	fmt.Printf("UTF-8MB4校对规则数量: %d\n", len(collations))
}

// ExampleConnectionAttributes 连接属性示例
func ExampleConnectionAttributes() {
	attrs := NewConnectionAttributes()

	// 设置标准属性
	attrs.Set(AttrClientName, "mysql-connector-go")
	attrs.Set(AttrClientVersion, "8.0.33")
	attrs.Set(AttrOS, "Linux")
	attrs.Set(AttrPlatform, "x86_64")
	attrs.Set(AttrPID, "12345")
	attrs.Set(AttrProgramName, "myapp")

	// 设置自定义属性
	attrs.Set("app_version", "1.0.0")
	attrs.Set("environment", "production")

	// 获取客户端信息
	clientInfo := attrs.GetClientInfo()
	fmt.Printf("客户端信息: %s\n", clientInfo.String())

	// 编码
	encoded := EncodeConnectionAttributes(attrs)
	fmt.Printf("编码后大小: %d bytes\n", len(encoded))

	// 解析
	parsed, bytesRead, err := ParseConnectionAttributes(encoded)
	if err != nil {
		fmt.Printf("解析失败: %v\n", err)
		return
	}

	fmt.Printf("解析成功，读取 %d bytes\n", bytesRead)
	fmt.Printf("属性数量: %d\n", parsed.Count())

	// 验证
	parser := NewConnectionAttributesParser()
	if err := parser.ValidateAttributes(attrs); err != nil {
		fmt.Printf("验证失败: %v\n", err)
	} else {
		fmt.Println("属性验证通过")
	}
}

// ExampleSessionTracker 会话状态跟踪示例
func ExampleSessionTracker() {
	tracker := NewSessionTracker(true)

	// 跟踪系统变量变更
	tracker.TrackSystemVariable("autocommit", "1")
	tracker.TrackSystemVariable("sql_mode", "STRICT_TRANS_TABLES")

	// 跟踪数据库变更
	tracker.TrackSchema("test_db")

	// 跟踪事务状态
	tracker.TrackTransactionState(TxStateActive)

	// 跟踪状态变更
	tracker.TrackStateChange(true)

	// 编码
	encoded := tracker.EncodeForOKPacket()
	fmt.Printf("会话跟踪数据大小: %d bytes\n", len(encoded))

	// 解析
	changes, err := ParseSessionTrack(encoded)
	if err != nil {
		fmt.Printf("解析失败: %v\n", err)
		return
	}

	fmt.Printf("跟踪变更数量: %d\n", len(changes))

	// 解析详细信息
	info, err := ParseSessionTrackInfo(changes)
	if err != nil {
		fmt.Printf("解析信息失败: %v\n", err)
		return
	}

	fmt.Printf("会话跟踪信息: %s\n", info.String())
}

// ExampleSessionStateManager 会话状态管理器示例
func ExampleSessionStateManager() {
	manager := NewSessionStateManager(true)

	// 设置系统变量
	manager.SetSystemVariable("autocommit", "ON")
	manager.SetSystemVariable("character_set_client", "utf8mb4")

	// 切换数据库
	manager.SetSchema("mydb")

	// 开始事务
	manager.BeginTransaction("READ WRITE")

	// 执行一些操作...

	// 提交事务
	manager.CommitTransaction()

	// 获取跟踪数据
	trackingData := manager.GetTrackingData()
	if trackingData != nil {
		fmt.Printf("跟踪数据大小: %d bytes\n", len(trackingData))
	}

	// 清空跟踪
	manager.ClearTracking()

	fmt.Printf("当前数据库: %s\n", manager.GetSchema())
	fmt.Printf("是否在事务中: %v\n", manager.IsInTransaction())
}

// ExampleIntegratedUsage 集成使用示例
func ExampleIntegratedUsage() {
	fmt.Println("=== MySQL协议高级特性集成示例 ===")

	// 1. 字符集管理
	fmt.Println("1. 字符集管理:")
	charsetMgr := GetGlobalCharsetManager()
	utf8mb4, _ := charsetMgr.GetCharsetByName("utf8mb4")
	fmt.Printf("   使用字符集: %s (ID: %d)\n\n", utf8mb4.Name, utf8mb4.ID)

	// 2. 连接属性
	fmt.Println("2. 连接属性:")
	attrs := NewConnectionAttributes()
	attrs.Set(AttrClientName, "xmysql-client")
	attrs.Set(AttrClientVersion, "1.0.0")
	clientInfo := attrs.GetClientInfo()
	fmt.Printf("   客户端: %s\n\n", clientInfo.String())

	// 3. 会话状态跟踪
	fmt.Println("3. 会话状态跟踪:")
	sessionMgr := NewSessionStateManager(true)
	sessionMgr.SetSchema("testdb")
	sessionMgr.SetSystemVariable("autocommit", "ON")
	fmt.Printf("   当前数据库: %s\n", sessionMgr.GetSchema())
	fmt.Printf("   跟踪变更数: %d\n\n", len(sessionMgr.GetTracker().GetChanges()))

	// 4. 压缩协议
	fmt.Println("4. 压缩协议:")
	compressor := NewCompressionHandler(true)
	testData := make([]byte, 1024)
	compressed, _ := compressor.CompressPacket(testData, 0)
	ratio := compressor.GetCompressionRatio(testData, compressed)
	fmt.Printf("   压缩率: %.2f%%\n\n", ratio*100)

	// 5. 大包分片
	fmt.Println("5. 大包分片:")
	splitter := NewPacketSplitter()
	largeData := make([]byte, 20*1024*1024) // 20MB
	packets := splitter.SplitPacket(largeData, 0)
	fmt.Printf("   20MB数据分片数: %d\n\n", len(packets))

	fmt.Println("=== 所有高级特性已就绪 ===")
}

// AdvancedProtocolConfig 高级协议配置
type AdvancedProtocolConfig struct {
	EnableCompression     bool
	EnableSessionTracking bool
	DefaultCharset        string
	MaxPacketSize         int
	CompressionThreshold  int
}

// NewDefaultAdvancedProtocolConfig 创建默认高级协议配置
func NewDefaultAdvancedProtocolConfig() *AdvancedProtocolConfig {
	return &AdvancedProtocolConfig{
		EnableCompression:     false, // 默认不启用压缩
		EnableSessionTracking: true,  // 默认启用会话跟踪
		DefaultCharset:        "utf8mb4",
		MaxPacketSize:         MaxPacketSize,
		CompressionThreshold:  CompressionThreshold,
	}
}

// AdvancedProtocolHandler 高级协议处理器
type AdvancedProtocolHandler struct {
	config             *AdvancedProtocolConfig
	charsetManager     *CharsetManager
	compressionHandler *CompressionHandler
	sessionManager     *SessionStateManager
	packetSplitter     *PacketSplitter
}

// NewAdvancedProtocolHandler 创建高级协议处理器
func NewAdvancedProtocolHandler(config *AdvancedProtocolConfig) *AdvancedProtocolHandler {
	if config == nil {
		config = NewDefaultAdvancedProtocolConfig()
	}

	return &AdvancedProtocolHandler{
		config:             config,
		charsetManager:     GetGlobalCharsetManager(),
		compressionHandler: NewCompressionHandler(config.EnableCompression),
		sessionManager:     NewSessionStateManager(config.EnableSessionTracking),
		packetSplitter:     NewPacketSplitter(),
	}
}

// GetCharsetManager 获取字符集管理器
func (aph *AdvancedProtocolHandler) GetCharsetManager() *CharsetManager {
	return aph.charsetManager
}

// GetCompressionHandler 获取压缩处理器
func (aph *AdvancedProtocolHandler) GetCompressionHandler() *CompressionHandler {
	return aph.compressionHandler
}

// GetSessionManager 获取会话管理器
func (aph *AdvancedProtocolHandler) GetSessionManager() *SessionStateManager {
	return aph.sessionManager
}

// GetPacketSplitter 获取包分片器
func (aph *AdvancedProtocolHandler) GetPacketSplitter() *PacketSplitter {
	return aph.packetSplitter
}

// EnableCompression 启用压缩
func (aph *AdvancedProtocolHandler) EnableCompression() {
	aph.config.EnableCompression = true
	aph.compressionHandler.Enable()
}

// DisableCompression 禁用压缩
func (aph *AdvancedProtocolHandler) DisableCompression() {
	aph.config.EnableCompression = false
	aph.compressionHandler.Disable()
}

// EnableSessionTracking 启用会话跟踪
func (aph *AdvancedProtocolHandler) EnableSessionTracking() {
	aph.config.EnableSessionTracking = true
	aph.sessionManager.GetTracker().Enable()
}

// DisableSessionTracking 禁用会话跟踪
func (aph *AdvancedProtocolHandler) DisableSessionTracking() {
	aph.config.EnableSessionTracking = false
	aph.sessionManager.GetTracker().Disable()
}

// ProcessPacket 处理包（支持分片和压缩）
func (aph *AdvancedProtocolHandler) ProcessPacket(payload []byte, sequenceId byte) ([][]byte, error) {
	var result [][]byte

	// 如果启用压缩
	if aph.config.EnableCompression && len(payload) >= aph.config.CompressionThreshold {
		compressed, err := aph.compressionHandler.CompressPacket(payload, sequenceId)
		if err != nil {
			return nil, fmt.Errorf("compression failed: %w", err)
		}
		payload = compressed
	}

	// 如果需要分片
	if len(payload) > MaxPacketSize {
		result = aph.packetSplitter.SplitPacket(payload, sequenceId)
	} else {
		// 不需要分片，直接添加包头
		packet := aph.packetSplitter.addPacketHeader(payload, sequenceId)
		result = [][]byte{packet}
	}

	return result, nil
}

// GetConfig 获取配置
func (aph *AdvancedProtocolHandler) GetConfig() *AdvancedProtocolConfig {
	return aph.config
}
