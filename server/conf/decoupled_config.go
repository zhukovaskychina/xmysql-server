package conf

// DecoupledConfig 解耦架构配置
type DecoupledConfig struct {
	// 消息总线配置
	MessageBus MessageBusConfig `yaml:"message_bus" json:"message_bus"`

	// 协议解析器配置
	ProtocolParser ProtocolParserConfig `yaml:"protocol_parser" json:"protocol_parser"`

	// 协议编码器配置
	ProtocolEncoder ProtocolEncoderConfig `yaml:"protocol_encoder" json:"protocol_encoder"`
}

// MessageBusConfig 消息总线配置
type MessageBusConfig struct {
	// 消息总线类型: "sync", "async"
	Type string `default:"sync" yaml:"type" json:"type"`

	// 异步消息总线配置
	BufferSize int `default:"1000" yaml:"buffer_size" json:"buffer_size"`
	Workers    int `default:"4" yaml:"workers" json:"workers"`

	// 消息处理超时时间
	HandlerTimeout string `default:"30s" yaml:"handler_timeout" json:"handler_timeout"`
}

// ProtocolParserConfig 协议解析器配置
type ProtocolParserConfig struct {
	// 是否启用协议验证
	EnableValidation bool `default:"true" yaml:"enable_validation" json:"enable_validation"`

	// 最大包大小
	MaxPacketSize int `default:"16777216" yaml:"max_packet_size" json:"max_packet_size"`

	// 是否启用协议缓存
	EnableCache bool `default:"true" yaml:"enable_cache" json:"enable_cache"`
}

// ProtocolEncoderConfig 协议编码器配置
type ProtocolEncoderConfig struct {
	// 是否启用压缩
	EnableCompression bool `default:"false" yaml:"enable_compression" json:"enable_compression"`

	// 压缩阈值（字节）
	CompressionThreshold int `default:"1024" yaml:"compression_threshold" json:"compression_threshold"`

	// 是否启用编码缓存
	EnableCache bool `default:"true" yaml:"enable_cache" json:"enable_cache"`
}

// GetDecoupledConfig 获取解耦架构配置
func (cfg *Cfg) GetDecoupledConfig() *DecoupledConfig {
	return &DecoupledConfig{
		MessageBus: MessageBusConfig{
			Type:           "sync",
			BufferSize:     1000,
			Workers:        4,
			HandlerTimeout: "30s",
		},
		ProtocolParser: ProtocolParserConfig{
			EnableValidation: true,
			MaxPacketSize:    16777216, // 16MB
			EnableCache:      true,
		},
		ProtocolEncoder: ProtocolEncoderConfig{
			EnableCompression:    false,
			CompressionThreshold: 1024,
			EnableCache:          true,
		},
	}
}
