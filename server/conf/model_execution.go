package conf

import (
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"
	"gopkg.in/ini.v1"
)

type AIModelExecutionConfig struct {
	MaxCallsPerQuery      uint64        `json:"max_calls_per_query" yaml:"max_calls_per_query"`
	MaxInputBytesPerQuery uint64        `json:"max_input_bytes_per_query" yaml:"max_input_bytes_per_query"`
	CallTimeout           time.Duration `json:"call_timeout" yaml:"call_timeout"`
}

func DefaultAIModelExecutionConfig() AIModelExecutionConfig {
	return AIModelExecutionConfig{MaxCallsPerQuery: 64, MaxInputBytesPerQuery: 64 << 20, CallTimeout: 30 * time.Second}
}

func (cfg *Cfg) parseAIModelExecutionCfg(section *ini.Section) {
	limits := DefaultAIModelExecutionConfig()
	positiveInteger := func(name string, fallback uint64) uint64 {
		if section == nil || !section.HasKey(name) {
			return fallback
		}
		value, err := section.Key(name).Uint64()
		if err != nil || value == 0 {
			logger.Warnf("ai.%s must be a positive integer; using default %d", name, fallback)
			return fallback
		}
		return value
	}
	limits.MaxCallsPerQuery = positiveInteger("max_calls_per_query", limits.MaxCallsPerQuery)
	limits.MaxInputBytesPerQuery = positiveInteger("max_input_bytes_per_query", limits.MaxInputBytesPerQuery)
	if section != nil && section.HasKey("call_timeout") {
		value, err := time.ParseDuration(section.Key("call_timeout").String())
		if err != nil || value <= 0 {
			logger.Warnf("ai.call_timeout must be a positive duration; using default %s", limits.CallTimeout)
		} else {
			limits.CallTimeout = value
		}
	}
	cfg.AIModelExecution = limits
}
