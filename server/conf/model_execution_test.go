package conf

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCfgLoadModelExecutionLimits(t *testing.T) {
	previousConfigPath := ConfigPath
	t.Cleanup(func() { ConfigPath = previousConfigPath })
	for _, test := range []struct {
		name string
		text string
		want AIModelExecutionConfig
	}{
		{"missing_section", "[mysqld]\nport=3307\n", DefaultAIModelExecutionConfig()},
		{"custom", "[ai]\nmax_calls_per_query=3\nmax_input_bytes_per_query=2097152\ncall_timeout=750ms\n", AIModelExecutionConfig{3, 2097152, 750 * time.Millisecond}},
		{"zero", "[ai]\nmax_calls_per_query=0\nmax_input_bytes_per_query=0\ncall_timeout=0s\n", DefaultAIModelExecutionConfig()},
		{"negative", "[ai]\nmax_calls_per_query=-1\nmax_input_bytes_per_query=-1\ncall_timeout=-1s\n", DefaultAIModelExecutionConfig()},
		{"invalid", "[ai]\nmax_calls_per_query=abc\nmax_input_bytes_per_query=1MB\ncall_timeout=forever\n", DefaultAIModelExecutionConfig()},
		{"overflow", "[ai]\nmax_calls_per_query=18446744073709551616\nmax_input_bytes_per_query=18446744073709551616\ncall_timeout=999999999999h\n", DefaultAIModelExecutionConfig()},
		{"partial", "[ai]\nmax_calls_per_query=2\n", AIModelExecutionConfig{2, 64 << 20, 30 * time.Second}},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "server.ini")
			require.NoError(t, os.WriteFile(path, []byte(test.text), 0600))
			cfg := NewCfg().Load(&CommandLineArgs{ConfigPath: path})
			require.Equal(t, test.want, cfg.AIModelExecution)
		})
	}
}
