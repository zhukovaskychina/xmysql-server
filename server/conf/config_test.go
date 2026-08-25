package conf

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeBindAddress_Localhost(t *testing.T) {
	got := normalizeBindAddress("localhost")
	if got != "127.0.0.1" {
		t.Fatalf("normalizeBindAddress() = %q, want 127.0.0.1", got)
	}
}

func TestIsLocalBindAddress(t *testing.T) {
	tests := []struct {
		name    string
		bind    string
		isLocal bool
	}{
		{name: "ipv4 loopback", bind: "127.0.0.1", isLocal: true},
		{name: "ipv6 loopback", bind: "::1", isLocal: true},
		{name: "listen all", bind: "0.0.0.0", isLocal: false},
		{name: "private ipv4", bind: "192.168.1.10", isLocal: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isLocalBindAddress(tc.bind); got != tc.isLocal {
				t.Fatalf("isLocalBindAddress(%q) = %v, want %v", tc.bind, got, tc.isLocal)
			}
		})
	}
}

func TestShouldRejectDevBypass(t *testing.T) {
	tests := []struct {
		name         string
		bindAddress  string
		devBypass    bool
		shouldReject bool
	}{
		{name: "local ipv4 allow", bindAddress: "127.0.0.1", devBypass: true, shouldReject: false},
		{name: "non-local disallow", bindAddress: "0.0.0.0", devBypass: true, shouldReject: true},
		{name: "non-local no bypass", bindAddress: "0.0.0.0", devBypass: false, shouldReject: false},
		{name: "normalized localhost allow", bindAddress: "127.0.0.1", devBypass: false, shouldReject: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldRejectDevBypass(tc.bindAddress, tc.devBypass); got != tc.shouldReject {
				t.Fatalf("shouldRejectDevBypass(%q, %v) = %v, want %v", tc.bindAddress, tc.devBypass, got, tc.shouldReject)
			}
		})
	}
}

func TestCfg_ParseLogsCfg(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "my.ini")
	content := `
[logs]
slow_query_log = true
slow_query_log_file = /tmp/xmysql-slow.log
long_query_time_ms = 1500
`
	require.NoError(t, os.WriteFile(cfgPath, []byte(content), 0o644))

	cfg := NewCfg()
	cfg.Load(&CommandLineArgs{ConfigPath: cfgPath})

	assert.True(t, cfg.SlowQueryLog)
	assert.Equal(t, "/tmp/xmysql-slow.log", cfg.SlowQueryLogFile)
	assert.Equal(t, 1500, cfg.LongQueryTimeMs)
}

func TestCfg_ParseLogsCfgInvalidLongQueryTimeFallsBack(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "my-invalid.ini")
	content := `
[logs]
slow_query_log = true
slow_query_log_file = /tmp/xmysql-slow-fallback.log
long_query_time_ms = abc
`
	require.NoError(t, os.WriteFile(cfgPath, []byte(content), 0o644))

	cfg := NewCfg()
	cfg.Load(&CommandLineArgs{ConfigPath: cfgPath})

	assert.Equal(t, 1000, cfg.LongQueryTimeMs)
	assert.Equal(t, "/tmp/xmysql-slow-fallback.log", cfg.SlowQueryLogFile)
}

func TestJdbcLocalConfigUsesWorkspaceRelativePaths(t *testing.T) {
	cfg := NewCfg()
	cfg.Load(&CommandLineArgs{ConfigPath: filepath.Join("..", "..", "conf", "jdbc_local.ini")})

	assert.Equal(t, "server/net/data", cfg.DataDir)
	assert.Equal(t, "server/net/data", cfg.InnodbDataDir)
	assert.Equal(t, "tmp/jdbc_logs/error.log", cfg.LogError)
	assert.Equal(t, "tmp/jdbc_logs/mysql.log", cfg.LogInfos)
}
