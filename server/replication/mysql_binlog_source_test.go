package replication

import (
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseMySQLBinlogSourceURL(t *testing.T) {
	cfg, err := ParseMySQLBinlogSourceURL("mysql://repl:p%40ss@127.0.0.1:3307/?server_id=77&binlog_file=mysql-bin.000009&binlog_pos=123&read_timeout=4s", 12)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", cfg.Host)
	require.Equal(t, uint16(3307), cfg.Port)
	require.Equal(t, "repl", cfg.User)
	require.Equal(t, "p@ss", cfg.Password)
	require.Equal(t, uint32(77), cfg.ServerID)
	require.Equal(t, "mysql-bin.000009", cfg.BinlogFile)
	require.Equal(t, uint64(123), cfg.BinlogPosition)
	require.Equal(t, "4s", cfg.ReadTimeout.String())
	require.True(t, cfg.GTIDAutoPosition)
}

func TestParseMySQLBinlogSourceURLAcceptsGTIDAutoPositionWithoutBinlogFile(t *testing.T) {
	cfg, err := ParseMySQLBinlogSourceURL("mysql://repl@127.0.0.1:3306/?gtid_set=00112233-4455-6677-8899-aabbccddeeff%3A1-2", 12)
	require.NoError(t, err)
	require.Empty(t, cfg.BinlogFile)
	require.Equal(t, "00112233-4455-6677-8899-aabbccddeeff:1-2", cfg.GTIDSet)
	require.True(t, cfg.GTIDAutoPosition)
}

func TestNewMySQLBinlogSourceAllowsGTIDAutoPositionWithoutBinlogFile(t *testing.T) {
	source, err := NewMySQLBinlogSource(MySQLBinlogSourceConfig{
		Host:             "127.0.0.1",
		Port:             3306,
		User:             "repl",
		ServerID:         12,
		GTIDSet:          "00112233-4455-6677-8899-aabbccddeeff:1-2",
		GTIDAutoPosition: true,
		BinlogPosition:   4,
	})
	require.NoError(t, err)
	require.NotNil(t, source)
}

func TestNewMySQLBinlogSourceAllowsEmptyGTIDAutoPosition(t *testing.T) {
	source, err := NewMySQLBinlogSource(MySQLBinlogSourceConfig{
		Host:             "127.0.0.1",
		Port:             3306,
		User:             "repl",
		ServerID:         13,
		GTIDAutoPosition: true,
		BinlogPosition:   4,
	})
	require.NoError(t, err)
	require.NotNil(t, source)
}

func TestNativeMySQLGTIDSetFiltersNonUUIDLocalIdentities(t *testing.T) {
	set := GTIDSet{}
	set.Add(GTID{UUID: "00112233-4455-6677-8899-aabbccddeeff", Seq: 1})
	set.Add(GTID{UUID: "00112233-4455-6677-8899-aabbccddeeff", Seq: 2})
	set.Add(GTID{UUID: "local-source", Seq: 1})
	require.Equal(t, "00112233-4455-6677-8899-aabbccddeeff:1-2", nativeMySQLGTIDSet(set))
}

func TestParseMySQLBinlogSourceURLUsesDefaultsAndRejectsInvalidValues(t *testing.T) {
	cfg, err := ParseMySQLBinlogSourceURL("mysql://root@mysql.example/", 12)
	require.NoError(t, err)
	require.Equal(t, uint16(3306), cfg.Port)
	require.Equal(t, uint32(12), cfg.ServerID)
	require.Equal(t, uint64(4), cfg.BinlogPosition)
	require.Equal(t, "", cfg.BinlogFile)

	for _, raw := range []string{
		"http://127.0.0.1:3306/",
		"mysql://127.0.0.1:3306/",
		"mysql://root@127.0.0.1:3306/?server_id=0",
		"mysql://root@127.0.0.1:3306/?binlog_pos=bad",
		"mysql://root@127.0.0.1:3306/?read_timeout=bad",
	} {
		_, err := ParseMySQLBinlogSourceURL(raw, 12)
		require.Error(t, err, raw)
	}
}

func TestParseMySQLBinlogSourceURLParsesTLSOptions(t *testing.T) {
	cfg, err := ParseMySQLBinlogSourceURL("mysql://repl@mysql.example/?ssl=true&ssl_verify_server_cert=false&ssl_ca=ca.pem&ssl_cert=client.pem&ssl_key=client.key", 12)
	require.NoError(t, err)
	require.True(t, cfg.TLSEnabled)
	require.False(t, cfg.TLSVerifyServerCertificate)
	require.Equal(t, "ca.pem", cfg.TLSCAFile)
	require.Equal(t, "client.pem", cfg.TLSCertificateFile)
	require.Equal(t, "client.key", cfg.TLSKeyFile)
}

func TestParseMySQLBinlogSourceURLParsesConnectionRuntimeOptions(t *testing.T) {
	cfg, err := ParseMySQLBinlogSourceURL("mysql://repl@mysql.example/?connect_retry=7&connect_retry_count=3&heartbeat_interval=30.5&compression_algorithm=zstd&zstd_compression_level=3", 12)
	require.NoError(t, err)
	require.Equal(t, uint64(7), cfg.ConnectionRetryInterval)
	require.Equal(t, uint64(3), cfg.ConnectionRetryCount)
	require.InDelta(t, 30.5, cfg.HeartbeatInterval, 0.001)
	require.Equal(t, "zstd", cfg.CompressionAlgorithm)
	require.Equal(t, int64(3), cfg.ZstdCompressionLevel)
}

func TestNewMySQLBinlogSourceRetainsInjectedTLSConfig(t *testing.T) {
	tlsConfig := &tls.Config{InsecureSkipVerify: true} //nolint:gosec -- test-only injected config
	source, err := NewMySQLBinlogSource(MySQLBinlogSourceConfig{
		Host:             "127.0.0.1",
		Port:             3306,
		User:             "repl",
		ServerID:         15,
		GTIDAutoPosition: true,
		TLSConfig:        tlsConfig,
		TLSEnabled:       true,
		BinlogPosition:   4,
	})
	require.NoError(t, err)
	require.Same(t, tlsConfig, source.config.TLSConfig)
}

func TestMySQLBinlogSourceBuildsHeartbeatAndRetrySyncerConfig(t *testing.T) {
	source, err := NewMySQLBinlogSource(MySQLBinlogSourceConfig{
		Host:                 "127.0.0.1",
		Port:                 3306,
		User:                 "repl",
		ServerID:             16,
		GTIDAutoPosition:     true,
		BinlogPosition:       4,
		HeartbeatInterval:    30.5,
		ConnectionRetryCount: 3,
	})
	require.NoError(t, err)

	config := source.binlogSyncerConfig()
	require.Equal(t, 3, config.MaxReconnectAttempts)
	require.False(t, config.DisableRetrySync)
	require.Equal(t, time.Duration(30.5*float64(time.Microsecond)), config.HeartbeatPeriod)
}

func TestNativeXAQueryDecodesMySQLHexXIDLiterals(t *testing.T) {
	action, key, ok := nativeXAQuery("XA COMMIT X'6774726964',X'6272616e6368',1")
	require.True(t, ok)
	require.Equal(t, "COMMIT", action)
	require.Equal(t, nativeXAKey(1, []byte("gtrid"), []byte("branch")), key)
}

func TestIsNativeMySQLStalePositionError(t *testing.T) {
	require.True(t, isNativeMySQLStalePositionError(fmt.Errorf("ERROR 1236 (HY000): Client requested source to start replication from position > file size")))
	require.True(t, isNativeMySQLStalePositionError(fmt.Errorf("ERROR 1236 (HY000): Could not find first log file name in binary log index file")))
	require.False(t, isNativeMySQLStalePositionError(fmt.Errorf("ERROR 1236 (HY000): malformed packet")))
	require.False(t, isNativeMySQLStalePositionError(fmt.Errorf("ERROR 1045 (28000): access denied")))
}
