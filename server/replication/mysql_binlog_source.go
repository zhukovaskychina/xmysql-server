package replication

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	mysqlclient "github.com/go-mysql-org/go-mysql/client"
	mysqlproto "github.com/go-mysql-org/go-mysql/mysql"
	mysqlreplication "github.com/go-mysql-org/go-mysql/replication"
)

// MySQLBinlogSourceConfig describes a native MySQL binlog endpoint. It is
// deliberately separate from RuntimeConfig: the existing HTTP source URL is
// still supported, while this config carries credentials and protocol
// settings needed by a real COM_BINLOG_DUMP client.
type MySQLBinlogSourceConfig struct {
	Host                       string
	Port                       uint16
	User                       string
	Password                   string
	ServerID                   uint32
	BinlogFile                 string
	BinlogPosition             uint64
	ReadTimeout                time.Duration
	GTIDSet                    string
	GTIDAutoPosition           bool
	TLSEnabled                 bool
	TLSVerifyServerCertificate bool
	TLSCAFile                  string
	TLSCertificateFile         string
	TLSKeyFile                 string
	TLSConfig                  *tls.Config
	ConnectionRetryInterval    uint64
	ConnectionRetryCount       uint64
	HeartbeatInterval          float64
	CompressionAlgorithm       string
	ZstdCompressionLevel       int64
}

// NativeBinlogSource is the minimal pull contract used by a replica runtime.
// Keeping it small lets runtime tests exercise transaction splitting without
// requiring a live mysqld, while MySQLBinlogSource remains the production
// implementation.
type NativeBinlogSource interface {
	Dump(context.Context, int) ([]NativeBinlogEvent, error)
}

// NativeBinlogSourceMetadata is an optional capability implemented by native
// sources that can expose the upstream server identity.  Keeping it optional
// preserves the injected test/source contract and the existing HTTP source.
type NativeBinlogSourceMetadata interface {
	SourceUUID(context.Context) (string, error)
}

// ParseMySQLBinlogSourceURL parses the explicit mysql:// source form used by
// the native MySQL source client. Passwords remain in memory only; callers
// must not persist the resulting config in reports or logs.
//
// Supported query parameters are server_id, binlog_file, binlog_pos,
// gtid_set, gtid_auto_position, read_timeout, ssl, ssl_verify_server_cert,
// ssl_ca, ssl_cert and ssl_key. A GTID auto-position source may omit
// binlog_file because COM_BINLOG_DUMP_GTID selects the source file.
// The default position is the first legal binlog event offset (4), matching
// MySQL's COM_BINLOG_DUMP contract.
func ParseMySQLBinlogSourceURL(raw string, defaultServerID uint32) (MySQLBinlogSourceConfig, error) {
	var cfg MySQLBinlogSourceConfig
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return cfg, fmt.Errorf("parse MySQL binlog source URL: %w", err)
	}
	if !strings.EqualFold(parsed.Scheme, "mysql") {
		return cfg, fmt.Errorf("MySQL binlog source URL must use mysql scheme")
	}
	if parsed.Hostname() == "" {
		return cfg, fmt.Errorf("MySQL binlog source URL host is required")
	}
	port := uint64(3306)
	if parsed.Port() != "" {
		port, err = strconv.ParseUint(parsed.Port(), 10, 16)
		if err != nil || port == 0 {
			return cfg, fmt.Errorf("invalid MySQL binlog source port %q", parsed.Port())
		}
	}
	user := ""
	password := ""
	if parsed.User != nil {
		user = parsed.User.Username()
		password, _ = parsed.User.Password()
	}
	if strings.TrimSpace(user) == "" {
		return cfg, fmt.Errorf("MySQL binlog source user is required")
	}

	cfg = MySQLBinlogSourceConfig{
		Host:                       parsed.Hostname(),
		Port:                       uint16(port),
		User:                       user,
		Password:                   password,
		ServerID:                   defaultServerID,
		BinlogPosition:             4,
		ReadTimeout:                15 * time.Second,
		GTIDAutoPosition:           true,
		TLSVerifyServerCertificate: true,
	}
	values := parsed.Query()
	if rawServerID := strings.TrimSpace(values.Get("server_id")); rawServerID != "" {
		serverID, parseErr := strconv.ParseUint(rawServerID, 10, 32)
		if parseErr != nil || serverID == 0 {
			return MySQLBinlogSourceConfig{}, fmt.Errorf("invalid MySQL binlog source server_id %q", rawServerID)
		}
		cfg.ServerID = uint32(serverID)
	}
	if cfg.ServerID == 0 {
		return MySQLBinlogSourceConfig{}, fmt.Errorf("MySQL binlog source server_id must be non-zero")
	}
	cfg.BinlogFile = strings.TrimSpace(values.Get("binlog_file"))
	cfg.GTIDSet = strings.TrimSpace(values.Get("gtid_set"))
	if rawPosition := strings.TrimSpace(values.Get("binlog_pos")); rawPosition != "" {
		position, parseErr := strconv.ParseUint(rawPosition, 10, 64)
		if parseErr != nil || position < 4 {
			return MySQLBinlogSourceConfig{}, fmt.Errorf("invalid MySQL binlog source binlog_pos %q", rawPosition)
		}
		cfg.BinlogPosition = position
	}
	if rawTimeout := strings.TrimSpace(values.Get("read_timeout")); rawTimeout != "" {
		timeout, parseErr := time.ParseDuration(rawTimeout)
		if parseErr != nil || timeout <= 0 {
			return MySQLBinlogSourceConfig{}, fmt.Errorf("invalid MySQL binlog source read_timeout %q", rawTimeout)
		}
		cfg.ReadTimeout = timeout
	}
	if rawGTID := strings.TrimSpace(values.Get("gtid_auto_position")); rawGTID != "" {
		useGTID, parseErr := strconv.ParseBool(rawGTID)
		if parseErr != nil {
			return MySQLBinlogSourceConfig{}, fmt.Errorf("invalid MySQL binlog source gtid_auto_position %q", rawGTID)
		}
		cfg.GTIDAutoPosition = useGTID
	}
	if rawTLS := strings.TrimSpace(values.Get("ssl")); rawTLS != "" {
		useTLS, parseErr := parseMySQLSourceBool(rawTLS)
		if parseErr != nil {
			return MySQLBinlogSourceConfig{}, fmt.Errorf("invalid MySQL binlog source ssl %q", rawTLS)
		}
		cfg.TLSEnabled = useTLS
	}
	if rawVerify := strings.TrimSpace(values.Get("ssl_verify_server_cert")); rawVerify != "" {
		verify, parseErr := parseMySQLSourceBool(rawVerify)
		if parseErr != nil {
			return MySQLBinlogSourceConfig{}, fmt.Errorf("invalid MySQL binlog source ssl_verify_server_cert %q", rawVerify)
		}
		cfg.TLSVerifyServerCertificate = verify
	}
	cfg.TLSCAFile = strings.TrimSpace(values.Get("ssl_ca"))
	cfg.TLSCertificateFile = strings.TrimSpace(values.Get("ssl_cert"))
	cfg.TLSKeyFile = strings.TrimSpace(values.Get("ssl_key"))
	if rawRetry := strings.TrimSpace(values.Get("connect_retry")); rawRetry != "" {
		retry, parseErr := strconv.ParseUint(rawRetry, 10, 64)
		if parseErr != nil {
			return MySQLBinlogSourceConfig{}, fmt.Errorf("invalid MySQL binlog source connect_retry %q", rawRetry)
		}
		cfg.ConnectionRetryInterval = retry
	}
	if rawRetryCount := strings.TrimSpace(values.Get("connect_retry_count")); rawRetryCount != "" {
		retryCount, parseErr := strconv.ParseUint(rawRetryCount, 10, 64)
		if parseErr != nil {
			return MySQLBinlogSourceConfig{}, fmt.Errorf("invalid MySQL binlog source connect_retry_count %q", rawRetryCount)
		}
		cfg.ConnectionRetryCount = retryCount
	}
	if rawHeartbeat := strings.TrimSpace(values.Get("heartbeat_interval")); rawHeartbeat != "" {
		heartbeat, parseErr := strconv.ParseFloat(rawHeartbeat, 64)
		if parseErr != nil || heartbeat < 0 {
			return MySQLBinlogSourceConfig{}, fmt.Errorf("invalid MySQL binlog source heartbeat_interval %q", rawHeartbeat)
		}
		cfg.HeartbeatInterval = heartbeat
	}
	cfg.CompressionAlgorithm = strings.TrimSpace(values.Get("compression_algorithm"))
	if rawZstd := strings.TrimSpace(values.Get("zstd_compression_level")); rawZstd != "" {
		level, parseErr := strconv.ParseInt(rawZstd, 10, 64)
		if parseErr != nil || level < 0 {
			return MySQLBinlogSourceConfig{}, fmt.Errorf("invalid MySQL binlog source zstd_compression_level %q", rawZstd)
		}
		cfg.ZstdCompressionLevel = level
	}
	return cfg, nil
}

// MySQLBinlogSource consumes raw native event frames from an official MySQL
// or compatible server. The returned frames are intentionally passed through
// unchanged so the existing NativeBinlogDecoder remains the single source of
// truth for GTID, XA, row-event and checksum semantics.
type MySQLBinlogSource struct {
	config MySQLBinlogSourceConfig
}

func NewMySQLBinlogSource(config MySQLBinlogSourceConfig) (*MySQLBinlogSource, error) {
	if strings.TrimSpace(config.Host) == "" || config.Port == 0 || strings.TrimSpace(config.User) == "" {
		return nil, fmt.Errorf("MySQL binlog source requires host, port and user")
	}
	if config.ServerID == 0 {
		return nil, fmt.Errorf("MySQL binlog source server_id must be non-zero")
	}
	if strings.TrimSpace(config.BinlogFile) == "" && !config.GTIDAutoPosition {
		return nil, fmt.Errorf("MySQL binlog source binlog_file is required")
	}
	if config.GTIDAutoPosition && strings.TrimSpace(config.GTIDSet) != "" {
		if _, err := mysqlproto.ParseGTIDSet("mysql", config.GTIDSet); err != nil {
			return nil, fmt.Errorf("parse MySQL binlog source GTID set: %w", err)
		}
	}
	if config.BinlogPosition < 4 || config.BinlogPosition > uint64(^uint32(0)) {
		return nil, fmt.Errorf("MySQL binlog source binlog position %d is out of range", config.BinlogPosition)
	}
	if config.ReadTimeout <= 0 {
		config.ReadTimeout = 15 * time.Second
	}
	if config.TLSEnabled && config.TLSConfig == nil {
		caPem, err := readOptionalTLSFile(config.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("read MySQL binlog source TLS CA: %w", err)
		}
		certPem, err := readOptionalTLSFile(config.TLSCertificateFile)
		if err != nil {
			return nil, fmt.Errorf("read MySQL binlog source TLS certificate: %w", err)
		}
		keyPem, err := readOptionalTLSFile(config.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("read MySQL binlog source TLS key: %w", err)
		}
		config.TLSConfig = mysqlclient.NewClientTLSConfig(caPem, certPem, keyPem, !config.TLSVerifyServerCertificate, config.Host)
	}
	return &MySQLBinlogSource{config: config}, nil
}

func readOptionalTLSFile(path string) ([]byte, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	return os.ReadFile(path)
}

func mysqlClientTLSOptions(config *tls.Config) []mysqlclient.Option {
	if config == nil {
		return nil
	}
	return []mysqlclient.Option{func(conn *mysqlclient.Conn) error {
		conn.SetTLSConfig(config)
		return nil
	}}
}

func parseMySQLSourceBool(value string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "on", "true":
		return true, nil
	case "0", "off", "false":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean %q", value)
	}
}

// Dump reads up to maxEvents native frames. A non-positive maxEvents means
// read until the caller's context is canceled. The context must have a
// deadline for a blocking binlog stream when maxEvents is non-positive.
func (s *MySQLBinlogSource) Dump(ctx context.Context, maxEvents int) ([]NativeBinlogEvent, error) {
	return s.dump(ctx, maxEvents, false)
}

// DumpTransaction reads through the first committed XID event after the
// configured position. It is useful for integration probes that need one
// complete transaction without keeping a blocking replication stream open.
func (s *MySQLBinlogSource) DumpTransaction(ctx context.Context) ([]NativeBinlogEvent, error) {
	return s.dump(ctx, 0, true)
}

// SourceUUID reads the upstream identity from the server metadata channel.
// COM_BINLOG_DUMP does not carry @@GLOBAL.server_uuid, so this is deliberately
// a separate short-lived connection and is not inferred from a GTID set.
func (s *MySQLBinlogSource) SourceUUID(ctx context.Context) (string, error) {
	if s == nil {
		return "", fmt.Errorf("MySQL binlog source is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := s.config.ReadTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	conn, err := mysqlclient.ConnectWithContext(ctx, net.JoinHostPort(s.config.Host, strconv.Itoa(int(s.config.Port))), s.config.User, s.config.Password, "", timeout, mysqlClientTLSOptions(s.config.TLSConfig)...)
	if err != nil {
		return "", fmt.Errorf("connect to MySQL source metadata: %w", err)
	}
	defer conn.Close()
	result, err := conn.Execute("SELECT @@GLOBAL.server_uuid")
	if err != nil {
		return "", fmt.Errorf("query MySQL source server_uuid: %w", err)
	}
	if result == nil || result.Resultset == nil || len(result.Values) == 0 || len(result.Values[0]) == 0 {
		return "", fmt.Errorf("MySQL source returned no server_uuid")
	}
	value := result.Values[0][0].Value()
	if value == nil {
		return "", fmt.Errorf("MySQL source returned NULL server_uuid")
	}
	var serverUUID string
	switch typed := value.(type) {
	case []byte:
		serverUUID = strings.TrimSpace(string(typed))
	case string:
		serverUUID = strings.TrimSpace(typed)
	default:
		serverUUID = strings.TrimSpace(fmt.Sprint(typed))
	}
	if serverUUID == "" {
		return "", fmt.Errorf("MySQL source returned empty server_uuid")
	}
	return serverUUID, nil
}

func (s *MySQLBinlogSource) dump(ctx context.Context, maxEvents int, stopAtCommit bool) ([]NativeBinlogEvent, error) {
	if s == nil {
		return nil, fmt.Errorf("MySQL binlog source is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	config := s.binlogSyncerConfig()
	syncer := mysqlreplication.NewBinlogSyncer(config)
	defer syncer.Close()
	var (
		streamer *mysqlreplication.BinlogStreamer
		err      error
	)
	if s.config.GTIDAutoPosition {
		gtidSet, parseErr := mysqlproto.ParseGTIDSet("mysql", strings.TrimSpace(s.config.GTIDSet))
		if parseErr != nil {
			return nil, fmt.Errorf("parse MySQL binlog source GTID set: %w", parseErr)
		}
		streamer, err = syncer.StartSyncGTID(gtidSet)
	} else {
		streamer, err = syncer.StartSync(mysqlproto.Position{Name: s.config.BinlogFile, Pos: uint32(s.config.BinlogPosition)})
	}
	if err != nil {
		return nil, fmt.Errorf("start MySQL binlog dump: %w", err)
	}
	frames := make([]NativeBinlogEvent, 0)
	checksumLength := -1
	cursor := s.config.BinlogPosition
	if s.config.GTIDAutoPosition {
		cursor = 4
	}
	currentFile := s.config.BinlogFile
	for maxEvents <= 0 || len(frames) < maxEvents {
		event, getErr := streamer.GetEvent(ctx)
		if getErr != nil {
			if ctx.Err() != nil {
				return frames, ctx.Err()
			}
			return frames, fmt.Errorf("read MySQL binlog event: %w", getErr)
		}
		if event == nil || len(event.RawData) == 0 {
			continue
		}
		raw := append([]byte(nil), event.RawData...)
		if nativeBinlogFrameType(raw) == 15 { // FORMAT_DESCRIPTION_EVENT
			if nativeFrameHasChecksum(raw, nativeChecksumLength) {
				checksumLength = nativeChecksumLength
			} else if len(raw) > nativeEventHeaderLength && raw[len(raw)-1] == 0 {
				checksumLength = 0
			}
		} else if checksumLength < 0 {
			// A ROTATE_EVENT emitted before the format description can be
			// checksum-free. For a stream that starts later, the first frame's
			// CRC is the best available framing signal until FORMAT_DESCRIPTION.
			if nativeFrameHasChecksum(raw, nativeChecksumLength) {
				checksumLength = nativeChecksumLength
			} else {
				checksumLength = 0
			}
		}
		position := cursor
		endPosition := uint64(event.Header.LogPos)
		eventSize := uint64(event.Header.EventSize)
		if eventSize == 0 {
			eventSize = uint64(len(raw))
		}
		if endPosition <= cursor || endPosition < eventSize {
			endPosition = cursor + eventSize
		} else {
			position = endPosition - eventSize
		}
		frame := NativeBinlogEvent{
			File:           currentFile,
			Position:       position,
			EndPosition:    endPosition,
			Type:           byte(event.Header.EventType),
			Raw:            raw,
			ChecksumLength: checksumLength,
			ChecksumKnown:  checksumLength >= 0,
		}
		frames = append(frames, frame)
		cursor = endPosition
		if rotateFile, rotatePosition, ok := nativeRotateTargetFile(frame); ok {
			currentFile = rotateFile
			cursor = rotatePosition
		}
		if stopAtCommit && nativeBinlogFrameEndsTransaction(raw, checksumLength) {
			return frames, nil
		}
	}
	return frames, nil
}

func (s *MySQLBinlogSource) binlogSyncerConfig() mysqlreplication.BinlogSyncerConfig {
	config := mysqlreplication.BinlogSyncerConfig{
		ServerID:             s.config.ServerID,
		Flavor:               "mysql",
		Host:                 s.config.Host,
		Port:                 s.config.Port,
		User:                 s.config.User,
		Password:             s.config.Password,
		Charset:              "utf8mb4",
		RawModeEnabled:       true,
		VerifyChecksum:       true,
		ReadTimeout:          s.config.ReadTimeout,
		TLSConfig:            s.config.TLSConfig,
		DisableRetrySync:     s.config.ConnectionRetryCount == 0,
		MaxReconnectAttempts: int(s.config.ConnectionRetryCount),
		DumpCommandFlag:      mysqlreplication.BINLOG_DUMP_NEVER_STOP,
	}
	if s.config.HeartbeatInterval > 0 {
		// MySQL's source heartbeat session variable is expressed in
		// microseconds; go-mysql stores that integer in time.Duration.
		config.HeartbeatPeriod = time.Duration(s.config.HeartbeatInterval * float64(time.Microsecond))
	}
	return config
}

func nativeRotateTargetFile(event NativeBinlogEvent) (string, uint64, bool) {
	if nativeBinlogFrameType(event.Raw) != 4 {
		return "", 0, false
	}
	body := nativeEventBody(event)
	if len(body) < 8 {
		return "", 0, false
	}
	position := binary.LittleEndian.Uint64(body[:8])
	name := strings.TrimRight(string(body[8:]), "\x00")
	if name == "" || position < 4 {
		return "", 0, false
	}
	return name, position, true
}

func nativeBinlogFrameType(frame []byte) byte {
	if len(frame) < 5 {
		return 0
	}
	return frame[4]
}

func nativeBinlogFrameEndsTransaction(frame []byte, checksumLength int) bool {
	switch nativeBinlogFrameType(frame) {
	case 16: // XID_EVENT
		return true
	case 2: // QUERY_EVENT, including XA terminal statements
		if checksumLength < 0 {
			return false
		}
		statement, _, err := decodeNativeQuery(nativeEventBody(NativeBinlogEvent{
			Raw:            frame,
			ChecksumLength: checksumLength,
			ChecksumKnown:  true,
		}))
		if err != nil {
			return false
		}
		if boundary := nativeQueryBoundary(statement); boundary == "commit" || boundary == "rollback" {
			return true
		}
		_, _, xa := nativeXAQuery(statement)
		return xa && (strings.HasPrefix(strings.ToUpper(strings.TrimSpace(statement)), "XA COMMIT") || strings.HasPrefix(strings.ToUpper(strings.TrimSpace(statement)), "XA ROLLBACK"))
	default:
		return false
	}
}
