/*
 * This code was derived from https://github.com/youtube/vitess.
 *
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package common

const EVENT_TABLE_MAP = BinlogEventTableMap
const EVENT_WRITE_ROWS = BinlogEventWriteRowsV2
const EVENT_UPDATE_ROWS = BinlogEventUpdateRowsV2
const EVENT_DELETE_ROWS = BinlogEventDeleteRowsV2
const EVENT_QUERY = BinlogEventQuery

const COLUMN_TYPE_DECIMAL = 0
const COLUMN_TYPE_TINY = 1
const COLUMN_TYPE_SHORT = 2
const COLUMN_TYPE_LONG = 3
const COLUMN_TYPE_FLOAT = 4
const COLUMN_TYPE_DOUBLE = 5
const COLUMN_TYPE_NULL = 6
const COLUMN_TYPE_TIMESTAMP = 7
const COLUMN_TYPE_LONGLONG = 8
const COLUMN_TYPE_INT24 = 9
const COLUMN_TYPE_DATE = 10
const COLUMN_TYPE_TIME = 11
const COLUMN_TYPE_DATETIME = 12
const COLUMN_TYPE_YEAR = 13
const COLUMN_TYPE_NEWDATE = 14
const COLUMN_TYPE_VARCHAR = 15
const COLUMN_TYPE_BIT = 16

// (TIMESTAMP|DATETIME|TIME)_V2 data basic appeared in MySQL 5.6.4
// @see http://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html
const COLUMN_TYPE_TIMESTAMP_V2 = 17
const COLUMN_TYPE_DATETIME_V2 = 18
const COLUMN_TYPE_TIME_V2 = 19
const COLUMN_TYPE_JSON = 245
const COLUMN_TYPE_NEWDECIMAL = 246
const COLUMN_TYPE_ENUM = 247
const COLUMN_TYPE_SET = 248
const COLUMN_TYPE_TINY_BLOB = 249
const COLUMN_TYPE_MEDIUM_BLOB = 250
const COLUMN_TYPE_LONG_BLOB = 251
const COLUMN_TYPE_BLOB = 252
const COLUMN_TYPE_VAR_STRING = 253
const COLUMN_TYPE_STRING = 254
const COLUMN_TYPE_GEOMETRY = 255

// https://dev.mysql.com/doc/internals/en/capability-flags.html
// include/mysql_com.h
const (
	// new more secure password
	CLIENT_LONG_PASSWORD = CapabilityLongPassword

	// Found instead of affected rows
	CLIENT_FOUND_ROWS = uint32(CapabilityFoundRows)

	// Get all column flags
	CLIENT_LONG_FLAG = uint32(CapabilityLongFlag)

	// One can specify db on connect
	CLIENT_CONNECT_WITH_DB = uint32(CapabilityConnectWithDb)

	// Don't allow database.table.column
	CLIENT_NO_SCHEMA = uint32(CapabilityNoSchema)

	// Can use compression protocol
	CLIENT_COMPRESS = uint32(CapabilityCompress)

	// Odbc client
	CLIENT_ODBC = uint32(CapabilityOdbc)

	// Can use LOAD DATA LOCAL
	CLIENT_LOCAL_FILES = uint32(CapabilityLocalFiles)

	// Ignore spaces before '('
	CLIENT_IGNORE_SPACE = uint32(CapabilityIgnoreSpace)

	// New 4.1 protocol
	CLIENT_PROTOCOL_41 = uint32(CapabilityProtocol41)

	// This is an interactive client
	CLIENT_INTERACTIVE = uint32(CapabilityInteractive)

	// Switch to SSL after handshake
	CLIENT_SSL = uint32(CapabilitySsl)

	// IGNORE sigpipes
	CLIENT_IGNORE_SIGPIPE = uint32(CapabilityIgnoreSigpipe)

	// Client knows about transactions
	CLIENT_TRANSACTIONS = uint32(CapabilityTransactions)

	// Old flag for 4.1 protocol
	CLIENT_RESERVED = uint32(CapabilityReserved)

	// Old flag for 4.1 authentication
	CLIENT_SECURE_CONNECTION = uint32(CapabilitySecureConnection)

	// Enable/disable multi-stmt support
	CLIENT_MULTI_STATEMENTS = uint32(CapabilityMultiStatements)

	// Enable/disable multi-results
	CLIENT_MULTI_RESULTS = uint32(CapabilityMultiResults)

	// Multi-results in PS-protocol
	CLIENT_PS_MULTI_RESULTS = uint32(CapabilityPsMultiResults)

	// Client supports plugin authentication
	CLIENT_PLUGIN_AUTH = uint32(CapabilityPluginAuth)

	// Client supports connection attributes
	CLIENT_CONNECT_ATTRS = uint32(CapabilityConnectAttrs)

	//  Enable authentication response packet to be larger than 255 bytes
	CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = uint32(CapabilityPluginAuthLenencClientData)

	// Don't close the connection for a connection with expired password
	CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = uint32(CapabilityCanHandleExpiredPasswords)

	// Capable of handling server state change information. Its a hint to the
	// server to include the state change information in Ok packet.
	CLIENT_SESSION_TRACK = uint32(CapabilitySessionTrack)

	//Client no longer needs EOF packet
	CLIENT_DEPRECATE_EOF = uint32(CapabilityDeprecateEof)
)

const (
	SSUnknownSQLState = DefaultMySQLState
)

// Status flags. They are returned by the server in a few cases.
// Originally found in include/mysql/mysql_com.h
// See http://dev.mysql.com/doc/internals/en/status-flags.html
const (
	SERVER_STATUS_AUTOCOMMIT = 0x0002
)

// A few interesting character set values.
// See http://dev.mysql.com/doc/internals/en/character-set.html#packet-Protocol::CharacterSet
const (
	// CharacterSetUtf8 is for UTF8. We use this by default.
	CharacterSetUtf8 = CollationUTF8GeneralCIID

	// CharacterSetBinary is for binary. Use by integer fields for instance.
	CharacterSetBinary = CollationBinaryID
)

// CharacterSetMap maps the charset name (used in ConnParams) to the
// integer valueImpl.  Interesting ones have their own constant above.
var CharacterSetMap = ProtocolCharsetIDs()

const (
	// Error codes for server-side errors.
	// Originally found in include/mysql/mysqld_error.h
	ER_ERROR_FIRST                  uint16 = ErrErrorFirst
	ER_CON_COUNT_ERROR                     = ErrConCount
	ER_ACCESS_DENIED_ERROR                 = ErrAccessDenied
	ER_NO_DB_ERROR                         = ErrNoDB
	ER_BAD_DB_ERROR                        = ErrBadDB
	ER_UNKNOWN_ERROR                       = ErrUnknown
	ER_HOST_NOT_PRIVILEGED                 = ErrHostNotPrivileged
	ER_NO_SUCH_TABLE                       = ErrNoSuchTable
	ER_SYNTAX_ERROR                        = ErrSyntax
	ER_SPECIFIC_ACCESS_DENIED_ERROR        = ErrSpecificAccessDenied
	ER_OPTION_PREVENTS_STATEMENT           = ErrOptionPreventsStatement
	ER_MALFORMED_PACKET                    = ErrMalformedPacket

	// Error codes for client-side errors.
	// Originally found in include/mysql/errmsg.h
	// Used when:
	// - the client cannot write an initial auth packet.
	// - the client cannot read an initial auth packet.
	// - the client cannot read a response from the server.
	CR_SERVER_LOST = 2013
	// This is returned if the server versions don't match what we support.
	CR_VERSION_ERROR = 2007
)

//
//var SQLErrors = map[uint16]*SQLError{
//	ER_CON_COUNT_ERROR:              &SQLError{Num: ER_CON_COUNT_ERROR, State: "08004", Message: "Too many connections"},
//	ER_ACCESS_DENIED_ERROR:          &SQLError{Num: ER_ACCESS_DENIED_ERROR, State: "28000", Message: "Access denied for user '%-.48s'@'%-.64s' (using password: %s)"},
//	ER_NO_DB_ERROR:                  &SQLError{Num: ER_NO_DB_ERROR, State: "3D000", Message: "No database selected"},
//	ER_BAD_DB_ERROR:                 &SQLError{Num: ER_BAD_DB_ERROR, State: "42000", Message: "Unknown database '%-.192s'"},
//	ER_UNKNOWN_ERROR:                &SQLError{Num: ER_UNKNOWN_ERROR, State: "HY000", Message: ""},
//	ER_HOST_NOT_PRIVILEGED:          &SQLError{Num: ER_HOST_NOT_PRIVILEGED, State: "HY000", Message: "Host '%-.64s' is not allowed to connect to this MySQL server"},
//	ER_NO_SUCH_TABLE:                &SQLError{Num: ER_NO_SUCH_TABLE, State: "42S02", Message: "Table '%s' doesn't exist"},
//	ER_SYNTAX_ERROR:                 &SQLError{Num: ER_SYNTAX_ERROR, State: "42000", Message: "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use, %s"},
//	ER_SPECIFIC_ACCESS_DENIED_ERROR: &SQLError{Num: ER_SPECIFIC_ACCESS_DENIED_ERROR, State: "42000", Message: "Access denied; you need (at least one of) the %-.128s privilege(s) for this operation"},
//	ER_OPTION_PREVENTS_STATEMENT:    &SQLError{Num: ER_OPTION_PREVENTS_STATEMENT, State: "42000", Message: "The MySQL server is running with the %s option so it cannot execute this statement"},
//	ER_MALFORMED_PACKET:             &SQLError{Num: ER_MALFORMED_PACKET, State: "HY000", Message: "Malformed communication packet."},
//	CR_SERVER_LOST:                  &SQLError{Num: CR_SERVER_LOST, State: "HY000", Message: ""},
//}

const (
	UN_AUTH      = 1
	AUTH_SUCCESS = 2
	AUTH_FAIL    = 3
)

const INNODB_SYS_TABLESPACES = "INNODB_SYS_TABLESPACES"

const INNODB_SYS_TABLES = "INNODB_SYS_TABLES"

const INNODB_SYS_INDEXES = "INNODB_SYS_INDEXES"

const INNODB_SYS_FIELDS = "INNODB_SYS_FIELDS"

const INNODB_SYS_COLUMNS = "INNODB_SYS_COLUMNS"

const INNODB_SYS_DATAFILES = "INNODB_SYS_DATAFILES"

const INFORMATION_SCHEMAS = "INFORMATION_SCHEMAS"

type XDES_STATE uint32

// 空闲的区
const XDES_FREE XDES_STATE = 1

// 有剩余空闲页面的碎片区
const XDES_FREE_FRAG XDES_STATE = 2

// 没有剩余空闲页面碎片的区
const XDES_FULL_FRAG XDES_STATE = 3

// 归属某个段的区
const XDES_FSEG XDES_STATE = 4

const RESULT_TYPE_QUERY = "QUERY"

const RESULT_TYPE_DDL = "DDL"

const RESULT_TYPE_SET = "SET"

type LSNT uint64

type MaxSlotsPerPage uint16

// Record header bit offsets for bit manipulation
const (
	DELETE_OFFSET  = 0 // Delete flag bit offset
	MIN_REC_OFFSET = 1 // Min record flag bit offset
	COMMON_FALSE   = 0 // Common false value for bit operations
)

// InnoDB page size constant
const UNIV_PAGE_SIZE = PageSize // 16KB default page size
