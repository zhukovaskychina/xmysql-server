package common

// Untyped capability values are the single source for both historical public
// APIs: common.CLIENT_* uses uint32 while protocol.CLIENT_* is untyped.
const (
	CapabilityLongPassword               = 0x00000001
	CapabilityFoundRows                  = 0x00000002
	CapabilityLongFlag                   = 0x00000004
	CapabilityConnectWithDb              = 0x00000008
	CapabilityNoSchema                   = 0x00000010
	CapabilityCompress                   = 0x00000020
	CapabilityOdbc                       = 0x00000040
	CapabilityLocalFiles                 = 0x00000080
	CapabilityIgnoreSpace                = 0x00000100
	CapabilityProtocol41                 = 0x00000200
	CapabilityInteractive                = 0x00000400
	CapabilitySsl                        = 0x00000800
	CapabilityIgnoreSigpipe              = 0x00001000
	CapabilityTransactions               = 0x00002000
	CapabilityReserved                   = 0x00004000
	CapabilitySecureConnection           = 0x00008000
	CapabilityMultiStatements            = 0x00010000
	CapabilityMultiResults               = 0x00020000
	CapabilityPsMultiResults             = 0x00040000
	CapabilityPluginAuth                 = 0x00080000
	CapabilityConnectAttrs               = 0x00100000
	CapabilityPluginAuthLenencClientData = 0x00200000
	CapabilityCanHandleExpiredPasswords  = 0x00400000
	CapabilitySessionTrack               = 0x00800000
	CapabilityDeprecateEof               = 0x01000000
	CapabilityOptionalResultsetMetadata  = 0x02000000
)

// CLIENT_OPTIONAL_RESULTSET_METADATA extends the common capability API.
const CLIENT_OPTIONAL_RESULTSET_METADATA uint32 = CapabilityOptionalResultsetMetadata

// Server status bits. AUTOCOMMIT remains defined in constant.go.
const (
	SERVER_STATUS_IN_TRANS             = 0x0001
	SERVER_MORE_RESULTS_EXISTS         = 0x0008
	SERVER_STATUS_NO_GOOD_INDEX_USED   = 0x0010
	SERVER_STATUS_NO_INDEX_USED        = 0x0020
	SERVER_STATUS_CURSOR_EXISTS        = 0x0040
	SERVER_STATUS_LAST_ROW_SENT        = 0x0080
	SERVER_STATUS_DB_DROPPED           = 0x0100
	SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200
	SERVER_STATUS_METADATA_CHANGED     = 0x0400
	SERVER_QUERY_WAS_SLOW              = 0x0800
	SERVER_PS_OUT_PARAMS               = 0x1000
)
