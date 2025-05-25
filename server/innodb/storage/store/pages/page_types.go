package pages

import (
	"xmysql-server/server/common"
)

// Page types - using basic package types
const (
	// Use basic package types directly
	_ = common.FIL_PAGE_TYPE_ALLOCATED  // Latest allocated, not yet used
	_ = common.FIL_PAGE_TYPE_FSP_HDR    // File space header
	_ = common.FIL_PAGE_INODE           // Index node page
	_ = common.FIL_PAGE_TYPE_SYS        // System page
	_ = common.FIL_PAGE_UNDO_LOG        // Undo log page
	_ = common.FIL_PAGE_INDEX           // Index page
	_ = common.FIL_PAGE_TYPE_BLOB       // BLOB page
	_ = common.FIL_PAGE_TYPE_COMPRESSED // Compressed page
	_ = common.FIL_PAGE_TYPE_ENCRYPTED  // Encrypted page
)
