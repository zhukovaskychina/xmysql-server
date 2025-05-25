package logs

const (
	/* Logging modes for a mini-transaction */
	MTR_LOG_ALL = 21
	/* default mode: log all operations
	modifying disk-based data */
	MTR_LOG_NONE    = 22 /* log no operations */
	MTR_LOG_NO_REDO = 23 /* Don't generate REDO */
	/*	MTR_LOG_SPACE	23 */ /* log only operations modifying
	file space page allocation data
	(operations in fsp0fsp.* ) */
	MTR_LOG_SHORT_INSERTS = 24 /* inserts are logged in a shorter
	form */

	/* Types for the mlock objects to store in the mtr memo; NOTE that the
	   first 3 values must be RW_S_LATCH, RW_X_LATCH, RW_NO_LATCH */
	MTR_MEMO_PAGE_S_FIX = 1
	MTR_MEMO_PAGE_X_FIX = 2
	MTR_MEMO_BUF_FIX    = 3

	MTR_MEMO_MODIFY = 54

	MTR_MEMO_S_LOCK = 55
	MTR_MEMO_X_LOCK = 56

	/** @name Log item types
	  The log items are declared 'byte' so that the compiler can warn if val
	  and type parameters are switched in a call to mlog_write_ulint. NOTE!
	  For 1 - 8 bytes, the flag valueImpl must give the length also! @{ */
	MLOG_SINGLE_REC_FLAG = 128 /*!< if the mtr contains only
	one log record for one page,
	i.e., write_initial_log_record
	has been called only once,
	this flag is ORed to the type
	of that first log record */
	MLOG_1BYTE                 = (1) /*!< one byte is written */
	MLOG_2BYTES                = (2) /*!< 2 bytes ... */
	MLOG_4BYTES                = (4) /*!< 4 bytes ... */
	MLOG_8BYTES                = (8) /*!< 8 bytes ... */
	MLOG_REC_INSERT            = 9   /*!< record insert */
	MLOG_REC_CLUST_DELETE_MARK = 10  /*!< mark clustered index record
	deleted */
	MLOG_REC_SEC_DELETE_MARK = (11) /*!< mark secondary index record
	deleted */
	MLOG_REC_UPDATE_IN_PLACE = (13) /*!< update of a record,
	preserves record field sizes */
	MLOG_REC_DELETE = (14) /*!< delete a record from a
	page */
	MLOG_LIST_END_DELETE = (15) /*!< delete record list end on
	index page */
	MLOG_LIST_START_DELETE = (16) /*!< delete record list start on
	index page */
	MLOG_LIST_END_COPY_CREATED = (17) /*!< copy record list end to a
	new created index page */
	MLOG_PAGE_REORGANIZE = (18) /*!< reorganize an
	index page in
	ROW_FORMAT=REDUNDANT */
	MLOG_PAGE_CREATE = (19) /*!< create an index page */
	MLOG_UNDO_INSERT = (20) /*!< insert entry in an undo
	log */
	MLOG_UNDO_ERASE_END = (21) /*!< erase an undo log
	page end */
	MLOG_UNDO_INIT = (22) /*!< initialize a page in an
	undo log */
	MLOG_UNDO_HDR_DISCARD = (23) /*!< discard an update undo log
	header */
	MLOG_UNDO_HDR_REUSE = (24) /*!< reuse an insert undo log
	header */
	MLOG_UNDO_HDR_CREATE = (25) /*!< create an undo
	log header */
	MLOG_REC_MIN_MARK = (26) /*!< mark an index
	record as the
	predefined minimum
	record */
	MLOG_IBUF_BITMAP_INIT = (27) /*!< initialize an
	ibuf bitmap page */
	/*	MLOG_FULL_PAGE	(28)	full contents of a page */

	MLOG_LSN = (28) /* current LSN */

	MLOG_INIT_FILE_PAGE = (29) /*!< this means that a
	file page is taken
	into use and the prior
	contents of the page
	should be ignored: in
	recovery we must not
	trust the lsn values
	stored to the file
	page */
	MLOG_WRITE_STRING = (30) /*!< write a string to
	a page */
	MLOG_MULTI_REC_END = (31) /*!< if a single mtr writes
	several log records,
	this log record ends the
	sequence of these records */
	MLOG_DUMMY_RECORD = (32) /*!< dummy log record used to
	pad a log block full */
	MLOG_FILE_CREATE = (33) /*!< log record about an .ibd
	file creation */
	MLOG_FILE_RENAME = (34) /*!< log record about an .ibd
	file rename */
	MLOG_FILE_DELETE = (35) /*!< log record about an .ibd
	file deletion */
	MLOG_COMP_REC_MIN_MARK = (36) /*!< mark a compact
	index record as the
	predefined minimum
	record */
	MLOG_COMP_PAGE_CREATE = (37) /*!< create a compact
	index page */
	MLOG_COMP_REC_INSERT            = (38) /*!< compact record insert */
	MLOG_COMP_REC_CLUST_DELETE_MARK = (39)
	/*!< mark compact
	  clustered index record
	  deleted */
	MLOG_COMP_REC_SEC_DELETE_MARK = (40) /*!< mark compact
	secondary index record
	deleted; this log
	record type is
	redundant, as
	MLOG_REC_SEC_DELETE_MARK
	is independent of the
	record format. */
	MLOG_COMP_REC_UPDATE_IN_PLACE = (41) /*!< update of a
	compact record,
	preserves record field
	sizes */
	MLOG_COMP_REC_DELETE = (42) /*!< delete a compact record
	from a page */
	MLOG_COMP_LIST_END_DELETE = (43) /*!< delete compact record list
	end on index page */
	MLOG_COMP_LIST_START_DELETE = (44) /*!< delete compact record list
	start on index page */
	MLOG_COMP_LIST_END_COPY_CREATED = (45)
	/*!< copy compact
	  record list end to a
	  new created index
	  page */
	MLOG_COMP_PAGE_REORGANIZE = (46) /*!< reorganize an index page */
	MLOG_FILE_CREATE2         = (47) /*!< log record about creating
	an .ibd file, with format */
	MLOG_ZIP_WRITE_NODE_PTR = (48) /*!< write the node pointer of
	a record on a compressed
	non-leaf B-tree page */
	MLOG_ZIP_WRITE_BLOB_PTR = (49) /*!< write the BLOB pointer
	of an externally stored column
	on a compressed page */
	MLOG_ZIP_WRITE_HEADER = (50) /*!< write to compressed page
	header */
	MLOG_ZIP_PAGE_COMPRESS         = (51) /*!< compress an index page */
	MLOG_ZIP_PAGE_COMPRESS_NO_DATA = (52) /*!< compress an index page
	//without logging it's image */
	// MLOG_ZIP_PAGE_REORGANIZE= (53)	/*!< reorganize a compressed
	//page */
	MLOG_BIGGEST_TYPE = (53) /*!< biggest valueImpl (used in
	assertions) */
	/* @} */

	/** @name Flags for MLOG_FILE operations
	  (stored in the page number parameter, called log_flags in the
	  functions).  The page number parameter was originally written as 0. @{ */
	MLOG_FILE_FLAG_TEMP = 1 /*!< identifies TEMPORARY TABLE in
	//MLOG_FILE_CREATE, MLOG_FILE_CREATE2 */
)
