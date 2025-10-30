# Design Document - Page Simplification

## Overview

This design document describes the architecture for simplifying and unifying the Page implementation in XMySQL Server's InnoDB storage engine. The design consolidates 35 page types across 56 files into a unified architecture with 9 core page types plus 3 feature decorators, eliminating 76% of duplicate code while improving maintainability.

### Design Goals

1. **Single Source of Truth**: One unified base page implementation
2. **Composition Over Inheritance**: Use composition pattern for specialized pages
3. **Decorator Pattern**: Implement features (compression, encryption, MVCC) as decorators
4. **Zero Duplication**: Eliminate all duplicate wrapper/store implementations
5. **Backward Compatibility**: Maintain existing APIs and file formats
6. **Performance**: Match or exceed current implementation performance

### Success Metrics

- Reduce page-related files from 56 to ~15 (73% reduction)
- Reduce page types from 35 to 12 (9 core + 3 decorators, 66% reduction)
- Eliminate ~2000 lines of duplicate code (100% reduction)
- Maintain 100% API compatibility
- Achieve 80%+ test coverage
- Zero performance regression

## Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
│              (Executor, Storage Manager, etc.)              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Page Interface Layer                      │
│                    (IPage, IPageWrapper)                    │
└─────────────────────────────────────────────────────────────┘
                              │
                ┌─────────────┴─────────────┐
                ▼                           ▼
┌───────────────────────────┐   ┌──────────────────────────┐
│   Feature Decorators      │   │   Core Page Types        │
│  ┌──────────────────────┐ │   │  ┌──────────────────┐   │
│  │ CompressedDecorator  │ │   │  │  UnifiedPage     │   │
│  │ EncryptedDecorator   │ │   │  │  (Base Class)    │   │
│  │ MVCCDecorator        │ │   │  └──────────────────┘   │
│  └──────────────────────┘ │   │           │              │
└───────────────────────────┘   │           ▼              │
                                │  ┌──────────────────┐   │
                                │  │  IndexPage       │   │
                                │  │  BlobPage        │   │
                                │  │  UndoPage        │   │
                                │  │  FspHdrPage      │   │
                                │  │  XdesPage        │   │
                                │  │  InodePage       │   │
                                │  │  IBufBitmapPage  │   │
                                │  │  AllocatedPage   │   │
                                │  │  SystemPage      │   │
                                │  └──────────────────┘   │
                                └──────────────────────────┘
```


## Components and Interfaces

### 1. UnifiedPage (Base Class)

The UnifiedPage is the foundation of all page types, consolidating functionality from AbstractPage, BasePage, and BasePageWrapper.

#### Structure

```go
// Location: server/innodb/storage/wrapper/types/unified_page.go

type UnifiedPage struct {
    // Concurrency control
    mu sync.RWMutex
    
    // Core page structure (InnoDB format)
    header  FileHeader    // 38 bytes - File header
    body    []byte        // Variable - Page body content
    trailer FileTrailer   // 8 bytes - File trailer
    
    // Page metadata
    spaceID  uint32       // Tablespace ID
    pageNo   uint32       // Page number within tablespace
    pageType PageType     // Page type (INDEX, BLOB, UNDO, etc.)
    size     uint32       // Page size (typically 16KB)
    
    // State management (atomic for thread-safety)
    state    atomic.Uint32  // Page state (Clean, Dirty, Loading, etc.)
    lsn      atomic.Uint64  // Log Sequence Number
    dirty    atomic.Bool    // Dirty flag
    pinCount atomic.Int32   // Pin count for buffer pool
    
    // Statistics
    stats PageStats       // Access statistics
    
    // Persistence
    rawData []byte        // Raw page data for serialization
    
    // Buffer pool integration
    bufferPage *buffer_pool.BufferPage  // Associated buffer page
}
```

#### Key Methods

```go
// Creation and initialization
func NewUnifiedPage(spaceID, pageNo uint32, pageType PageType) *UnifiedPage
func (p *UnifiedPage) Init() error

// Data access
func (p *UnifiedPage) GetData() []byte
func (p *UnifiedPage) SetData(data []byte) error
func (p *UnifiedPage) GetBody() []byte
func (p *UnifiedPage) SetBody(body []byte)

// Metadata access
func (p *UnifiedPage) GetPageID() uint32
func (p *UnifiedPage) GetPageNo() uint32
func (p *UnifiedPage) GetSpaceID() uint32
func (p *UnifiedPage) GetPageType() PageType
func (p *UnifiedPage) GetSize() uint32

// State management
func (p *UnifiedPage) IsDirty() bool
func (p *UnifiedPage) MarkDirty()
func (p *UnifiedPage) ClearDirty()
func (p *UnifiedPage) GetState() PageState
func (p *UnifiedPage) SetState(state PageState)

// LSN management
func (p *UnifiedPage) GetLSN() uint64
func (p *UnifiedPage) SetLSN(lsn uint64)

// Buffer pool management
func (p *UnifiedPage) Pin()
func (p *UnifiedPage) Unpin()
func (p *UnifiedPage) GetPinCount() int32

// Serialization
func (p *UnifiedPage) Serialize() ([]byte, error)
func (p *UnifiedPage) Deserialize(data []byte) error
func (p *UnifiedPage) ToBytes() ([]byte, error)
func (p *UnifiedPage) ParseFromBytes(data []byte) error

// Integrity checking
func (p *UnifiedPage) CalculateChecksum() uint32
func (p *UnifiedPage) ValidateChecksum() error
func (p *UnifiedPage) UpdateChecksum()
func (p *UnifiedPage) IsCorrupted() bool

// File header/trailer access
func (p *UnifiedPage) GetFileHeader() *FileHeader
func (p *UnifiedPage) GetFileTrailer() *FileTrailer
func (p *UnifiedPage) GetFileHeaderBytes() []byte
func (p *UnifiedPage) GetFileTrailerBytes() []byte

// Statistics
func (p *UnifiedPage) GetStats() *PageStats
func (p *UnifiedPage) UpdateAccessStats()

// IO operations
func (p *UnifiedPage) Read() error
func (p *UnifiedPage) Write() error
func (p *UnifiedPage) Flush() error

// Lifecycle
func (p *UnifiedPage) Release()
```


### 2. Core Page Types (Composition Pattern)

Each specialized page type embeds UnifiedPage and adds type-specific fields and methods.

#### 2.1 IndexPage (B+Tree Index Page)

```go
// Location: server/innodb/storage/wrapper/types/index_page.go

type IndexPage struct {
    *UnifiedPage  // Embedded base page
    
    // Index-specific header (56 bytes)
    indexHeader IndexHeader
    
    // Page content sections
    infimumSupremum []byte  // Min/max records (26 bytes)
    userRecords     []byte  // Actual user records
    freeSpace       []byte  // Free space
    pageDirectory   []byte  // Slot directory
}

type IndexHeader struct {
    NDirSlots     uint16  // Number of directory slots
    HeapTop       uint16  // Top of heap
    NHeap         uint16  // Number of records in heap
    Free          uint16  // Pointer to free list
    Garbage       uint16  // Garbage bytes
    LastInsert    uint16  // Last insert position
    Direction     uint16  // Insert direction
    NDirections   uint16  // Consecutive inserts in direction
    NRecs         uint16  // Number of user records
    MaxTrxID      uint64  // Max transaction ID
    Level         uint16  // B+tree level
    IndexID       uint64  // Index ID
    BtrSegLeaf    [10]byte // Leaf segment header
    BtrSegTop     [10]byte // Non-leaf segment header
}

// Methods
func NewIndexPage(spaceID, pageNo uint32) *IndexPage
func (ip *IndexPage) GetIndexHeader() *IndexHeader
func (ip *IndexPage) GetUserRecords() []byte
func (ip *IndexPage) AddRecord(record []byte) error
func (ip *IndexPage) DeleteRecord(offset uint16) error
func (ip *IndexPage) GetPageDirectory() []byte
func (ip *IndexPage) IsLeafPage() bool
```

#### 2.2 BlobPage (BLOB Data Page)

```go
// Location: server/innodb/storage/wrapper/types/blob_page.go

type BlobPage struct {
    *UnifiedPage  // Embedded base page
    
    // BLOB-specific header (20 bytes)
    blobHeader BlobHeader
    
    // BLOB data (16318 bytes)
    data []byte
}

type BlobHeader struct {
    Length    uint32  // Total BLOB length
    NextPage  uint32  // Next BLOB page (0 if last)
    Offset    uint32  // Offset in total BLOB
    SegmentID uint64  // Segment ID
}

// Methods
func NewBlobPage(spaceID, pageNo uint32, segmentID uint64) *BlobPage
func (bp *BlobPage) SetBlobData(data []byte, totalLength, offset, nextPage uint32) error
func (bp *BlobPage) GetBlobData() []byte
func (bp *BlobPage) GetNextPageNo() uint32
func (bp *BlobPage) IsLastPage() bool
func (bp *BlobPage) GetSegmentID() uint64
```

#### 2.3 UndoPage (Undo Log Page)

```go
// Location: server/innodb/storage/wrapper/types/undo_page.go

type UndoPage struct {
    *UnifiedPage  // Embedded base page
    
    // Undo-specific header
    undoHeader UndoHeader
    
    // Undo records
    undoRecords []UndoRecord
}

type UndoHeader struct {
    Type          uint16  // Undo log type
    State         uint16  // Undo log state
    LastLogOffset uint16  // Last log record offset
    FreeListLen   uint16  // Free list length
    FreeListStart uint16  // Free list start
}

type UndoRecord struct {
    Type      uint8   // Record type
    TrxID     uint64  // Transaction ID
    RollPtr   uint64  // Roll pointer
    Data      []byte  // Undo data
}

// Methods
func NewUndoPage(spaceID, pageNo uint32) *UndoPage
func (up *UndoPage) AddUndoRecord(record UndoRecord) error
func (up *UndoPage) GetUndoRecords() []UndoRecord
func (up *UndoPage) GetFreeSpace() uint16
```


#### 2.4 FspHdrPage (File Space Header Page)

```go
// Location: server/innodb/storage/wrapper/types/fsp_hdr_page.go

type FspHdrPage struct {
    *UnifiedPage  // Embedded base page
    
    // File space header
    fspHeader FspHeader
    
    // Extent descriptors (256 extents)
    xdesEntries []XdesEntry
}

type FspHeader struct {
    SpaceID       uint32  // Space ID
    NotUsed       uint32  // Not used
    Size          uint32  // Current size in pages
    FreeLimit     uint32  // Free limit
    Flags         uint32  // Tablespace flags
    FragNUsed     uint32  // Number of used fragment pages
    FreeList      ListNode // Free extent list
    FreeFragList  ListNode // Free fragment list
    FullFragList  ListNode // Full fragment list
    SegID         uint64  // Next unused segment ID
    SegInodesFull ListNode // Full inode pages
    SegInodesFree ListNode // Free inode pages
}

// Methods
func NewFspHdrPage(spaceID, pageNo uint32) *FspHdrPage
func (fp *FspHdrPage) GetFspHeader() *FspHeader
func (fp *FspHdrPage) GetXdesEntries() []XdesEntry
func (fp *FspHdrPage) AllocateExtent() (*XdesEntry, error)
func (fp *FspHdrPage) FreeExtent(extentNo uint32) error
```

#### 2.5 Other Core Page Types

```go
// XdesPage - Extent Descriptor Page
type XdesPage struct {
    *UnifiedPage
    xdesEntries []XdesEntry  // 256 extent descriptors
}

// InodePage - Index Node Page
type InodePage struct {
    *UnifiedPage
    inodeEntries []InodeEntry  // Inode entries
}

// IBufBitmapPage - Insert Buffer Bitmap Page
type IBufBitmapPage struct {
    *UnifiedPage
    bitmap []byte  // Bitmap data
}

// AllocatedPage - Allocated but not initialized
type AllocatedPage struct {
    *UnifiedPage
    // No additional fields
}

// SystemPage - Generic system page
type SystemPage struct {
    *UnifiedPage
    sysData []byte  // System-specific data
}
```

### 3. Feature Decorators

Decorators wrap any IPageWrapper to add features without modifying the page type.

#### 3.1 CompressedPageDecorator

```go
// Location: server/innodb/storage/wrapper/decorators/compressed_decorator.go

type CompressionAlgorithm int

const (
    CompressionNone CompressionAlgorithm = iota
    CompressionSnappy
    CompressionLZ4
    CompressionZlib
)

type CompressedPageDecorator struct {
    page      IPageWrapper         // Wrapped page
    algorithm CompressionAlgorithm // Compression algorithm
    
    // Compression metadata
    originalSize   uint32  // Original uncompressed size
    compressedSize uint32  // Compressed size
    compressionRatio float64 // Compression ratio
}

// Methods
func NewCompressedPageDecorator(page IPageWrapper, algo CompressionAlgorithm) *CompressedPageDecorator
func (d *CompressedPageDecorator) Compress() error
func (d *CompressedPageDecorator) Decompress() error
func (d *CompressedPageDecorator) GetCompressionRatio() float64
func (d *CompressedPageDecorator) GetOriginalSize() uint32

// Delegate all IPageWrapper methods to wrapped page
func (d *CompressedPageDecorator) GetPageID() uint32 { return d.page.GetPageID() }
func (d *CompressedPageDecorator) GetSpaceID() uint32 { return d.page.GetSpaceID() }
// ... (all other interface methods)

// Override serialization to compress/decompress
func (d *CompressedPageDecorator) ToBytes() ([]byte, error) {
    data, err := d.page.ToBytes()
    if err != nil {
        return nil, err
    }
    return d.compress(data)
}

func (d *CompressedPageDecorator) ParseFromBytes(data []byte) error {
    decompressed, err := d.decompress(data)
    if err != nil {
        return err
    }
    return d.page.ParseFromBytes(decompressed)
}
```


#### 3.2 EncryptedPageDecorator

```go
// Location: server/innodb/storage/wrapper/decorators/encrypted_decorator.go

type EncryptionAlgorithm int

const (
    EncryptionNone EncryptionAlgorithm = iota
    EncryptionAES256
)

type EncryptedPageDecorator struct {
    page      IPageWrapper        // Wrapped page
    algorithm EncryptionAlgorithm // Encryption algorithm
    key       []byte              // Encryption key
    iv        []byte              // Initialization vector
    
    // Encryption metadata
    encrypted bool    // Whether page is currently encrypted
    keyID     uint32  // Key ID for key rotation
}

// Methods
func NewEncryptedPageDecorator(page IPageWrapper, key []byte) *EncryptedPageDecorator
func (d *EncryptedPageDecorator) Encrypt() error
func (d *EncryptedPageDecorator) Decrypt() error
func (d *EncryptedPageDecorator) RotateKey(newKey []byte) error
func (d *EncryptedPageDecorator) IsEncrypted() bool

// Delegate all IPageWrapper methods
func (d *EncryptedPageDecorator) GetPageID() uint32 { return d.page.GetPageID() }
// ... (all other interface methods)

// Override serialization to encrypt/decrypt
func (d *EncryptedPageDecorator) ToBytes() ([]byte, error) {
    data, err := d.page.ToBytes()
    if err != nil {
        return nil, err
    }
    return d.encrypt(data)
}

func (d *EncryptedPageDecorator) ParseFromBytes(data []byte) error {
    decrypted, err := d.decrypt(data)
    if err != nil {
        return err
    }
    return d.page.ParseFromBytes(decrypted)
}
```

#### 3.3 MVCCPageDecorator

```go
// Location: server/innodb/storage/wrapper/decorators/mvcc_decorator.go

type MVCCPageDecorator struct {
    page     IPageWrapper  // Wrapped page
    readView *ReadView     // MVCC read view
    
    // MVCC metadata
    visibleRecords []Record  // Cached visible records
    cacheValid     bool      // Whether cache is valid
}

type ReadView struct {
    TrxID      uint64    // Current transaction ID
    LowLimitID uint64    // Low water mark
    UpLimitID  uint64    // High water mark
    TrxIDs     []uint64  // Active transaction IDs
}

// Methods
func NewMVCCPageDecorator(page IPageWrapper, readView *ReadView) *MVCCPageDecorator
func (d *MVCCPageDecorator) SetReadView(readView *ReadView)
func (d *MVCCPageDecorator) GetVisibleRecords() []Record
func (d *MVCCPageDecorator) IsRecordVisible(record Record) bool
func (d *MVCCPageDecorator) InvalidateCache()

// Delegate all IPageWrapper methods
func (d *MVCCPageDecorator) GetPageID() uint32 { return d.page.GetPageID() }
// ... (all other interface methods)

// Override data access to filter by visibility
func (d *MVCCPageDecorator) GetData() []byte {
    if !d.cacheValid {
        d.filterVisibleRecords()
    }
    return d.serializeVisibleRecords()
}
```

### 4. Interfaces

#### 4.1 IPage Interface (from basic/interfaces.go)

```go
type IPage interface {
    // Basic information
    GetPageID() uint32
    GetPageNo() uint32
    GetSpaceID() uint32
    GetPageType() PageType
    GetSize() uint32
    
    // Data access
    GetData() []byte
    GetContent() []byte
    SetData(data []byte) error
    SetContent(content []byte)
    
    // State management
    IsDirty() bool
    SetDirty(dirty bool)
    MarkDirty()
    ClearDirty()
    GetState() PageState
    SetState(state PageState)
    
    // LSN management
    GetLSN() uint64
    SetLSN(lsn uint64)
    
    // Buffer pool management
    Pin()
    Unpin()
    
    // IO operations
    Read() error
    Write() error
    
    // Type checking
    IsLeafPage() bool
    
    // Lifecycle
    Init() error
    Release()
}
```


#### 4.2 IPageWrapper Interface (Extended)

```go
type IPageWrapper interface {
    IPage  // Embed IPage interface
    
    // Additional wrapper-specific methods
    GetFileHeader() []byte
    GetFileTrailer() []byte
    GetFileHeaderStruct() *FileHeader
    GetFileTrailerStruct() *FileTrailer
    
    // Serialization
    ToBytes() ([]byte, error)
    ToByte() []byte  // Legacy compatibility
    ParseFromBytes(content []byte) error
    
    // Checksum
    UpdateChecksum()
    ValidateChecksum() bool
    CalculateChecksum() uint32
    IsCorrupted() bool
    
    // Statistics
    GetStats() *PageStats
    GetPinCount() int32
    
    // IO
    Flush() error
}
```

## Data Models

### Page State Machine

```
┌─────────────┐
│   Created   │
└──────┬──────┘
       │ Init()
       ▼
┌─────────────┐
│    Clean    │◄──────────────┐
└──────┬──────┘               │
       │ MarkDirty()           │ ClearDirty()
       ▼                       │
┌─────────────┐               │
│    Dirty    │───────────────┘
└──────┬──────┘
       │ Flush()
       ▼
┌─────────────┐
│   Flushing  │
└──────┬──────┘
       │ Complete
       ▼
┌─────────────┐
│    Clean    │
└─────────────┘
```

### Page Type Hierarchy

```
UnifiedPage (Base)
├── IndexPage (B+Tree index)
├── BlobPage (BLOB data)
├── UndoPage (Undo log)
├── FspHdrPage (File space header)
├── XdesPage (Extent descriptor)
├── InodePage (Index node)
├── IBufBitmapPage (Insert buffer bitmap)
├── AllocatedPage (Allocated placeholder)
└── SystemPage (Generic system)

Decorators (wrap any page)
├── CompressedPageDecorator
├── EncryptedPageDecorator
└── MVCCPageDecorator
```

### File Format

```
┌────────────────────────────────────────────────────────┐
│                   File Header (38 bytes)               │
├────────────────────────────────────────────────────────┤
│  Checksum (4) │ Page No (4) │ Prev (4) │ Next (4)    │
│  LSN (8) │ Type (2) │ Flush LSN (8) │ Space ID (4)   │
├────────────────────────────────────────────────────────┤
│                   Page Body (16338 bytes)              │
│                                                        │
│  Type-specific header + data                          │
│                                                        │
├────────────────────────────────────────────────────────┤
│                   File Trailer (8 bytes)               │
│                   Checksum (8)                         │
└────────────────────────────────────────────────────────┘
Total: 16384 bytes (16KB)
```

## Error Handling

### Error Types

```go
var (
    // Page errors
    ErrInvalidPage       = errors.New("invalid page")
    ErrPageCorrupted     = errors.New("page is corrupted")
    ErrInvalidPageSize   = errors.New("invalid page size")
    ErrInvalidChecksum   = errors.New("invalid page checksum")
    ErrPageNotLoaded     = errors.New("page not loaded")
    ErrPageLocked        = errors.New("page locked")
    
    // State errors
    ErrInvalidState      = errors.New("invalid page state")
    ErrPageAlreadyDirty  = errors.New("page already dirty")
    ErrPageNotDirty      = errors.New("page not dirty")
    
    // Data errors
    ErrInvalidData       = errors.New("invalid page data")
    ErrDataTooLarge      = errors.New("data too large for page")
    ErrInsufficientSpace = errors.New("insufficient space in page")
    
    // Decorator errors
    ErrCompressionFailed = errors.New("compression failed")
    ErrDecompressionFailed = errors.New("decompression failed")
    ErrEncryptionFailed  = errors.New("encryption failed")
    ErrDecryptionFailed  = errors.New("decryption failed")
)
```

### Error Handling Strategy

1. **Validation Errors**: Return immediately with descriptive error
2. **Corruption Errors**: Log error, mark page as corrupted, return error
3. **IO Errors**: Retry with exponential backoff, then fail
4. **Decorator Errors**: Unwrap and return original page if possible
5. **Concurrent Access**: Use RWMutex to prevent race conditions

