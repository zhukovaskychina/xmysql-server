# Implementation Plan - Page Simplification

## Phase 1: Foundation - UnifiedPage Base Class

- [ ] 1. Create UnifiedPage base class and core infrastructure
  - Create `server/innodb/storage/wrapper/types/unified_page.go` with UnifiedPage struct
  - Implement core fields: header, body, trailer, metadata, state management
  - Use atomic operations for thread-safe state (dirty flag, pin count, LSN)
  - Add RWMutex for concurrency control
  - _Requirements: 1.1, 1.2, 1.3, 1.4_

- [ ] 1.1 Implement UnifiedPage creation and initialization methods
  - Write `NewUnifiedPage(spaceID, pageNo, pageType)` constructor
  - Implement `Init()` method to initialize page structure
  - Set up default file header and trailer
  - Initialize statistics tracking
  - _Requirements: 1.1, 1.2_

- [ ] 1.2 Implement UnifiedPage data access methods
  - Write `GetData()`, `SetData()`, `GetBody()`, `SetBody()` methods
  - Implement `GetPageID()`, `GetPageNo()`, `GetSpaceID()`, `GetPageType()`, `GetSize()`
  - Add proper locking for concurrent access
  - _Requirements: 1.2, 5.1, 5.2_

- [ ] 1.3 Implement UnifiedPage state management
  - Write `IsDirty()`, `MarkDirty()`, `ClearDirty()` using atomic operations
  - Implement `GetState()`, `SetState()` for page state machine
  - Add `GetLSN()`, `SetLSN()` for log sequence number management
  - Implement `Pin()`, `Unpin()`, `GetPinCount()` for buffer pool integration
  - _Requirements: 1.3, 1.4, 5.4, 5.5_

- [ ] 1.4 Implement UnifiedPage serialization methods
  - Write `Serialize()` and `Deserialize()` for byte conversion
  - Implement `ToBytes()` and `ParseFromBytes()` for compatibility
  - Ensure proper byte order and alignment
  - Handle file header and trailer serialization
  - _Requirements: 1.5, 5.6, 5.7_

- [ ] 1.5 Implement UnifiedPage integrity checking
  - Write `CalculateChecksum()` using CRC32 algorithm
  - Implement `ValidateChecksum()` to verify page integrity
  - Add `UpdateChecksum()` to recalculate after modifications
  - Implement `IsCorrupted()` to check corruption status
  - _Requirements: 1.6, 8.2_

- [ ] 1.6 Implement UnifiedPage file header/trailer access
  - Write `GetFileHeader()`, `GetFileTrailer()` methods
  - Implement `GetFileHeaderBytes()`, `GetFileTrailerBytes()`
  - Ensure backward compatibility with existing format
  - _Requirements: 1.2, 5.4, 5.5_

- [ ] 1.7 Implement UnifiedPage statistics and IO operations
  - Write `GetStats()`, `UpdateAccessStats()` for tracking
  - Implement `Read()`, `Write()`, `Flush()` for IO operations
  - Add `Release()` for cleanup
  - _Requirements: 1.4, 1.8_

- [ ]* 1.8 Write comprehensive unit tests for UnifiedPage
  - Test all public methods with various inputs
  - Test concurrent access scenarios (multiple goroutines)
  - Test serialization round-trip (object → bytes → object)
  - Test checksum calculation and validation
  - Test state transitions and atomic operations
  - _Requirements: 6.1, 6.2, 6.3_

## Phase 2: Core Page Types Refactoring

- [ ] 2. Refactor IndexPage to use UnifiedPage composition
  - Create `server/innodb/storage/wrapper/types/index_page.go`
  - Embed UnifiedPage and add IndexHeader struct
  - Implement index-specific fields (infimumSupremum, userRecords, pageDirectory)
  - _Requirements: 2.1, 2.10_

- [ ] 2.1 Implement IndexPage creation and initialization
  - Write `NewIndexPage(spaceID, pageNo)` constructor
  - Initialize IndexHeader with default values
  - Set up infimum and supremum records
  - Initialize page directory
  - _Requirements: 2.1_

- [ ] 2.2 Implement IndexPage data access methods
  - Write `GetIndexHeader()` to access index header
  - Implement `GetUserRecords()` to retrieve records
  - Add `GetPageDirectory()` for directory access
  - Implement `IsLeafPage()` to check B+tree level
  - _Requirements: 2.1, 2.10_

- [ ] 2.3 Implement IndexPage record management
  - Write `AddRecord(record)` to insert new records
  - Implement `DeleteRecord(offset)` to remove records
  - Update page directory when records change
  - Maintain free space tracking
  - _Requirements: 2.1, 2.10_

- [ ]* 2.4 Write unit tests for IndexPage
  - Test IndexPage creation and initialization
  - Test record insertion and deletion
  - Test page directory management
  - Test integration with UnifiedPage methods
  - _Requirements: 6.4_

- [ ] 3. Refactor BlobPage to use UnifiedPage composition
  - Create `server/innodb/storage/wrapper/types/blob_page.go`
  - Embed UnifiedPage and add BlobHeader struct
  - Implement BLOB data storage (16318 bytes)
  - Add next page pointer for BLOB chains
  - _Requirements: 2.2, 2.10_

- [ ] 3.1 Implement BlobPage creation and data management
  - Write `NewBlobPage(spaceID, pageNo, segmentID)` constructor
  - Implement `SetBlobData(data, totalLength, offset, nextPage)`
  - Add `GetBlobData()` to retrieve BLOB content
  - Implement `GetNextPageNo()`, `IsLastPage()`, `GetSegmentID()`
  - _Requirements: 2.2, 2.10_

- [ ]* 3.2 Write unit tests for BlobPage
  - Test BlobPage creation with various segment IDs
  - Test BLOB data storage and retrieval
  - Test BLOB chain navigation (next page pointers)
  - Test integration with UnifiedPage
  - _Requirements: 6.4_

- [ ] 4. Refactor UndoPage to use UnifiedPage composition
  - Create `server/innodb/storage/wrapper/types/undo_page.go`
  - Embed UnifiedPage and add UndoHeader struct
  - Implement undo record storage and management
  - Add free list management for undo records
  - _Requirements: 2.3, 2.10_

- [ ] 4.1 Implement UndoPage undo record management
  - Write `NewUndoPage(spaceID, pageNo)` constructor
  - Implement `AddUndoRecord(record)` to append undo records
  - Add `GetUndoRecords()` to retrieve all records
  - Implement `GetFreeSpace()` for space tracking
  - _Requirements: 2.3, 2.10_

- [ ]* 4.2 Write unit tests for UndoPage
  - Test UndoPage creation and initialization
  - Test undo record addition and retrieval
  - Test free space management
  - Test integration with transaction manager
  - _Requirements: 6.4_

## Phase 3: Remaining Core Page Types

- [ ] 5. Refactor FspHdrPage (File Space Header Page)
  - Create `server/innodb/storage/wrapper/types/fsp_hdr_page.go`
  - Embed UnifiedPage and add FspHeader struct
  - Implement extent descriptor array (256 entries)
  - Add extent allocation and free list management
  - _Requirements: 2.4, 2.10_

- [ ] 5.1 Implement FspHdrPage extent management
  - Write `NewFspHdrPage(spaceID, pageNo)` constructor
  - Implement `GetFspHeader()`, `GetXdesEntries()`
  - Add `AllocateExtent()` to allocate new extents
  - Implement `FreeExtent(extentNo)` to release extents
  - _Requirements: 2.4, 2.10_

- [ ]* 5.2 Write unit tests for FspHdrPage
  - Test FspHdrPage creation and header access
  - Test extent allocation and deallocation
  - Test free list management
  - Test integration with space manager
  - _Requirements: 6.4_

- [ ] 6. Refactor XdesPage (Extent Descriptor Page)
  - Create `server/innodb/storage/wrapper/types/xdes_page.go`
  - Embed UnifiedPage
  - Implement 256 extent descriptor entries
  - Add extent state management (FREE, FREE_FRAG, FULL_FRAG, FSEG)
  - _Requirements: 2.5, 2.10_

- [ ]* 6.1 Write unit tests for XdesPage
  - Test XdesPage creation and descriptor access
  - Test extent state transitions
  - Test integration with extent management
  - _Requirements: 6.4_

- [ ] 7. Refactor InodePage (Index Node Page)
  - Create `server/innodb/storage/wrapper/types/inode_page.go`
  - Embed UnifiedPage
  - Implement inode entry array
  - Add segment management functionality
  - _Requirements: 2.6, 2.10_

- [ ]* 7.1 Write unit tests for InodePage
  - Test InodePage creation and inode access
  - Test segment management operations
  - Test integration with segment manager
  - _Requirements: 6.4_

- [ ] 8. Refactor IBufBitmapPage (Insert Buffer Bitmap Page)
  - Create `server/innodb/storage/wrapper/types/ibuf_bitmap_page.go`
  - Embed UnifiedPage
  - Implement bitmap data storage
  - Add bitmap manipulation methods
  - _Requirements: 2.7, 2.10_

- [ ]* 8.1 Write unit tests for IBufBitmapPage
  - Test IBufBitmapPage creation and bitmap access
  - Test bitmap bit manipulation
  - Test integration with insert buffer
  - _Requirements: 6.4_

- [ ] 9. Refactor AllocatedPage and SystemPage
  - Create `server/innodb/storage/wrapper/types/allocated_page.go`
  - Create `server/innodb/storage/wrapper/types/system_page.go`
  - Embed UnifiedPage in both types
  - AllocatedPage has no additional fields
  - SystemPage adds generic sysData field
  - _Requirements: 2.8, 2.9, 2.10_

- [ ]* 9.1 Write unit tests for AllocatedPage and SystemPage
  - Test page creation and basic operations
  - Test integration with storage manager
  - _Requirements: 6.4_

## Phase 4: Feature Decorators Implementation

- [ ] 10. Implement CompressedPageDecorator
  - Create `server/innodb/storage/wrapper/decorators/compressed_decorator.go`
  - Implement decorator pattern wrapping IPageWrapper
  - Support multiple algorithms (Snappy, LZ4, Zlib)
  - Add transparent compression/decompression in ToBytes/ParseFromBytes
  - _Requirements: 3.1, 3.2, 3.3, 3.9_

- [ ] 10.1 Implement compression algorithms
  - Integrate Snappy compression (default, fastest)
  - Integrate LZ4 compression (balanced)
  - Integrate Zlib compression (best ratio)
  - Add algorithm selection logic
  - _Requirements: 3.2, 8.5_

- [ ] 10.2 Implement compression metadata tracking
  - Track original size and compressed size
  - Calculate compression ratio
  - Add `GetCompressionRatio()`, `GetOriginalSize()` methods
  - _Requirements: 3.2_

- [ ]* 10.3 Write unit tests for CompressedPageDecorator
  - Test each compression algorithm
  - Test compression/decompression round-trip
  - Test decorator stacking with other decorators
  - Test performance (< 5% overhead)
  - _Requirements: 6.5, 6.6, 8.4_

- [ ] 11. Implement EncryptedPageDecorator
  - Create `server/innodb/storage/wrapper/decorators/encrypted_decorator.go`
  - Implement decorator pattern wrapping IPageWrapper
  - Support AES-256 encryption
  - Add transparent encryption/decryption in ToBytes/ParseFromBytes
  - _Requirements: 3.4, 3.5, 3.6, 3.9_

- [ ] 11.1 Implement encryption key management
  - Support key rotation with key ID tracking
  - Generate random IV for each encryption
  - Implement `RotateKey(newKey)` method
  - Add `IsEncrypted()` status check
  - _Requirements: 3.6, 8.6_

- [ ]* 11.2 Write unit tests for EncryptedPageDecorator
  - Test AES-256 encryption/decryption
  - Test key rotation functionality
  - Test decorator stacking (encrypt + compress)
  - Test error handling for invalid keys
  - _Requirements: 6.5, 6.6, 3.10_

- [ ] 12. Implement MVCCPageDecorator
  - Create `server/innodb/storage/wrapper/decorators/mvcc_decorator.go`
  - Implement decorator pattern wrapping IPageWrapper
  - Add ReadView struct for MVCC visibility
  - Filter records based on transaction visibility
  - _Requirements: 3.7, 3.8, 3.9_

- [ ] 12.1 Implement MVCC visibility logic
  - Implement `IsRecordVisible(record)` based on ReadView
  - Add `GetVisibleRecords()` with caching
  - Implement `SetReadView(readView)` to update view
  - Add `InvalidateCache()` when page changes
  - _Requirements: 3.8_

- [ ]* 12.2 Write unit tests for MVCCPageDecorator
  - Test record visibility filtering
  - Test ReadView updates
  - Test cache invalidation
  - Test integration with transaction manager
  - _Requirements: 6.5, 6.6_

## Phase 5: Duplicate Elimination and Migration

- [ ] 13. Remove duplicate page implementations in store layer
  - Identify all 13 duplicate files in `server/innodb/storage/store/pages/`
  - Delete duplicate implementations that mirror wrapper types
  - Update any direct references to store types
  - _Requirements: 4.1, 4.2_

- [ ] 13.1 Update imports across codebase
  - Search for imports of old page types
  - Replace with unified types from wrapper/types
  - Update buffer pool references
  - Update storage manager references
  - Update executor references
  - _Requirements: 4.3_

- [ ] 13.2 Remove old base classes
  - Delete `AbstractPage` after verifying no references
  - Delete `BasePage` after verifying no references
  - Delete `BasePageWrapper` after verifying no references
  - _Requirements: 4.4, 4.5_

- [ ] 13.3 Verify compilation and fix errors
  - Run `go build ./...` to check for compilation errors
  - Fix any remaining type mismatches
  - Update method signatures if needed
  - _Requirements: 4.7_

- [ ] 14. Update test files to use unified types
  - Update all test files referencing old page types
  - Replace old type instantiation with unified types
  - Update test assertions for new structure
  - _Requirements: 4.6_

- [ ] 14.1 Run full test suite and fix failures
  - Run `go test ./...` to execute all tests
  - Fix any test failures due to API changes
  - Ensure all tests pass with unified types
  - _Requirements: 4.8, 6.9_

## Phase 6: Integration Testing and Documentation

- [ ] 15. Write integration tests for unified page system
  - Test pages with buffer pool manager
  - Test pages with storage manager
  - Test pages with executor
  - Test page lifecycle (create → use → flush → release)
  - _Requirements: 6.7_

- [ ] 15.1 Write performance benchmarks
  - Benchmark UnifiedPage serialization vs old implementation
  - Benchmark decorator overhead (compression, encryption)
  - Benchmark concurrent access patterns
  - Verify no performance regression (within 5%)
  - _Requirements: 6.8, 8.1, 8.7_

- [ ]* 15.2 Measure test coverage
  - Run `go test -cover ./server/innodb/storage/wrapper/...`
  - Ensure at least 80% coverage for page-related code
  - Add tests for uncovered code paths
  - _Requirements: 6.10_

- [ ] 16. Create architecture documentation
  - Write architecture overview with diagrams
  - Document UnifiedPage structure and methods
  - Document each core page type
  - Document decorator pattern usage
  - Create examples for common use cases
  - _Requirements: 7.1, 7.2, 7.3, 7.7_

- [ ] 16.1 Create migration guide
  - Write step-by-step migration instructions
  - Create mapping table: old types → new types
  - Document common pitfalls and solutions
  - Add before/after code examples
  - _Requirements: 7.4, 7.5, 7.6_

- [ ] 16.2 Update existing documentation
  - Update README.md with new page architecture
  - Update storage engine documentation
  - Update developer guide
  - Add page simplification to changelog
  - _Requirements: 7.8_

## Phase 7: Code Quality and Final Review

- [ ] 17. Code quality review and refactoring
  - Run `gofmt` and `goimports` on all new code
  - Run `golint` and fix linting issues
  - Run `go vet` and fix reported issues
  - Check cyclomatic complexity (< 10 per function)
  - _Requirements: 10.1, 10.7, 10.8, 10.10_

- [ ] 17.1 Add comprehensive documentation comments
  - Add godoc comments for all public types
  - Add godoc comments for all public methods
  - Add inline comments for complex logic
  - Document error conditions and return values
  - _Requirements: 10.2, 10.3_

- [ ] 17.2 Improve error handling
  - Review all error returns for clarity
  - Add context to error messages
  - Ensure proper error wrapping
  - Test error paths in unit tests
  - _Requirements: 10.4_

- [ ] 17.3 Final code review
  - Review all changes with team
  - Address code review feedback
  - Ensure all requirements are met
  - Verify all acceptance criteria pass
  - _Requirements: 10.9_

## Phase 8: Deployment and Monitoring

- [ ] 18. Prepare for deployment
  - Create deployment checklist
  - Document rollback procedure
  - Prepare monitoring alerts
  - Update operational runbooks
  - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_

- [ ] 18.1 Gradual rollout plan
  - Deploy to development environment first
  - Run extended testing in staging
  - Monitor performance metrics
  - Deploy to production with feature flag
  - _Requirements: 9.6, 9.7_

- [ ] 18.2 Post-deployment verification
  - Verify all tests pass in production
  - Monitor error rates and performance
  - Collect feedback from team
  - Document lessons learned
  - _Requirements: 9.8_

---

## Summary

This implementation plan consolidates 35 page types into 9 core types + 3 decorators, eliminating 76% of duplicate code (~2000 lines) while maintaining full backward compatibility. The phased approach allows for incremental delivery and risk mitigation.

**Estimated Timeline:**
- Phase 1 (Foundation): 2-3 days
- Phase 2 (Core Types 1-3): 2-3 days
- Phase 3 (Core Types 4-9): 2-3 days
- Phase 4 (Decorators): 2-3 days
- Phase 5 (Migration): 1-2 days
- Phase 6 (Integration): 1-2 days
- Phase 7 (Quality): 1 day
- Phase 8 (Deployment): 1 day

**Total: 12-17 days** (full implementation) or **3-4 days** (partial: Phase 1-2 only)
