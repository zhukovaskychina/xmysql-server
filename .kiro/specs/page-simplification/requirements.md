# Requirements Document - Page Simplification

## Introduction

This specification defines the requirements for simplifying and unifying the Page implementation in the XMySQL Server InnoDB storage engine. Currently, the codebase has 35 different page types across 56 files with significant duplication (76% of wrapper types duplicate store types, approximately 2000 lines of duplicate code). This project will consolidate these into a unified architecture with 9 core page types plus 3 feature decorators, reducing code by 62% while improving maintainability and consistency.

The simplification follows the successful pattern established in the Extent unification (Stage 2), applying composition over inheritance principles to create a cleaner, more maintainable page architecture.

## Requirements

### Requirement 1: Create Unified Page Base Class

**User Story:** As a storage engine developer, I want a single unified base page implementation, so that all page types share consistent core functionality without duplication.

#### Acceptance Criteria

1. WHEN creating a new UnifiedPage THEN the system SHALL provide a single base class that consolidates functionality from AbstractPage, BasePage, and BasePageWrapper
2. WHEN the UnifiedPage is instantiated THEN it SHALL include file header, file trailer, page body, and metadata fields
3. WHEN the UnifiedPage manages state THEN it SHALL use atomic operations for thread-safe state management (dirty flag, pin count, LSN)
4. WHEN the UnifiedPage tracks statistics THEN it SHALL maintain read count, write count, and last access time
5. WHEN the UnifiedPage serializes THEN it SHALL support bidirectional conversion between in-memory structure and byte array
6. WHEN the UnifiedPage validates integrity THEN it SHALL calculate and verify checksums using CRC32 algorithm
7. WHEN the UnifiedPage manages concurrency THEN it SHALL use RWMutex for safe concurrent access
8. IF the UnifiedPage is accessed THEN it SHALL update access statistics automatically

### Requirement 2: Refactor Core Page Types Using Composition

**User Story:** As a storage engine developer, I want page types to use composition instead of inheritance, so that I can easily extend and maintain page functionality without tight coupling.

#### Acceptance Criteria

1. WHEN refactoring IndexPage THEN the system SHALL embed UnifiedPage and add index-specific fields (IndexHeader, records)
2. WHEN refactoring BlobPage THEN the system SHALL embed UnifiedPage and add BLOB-specific fields (BlobHeader, data)
3. WHEN refactoring UndoPage THEN the system SHALL embed UnifiedPage and add undo-specific fields (UndoHeader, undo records)
4. WHEN refactoring FspHdrPage THEN the system SHALL embed UnifiedPage and add file space header fields
5. WHEN refactoring XdesPage THEN the system SHALL embed UnifiedPage and add extent descriptor entries
6. WHEN refactoring InodePage THEN the system SHALL embed UnifiedPage and add inode entries
7. WHEN refactoring IBufBitmapPage THEN the system SHALL embed UnifiedPage and add bitmap data
8. WHEN refactoring AllocatedPage THEN the system SHALL embed UnifiedPage without additional fields
9. WHEN refactoring SystemPage THEN the system SHALL embed UnifiedPage and add generic system data field
10. WHEN any page type is created THEN it SHALL delegate common operations to the embedded UnifiedPage
11. IF a page type needs custom behavior THEN it SHALL override specific methods while reusing base functionality

### Requirement 3: Implement Feature Decorators

**User Story:** As a storage engine developer, I want compression, encryption, and MVCC to be implemented as decorators, so that these features can be applied to any page type without creating separate page classes.

#### Acceptance Criteria

1. WHEN creating CompressedPageDecorator THEN the system SHALL wrap any IPageWrapper and provide transparent compression/decompression
2. WHEN CompressedPageDecorator serializes THEN it SHALL compress page content using the configured algorithm (Snappy, LZ4, or Zlib)
3. WHEN CompressedPageDecorator deserializes THEN it SHALL decompress page content transparently
4. WHEN creating EncryptedPageDecorator THEN the system SHALL wrap any IPageWrapper and provide transparent encryption/decryption
5. WHEN EncryptedPageDecorator serializes THEN it SHALL encrypt page content using AES-256
6. WHEN EncryptedPageDecorator deserializes THEN it SHALL decrypt page content using the provided key
7. WHEN creating MVCCPageDecorator THEN the system SHALL wrap any IPageWrapper and provide MVCC read view filtering
8. WHEN MVCCPageDecorator reads records THEN it SHALL filter records based on the current read view
9. IF decorators are stacked THEN they SHALL work together (e.g., encrypt then compress)
10. IF a decorator operation fails THEN it SHALL return a clear error without corrupting the underlying page

### Requirement 4: Eliminate Duplicate Page Implementations

**User Story:** As a storage engine maintainer, I want to remove all duplicate page implementations between store and wrapper layers, so that bugs only need to be fixed once and the codebase is easier to understand.

#### Acceptance Criteria

1. WHEN removing duplicates THEN the system SHALL delete 13 duplicate wrapper files that mirror store implementations
2. WHEN consolidating implementations THEN the system SHALL keep only the unified versions in the wrapper/types directory
3. WHEN updating references THEN the system SHALL update all imports across the codebase to use unified types
4. WHEN removing old base classes THEN the system SHALL delete AbstractPage, BasePage, and BasePageWrapper after migration
5. IF any code references old types THEN the migration SHALL update those references to use UnifiedPage
6. IF tests reference old types THEN the migration SHALL update test code to use unified types
7. WHEN migration is complete THEN the system SHALL have zero compilation errors
8. WHEN migration is complete THEN all existing tests SHALL pass with unified types

### Requirement 5: Maintain Backward Compatibility

**User Story:** As a storage engine user, I want the page simplification to maintain API compatibility, so that existing code continues to work without modifications.

#### Acceptance Criteria

1. WHEN UnifiedPage implements IPage interface THEN it SHALL provide all methods from the original interface
2. WHEN UnifiedPage implements IPageWrapper interface THEN it SHALL provide all methods from the original interface
3. WHEN specialized page types are created THEN they SHALL maintain their original public APIs
4. IF existing code calls GetFileHeader() THEN it SHALL receive the same data structure as before
5. IF existing code calls GetFileTrailer() THEN it SHALL receive the same data structure as before
6. IF existing code calls serialization methods THEN they SHALL produce compatible byte formats
7. WHEN reading existing page files THEN the system SHALL correctly parse them with unified types
8. WHEN writing new page files THEN they SHALL be compatible with the existing file format

### Requirement 6: Comprehensive Testing

**User Story:** As a quality assurance engineer, I want comprehensive tests for the unified page implementation, so that I can verify correctness and prevent regressions.

#### Acceptance Criteria

1. WHEN testing UnifiedPage THEN the system SHALL have unit tests covering all public methods
2. WHEN testing UnifiedPage concurrency THEN the system SHALL have tests for concurrent read/write operations
3. WHEN testing UnifiedPage serialization THEN the system SHALL verify round-trip conversion (object → bytes → object)
4. WHEN testing specialized page types THEN each SHALL have unit tests for type-specific functionality
5. WHEN testing decorators THEN each SHALL have unit tests for wrapping behavior
6. WHEN testing decorator stacking THEN the system SHALL verify multiple decorators work together
7. WHEN running integration tests THEN the system SHALL verify pages work with buffer pool, storage manager, and executor
8. WHEN running performance tests THEN the system SHALL verify no performance regression compared to old implementation
9. IF any test fails THEN the system SHALL provide clear error messages indicating the failure reason
10. WHEN all tests pass THEN the system SHALL achieve at least 80% code coverage for page-related code

### Requirement 7: Documentation and Migration Guide

**User Story:** As a developer working with the codebase, I want clear documentation and migration guides, so that I can understand the new architecture and migrate existing code.

#### Acceptance Criteria

1. WHEN documentation is created THEN it SHALL include architecture diagrams showing the unified page structure
2. WHEN documentation is created THEN it SHALL include code examples for creating each page type
3. WHEN documentation is created THEN it SHALL include examples of using decorators
4. WHEN migration guide is created THEN it SHALL provide step-by-step instructions for updating code
5. WHEN migration guide is created THEN it SHALL include a mapping table from old types to new types
6. WHEN migration guide is created THEN it SHALL include common pitfalls and solutions
7. IF developers need to extend page functionality THEN documentation SHALL explain how to add new page types
8. IF developers need to add new features THEN documentation SHALL explain how to create new decorators

### Requirement 8: Performance Optimization

**User Story:** As a performance engineer, I want the unified page implementation to be as fast or faster than the original, so that the simplification doesn't negatively impact database performance.

#### Acceptance Criteria

1. WHEN UnifiedPage performs serialization THEN it SHALL complete in O(n) time where n is page size
2. WHEN UnifiedPage calculates checksums THEN it SHALL use optimized CRC32 implementation
3. WHEN UnifiedPage manages state THEN it SHALL use atomic operations to avoid lock contention
4. WHEN decorators wrap pages THEN they SHALL add minimal overhead (< 5% performance impact)
5. IF compression is enabled THEN it SHALL use fast algorithms (Snappy or LZ4) by default
6. IF encryption is enabled THEN it SHALL use hardware-accelerated AES when available
7. WHEN running benchmarks THEN UnifiedPage SHALL perform within 5% of original implementation
8. WHEN measuring memory usage THEN UnifiedPage SHALL use no more memory than original implementation

### Requirement 9: Phased Implementation

**User Story:** As a project manager, I want the implementation to be done in phases, so that we can deliver value incrementally and reduce risk.

#### Acceptance Criteria

1. WHEN Phase 1 completes THEN UnifiedPage base class SHALL be fully implemented and tested
2. WHEN Phase 2 completes THEN at least 3 core page types SHALL be refactored to use UnifiedPage
3. WHEN Phase 3 completes THEN all 3 decorators SHALL be implemented and tested
4. WHEN Phase 4 completes THEN all duplicate files SHALL be removed and references updated
5. WHEN Phase 5 completes THEN all tests SHALL pass and documentation SHALL be complete
6. IF any phase encounters blocking issues THEN the team SHALL pause and resolve before proceeding
7. IF any phase completes early THEN the team MAY begin the next phase
8. WHEN all phases complete THEN the system SHALL have achieved all simplification goals

### Requirement 10: Code Quality Standards

**User Story:** As a code reviewer, I want the new code to meet high quality standards, so that it's maintainable and follows best practices.

#### Acceptance Criteria

1. WHEN writing UnifiedPage code THEN it SHALL follow Go coding conventions and style guide
2. WHEN writing UnifiedPage code THEN all public methods SHALL have clear documentation comments
3. WHEN writing UnifiedPage code THEN complex logic SHALL have inline comments explaining the approach
4. WHEN writing UnifiedPage code THEN error handling SHALL be comprehensive with descriptive error messages
5. WHEN writing tests THEN they SHALL follow table-driven test patterns where appropriate
6. WHEN writing tests THEN they SHALL have clear test names describing what is being tested
7. IF code has cyclomatic complexity > 10 THEN it SHALL be refactored into smaller functions
8. IF code has duplicate logic THEN it SHALL be extracted into helper functions
9. WHEN code review is performed THEN all feedback SHALL be addressed before merging
10. WHEN code is merged THEN it SHALL pass all linting and static analysis checks
