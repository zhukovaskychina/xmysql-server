# Stage 2: Extent & Page Refactoring Analysis

## 📅 Date: 2025-10-29

## 🎯 Objectives

1. **Unify Extent Implementations**: Consolidate 3 different Extent implementations into 1 unified implementation
2. **Simplify Page Structures**: Reduce duplicate Page implementations from 20+ to ~10 using composition pattern
3. **Improve Code Maintainability**: Eliminate redundancy and improve code organization

---

## 📊 Part 1: Extent Implementation Analysis

### Current State: 3 Different Extent Implementations

#### 1. `server/innodb/storage/store/extents/extent.go` - ExtentEntry (Low-level)

**Purpose**: Low-level extent descriptor for serialization/deserialization

**Structure**:
```go
type ExtentEntry struct {
    SegmentID   uint64      // 8 bytes
    State       uint8       // 1 byte
    PageBitmap  [16]byte    // 16 bytes (128 bits, 2 bits per page)
    PageCount   uint8       // 1 byte
    FirstPageNo uint32      // 4 bytes
}
```

**Features**:
- ✅ Serialization/Deserialization support
- ✅ Bitmap-based page tracking (2 bits per page)
- ✅ Compact memory footprint (32 bytes)
- ❌ No concurrency control
- ❌ No statistics tracking
- ❌ No interface implementation

**Methods**: 9 methods
- `NewExtentEntry()`, `IsPageFree()`, `AllocatePage()`, `FreePage()`
- `SetSegmentID()`, `GetSegmentID()`, `GetState()`, `GetUsedPages()`
- `Serialize()`, `Deserialize()`

---

#### 2. `server/innodb/storage/wrapper/extent/extent.go` - BaseExtent (Mid-level)

**Purpose**: Wrapper-layer extent with concurrency control and statistics

**Structure**:
```go
type BaseExtent struct {
    basic.Extent              // Interface embedding
    mu       sync.RWMutex     // Concurrency control
    header   basic.ExtentHeader
    stats    basic.ExtentStats
    pages    map[uint32]bool  // Page allocation map
    pageList []uint32         // Ordered page list
}
```

**Features**:
- ✅ Implements `basic.Extent` interface
- ✅ Concurrency control (RWMutex)
- ✅ Statistics tracking
- ✅ Map-based page tracking
- ❌ Higher memory overhead
- ❌ No serialization support

**Methods**: 17 methods (implements full `basic.Extent` interface)

---

#### 3. `server/innodb/storage/wrapper/space/extent.go` - ExtentImpl (High-level)

**Purpose**: Space-layer extent with purpose tracking

**Structure**:
```go
type ExtentImpl struct {
    mu sync.RWMutex
    basic.Extent
    id        uint32
    spaceID   uint32
    startPage uint32
    purpose   basic.ExtentPurpose  // Data/Index/Undo/System
    state     basic.ExtentState
    pages     map[uint32]bool
    stats     ExtentStats          // Local stats struct
}
```

**Features**:
- ✅ Implements `basic.Extent` interface
- ✅ Purpose tracking (Data/Index/Undo/System)
- ✅ Concurrency control
- ✅ Statistics tracking
- ✅ Bitmap generation support
- ❌ Duplicate stats structure
- ❌ No serialization support

**Methods**: 25 methods (implements full `basic.Extent` interface + extras)

---

### Comparison Matrix

| Feature | ExtentEntry | BaseExtent | ExtentImpl |
|---------|-------------|------------|------------|
| **Layer** | Store (Low) | Wrapper (Mid) | Space (High) |
| **Size** | 32 bytes | ~100+ bytes | ~100+ bytes |
| **Serialization** | ✅ Yes | ❌ No | ❌ No |
| **Concurrency** | ❌ No | ✅ RWMutex | ✅ RWMutex |
| **Statistics** | ❌ No | ✅ Yes | ✅ Yes |
| **Interface** | ❌ No | ✅ basic.Extent | ✅ basic.Extent |
| **Page Tracking** | Bitmap (2 bits/page) | Map | Map |
| **Purpose** | ❌ No | ❌ No | ✅ Yes |
| **Memory** | Low | High | High |
| **Performance** | Fast | Medium | Medium |

---

### Problems Identified

1. **Duplication**: 3 implementations with overlapping functionality
2. **Inconsistency**: Different page tracking mechanisms (bitmap vs map)
3. **No Layering**: No clear separation between storage format and runtime representation
4. **Memory Overhead**: Two implementations use maps instead of bitmaps
5. **Missing Features**: 
   - ExtentEntry lacks concurrency control
   - BaseExtent/ExtentImpl lack serialization
6. **Stats Duplication**: Two different `ExtentStats` structures

---

### Proposed Unified Design

#### Strategy: Two-Layer Architecture

**Layer 1: Storage Format** (`store/extents/extent_entry.go`)
- Keep `ExtentEntry` for serialization/deserialization
- Compact, bitmap-based representation
- No concurrency control (immutable after deserialization)

**Layer 2: Runtime Representation** (`wrapper/extent/extent.go`)
- Unified `Extent` implementation
- Implements `basic.Extent` interface
- Uses `ExtentEntry` for persistence
- Adds concurrency control, statistics, and caching

#### Unified Extent Structure

```go
// wrapper/extent/extent.go
type Extent struct {
    mu sync.RWMutex
    
    // Core fields
    id        uint32
    spaceID   uint32
    segmentID uint64
    startPage uint32
    extType   basic.ExtentType
    purpose   basic.ExtentPurpose
    state     basic.ExtentState
    
    // Page tracking (hybrid approach)
    bitmap    [16]byte         // Persistent bitmap (2 bits/page)
    pages     map[uint32]bool  // Runtime cache for fast lookup
    pageList  []uint32         // Ordered list for iteration
    
    // Statistics
    stats     basic.ExtentStats
    
    // Persistence
    entry     *extents.ExtentEntry  // Underlying storage format
}
```

#### Benefits

1. ✅ **Single Source of Truth**: One implementation for all layers
2. ✅ **Best of Both Worlds**: Bitmap for storage, map for runtime
3. ✅ **Clear Separation**: Storage format vs runtime representation
4. ✅ **Full Feature Set**: Serialization + concurrency + statistics
5. ✅ **Memory Efficient**: Lazy loading of runtime structures
6. ✅ **Backward Compatible**: Can read existing ExtentEntry data

---

## 📊 Part 2: Page Implementation Analysis

### Current State: 20+ Page Types

#### Page Type Categories

**1. System Pages** (6 types)
- `FSP_HDR_Page` - File Space Header
- `XDES_Page` - Extent Descriptor
- `INODE_Page` - Index Node
- `IBUF_BITMAP_Page` - Insert Buffer Bitmap
- `SYS_Page` - System Page
- `TRX_SYS_Page` - Transaction System

**2. Data Pages** (8 types)
- `INDEX_Page` - B+Tree Index Page
- `UNDO_LOG_Page` - Undo Log Page
- `BLOB_Page` - BLOB Data Page
- `ALLOCATED_Page` - Allocated but not initialized
- `COMPRESSED_Page` - Compressed Page
- `ENCRYPTED_Page` - Encrypted Page
- `SDI_Page` - Serialized Dictionary Information
- `RTREE_Page` - R-Tree Index Page

**3. Wrapper Pages** (6+ types)
- `AllocatedPageWrapper`
- `IBufPageWrapper`
- `InodePageWrapper`
- `FspPageWrapper`
- `XdesPageWrapper`
- `BlobPageWrapper`

---

### Problems Identified

1. **Massive Duplication**: Each page type reimplements common functionality
2. **Inheritance Abuse**: Deep inheritance hierarchies instead of composition
3. **Inconsistent APIs**: Different page types have different method signatures
4. **No Base Class**: Missing a proper `BasePage` implementation
5. **Wrapper Redundancy**: Wrapper pages duplicate store pages

---

### Proposed Simplified Design

#### Strategy: Composition Over Inheritance

**Base Page** (1 type)
```go
type BasePage struct {
    header  FileHeader
    trailer FileTrailer
    body    []byte
    pageType common.PageType
}
```

**Specialized Pages** (Use composition)
```go
type IndexPage struct {
    *BasePage
    indexHeader IndexHeader
    records     []Record
}

type BlobPage struct {
    *BasePage
    blobHeader BlobHeader
    data       []byte
}
```

#### Target: Reduce to ~10 Core Page Types

1. `BasePage` - Common functionality
2. `IndexPage` - B+Tree index
3. `BlobPage` - BLOB data
4. `UndoPage` - Undo log
5. `FspHdrPage` - File space header
6. `XdesPage` - Extent descriptor
7. `InodePage` - Index node
8. `IBufBitmapPage` - Insert buffer bitmap
9. `AllocatedPage` - Allocated placeholder
10. `SystemPage` - Generic system page

**Eliminated**: Compressed, Encrypted, SDI, RTree (use decorators/wrappers instead)

---

## 📋 Implementation Plan

### Task 2.1: Unify Extent Implementations ✅ (Current)

**Steps**:
1. ✅ Analyze all 3 implementations
2. ⏳ Design unified architecture
3. ⏳ Implement unified `Extent` in `wrapper/extent/extent.go`
4. ⏳ Keep `ExtentEntry` in `store/extents/extent_entry.go` for serialization
5. ⏳ Add conversion methods between layers
6. ⏳ Update tests

**Estimated Time**: 2-3 days

---

### Task 2.2: Simplify Page Implementations

**Steps**:
1. Create `BasePage` with common functionality
2. Refactor existing pages to use composition
3. Merge duplicate wrapper/store pages
4. Remove unnecessary page types
5. Update all references
6. Update tests

**Estimated Time**: 5-7 days

---

### Task 2.3: Update References

**Steps**:
1. Find all usages of old Extent types
2. Update to use unified Extent
3. Find all usages of removed Page types
4. Update to use new Page types
5. Update imports

**Estimated Time**: 2-3 days

---

### Task 2.4: Testing

**Steps**:
1. Run all extent tests
2. Run all page tests
3. Run integration tests
4. Fix any regressions
5. Add new tests for unified types

**Estimated Time**: 2-3 days

---

## 📈 Expected Benefits

### Code Reduction
- **Extent**: 3 implementations → 1 unified implementation (~40% code reduction)
- **Page**: 20+ types → 10 types (~50% code reduction)
- **Total**: ~1500 lines of code eliminated

### Quality Improvements
- ✅ Single source of truth for each concept
- ✅ Consistent APIs across all types
- ✅ Better separation of concerns
- ✅ Easier to maintain and extend
- ✅ Reduced cognitive load

### Performance
- ✅ Hybrid bitmap/map approach for extents
- ✅ Lazy loading of runtime structures
- ✅ Reduced memory footprint
- ✅ Better cache locality

---

## 🚀 Next Steps

1. **Complete Task 2.1**: Implement unified Extent
2. **Start Task 2.2**: Begin Page simplification
3. **Continuous Testing**: Run tests after each change
4. **Documentation**: Update docs as we go

---

**Status**: Task 2.1 Analysis Complete ✅  
**Next**: Implement unified Extent design

