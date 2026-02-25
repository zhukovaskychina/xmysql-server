# Task 2.3: Migration Plan - Update References to UnifiedExtent

## 📅 Date: 2025-10-29

## 🎯 Objective

Update all code that references old Extent implementations to use the new `UnifiedExtent` API.

---

## 📊 Current State Analysis

### Old Extent Implementations

| Implementation | Location | Status | Action |
|----------------|----------|--------|--------|
| `BaseExtent` | `wrapper/extent/extent.go` | ⚠️ Old | Deprecate |
| `ExtentImpl` | `wrapper/space/extent.go` | ⚠️ Old | Deprecate |
| `ExtentEntry` | `store/extents/extent.go` | ✅ Keep | Storage format |

### New Implementation

| Implementation | Location | Status |
|----------------|----------|--------|
| `UnifiedExtent` | `wrapper/extent/unified_extent.go` | ✅ New |

---

## 🔍 References Found

### 1. BaseExtent References

**File**: `server/innodb/storage/wrapper/segment/base.go`
- Line: `extents *extent3.BaseExtentList`
- Line: `extents: extent3.NewBaseExtentList()`
- Line: `ext := extent3.NewBaseExtent(...)`

**File**: `server/innodb/storage/wrapper/extent/list.go`
- Type: `BaseExtentList` (extent list implementation)
- Type: `BaseExtentIterator` (iterator implementation)
- Note: These are generic and work with `basic.Extent` interface

**Action**: 
- ✅ `BaseExtentList` and `BaseExtentIterator` are OK (use interface)
- ⚠️ Update `segment/base.go` to use `NewUnifiedExtent` instead of `NewBaseExtent`

### 2. ExtentImpl References

**File**: `server/innodb/storage/wrapper/space/extent.go`
- Type definition: `type ExtentImpl struct`
- Constructor: `func NewExtent(...) *ExtentImpl`

**File**: `server/innodb/storage/wrapper/space/ibd_space.go`
- Field: `extents map[uint32]*ExtentImpl`
- Usage: `make(map[uint32]*ExtentImpl)`

**File**: `server/innodb/storage/wrapper/page/fsp_page_wrapper.go`
- Type: `type ExtentImpl struct` (local definition!)
- Fields: `freeExtents []*ExtentImpl`
- Fields: `fullExtents []*ExtentImpl`
- Fields: `fragExtents []*ExtentImpl`

**Action**:
- ⚠️ Replace `ExtentImpl` with `UnifiedExtent` in `ibd_space.go`
- ⚠️ Replace local `ExtentImpl` in `fsp_page_wrapper.go` with `UnifiedExtent`
- ⚠️ Deprecate `space/extent.go`

### 3. ExtentEntry References (Storage Layer)

**File**: `server/innodb/storage/store/segs/segment.go`
- Fields: `FreeExtents []*extents.ExtentEntry`
- Fields: `FragExtents []*extents.ExtentEntry`
- Fields: `FullExtents []*extents.ExtentEntry`
- Methods: Various extent management methods

**Action**:
- ✅ Keep as-is (storage format)
- ✅ `UnifiedExtent` can convert to/from `ExtentEntry`

---

## 📋 Migration Strategy

### Phase 1: Update Direct References (Low Risk)

**Files to Update**:
1. `server/innodb/storage/wrapper/segment/base.go`
2. `server/innodb/storage/wrapper/space/ibd_space.go`

**Changes**:
- Replace `NewBaseExtent` with `NewUnifiedExtent`
- Replace `*ExtentImpl` with `*UnifiedExtent`
- Update imports

**Risk**: Low (simple type replacement)

### Phase 2: Refactor Local ExtentImpl (Medium Risk)

**File**: `server/innodb/storage/wrapper/page/fsp_page_wrapper.go`

**Problem**: Has its own local `ExtentImpl` definition (duplicate!)

**Changes**:
- Remove local `ExtentImpl` type
- Use `extent.UnifiedExtent` instead
- Update all methods that use `ExtentImpl`

**Risk**: Medium (need to verify API compatibility)

### Phase 3: Deprecate Old Implementations (Low Risk)

**Files to Deprecate**:
1. `server/innodb/storage/wrapper/extent/extent.go` (BaseExtent)
2. `server/innodb/storage/wrapper/space/extent.go` (ExtentImpl)

**Changes**:
- Add deprecation comments
- Keep files for backward compatibility (temporary)
- Plan removal in future sprint

**Risk**: Low (no code changes)

### Phase 4: Testing & Validation (Critical)

**Tests to Run**:
1. Extent unit tests
2. Segment tests
3. Space tests
4. Integration tests

**Validation**:
- All tests pass
- No compilation errors
- No runtime errors

---

## 🔧 Detailed Implementation Plan

### Step 1: Update segment/base.go

**Current Code**:
```go
import extent3 "github.com/.../wrapper/extent"

type BaseSegment struct {
    extents *extent3.BaseExtentList
}

func NewBaseSegment() *BaseSegment {
    return &BaseSegment{
        extents: extent3.NewBaseExtentList(),
    }
}

func (bs *BaseSegment) AllocateExtent() {
    ext := extent3.NewBaseExtent(spaceID, extentID, extType)
    // ...
}
```

**New Code**:
```go
import extent3 "github.com/.../wrapper/extent"

type BaseSegment struct {
    extents *extent3.BaseExtentList  // OK - uses interface
}

func NewBaseSegment() *BaseSegment {
    return &BaseSegment{
        extents: extent3.NewBaseExtentList(),  // OK
    }
}

func (bs *BaseSegment) AllocateExtent() {
    ext := extent3.NewUnifiedExtent(spaceID, extentID, extType, 0)
    // ...
}
```

**Changes**: Replace `NewBaseExtent` → `NewUnifiedExtent`

### Step 2: Update space/ibd_space.go

**Current Code**:
```go
import "github.com/.../wrapper/space"

type IBDSpace struct {
    extents map[uint32]*space.ExtentImpl
}

func NewIBDSpace() *IBDSpace {
    return &IBDSpace{
        extents: make(map[uint32]*space.ExtentImpl),
    }
}
```

**New Code**:
```go
import "github.com/.../wrapper/extent"

type IBDSpace struct {
    extents map[uint32]*extent.UnifiedExtent
}

func NewIBDSpace() *IBDSpace {
    return &IBDSpace{
        extents: make(map[uint32]*extent.UnifiedExtent),
    }
}
```

**Changes**: 
- Replace `*space.ExtentImpl` → `*extent.UnifiedExtent`
- Update import

### Step 3: Refactor page/fsp_page_wrapper.go

**Current Code** (has local ExtentImpl!):
```go
type ExtentImpl struct {
    id        uint32
    state     basic.ExtentState
    pages     map[uint32]bool
}

type FSPPageWrapper struct {
    freeExtents []*ExtentImpl
    fullExtents []*ExtentImpl
    fragExtents []*ExtentImpl
}

func (p *FSPPageWrapper) AllocateExtent() *ExtentImpl {
    ext := &ExtentImpl{
        id:    nextID,
        state: basic.ExtentStateFree,
        pages: make(map[uint32]bool),
    }
    return ext
}
```

**New Code**:
```go
import "github.com/.../wrapper/extent"

// Remove local ExtentImpl definition

type FSPPageWrapper struct {
    freeExtents []*extent.UnifiedExtent
    fullExtents []*extent.UnifiedExtent
    fragExtents []*extent.UnifiedExtent
}

func (p *FSPPageWrapper) AllocateExtent() *extent.UnifiedExtent {
    ext := extent.NewUnifiedExtent(spaceID, nextID, extType, 0)
    return ext
}
```

**Changes**:
- Remove local `ExtentImpl` type
- Replace with `extent.UnifiedExtent`
- Update all methods

### Step 4: Add Deprecation Notices

**File**: `wrapper/extent/extent.go`
```go
// Deprecated: BaseExtent is deprecated. Use UnifiedExtent instead.
// This type will be removed in a future version.
type BaseExtent struct {
    // ...
}

// Deprecated: NewBaseExtent is deprecated. Use NewUnifiedExtent instead.
func NewBaseExtent(...) *BaseExtent {
    // ...
}
```

**File**: `wrapper/space/extent.go`
```go
// Deprecated: ExtentImpl is deprecated. Use extent.UnifiedExtent instead.
// This type will be removed in a future version.
type ExtentImpl struct {
    // ...
}

// Deprecated: NewExtent is deprecated. Use extent.NewUnifiedExtent instead.
func NewExtent(...) *ExtentImpl {
    // ...
}
```

---

## ✅ Acceptance Criteria

| Criterion | Status | Verification |
|-----------|--------|--------------|
| All `NewBaseExtent` calls replaced | ⏳ | Code search |
| All `*ExtentImpl` references updated | ⏳ | Code search |
| Local `ExtentImpl` in fsp_page_wrapper removed | ⏳ | File inspection |
| Deprecation notices added | ⏳ | File inspection |
| All tests pass | ⏳ | `go test ./...` |
| No compilation errors | ⏳ | `go build ./...` |
| No runtime errors | ⏳ | Integration tests |

---

## 📈 Expected Impact

### Code Changes

| File | Lines Changed | Risk Level |
|------|---------------|------------|
| `segment/base.go` | ~5 lines | Low |
| `space/ibd_space.go` | ~10 lines | Low |
| `page/fsp_page_wrapper.go` | ~50 lines | Medium |
| `extent/extent.go` | +10 lines (comments) | Low |
| `space/extent.go` | +10 lines (comments) | Low |
| **Total** | **~85 lines** | **Low-Medium** |

### Benefits

- ✅ Unified extent implementation
- ✅ Reduced code duplication
- ✅ Consistent API
- ✅ Better maintainability
- ✅ Improved performance (hybrid bitmap/map)

---

## 🚀 Execution Order

1. ✅ **Step 1**: Update `segment/base.go` (5 min)
2. ✅ **Step 2**: Update `space/ibd_space.go` (10 min)
3. ⏳ **Step 3**: Refactor `page/fsp_page_wrapper.go` (30 min)
4. ⏳ **Step 4**: Add deprecation notices (10 min)
5. ⏳ **Step 5**: Run tests (10 min)
6. ⏳ **Step 6**: Fix any issues (variable)
7. ⏳ **Step 7**: Final validation (10 min)

**Total Estimated Time**: 1.5 - 2 hours

---

**Status**: Ready to execute  
**Next**: Begin Step 1

