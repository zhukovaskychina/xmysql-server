# Storage Engine Architecture

This document outlines the architecture of the XMySQL storage engine, focusing on the B+ tree and page management components.

## Overview

The storage engine is organized into several key components:

1. **Page Management** - Handles reading/writing pages to disk and caching
2. **B+ Tree** - Implements the B+ tree data structure for indexing
3. **Storage** - Top-level component that coordinates between different parts

## Directory Structure

```
wrapper/store/
├── btree/           # B+ tree implementation
├── page/            # Page management
└── types/           # Type definitions and interfaces
    ├── btree/       # B+ tree specific types
    ├── common/      # Common types and constants
    ├── errors/      # Error definitions
    ├── interfaces/  # Core interfaces
    └── page/       # Page specific types
```

## Core Interfaces

### Page Management

- `IPageWrapper` - Interface for page operations
- `IIndexPage` - Interface for index pages
- `IPageManager` - Manages page allocation/deallocation

### B+ Tree

- `IBTree` - Core B+ tree operations
- `IBTreeManager` - Manages multiple B+ trees

## Usage Example

```go
// Initialize storage
storage := store.NewStorage()
defer storage.Close()

// Get managers
pageMgr := storage.GetPageManager()
btreeMgr := storage.GetBTreeManager()

// Create a new B+ tree
tree, err := btreeMgr.CreateTree(1, 1) // spaceID=1, indexID=1
if err != nil {
    // handle error
}

// Insert data
if err := tree.Insert([]byte("key"), []byte("value")); err != nil {
    // handle error
}

// Retrieve data
value, err := tree.Find([]byte("key"))
if err != nil {
    // handle error
}
```

## Error Handling

All errors are defined in the `errors` package. Common errors include:

- `ErrPageNotFound`
- `ErrTreeNotFound`
- `ErrKeyNotFound`
- `ErrKeyExists`

## Concurrency

- Page operations are thread-safe
- B+ tree operations are thread-safe at the tree level
- The storage engine uses fine-grained locking for better concurrency

## Performance Considerations

- Pages are cached in memory using an LRU cache
- Dirty pages are written to disk asynchronously
- Batch operations are supported for better performance

## Testing

Run the tests with:

```bash
go test_simple_protocol -v ./...
```

## Future Improvements

- Implement compression for pages
- Add support for transactions
- Improve concurrency with lock-free algorithms
- Add metrics collection
