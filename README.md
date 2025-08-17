# Mini-LSM C++

A high-performance Log-Structured Merge-tree (LSM) storage engine implementation in modern C++20.

## Features

- **LSM Storage Engine**: Efficient key-value storage with memtables, SSTables, and compaction
- **MVCC Support**: Multi-Version Concurrency Control for transaction isolation
- **Write-Ahead Logging**: Crash recovery with WAL segments
- **Multiple Compaction Strategies**: Simple leveled, tiered, and leveled compaction
- **Bloom Filters**: Optimized SSTable lookup with configurable false positive rates
- **Iterator Interface**: Range scans and point lookups with various iterator types
- **Thread Safety**: Concurrent operations with proper synchronization

## Architecture

### Core Components

- **MemTable**: In-memory sorted data structure using skip lists
- **SSTable**: Sorted String Table files with block-based storage
- **WAL**: Write-Ahead Log for durability and crash recovery
- **Manifest**: Metadata management for LSM tree structure
- **Compaction**: Background compaction with multiple strategies
- **MVCC**: Transaction support with snapshot isolation

### Storage Layout

```
/path/to/storage/
├── MANIFEST                 # LSM metadata
├── wal/                     # Write-ahead log segments
│   ├── wal-lsm-000000.wal
│   └── ...
├── 1.sst                    # SSTable files
├── 2.sst
└── ...
```

## Building

### Prerequisites

- C++20 compatible compiler (GCC 10+, Clang 12+)
- XMake build system
- Google Test (automatically installed)

### Build Commands

```bash
# Configure and build
xmake

# Run tests
xmake test

# Build specific target
xmake build lsm_recovery_test

# Clean build
xmake clean
```

## Testing

The project includes comprehensive test suites:

- **LSM Recovery Test**: WAL recovery and manifest replay
- **MVCC Tests**: Transaction isolation and concurrency control
- **Iterator Safety Tests**: Iterator correctness and safety
- **Compaction Tests**: Background compaction strategies
- **Comprehensive Test**: End-to-end LSM functionality

```bash
# Run all tests
xmake test

# Run specific test
xmake run mvcc_test
```

## Usage

### Basic Usage

```cpp
#include "mini_lsm.hpp"
#include "byte_buffer.hpp"

// Create storage with options
LsmStorageOptions options;
options.enable_wal = true;
options.target_sst_size = 2 << 20;  // 2MB

auto storage = MiniLsm::Open("/path/to/storage", options);

// Put and get operations
ByteBuffer key("my_key");
ByteBuffer value("my_value");
storage->Put(key, value);

auto result = storage->Get(key);
if (result.has_value()) {
    std::cout << "Value: " << result->ToString() << std::endl;
}
```

### MVCC Transactions

```cpp
#include "mini_lsm_mvcc.hpp"

auto mvcc_storage = MiniLsmMvcc::Open("/path/to/storage", options);

// Begin transaction
auto txn = mvcc_storage->NewTransaction();

// Transactional operations
txn->Put(ByteBuffer("key1"), ByteBuffer("value1"));
txn->Put(ByteBuffer("key2"), ByteBuffer("value2"));

// Commit transaction
bool committed = txn->Commit();
```

## Performance

The implementation is optimized for:

- **High Write Throughput**: LSM tree structure optimizes for sequential writes
- **Efficient Compaction**: Multiple compaction strategies balance read/write performance
- **Memory Efficiency**: Reference-counted byte buffers minimize copying
- **Concurrent Access**: Lock-free read paths and fine-grained locking

## License

This project is part of a learning implementation of LSM trees and MVCC systems.
