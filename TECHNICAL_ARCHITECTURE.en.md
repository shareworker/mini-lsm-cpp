# mini-lsm-cpp Technical Architecture

This is the English version of the technical architecture document.

Note: Content translation from the Chinese document (`TECHNICAL_ARCHITECTURE.zh-CN.md`) can be added progressively. For now, this file serves as the English entry point.

## Module 1: SSTable — The Physical Foundation of Data

### 1. Overview

SSTable (Sorted String Table) is the main on-disk format of an LSM-Tree. It is ordered and immutable. Understanding its internal structure, build path, and read path is critical to grasping the engine's behavior and performance.

We will examine:
* **Block / BlockBuilder** — the basic physical unit of an SSTable.
* **BlockIterator** — iterator for scanning/locating entries inside a single Block.
* **SsTableBuilder** — builds a full .sst file from an ordered KV stream.
* **SSTable file layout** — the binary format of .sst.
* **SsTable** — reading APIs and on-disk parsing.

---

### 2. Block and BlockBuilder (`include/block.hpp`)

Blocks are the smallest read/write unit in an SSTable. Splitting data into blocks lets readers load only what is necessary.

#### 2.1. Block layout

```
+----------------------------------------------------+
|                  Entry 0 (K-V Pair)                |
+----------------------------------------------------+
|                  Entry 1 (K-V Pair)                |
+----------------------------------------------------+
|                        ...                         |
+----------------------------------------------------+
|                  Entry N (K-V Pair)                |
+----------------------------------------------------+
|      Offset 0 (u16) | Offset 1 (u16) | ...         |
+----------------------------------------------------+
|                 Entry Count (u16)                  |
+----------------------------------------------------+
```

* **Entries**: KV data, prefix-compressed.
* **Offsets**: array of u16 pointing into the entry region for O(1) entry access.
* **Entry Count**: number of entries.

#### 2.2. Entry layout

```
+------------------+
| Overlap (u16)    |  <-- prefix overlap with the first key in the block
+------------------+
| Key Len (u16)    |  <-- length of the remaining key bytes
+------------------+
| Key Remainder    |
+------------------+
| Value Len (u16)  |
+------------------+
| Value            |
+------------------+
```

This leverages key ordering inside a block: only the delta to the first key is stored for subsequent keys, reducing space.

#### 2.3. BlockBuilder::Add — the core of block construction

Key points of the implementation:
1. Estimate encoded size; if adding would exceed `block_size_` and block is not empty, return false (signals caller to start a new block).
2. Append current entry start offset to `offsets_`.
3. Compute prefix `overlap` against `first_key_`, then encode overlap, remaining key, and value.
4. If this is the first entry, remember `first_key_` for future overlap calculations.

---

### 3. BlockIterator (`include/block_iterator.hpp`)

`BlockIterator` decodes block bytes and provides ordered traversal plus efficient seek.

#### 3.1. SeekToKey

Binary search over entry indices using decoded keys reconstructed via prefix information. On exit, the iterator points to the first entry >= target, or end if none.

---

### 4. SsTableBuilder (`include/sstable_builder.hpp`)

Builds a complete `.sst` by:
* Incrementally filling data blocks with `BlockBuilder`.
* On block full, `FinishBlock()` encodes the block, appends checksum, records `BlockMeta` (offset, first/last key), and resets the builder.

#### 4.1. Build — assembling the final SSTable (see `src/sstable.cpp`)
1. Final `FinishBlock()` for the last (possibly not-full) block.
2. Build Meta Block from collected `BlockMeta` entries; remember `meta_offset` and append.
3. Build Bloom Filter from all key hashes; encode, remember `bloom_offset`, and append.
4. Append fixed-size Footer: `(meta_offset, bloom_offset)`.
5. Write file and return an `SsTable` opened on it.

---

### 5. SSTable Reading Path (`sstable.hpp`, `src/sstable.cpp`)

#### 5.1. File format (full)

```
+----------------------------------------------------+
|              Data Block 1 + Checksum               |
+----------------------------------------------------+
|              Data Block 2 + Checksum               |
+----------------------------------------------------+
|                        ...                         |
+----------------------------------------------------+
|              Meta Block + Checksum                 | <-- index of blocks
+----------------------------------------------------+
|            Bloom Filter + Checksum                 |
+----------------------------------------------------+
|          Footer (meta_offset, bloom_offset)        | <-- fixed 8B
+----------------------------------------------------+
```

#### 5.2. SsTable::Open

Parse from the tail: read Footer to get `meta_offset`/`bloom_offset`, decode Bloom Filter and Meta Block, then assemble an `SsTable` object holding the file handle, metas and Bloom in memory. Data blocks stay on disk until accessed.

#### 5.3. SsTable::ReadBlockCached

Lookup a block in `BlockCache` using `(table_id << 32) | idx`. On miss, read block bytes using offsets from `metas_`, verify checksum, decode `Block`, insert into cache, and return it.

#### 5.4. BlockCache (`include/block_cache.hpp`)

Thread-safe LRU keyed by `(sst_id << 32) | block_idx`:
* `Insert(key, block)` adds to head; evict from tail when exceeding capacity.
* `Lookup(key)` moves the node to head on hit.

## Module 2: WAL and MemTable — Write Path and Recovery Guarantees

### 1. Overview

Before data reaches SSTables, it is written into memory first. `MemTable` is the in-memory sorted buffer, and `WAL` (Write-Ahead Log) guarantees durability and crash recovery.

---

### 2. MemTable and SkipList (`mem_table.hpp`, `skiplist.hpp`)

The `MemTable` is a sorted, mutable in-memory KV store. It uses a concurrent SkipList to provide average O(log n) insert/lookup with simpler concurrency than balanced trees.

#### 2.1. MemTable::Put — write into memory (with WAL first)

Key steps (see `src/mem_table.cpp`):
1. Write to WAL first (`wal_segment_->Put` or legacy `wal_->Put`). If WAL write fails, abort.
2. Compute entry size (`key.Size() + value.Size()`).
3. Insert into skiplist (`skiplist_->Insert`).
4. If success, atomically add to `approximate_size_` for flush thresholding.

---

### 3. WAL (Write-Ahead Log) (`wal.hpp`, `wal.cpp`)

Append-only log for durability and recovery.

#### 3.1. Record format

```
+-----------------+---------------+------------------+---------------+-----------------+
| Key Len (u16)   | Key Bytes     | Value Len (u16)  | Value Bytes   | Checksum (u32)  |
+-----------------+---------------+------------------+---------------+-----------------+
```

Checksum covers from Key Len to Value Bytes, used during recovery to detect torn writes.

#### 3.2. Wal::Put — append a record

Serialize key/value and lengths, compute checksum, append all to the file under a mutex. Return false on exception.

#### 3.3. Wal::Sync — ensure persistence

Flush stream buffers and, on POSIX, call `fsync(fd)` to ensure bytes hit disk media.

#### 3.4. Wal::Recover — rebuild MemTable on startup

Read the log into memory, parse records sequentially, verify checksum; stop at first corruption (likely partial tail). Reinsert valid KVs into a new SkipList, and truncate the file to the last good position.

## Module 3: LsmStorage — Orchestration and Scheduling

### 1. Overview

`LsmStorage` is the brain/coordinator of the engine. It encapsulates all internal state (MemTables, SSTables) and exposes `Put`, `Get`, `Scan`, while orchestrating background `Flush` and `Compaction`.

---

### 2. State management (`LsmStorageState`)

`LsmStorageState` holds a complete snapshot of the LSM-Tree at a point in time:

```cpp
class LsmStorageState {
public:
    std::shared_ptr<MemTable> memtable;                     // active MemTable
    std::vector<std::shared_ptr<MemTable>> imm_memtables;   // immutable MemTables
    std::vector<size_t> l0_sstables;                        // L0 table IDs
    std::vector<std::pair<size_t, std::vector<size_t>>> levels; // L1+ tables per level
    std::unordered_map<size_t, std::shared_ptr<SsTable>> sstables; // id -> SsTable
};
```

Concurrency model (copy-on-write snapshotting):
* **Reads (`Get`, `Scan`)**: take shared lock, copy a shared_ptr to current `state_` and operate on this immutable snapshot.
* **State changes (`Flush`, `Compact`)**: take unique lock, clone `state_`, mutate the clone, then atomically replace `state_` with the new snapshot.

This minimizes lock contention and gives high read concurrency.

#### 2.1. Iterator safety

Because scans hold a snapshot (`shared_ptr<LsmStorageState>`), iterators see a stable view even while background threads replace the main state. Old snapshots remain alive until all users release them, eliminating iterator invalidation.

---

### 3. Compaction Strategies

Compaction is the background process responsible for merging SSTables to clean up deleted or updated values, reduce the number of files, and maintain the structure of the LSM-Tree for efficient reads. The `CompactionController` is a pluggable component that implements the specific strategy for when and how to perform compaction.

A compaction process is defined by a `CompactionTask`. This structure specifies which SSTables to merge and into which level the results should be placed. The engine generates a task, executes the merge, and then applies the result to the LSM state. mini-lsm-cpp supports multiple compaction strategies, each with different trade-offs between read, write, and space amplification.

#### 3.1. Tiered Compaction (`tiered_compaction_controller.hpp`)

Also known as Size-Tiered Compaction (STCS). This strategy groups SSTables of similar sizes into "tiers".

*   **How it works**: When enough SSTables accumulate in a tier (controlled by `min_merge_width`), they are merged together into a single, larger SSTable in the next tier.
*   **Pros**: Low write amplification, as data is rewritten less frequently. Good for write-heavy workloads.
*   **Cons**: Higher space amplification, as multiple copies of the same key can exist across different SSTs of various sizes. Read amplification is also higher because a key lookup may need to check multiple SSTs.

#### 3.2. Leveled Compaction (`leveled_compaction_controller.hpp`)

This strategy (inspired by LevelDB) aims to control space amplification more strictly. The total size of each level `L` is capped at a target size.

*   **How it works**: When a level `L` exceeds its size limit, a single SSTable from that level is chosen and merged with all of its overlapping SSTables in the next level `L+1`.
*   **Pros**: Low space and read amplification. Data for a given key range is likely to be in a single SSTable at any given level, making reads fast.
*   **Cons**: Higher write amplification compared to tiered compaction, because merging a small SST from level `L` can cause a large amount of data in level `L+1` to be rewritten.

#### 3.3. Simple Leveled Compaction (`simple_leveled_compaction_controller.hpp`)

This is a simplified version of leveled compaction.

*   **How it works**: Compaction is triggered when L0 reaches a certain number of files. All L0 files are then merged into L1. If L0 is not full, it checks other levels based on size ratios and compacts a level `L` into `L+1` if `size(L) / size(L+1)` exceeds a threshold.
*   **Trade-offs**: Provides a balance between the tiered and leveled strategies.

---

### 5. Iterators

Iterators are fundamental components in an LSM-Tree, providing a unified way to traverse key-value pairs across different storage layers (memtables, SSTables, and levels). They abstract away the underlying storage details and present a single, sorted stream of data.

#### 5.1. Base Interface (`storage_iterator.hpp`)

All iterators in mini-lsm-cpp implement the `StorageIterator` interface, which defines the core methods for traversal:
*   `Key()`: Returns the current key.
*   `Value()`: Returns the current value.
*   `IsValid()`: Checks if the iterator is currently positioned at a valid entry.
*   `Next()`: Advances the iterator to the next entry.

#### 5.2. Basic Iterators

These iterators operate on the most granular levels of storage:
*   **`BlockIterator` (`block_iterator.hpp`)**: Iterates over key-value pairs within a single data block of an SSTable. It handles prefix compression and reconstructs the full key for each entry.
*   **`SsTableIterator` (`sstable_iterator.hpp`)**: Iterates over key-value pairs within a single SSTable. It manages reading blocks from the SSTable and uses `BlockIterator` for in-block traversal, seamlessly advancing to the next block when the current one is exhausted.

#### 5.3. Merging and Combining Iterators

These iterators combine streams from multiple underlying iterators:
*   **`MergeIterator` (`merge_iterator.hpp`)**: Merges `N` sorted `StorageIterator` instances into a single, globally sorted stream. It uses a min-heap to efficiently find the smallest key across all input iterators. When multiple iterators have the same key, it prioritizes the entry from the iterator with the lowest index.
*   **`TwoMergeIterator` (`two_merge_iterator.hpp`)**: A specialized version of `MergeIterator` designed for merging exactly two sorted `StorageIterator` instances. It's optimized for this common scenario, preferring the entry from the first iterator (`a_`) if keys are identical.
*   **`FusedIterator` (`fused_iterator.hpp`)**: An abstract base class that provides enhanced safety guarantees. Once an iterator (or its underlying wrapped iterator) becomes invalid (e.g., reaches the end of its data), it remains permanently invalid. Subsequent calls to `Key()`, `Value()`, or `Next()` on a fused iterator are guaranteed to be safe (returning empty values or performing no-op).
    *   **`SafeIteratorWrapper`**: A concrete implementation that wraps any `StorageIterator` to provide the `FusedIterator` guarantees.
    *   **`SafeBlockIterator`, `SafeMergeIterator`, `SafeTwoMergeIterator`**: Specific implementations of `FusedIterator` that wrap their respective basic iterators, adding the fused safety and internal state validation.

#### 5.4. Concatenating Iterators

*   **`SstConcatIterator` (`sst_concat_iterator.hpp`)**: Concatenates multiple `SsTableIterator` instances. This iterator is used when iterating over a sequence of SSTables (e.g., all SSTables within a single level) whose key ranges are non-overlapping and already sorted. It lazily initializes `SsTableIterator` instances as it moves from one SSTable to the next.

#### 5.5. LSM and MVCC-Specific Iterators

These iterators are tailored for the specific structure and features of the LSM-Tree and MVCC:
*   **`LsmIterator` (`lsm_iterator.hpp`)**: A generic wrapper that sits atop other `StorageIterator` instances. It enforces an iteration `Bound` (upper limit) and filters out delete tombstones (entries with empty values), presenting a clean view of the data to the user.
*   **`MvccLsmIterator` (`mvcc_lsm_iterator.hpp`)**: The top-level MVCC-aware iterator. It combines iterators from all LSM components (active memtable, immutable memtables, L0 SSTables, and leveled SSTables) and applies timestamp filtering based on the transaction's `read_ts`. It ensures that only versions visible to the current transaction are returned.
*   **`MvccMergeIterator` (`mvcc_merge_iterator.hpp`)**: An MVCC-aware version of `MergeIterator`. It merges multiple versioned iterators, applying timestamp filtering to ensure that only the newest visible version of each key (as of the `read_ts`) is returned.
*   **`MvccSsTableIterator` (`mvcc_sstable_iterator.hpp`)**: An MVCC-aware `SsTableIterator` that applies timestamp filtering during iteration over a single SSTable.
*   **`MvccTwoMergeIterator` (`mvcc_two_merge_iterator.hpp`)**: An MVCC-aware `TwoMergeIterator`. When merging two versioned iterators, if both contain the same user key, it prefers the entry with the higher timestamp (newer version) that is still visible within the `read_ts`.

---

### 6. Key flows

#### 6.1. Engine startup: `LsmStorageInner::Open`
1. Open and parse `MANIFEST`; rebuild per-level SSTable lists.
2. Open all referenced SSTables (`SsTable::Open`) and populate `sstables` map.
3. Recover WAL (`WalSegment::Recover`): verify checksums, rebuild a SkipList, and truncate any corrupted tail.
4. Create active `MemTable` from recovered SkipList.
5. Assemble initial `LsmStorageState` and publish it.

#### 6.2. Range scan: `LsmStorageInner::Scan`
1. Grab a snapshot under shared lock.
2. Build iterators:
   - MemTable iterator for active and each immutable MemTable.
   - L0: one `SsTableIterator` per table.
   - L1+: build per-level concat iterators (`SstConcatIterator`).
3. Merge all with `MergeIterator` and wrap in `LsmIterator<MergeIterator>` with upper bound handling.

## Module 4: MVCC and Transactions — Snapshot Isolation and Concurrency

### 1. Overview

To support concurrent reads and writes safely, mini-lsm-cpp implements Multi-Version Concurrency Control (MVCC). This model allows multiple transactions to run simultaneously without interfering with each other by providing each transaction with a consistent "snapshot" of the database. It is the foundation for providing both Snapshot Isolation and Serializable isolation levels.

Key goals of the MVCC implementation:
*   **Snapshot Isolation**: Reads within a transaction see a consistent view of the database as it existed at the start of the transaction.
*   **Serializable Isolation**: A stricter guarantee that transactions appear to execute serially, preventing subtle anomalies that can occur under snapshot isolation.
*   **Garbage Collection**: A mechanism to safely remove old, no-longer-visible data versions to reclaim space.

---

### 2. Core Concepts

#### 2.1. Timestamped Keys (`key_ts.hpp`)

The core of MVCC is versioning. Every key in the system is associated with a timestamp, managed by the `KeyTs` struct.

*   **Structure**: `KeyTs` wraps a user key and a `uint64_t` timestamp (`ts`).
*   **Ordering**: The ordering logic is critical. `KeyTs` objects are sorted first by the user key (ascending), and then by the timestamp in **descending** order. This clever arrangement ensures that when searching for a key, the most recent version appears first.

When a transaction reads data at a specific `read_ts`, it can seek for `(user_key, read_ts)` and the first entry it finds will be the correct version—the one with the highest timestamp that is less than or equal to `read_ts`.

#### 2.2. Transactions (`mvcc_txn.hpp`)

The `Transaction` object encapsulates the logic for a single transactional scope.

*   **Creation**: A transaction is created with a `start_ts` obtained from the central `LsmMvccInner` manager. This timestamp defines its snapshot view.
*   **Write Buffering**: All writes (`Put`, `Delete`) within a transaction are buffered in a local, in-memory `SkipMap` (`local_storage_`). They are not visible to other transactions until commit.
*   **Read Path (`Get`)**: When a key is requested, the transaction first checks its local write buffer. If not found, it reads from the main LSM-Tree, looking for the latest version of the key with a timestamp `<= start_ts`.
*   **Commit Process**:
    1.  **Conflict Check (Serializable only)**: If the transaction is `serializable`, it submits its read and write sets to `LsmMvccInner::CheckSerializableNoConflicts`. This checks if any other transaction wrote to a key that this transaction read since it started. If a conflict exists, the commit fails.
    2.  **Get Commit Timestamp**: If there are no conflicts, the transaction obtains a new `commit_ts` from `LsmMvccInner`.
    3.  **Write to LSM-Tree**: The buffered writes are written to the MemTable, with each key's timestamp set to the `commit_ts`.
    4.  **Update State**: The transaction is marked as committed.

#### 2.3. Watermark and Garbage Collection (`mvcc_watermark.hpp`)

MVCC creates multiple versions of data, so a mechanism is needed to clean up old versions.

*   **Active Readers**: The `Watermark` object tracks the `start_ts` of all currently active readers (transactions or iterators).
*   **Calculating the Watermark**: The watermark is the minimum timestamp among all active readers.
*   **GC Rule**: Any data version with a timestamp *strictly less than* the watermark is guaranteed to be invisible to all current and future transactions. Therefore, it can be safely removed during compaction. The `LsmMvccInner` component manages this process, removing old transaction records and allowing compaction to reclaim space.

---

### 3. MVCC-aware Components

The introduction of MVCC requires changes to many core components:

*   **MemTable / SSTable**: Now store `KeyTs` instead of raw keys.
*   **Iterators**: `MvccLsmIterator` and other `mvcc_*` iterators are designed to handle `KeyTs`, correctly merging data from different levels while respecting the transaction's `start_ts` and filtering out keys from uncommitted transactions.
*   **WAL**: The `mvcc_wal` logs transactional changes, ensuring that commits are atomic and recoverable.
*   **Compaction**: The compaction process is modified to use the watermark to identify and discard obsolete data versions.

## Module 5: Core Utilities

Beyond the core storage and concurrency logic, `mini-lsm-cpp` relies on several key utility components that provide foundational services like efficient memory management, file handling, and logging.

### 1. ByteBuffer (`byte_buffer.hpp`)

`ByteBuffer` is a cornerstone utility for efficient, zero-copy data handling. It is a reference-counted, immutable byte buffer.

*   **Zero-Copy Slicing**: Creating a slice of a `ByteBuffer` does not copy the underlying data. Instead, it creates a new `ByteBuffer` view over a portion of the original data, sharing the same memory buffer and updating the reference count. This is extremely efficient for parsing keys, values, and block fragments without memory allocation overhead.
*   **Thread Safety**: The reference counting is atomic, making it safe to share `ByteBuffer` instances across multiple threads.
*   **Copy-on-Write**: While `ByteBuffer` itself is immutable, it facilitates a copy-on-write pattern. If a modification is needed, a new, separate copy of the data is created.

### 2. Logger (`logger.hpp`)

A flexible, asynchronous logging framework is provided to aid in debugging and monitoring.

*   **Levels**: Supports standard logging levels (DEBUG, INFO, WARNING, ERROR).
*   **Asynchronous Mode**: By default, log messages are queued and written to disk by a dedicated background thread to minimize the impact on the critical path of storage operations.
*   **Configuration**: Can be configured to control log directory, file size, rotation, and minimum log level.

### 3. FileObject & Crc32c (`file_object.hpp`, `crc32c.hpp`)

*   **`FileObject`**: A simple RAII (Resource Acquisition Is Initialization) wrapper around a file handle. It ensures that files are properly closed when they go out of scope and provides basic read/create operations.
*   **`Crc32c`**: Provides a software implementation of the CRC32C (Castagnoli) checksum algorithm. Checksums are appended to data blocks and WAL records to verify data integrity and detect corruption during reads or recovery.

