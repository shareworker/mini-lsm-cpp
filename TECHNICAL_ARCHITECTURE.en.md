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

### 3. Key flows

#### 3.1. Engine startup: `LsmStorageInner::Open`
1. Open and parse `MANIFEST`; rebuild per-level SSTable lists.
2. Open all referenced SSTables (`SsTable::Open`) and populate `sstables` map.
3. Recover WAL (`WalSegment::Recover`): verify checksums, rebuild a SkipList, and truncate any corrupted tail.
4. Create active `MemTable` from recovered SkipList.
5. Assemble initial `LsmStorageState` and publish it.

#### 3.2. Range scan: `LsmStorageInner::Scan`
1. Grab a snapshot under shared lock.
2. Build iterators:
   - MemTable iterator for active and each immutable MemTable.
   - L0: one `SsTableIterator` per table.
   - L1+: build per-level concat iterators (`SstConcatIterator`).
3. Merge all with `MergeIterator` and wrap in `LsmIterator<MergeIterator>` with upper bound handling.
