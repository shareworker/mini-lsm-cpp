# `mini-lsm-cpp` 技术架构文档 (终极源码剖析版)

## 模块一：`SSTable` - 数据的物理基石

### 1. 概述

`SSTable` (Sorted String Table) 是 LSM-Tree 在磁盘上存储数据的主要形式。它是一个**有序且不可变**的文件。理解 `SSTable` 的内部结构、构建过程和读取方式，是理解整个存储引擎性能和行为的关键。本章将深入到 `SSTable` 的每一个实现细节。

我们将剖析以下几个核心组件：
*   **`Block` / `BlockBuilder`**: `SSTable` 的基本组成单元。
*   **`BlockIterator`**: 在单个 `Block` 内部进行遍历和查找的迭代器。
*   **`SsTableBuilder`**: 负责将内存中的有序数据流构建成一个完整的 `.sst` 文件。
*   **`SSTable` 文件格式**: 详解 `.sst` 文件的二进制布局。
*   **`SsTable`**: 负责解析 `.sst` 文件，并提供对外的读取接口。

---

### 2. `Block` 与 `BlockBuilder` (`block.hpp`)

`Block` 是 `SSTable` 中最基本的数据读写单元。一个 `SSTable` 由多个 `Block` 组成。将数据分块，使得读取时可以只加载必要的 `Block` 到内存，而不是整个文件。

#### 2.1. `Block` 的物理布局

一个 `Block` 内部存储了多个有序的键值对。其二进制布局如下：

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

*   **Entries**: 键值对数据本身。为了节省空间，`Entry` 采用了前缀压缩编码。
*   **Offsets**: 一个 `uint16_t` 数组，存储了每个 `Entry` 在 `Block` 数据区内的起始偏移量。通过这个数组可以快速定位到任何一个 `Entry`。
*   **Entry Count**: `Block` 中 `Entry` 的总数。

#### 2.2. `Entry` 的物理布局

```
+------------------+
| Overlap (u16)    |  <-- 与 Block 内第一个 Key 的重叠长度
+------------------+
| Key Len (u16)    |  <-- Key 剩余部分的长度
+------------------+
| Key Remainder    |  <-- Key 剩余部分的字节
+------------------+
| Value Len (u16)  |  <-- Value 的长度
+------------------+
| Value            |  <-- Value 的字节
+------------------+
```

这种设计（特别是 `Overlap`）利用了 `Block` 内部 Key 的有序性。除了第一个 Key 是完整存储的，后续的 Key 都只存储与第一个 Key 的差异部分，大大减少了存储空间。

#### 2.3. `BlockBuilder::Add` - `Block` 的构建核心

**功能说明**: `BlockBuilder` 负责增量地构建一个 `Block`。`Add` 方法是其核心，负责添加一个键值对并处理前缀压缩。

**源码片段** (`include/block.hpp`):
```cpp
bool BlockBuilder::Add(const ByteBuffer& key, const std::vector<uint8_t>& value) {
    assert(!key.Empty());
    // 1. 预估大小，判断是否会超出 Block 限制
    size_t extra = key.Size() + value.size() + 3 * kSizeOfU16; // key_len, val_len, offset + overlap u16
    if (EstimatedSize() + extra > block_size_ && !IsEmpty()) {
        return false;
    }
    // 2. 记录当前 Entry 的偏移量
    offsets_.push_back(static_cast<uint16_t>(data_.size()));

    // 3. 计算前缀压缩的重叠长度
    size_t overlap = ComputeOverlap(first_key_, key);
    
    // 4. 编码并写入 Entry 数据
    PutU16(static_cast<uint16_t>(overlap));
    PutU16(static_cast<uint16_t>(key.Size() - overlap));
    data_.insert(data_.end(), key.Data() + overlap, key.Data() + key.Size());
    PutU16(static_cast<uint16_t>(value.size()));
    data_.insert(data_.end(), value.begin(), value.end());

    // 5. 如果是第一个 Key，则保存它用于后续的 overlap 计算
    if (first_key_.Empty()) {
        first_key_ = key;
    }
    return true;
}
```

**逐行解读**:
1.  **预估大小**: 在添加前，先计算该键值对编码后的大致大小，如果加入后会超过 `block_size_` 限制，并且当前 `Block` 不为空，则拒绝添加（返回 `false`）。这通知上层调用者（`SsTableBuilder`）当前 `Block` 已经满了。
2.  **记录偏移量**: 将当前数据缓冲区的长度（`data_.size()`）作为新 `Entry` 的起始偏移量，存入 `offsets_` 数组。
3.  **计算重叠**: `ComputeOverlap` 计算当前 `key` 与 `Block` 的第一个 `key` (`first_key_`) 之间的公共前缀长度。
4.  **编码 Entry**: 按照 `Entry` 的物理布局，依次将 `overlap`、`key` 的剩余长度、`key` 的剩余部分、`value` 的长度和 `value` 的内容追加到 `data_` 字节缓冲区中。
5.  **保存首个 Key**: 如果这是 `Block` 的第一个键值对，将其 `key` 保存到 `first_key_` 成员中，作为后续所有 `key` 进行前缀压缩的基准。

---

### 3. `BlockIterator` (`block_iterator.hpp`)

**功能说明**: `BlockIterator` 负责解析一个 `Block` 的二进制数据，并提供按键顺序遍历其中所有键值对的能力。

#### 3.1. `BlockIterator::SeekToKey` - 在 `Block` 内查找

**功能说明**: 在 `Block` 内部高效地定位到第一个大于或等于 `target` Key 的位置。

**源码片段** (`include/block_iterator.hpp`):
```cpp
void SeekToKey(const ByteBuffer &target) noexcept {
    size_t low = 0;
    size_t high = block_->Offsets().size();
    while (low < high) {
        size_t mid = low + (high - low) / 2;
        // 1. 定位到中间位置的 Entry 并解码出 Key
        SeekTo(mid);
        if (!IsValid()) break;
        // 2. 二分查找比较
        if (Key() < target) {
            low = mid + 1;
        } else if (Key() > target) {
            high = mid;
        } else {
            return; // 精确匹配
        }
    }
    // 3. 定位到最终位置
    SeekTo(low);
}
```

**逐行解读**:
1.  **`SeekTo(mid)`**: 这个辅助函数会根据 `mid` 索引，从 `offsets_` 数组中找到对应 `Entry` 的偏移量，然后跳转到该偏移量，解码出 `Entry` 的 `Key` 和 `Value` 的位置，并将解码出的 `Key` 存入 `key_` 成员变量。解码 `Key` 的过程需要利用 `first_key_` 和 `Entry` 中存储的 `overlap` 来完整地重构出原始 `Key`。
2.  **二分查找**: 这是一个标准的二分查找算法。它利用 `Block` 内 `Key` 的有序性，通过不断与中间位置的 `Key` 比较，来缩小查找范围。
3.  **最终定位**: 循环结束后，`low` 指向的就是第一个不小于 `target` 的 `Entry` 的索引，调用 `SeekTo(low)` 将迭代器定位到这个最终位置。

---

### 4. `SsTableBuilder` (`sstable_builder.hpp`)

**功能说明**: `SsTableBuilder` 是构建 `SSTable` 的总指挥。它接收一系列有序的键值对，在内部使用 `BlockBuilder` 构建一个个 `Data Block`，最后将所有部分组装成一个完整的 `.sst` 文件。

#### 4.1. `SsTableBuilder::Add` 与 `FinishBlock`

`Add` 方法是 `SsTableBuilder` 的入口，它直接调用内部 `BlockBuilder` 的 `Add`。当 `BlockBuilder::Add` 返回 `false`（表示当前 `Block` 已满）时，`SsTableBuilder::Add` 会调用 `FinishBlock`。

**源码片段** (`include/sstable_builder.hpp`):
```cpp
void SsTableBuilder::FinishBlock() {
    // 1. 从 BlockBuilder 中取出构建好的 Block
    Block block = builder_.Build();
    std::vector<uint8_t> encoded = block.Encode();
    
    // 2. 记录这个 Block 的元信息 (Meta)
    meta_.push_back(BlockMeta{
        /*offset=*/data_.size(),
        /*first_key=*/first_key_.CopyToBytes(),
        /*last_key=*/last_key_.CopyToBytes()});
        
    // 3. 计算校验和并追加数据
    uint32_t checksum = Crc32(encoded);
    data_.insert(data_.end(), encoded.begin(), encoded.end());
    PutU32(checksum);
    
    // 4. 重置 BlockBuilder 以准备下一个 Block
    builder_ = BlockBuilder(block_size_);
    first_key_.Clear();
    last_key_.Clear();
}
```

**逐行解读**:
1.  **`builder_.Build()`**: 调用内部 `BlockBuilder` 的 `Build` 方法，获取最终编码好的 `Block` 对象。
2.  **记录元信息**: 创建一个 `BlockMeta` 结构体，记录下当前 `Block` 在整个 `SSTable` 文件中的起始偏移量（`data_.size()`），以及它的第一个和最后一个 `Key`。这个 `BlockMeta` 被存入 `meta_` 列表中，这个列表最终会成为 `SSTable` 的索引（`Meta Block`）。
3.  **追加数据**: 将编码后的 `Block` 数据和其 CRC32C 校验和追加到 `SsTableBuilder` 的主数据缓冲区 `data_` 中。
4.  **重置**: 创建一个新的 `BlockBuilder` 实例，为下一个 `Block` 的构建做准备。

#### 4.2. `SsTableBuilder::Build` - 组装最终的 `SSTable`

**功能说明**: 当所有键值对都 `Add` 完毕后，`Build` 方法负责完成最后的组装和文件写入工作。

**核心逻辑步骤** (实现位于 `src/sstable.cpp`):
1.  **`FinishBlock()`**: 首先调用一次 `FinishBlock()`，将最后一个（可能未满的）`Block` 进行处理并追加到 `data_`。
2.  **构建 `Meta Block`**: 
    *   调用 `BlockMeta::EncodeBlockMeta(meta_, ...)` 将 `meta_` 列表（所有 `Block` 的索引）序列化成字节流。
    *   记录下 `Meta Block` 在文件中的偏移量 `meta_offset`。
    *   将序列化后的 `Meta Block` 追加到 `data_`。
3.  **构建 `Bloom Filter`**:
    *   使用之前收集的所有 `key_hashes_`，调用 `BloomFilter::BuildFromKeyHashes` 构建一个 `BloomFilter` 对象。
    *   调用 `bloom.Encode(...)` 将其序列化。
    *   记录下 `Bloom Filter` 在文件中的偏移量 `bloom_offset`。
    *   将序列化后的 `Bloom Filter` 追加到 `data_`。
4.  **构建 `Footer`**:
    *   `Footer` 是一个定长的部分，包含 `meta_offset` (4字节) 和 `bloom_offset` (4字节)。
    *   将 `Footer` 追加到 `data_` 的末尾。
5.  **写入文件**: 调用 `FileObject::Create(file_path, data_)`，将 `data_` 缓冲区中的所有内容一次性写入到指定的 `file_path`，创建一个新的 `.sst` 文件。
6.  **返回 `SsTable` 对象**: 调用 `SsTable::Open`，用刚刚创建的文件和已有的元数据（如 `BlockCache`）构建一个可供读取的 `SsTable` 对象并返回。

---

### 5. `SSTable` 文件与读取 (`sstable.hpp`, `sstable.cpp`)

#### 5.1. `SSTable` 完整文件格式

```
+----------------------------------------------------+
|              Data Block 1 + Checksum               |
+----------------------------------------------------+
|              Data Block 2 + Checksum               |
+----------------------------------------------------+
|                        ...                         |
+----------------------------------------------------+
|              Meta Block + Checksum                 | <-- Block 索引
+----------------------------------------------------+
|            Bloom Filter + Checksum                 |
+----------------------------------------------------+
|          Footer (meta_offset, bloom_offset)        | <-- 定长入口 (8字节)
+----------------------------------------------------+
```

#### 5.2. `SsTable::Open` - 解析 `SSTable` 文件

**功能说明**: `Open` 是 `SSTable` 读取路径的入口。它负责解析一个 `.sst` 文件，将必要的元数据加载到内存中。

**源码片段** (`src/sstable.cpp`):
```cpp
std::shared_ptr<SsTable> SsTable::Open(size_t id,
                                       std::shared_ptr<BlockCache> block_cache,
                                       FileObject file) {
    uint64_t len = file.Size();
    // 1. 读取文件末尾的 Footer (8字节)
    std::vector<uint8_t> footer = file.Read(len - 8, 8);
    uint32_t meta_offset = ...; // 从 footer 解码
    uint32_t bloom_offset = ...; // 从 footer 解码

    // 2. 根据 bloom_offset 读取并解码 Bloom Filter
    std::vector<uint8_t> bloom_buf = file.Read(bloom_offset, len - 8 - bloom_offset);
    BloomFilter bloom = BloomFilter::Decode(bloom_buf);

    // 3. 根据 meta_offset 读取并解码 Meta Block (索引)
    std::vector<uint8_t> meta_buf = file.Read(meta_offset, bloom_offset - meta_offset);
    std::vector<BlockMeta> metas = BlockMeta::DecodeBlockMeta(meta_buf.data(), meta_buf.size());

    // 4. 构建 SsTable 对象
    auto table = std::shared_ptr<SsTable>(new SsTable());
    table->metas_ = std::move(metas); // 索引存入内存
    table->bloom_ = std::make_shared<BloomFilter>(std::move(bloom)); // Bloom Filter 存入内存
    table->file_ = std::move(file); // 保存文件句柄
    // ... 其他字段 ...
    return table;
}
```

**逐行解读**:
1.  **读取 `Footer`**: `SSTable` 的解析是从文件尾部开始的。程序首先读取定长的 `Footer`，从中解码出 `Meta Block` 和 `Bloom Filter` 的确切位置。
2.  **读取 `Bloom Filter`**: 根据 `Footer` 提供的位置信息，从文件中读取 `Bloom Filter` 的数据，并反序列化成一个 `BloomFilter` 对象，保存在内存中。
3.  **读取 `Meta Block`**: 同样，根据 `Footer` 的信息，读取 `Meta Block` 的数据，并反序列化成一个 `BlockMeta` 的向量，即 `Data Block` 的完整索引，保存在内存中。
4.  **构建对象**: 将文件句柄、`Block` 缓存、`Bloom Filter` 和 `Block` 索引等组装成一个 `SsTable` 对象。此时，真正的 `Data Block` 数据**仍在磁盘上**，并未加载。

#### 5.3. `SsTable::ReadBlockCached` - `Block` 的懒加载与缓存

**功能说明**: 当查询需要访问某个具体的 `Data Block` 时，此函数负责将其从磁盘加载到内存，并利用 `BlockCache` 进行缓存。

**源码片段** (`src/sstable.cpp`):
```cpp
SsTable::BlockPtr SsTable::ReadBlockCached(size_t idx) const noexcept {
    // 1. 检查 BlockCache
    if (block_cache_) {
        size_t cache_key = (id_ << 32) | idx;
        auto cached = block_cache_->Lookup(cache_key);
        if (cached) {
            return cached;
        }
    }

    // 2. 从文件中读取 Block 数据
    size_t offset = metas_[idx].offset;
    size_t offset_end = (idx + 1 < metas_.size()) ? metas_[idx + 1].offset : block_meta_offset_;
    std::vector<uint8_t> data = file_.Read(offset, offset_end - offset);
    
    // 3. 校验和验证
    // ... 提取数据和 checksum，进行比较 ...
    
    // 4. 解码成 Block 对象
    auto blk = std::make_shared<Block>(Block::Decode(block_data.data(), block_data.size()));
    
    // 5. 存入缓存
    if (block_cache_) {
        size_t cache_key = (id_ << 32) | idx;
        block_cache_->Insert(cache_key, blk);
    }
    return blk;
}
```

**逐行解读**:
1.  **查缓存**: 首先根据 `SSTable` 的 ID 和 `Block` 的索引 `idx` 生成一个唯一的 `cache_key`，去 `BlockCache` 中查找。如果命中，直接返回内存中的 `Block` 指针，避免磁盘 I/O。
2.  **读文件**: 如果缓存未命中，则从内存中的 `metas_` 索引获取该 `Block` 在文件中的起止偏移量，然后调用 `file_.Read` 从磁盘读取完整的 `Block` 数据（包括其校验和）。
3.  **校验和**: 对读取到的 `Block` 数据计算校验和，并与存储的校验和进行比较，确保数据在传输和存储过程中没有损坏。
4.  **解码**: 调用 `Block::Decode` 将原始字节流反序列化成一个 `Block` 对象。
5.  **存入缓存**: 将新创建的 `Block` 对象存入 `BlockCache`，以便下次同样的请求可以直接从缓存中获取。

#### 5.4. `BlockCache` - 加速块读取 (`block_cache.hpp`)

当 `LSM-Tree` 层级较深时，一次点查可能需要访问多个 `SSTable` 文件。即使有 `Bloom Filter` 帮助跳过不含目标 `Key` 的文件，`Meta Block` 索引也只能将查询定位到特定的 `Data Block`，最终仍然免不了从磁盘读取 `Data Block` 的 I/O 操作。`BlockCache` 的目的就是为了缓存这些从磁盘读取的 `Data Block`，用内存加速后续的重复访问。

##### a. 核心接口

`BlockCache` 提供了一个简单的、线程安全的键值缓存接口。

**源码片段** (`include/block_cache.hpp`):
```cpp
class BlockCache {
public:
    // 构造函数，指定缓存的最大容量（以字节为单位）
    explicit BlockCache(size_t capacity);

    // 向缓存中插入一个 Block
    void Insert(size_t key, std::shared_ptr<Block> block);

    // 从缓存中查找一个 Block
    std::shared_ptr<Block> Lookup(size_t key);

    // 从缓存中移除一个 Block
    void Erase(size_t key);
};
```
*   **`key`**: 缓存的 `key` 通常由 `SSTable` 的 ID 和 `Block` 在 `SSTable` 内的索引 `idx` 组合而成，以保证其全局唯一性。例如 `(sst_id << 32) | block_idx`。
*   **`value`**: 缓存的 `value` 是一个指向 `Block` 对象的共享指针 `std::shared_ptr<Block>`。

##### b. 实现原理：LRU (Least Recently Used)

`mini-lsm-cpp` 中的 `BlockCache` 采用经典的 **LRU (最近最少使用)** 淘汰策略。其内部实现依赖于两个核心数据结构：

1.  **`std::unordered_map`**: 一个哈希表，用于存储 `key` 到 `Block` 数据及其在链表中位置的映射。这保证了 `Lookup` 操作的平均时间复杂度为 O(1)。
2.  **`std::list`**: 一个双向链表，用于维护所有 `Block` 的“新鲜度”顺序。

**工作流程**:
*   **`Insert(key, block)`**:
    1.  当一个新的 `Block` 被插入时，它会被添加到哈希表和双向链表的**头部**。
    2.  如果此时缓存的总大小超过了设定的 `capacity`，则会从链表的**尾部**移除节点（即最久未被使用的 `Block`），并从哈希表中同步删除对应的条目，直到总大小符合限制。
*   **`Lookup(key)`**:
    1.  通过哈希表快速定位到 `Block`。
    2.  如果命中，会将该 `Block` 对应的节点从其在链表中的当前位置移动到链表的**头部**，表示它刚刚被访问过。
    3.  如果未命中，则返回空指针。

##### c. 与 `SSTable` 的集成

`BlockCache` 被无缝集成在 `SsTable::ReadBlockCached` 方法中，形成一个完整的“缓存代理”模式。

**`SsTable::ReadBlockCached` 逻辑回顾**:
1.  **查缓存**: `ReadBlockCached` 首先调用 `block_cache_->Lookup(cache_key)`。如果命中，直接返回内存中的 `Block` 指针，避免磁盘 I/O。
2.  **缓存未命中**:
    *   从磁盘读取原始的 `Block` 字节流。
    *   校验并解码成一个 `Block` 对象。
    *   调用 `block_cache_->Insert(cache_key, new_block)`，将这个新从磁盘加载的 `Block` 存入缓存，以备后续使用。
    *   返回这个新的 `Block` 对象。

通过这种方式，`BlockCache` 对上层调用者是完全透明的，但极大地提升了对热点数据的查询性能。

## 模块二：`WAL` 与 `MemTable` - 写入与恢复的保障

### 1. 概述

在数据写入 `SSTable` 之前，它首先被写入内存。`MemTable` 就是这个内存中的高速缓冲区，而 `WAL` (Write-Ahead Log) 则是保证 `MemTable` 中数据在系统崩溃后不丢失的关键机制。这两者共同确保了写入操作的高性能和持久性。

---

### 2. `MemTable` 与 `SkipList` (`mem_table.hpp`, `skiplist.hpp`)

**核心职责**: `MemTable` 提供了一个有序的、在内存中可变的键值存储。所有新的写入请求都首先在这里汇集。

为了在支持高并发读写的同时保持键的有序性，`MemTable` 的底层核心数据结构是 `SkipList`（跳表）。跳表是一种概率性数据结构，它通过在不同层级建立“捷径”来实现平均 `O(log n)` 的查找、插入和删除性能，其并发控制的实现比平衡树更简单高效。

#### 2.1. `MemTable::Put` - 写入内存

**功能说明**: 这是向 `MemTable` 写入数据的核心方法。它必须确保在更新内存结构之前，操作已被记录到 `WAL` 中。

**源码片段** (`src/mem_table.cpp`):
```cpp
bool MemTable::Put(const ByteBuffer& key, const ByteBuffer& value) {
    // 1. Write to WAL first (if enabled) for durability
    if (wal_segment_) {
        if (!wal_segment_->Put(key, value)) {
            return false;
        }
    } else if (wal_) {
        // Fall back to legacy WAL
        if (!wal_->Put(key, value)) {
            return false;
        }
    }
    
    // 2. Calculate size of key and value
    size_t entry_size = key.Size() + value.Size();
    
    // 3. Insert into skiplist
    bool result = skiplist_->Insert(key, value);
    
    // 4. Update approximate size if insertion was successful
    if (result) {
        approximate_size_.fetch_add(entry_size, std::memory_order_relaxed);
    }
    
    return result;
}
```

**逐行解读**:
1.  **写入 WAL**: 这是最关键的一步。在对任何内存状态进行修改前，必须先将该操作（`Put(key, value)`）写入到 `WAL` 文件中。`wal_segment_->Put` 会将这个操作序列化并追加到日志文件里。只有当 `WAL` 写入成功后，才能继续下一步。这保证了即使在 `skiplist_->Insert` 执行前系统崩溃，重启时也能从 `WAL` 中恢复这个操作。
2.  **计算大小**: 计算该键值对的大小，用于后续更新 `MemTable` 的内存占用统计。
3.  **插入跳表**: 调用 `skiplist_->Insert`，将键值对插入到内存中的跳表里。这是一个线程安全的操作。
4.  **更新大小**: 原子地增加 `approximate_size_` 的值，用于上层 `LsmStorage` 判断 `MemTable` 是否已满。

---

### 3. `WAL` (Write-Ahead Log) (`wal.hpp`, `wal.cpp`)

**核心职责**: `WAL` 提供了一个只追加（Append-Only）的日志文件，用于在系统崩溃时恢复 `MemTable` 的内容。

#### 3.1. `WAL` 记录格式

`WAL` 文件由一条条的记录序列组成。每条记录的格式如下：

```
+-----------------+-----------------+------------------+-----------------+-----------------+
| Key Len (u16)   | Key Bytes       | Value Len (u16)  | Value Bytes     | Checksum (u32)  |
+-----------------+-----------------+------------------+-----------------+-----------------+
```
*   **Checksum**: 覆盖了从 `Key Len` 到 `Value Bytes` 的所有内容，用于在恢复时校验记录的完整性，防止因部分写入等问题导致的数据损坏。

#### 3.2. `Wal::Put` - 写入日志

**功能说明**: 将一个键值对序列化成一条 `WAL` 记录，并写入文件。

**源码片段** (`src/wal.cpp`):
```cpp
bool Wal::Put(const ByteBuffer& key, const ByteBuffer& value) {
    try {
        std::lock_guard<std::mutex> lock(file_mutex_);
        
        std::vector<uint8_t> buffer;
        // 1. 序列化 Key 和 Value
        AppendUint16(buffer, static_cast<uint16_t>(key.Size()));
        AppendBuffer(buffer, key);
        AppendUint16(buffer, static_cast<uint16_t>(value.Size()));
        AppendBuffer(buffer, value);
        
        // 2. 计算并追加 Checksum
        uint32_t checksum = CalculateChecksum(buffer);
        AppendUint32(buffer, checksum);
        
        // 3. 写入文件
        file_->write(reinterpret_cast<const char*>(buffer.data()), buffer.size());
        
        return true;
    } catch (const std::exception& e) {
        return false;
    }
}
```

**逐行解读**:
1.  **序列化**: 将 `key` 和 `value` 的长度及内容按格式依次追加到一个临时的 `buffer` 中。
2.  **计算校验和**: 对 `buffer` 中已有的数据（Key/Value及其长度）计算 CRC32C 校验和，并将校验和也追加到 `buffer` 的末尾。
3.  **写入文件**: 调用 `fstream::write` 将整个 `buffer` 的内容追加到 `WAL` 文件的末尾。这里利用了操作系统的文件缓冲区，写入速度很快，但数据不一定立刻落盘。

#### 3.3. `Wal::Sync` - 确保持久化

**功能说明**: `Sync` 是保证持久化的最后一步。它强制操作系统将文件缓冲区中的数据刷到物理磁盘上。

**源码片段** (`src/wal.cpp`):
```cpp
bool Wal::Sync() {
    std::lock_guard<std::mutex> lock(file_mutex_);
    try {
        // 1. 刷流
        file_->flush();
#ifndef _WIN32
        // 2. 同步文件描述符
        int fd = ::open(file_path_.c_str(), O_RDONLY);
        if (fd != -1) {
            if (::fsync(fd) == -1) { ok = false; }
            ::close(fd);
        }
#endif
        return true;
    } catch (...) { return false; }
}
```

**逐行解读**:
1.  **`file_->flush()`**: 将 C++ `fstream` 的内部缓冲区刷到操作系统的文件缓冲区。
2.  **`::fsync(fd)`**: (在非 Windows 系统上) 这是最关键的系统调用。它会阻塞，直到操作系统确认与文件描述符 `fd` 相关的所有缓冲区数据都已成功写入磁盘硬件。只有 `fsync` 调用成功返回后，我们才能认为 `WAL` 记录是真正持久化的。

#### 3.4. `Wal::Recover` - 从崩溃中恢复

**功能说明**: 这是在数据库启动时调用的核心恢复逻辑。它负责读取 `WAL` 文件，并用其中的数据重建 `MemTable`。

**源码片段** (`src/wal.cpp`):
```cpp
std::unique_ptr<Wal> Wal::Recover(
    const std::filesystem::path& path,
    std::shared_ptr<SkipList<ByteBuffer, ByteBuffer>> skiplist) {
    // ... 打开文件 ...
    // ... 将整个文件读入内存 buffer ...

    size_t pos = 0;
    size_t last_good_pos = 0;

    // 1. 循环读取记录
    while (pos + 8 /* minimal record size */ <= file_size) {
        // ... 解码 Key Len, Key, Value Len, Value ...

        // 2. 校验 Checksum
        uint32_t stored_checksum = ReadUint32(&buffer[pos]);
        uint32_t computed_checksum = CalculateChecksum(checksum_data);
        if (computed_checksum != stored_checksum) {
            // 校验失败，说明 WAL 文件末尾损坏，终止循环
            break;
        }

        // 3. 将合法记录重新插入 skiplist
        skiplist->Insert(key, value);
        last_good_pos = pos + 4; // 更新到这条记录的末尾
    }

    // 4. 处理文件末尾损坏的情况
    if (last_good_pos < file_size) {
        // 截断文件，丢弃损坏的部分
        std::filesystem::resize_file(path, last_good_pos);
    }

    return std::unique_ptr<Wal>(new Wal(std::move(file), path));
}
```

**逐行解读**:
1.  **循环读取**: 从文件（已加载到内存 `buffer`）的开头开始，按照 `WAL` 记录格式一条一条地解析数据。
2.  **校验 Checksum**: 每解析完一条记录，都重新计算其内容的校验和，并与记录中存储的 `stored_checksum` 进行比对。如果校验和不匹配，说明这条记录或之后的部分已经损坏（很可能是在写入时发生崩溃导致的“部分写”），此时必须停止恢复。
3.  **重建 `MemTable`**: 对于每一条校验通过的合法记录，调用 `skiplist->Insert(key, value)` 将其重新插入到 `MemTable` 的 `SkipList` 中。`last_good_pos` 会被更新到这条合法记录的末尾。
4.  **截断文件**: 循环结束后，如果 `last_good_pos` 小于文件总大小 `file_size`，说明文件末尾有损坏的数据。此时需要调用 `resize_file` 将文件截断到 `last_good_pos`，丢弃所有损坏的内容，以确保后续的 `WAL` 写入是从一个干净的状态开始。

## 模块三：`LsmStorage` - 引擎的总控与调度

### 1. 概述

`LsmStorage` 是整个引擎的“大脑”和“调度中心”。它封装了所有内部状态（MemTables, SSTables），对外提供统一的 `Put`, `Get`, `Scan` 等接口，并负责协调数据在内存和磁盘之间的流动，包括 `Flush` 和 `Compaction` 等关键的后台维护流程。

---

### 2. 状态管理 (`LsmStorageState`)

**核心职责**: `LsmStorageState` 是一个包含了某个时间点上 LSM-Tree 完整状态的结构体。它本身不包含逻辑，仅作为数据容器。

```cpp
class LsmStorageState {
public:
    std::shared_ptr<MemTable> memtable; // 当前可写的 MemTable
    std::vector<std::shared_ptr<MemTable>> imm_memtables; // 不可变 MemTable 列表
    std::vector<size_t> l0_sstables; // L0 SSTable ID 列表
    std::vector<std::pair<size_t, std::vector<size_t>>> levels; // L1+ 各层的 SSTable ID
    std::unordered_map<size_t, std::shared_ptr<SsTable>> sstables; // SSTable ID 到对象的映射
};
```

**并发与锁策略**: `LsmStorageInner` 中使用 `std::shared_ptr<LsmStorageState> state_` 来持有当前状态，并用一个 `std::shared_mutex` 来保护这个指针。
*   **读操作 (`Get`, `Scan`)**: 获取一个**共享锁** (`std::shared_lock`)，然后拷贝一份 `state_` 的共享指针。后续的整个读操作都基于这个“快照”进行，即使后台有 `Flush` 或 `Compaction` 正在改变状态，读操作看到的数据视图也是一致的、不变的。
*   **写操作/状态变更 (`Flush`, `Compact`)**: 获取一个**独占锁** (`std::unique_lock`)。然后：
    1.  `auto new_state = state_->Clone();` 创建一个当前状态的深拷贝。
    2.  在 `new_state` 上进行所有修改（例如，将 `MemTable` 移到 `imm_memtables` 列表，或增删 `SSTable`）。
    3.  `state_ = std::move(new_state);` 原子地将主状态指针 `state_` 指向这个修改后的新状态。

这种 **Copy-on-Write** 的策略，极大地降低了锁的争用，实现了高并发的读写。读操作几乎不受写操作阻塞。

#### 2.1. 关于迭代器安全性 (Iterator Safety)

这种基于快照的写时复制机制，还带来了一个至关重要的好处：**保证了迭代器的绝对安全**。

当一个 `Scan` 操作开始时，它会获取并持有一个当前 `LsmStorageState` 的共享指针 (`std::shared_ptr`)。由于该 `state` 对象及其引用的所有数据（`MemTable` 列表、`SSTable` 列表）都是**不可变**的，所以为这次 `Scan` 创建的迭代器在它的整个生命周期中，都拥有一个完全一致、不会被改变的数据视图。

与此同时，后台的 `Flush` 或 `Compaction` 线程可以自由地创建新的 `LsmStorageState` 版本并替换主指针，而完全不会影响到任何正在使用旧版本 `state` 的已存在迭代器。旧的状态会因为 `shared_ptr` 的引用计数而保持存活，直到所有使用它的迭代器都被销毁。

这一设计优雅地解决了并发环境下“迭代器失效”这一棘手问题，使得用户可以安全地进行长时间的范围扫描，而不必担心后台操作会干扰读取过程。


---

### 3. 核心流程分析

#### 3.1. 引擎启动: `LsmStorageInner::Open`

**功能说明**: `Open` 负责在启动时从磁盘加载 `MANIFEST` 和 `WAL`，恢复整个 LSM-Tree 到上次关闭前的状态。

**核心逻辑步骤**:
1.  **打开 `MANIFEST`**: 调用 `Manifest::Open`，读取 `MANIFEST` 文件中的所有记录，并在内存中重建每一层的 `SSTable` 列表。
2.  **加载 `SSTable`**: 遍历 `MANIFEST` 中记录的所有 `SSTable` ID，调用 `SsTable::Open` 来打开这些文件，并将返回的 `SsTable` 对象存入 `sstables` 哈希表中。
3.  **恢复 `WAL`**: 调用 `WalSegment::Recover`，读取 `WAL` 日志文件。`Recover` 内部会：
    *   逐条解析 `WAL` 记录并验证校验和。
    *   将合法的键值对重新插入到一个新的 `SkipList` 实例中。
    *   截断 `WAL` 文件，移除末尾可能损坏的数据。
4.  **创建活跃 `MemTable`**: 使用上一步中由 `WAL` 恢复数据填充的 `SkipList`，创建一个新的 `MemTable` 作为当前活跃的 `MemTable`。
5.  **组装状态**: 将恢复的 `SSTable` 列表、`MemTable` 等全部信息组装成一个初始的 `LsmStorageState`，引擎准备就绪。

#### 3.2. 范围扫描: `LsmStorageInner::Scan`

**功能说明**: `Scan` 是 LSM-Tree 最复杂的操作之一。它需要将分散在内存（多个 `MemTable`）和磁盘（多个层级、多个 `SSTable`）中的数据，合并成一个统一的、有序的视图。

**源码片段** (`src/lsm_storage.cpp`):
```cpp
std::unique_ptr<StorageIterator> LsmStorageInner::Scan(const Bound& lower, const Bound& upper) const noexcept {
    // 1. 获取状态快照
    std::shared_ptr<LsmStorageState> snapshot; 
    {
        std::shared_lock<std::shared_mutex> lock(*state_mutex_);
        snapshot = state_;
    }
    
    std::vector<std::unique_ptr<StorageIterator>> iters;

    // 2. 创建 MemTable 的迭代器
    iters.push_back(make_mem_iter(snapshot->memtable));
    for (const auto& mt : snapshot->imm_memtables) {
        iters.push_back(make_mem_iter(mt));
    }

    // 3. 创建 L0 SSTable 的迭代器
    for (size_t sst_id : snapshot->l0_sstables) {
        // ... 为每个 L0 SSTable 创建一个 SsTableIterator ...
        iters.push_back(std::move(tbl_iter));
    }

    // 4. 创建 L1+ SSTable 的迭代器
    for (const auto& level_pair : snapshot->levels) {
        // ... 为每一层的所有 SSTable 创建一个 SstConcatIterator ...
        iters.push_back(std::make_unique<SstConcatIterator>(std::move(concat_it)));
    }

    // 5. 合并所有迭代器
    auto merged = std::make_unique<MergeIterator>(std::move(iters));

    // 6. 包装最终的 LSM 迭代器
    return std::make_unique<LsmIterator<MergeIterator>>(std::move(merged), upper);
}
```

**逐行解读**:
1.  **获取快照**: 与 `Get` 操作一样，首先获取一个线程安全的状态快照。
2.  **`MemTable` 迭代器**: 为当前活跃的 `MemTable` 和所有不可变的 `imm_memtables` 创建 `MemTableIterator`。这些迭代器提供了对内存中有序数据的访问。它们被最先加入 `iters` 列表，因此优先级最高。
3.  **L0 `SSTable` 迭代器**: L0 的 `SSTable` 键范围可能重叠，因此必须为**每一个** L0 的 `SSTable` 单独创建一个 `SsTableIterator`。

4.  **L1+ `SSTable` 迭代器**: L1 及以上层级的 `SSTable` 键范围互不重叠。为了优化，可以为**每一层**创建一个 `SstConcatIterator`。`SstConcatIterator` 内部封装了该层所有的 `SSTable`，并能像单个迭代器一样在它们之间无缝移动，避免了为该层每个 `SSTable` 都创建一个独立迭代器的开销。
5.  **`MergeIterator`**: 这是实现统一视图的关键。`MergeIterator` 接收一个迭代器列表（`iters`），内部使用一个**最小堆**（`std::priority_queue`）来管理它们。每次调用 `Next()`，它总能从所有子迭代器中返回 Key 最小的那个元素。如果 Key 相同，它会优先返回在 `iters` 列表中索引靠前的迭代器（即 `MemTable` > `L0` > `L1`...），从而自然地实现了新数据覆盖旧数据的逻辑。
6.  **`LsmIterator`**: 最后，`MergeIterator` 被 `LsmIterator` 包装。`LsmIterator` 负责处理上层逻辑：
    *   **过滤墓碑**: 检查 `MergeIterator` 返回的 `Value`，如果为空（墓碑标记），则跳过该条目，继续调用 `Next()`。
    *   **处理上界**: 确保迭代器在到达 `upper` 边界时停止。

#### 3.2.1. 可视化：LSM 迭代器栈

`LsmStorageInner::Scan` 的强大之处在于它将多个功能各异的迭代器组合成一个“迭代器栈”。每一个上层迭代器都包装一个或多个下层迭代器，对其输出进行加工，最终为用户提供一个统一、有序、干净的数据视图。

其典型的组合结构如下：

```
          ┌───────────────────┐
          │    LsmIterator    │ (用户最终获取的迭代器)
          └───────────────────┘
                   │ (包装)
                   ▼
          ┌───────────────────┐
          │   MergeIterator   │ (使用最小堆合并所有数据流)
          └───────────────────┘
                   │ (输入)
     ┌─────────────┼──────────────────┬──────────────────┐
     │             │                  │                  │
     ▼             ▼                  ▼                  ▼
┌───────────┐ ┌───────────┐ ┌───────────────────┐ ┌───────────────────┐
│MemTableIt │ │MemTableIt │ │ SstConcatIterator │ │ SstConcatIterator │
│ (active)  │ │ (imm)     │ │      (for L1)     │ │      (for L2)     │
└───────────┘ └───────────┘ └───────────────────┘ └───────────────────┘
     ▲
     │ (SstConcatIterator 内部也包含多个 SsTableIterator)
     │
┌───────────┐ ... ┌───────────┐
│SsTableIt  │     │SsTableIt  │ (为每个 L0 SSTable 创建)
└───────────┘     └───────────┘
```

**迭代器栈各层职责**:

*   **`SsTableIterator` / `MemTableIterator`**: 这是最底层的迭代器，分别负责遍历单个 `SSTable` 文件或单个 `MemTable` (跳表) 中的有序键值对。
*   **`SstConcatIterator`**: 这是一个针对 L1+ 层的性能优化。由于 L1 及以上层级的 `SSTable` 键范围保证互不重叠，因此无需使用 `MergeIterator` 的高代价最小堆。`SstConcatIterator` 可以简单地、按顺序地遍历该层内的所有 `SSTable`，如同它们是单个连接起来的大文件一样。
*   **`MergeIterator`**: 这是实现统一视图的核心。它接收一个迭代器列表（包括所有 `MemTable` 迭代器、所有 L0 的 `SSTable` 迭代器，以及每一高层的一个 `SstConcatIterator`）。其内部维护一个**最小堆**，每次 `Next()` 调用都能精确地从所有输入流中返回全局最小的那个键值对。这自然地完成了数据合并和新版本覆盖旧版本（对于相同 `user_key`）的逻辑。
*   **`LsmIterator`**: 这是最顶层的、直接面向用户的迭代器。它包装了 `MergeIterator`，并负责处理最终的业务逻辑：
    *   **过滤墓碑 (Tombstone)**: 检查从 `MergeIterator` 获取到的值，如果值为空，则说明该条目是一个删除标记，`LsmIterator` 会自动跳过它，继续调用 `Next()`，直到找到一个真实存在的条目。
    *   **处理边界**: 确保迭代过程在用户指定的上界 (`upper_bound`) 处正确停止。

通过这个栈式结构，系统将复杂的合并、过滤逻辑分解到不同的组件中，实现了高度的模块化和可扩展性。

#### 3.3. `Flush` 流程: 从 `MemTable` 到 `SSTable`

**功能说明**: 当 `MemTable` 写满时，需要将其内容持久化为磁盘上的一个 `SSTable` 文件。

**核心调用链**: `Put` -> `TryFreeze` -> `ForceFreezeMemtable` -> `(后台任务)` -> `ForceFlushNextImmMemtable`

1.  **`TryFreeze`**: 在每次 `Put` 后调用，检查 `MemTable` 大小。如果超过阈值，则调用 `ForceFreezeMemtable`。
2.  **`ForceFreezeMemtable` (`src/lsm_storage.cpp`)**: 
    *   获取独占锁。
    *   创建一个新的、空的 `MemTable`。
    *   克隆当前 `LsmStorageState`。
    *   在克隆的状态中，将旧的 `MemTable` 从 `memtable` 字段移到 `imm_memtables` 列表的头部（表示最新）。
    *   将新的空 `MemTable` 设置为 `memtable` 字段。
    *   原子地替换 `state_` 指针，发布新状态。
    *   （释放锁后）对旧 `MemTable` 的 `WAL` 调用 `Sync`，确保其所有日志都已落盘。
3.  **`ForceFlushNextImmMemtable` (`src/lsm_storage.cpp`)**: 这个函数通常由一个后台线程调用，负责处理 `imm_memtables` 列表。
    *   获取列表中最老的一个 `MemTable`（位于列表末尾）。
    *   调用 `BuildSstFromMemtable`，使用 `SsTableBuilder` 将该 `MemTable` 的内容转换并写入一个新的 `.sst` 文件（ID 与 `MemTable` 相同）。
    *   再次使用 Copy-on-Write 模式更新 `LsmStorageState`：从 `imm_memtables` 移除已处理的 `MemTable`，并将新的 `SSTable` ID 添加到 `l0_sstables` 列表的头部。
    *   向 `MANIFEST` 文件追加一条 `Flush` 记录。

#### 3.4. `Compaction` 流程: 后台优化

**功能说明**: `Compaction` 是 LSM-Tree 的后台管家，负责合并 `SSTable`，以控制文件数量、清理冗余数据（被覆盖或删除的键），从而优化读取性能。

**核心调用链**: `(后台任务)` -> `LsmStorageInner::Compact` -> `CompactionController::GenerateCompactionTask` -> `LsmStorageInner::Compact(task)`

1.  **`CompactionController::GenerateCompactionTask` (`simple_leveled_compaction_controller.cpp`)**: 
    *   这是 `Compaction` 的决策核心。对于分层压缩策略，它会检查：
        *   L0 层的 `SSTable` 数量是否超过阈值。如果超过，则生成一个将 L0 所有 `SSTable` 与 L1 中有重叠的 `SSTable` 进行合并的任务。
        *   对于 L1+ 的每一层，计算该层总大小与下一层总大小的比率。如果比率超过阈值（例如，`L(i)` 的大小远大于 `L(i+1)` 的 10%），则生成一个将 `L(i)` 的一个 `SSTable` 与 `L(i+1)` 中有重叠的 `SSTable` 进行合并的任务。
2.  **`LsmStorageInner::Compact(task)` (`src/lsm_storage.cpp`)**: 这是 `Compaction` 的执行核心。
    *   根据任务描述，收集所有需要参与合并的 `SSTable`。
    *   为这些 `SSTable` 创建迭代器，并用一个 `MergeIterator` 将它们合并成单一的、有序的数据流。
    *   创建一个或多个 `SsTableBuilder`，遍历 `MergeIterator`，将合并后的、去除了重复和墓碑标记的数据写入新的 `SSTable` 文件中。新的 `SSTable` 会被放置在 `task` 指定的 `lower_level`。
    *   使用 Copy-on-Write 模式更新 `LsmStorageState`：在 `levels` 中移除所有旧的 `SSTable` ID，并添加所有新的 `SSTable` ID。
    *   向 `MANIFEST` 文件追加一条 `Compaction` 记录，原子地记录这次状态变更。
    *   最后，安全地删除所有被合并掉的旧的 `.sst` 文件。

#### 3.5. 压缩策略控制器: `CompactionController`

`Compaction` 的时机和方式对 LSM-Tree 的性能至关重要，不同的策略在读放大、写放大和空间放大之间有不同的权衡。`mini-lsm-cpp` 通过 `CompactionController` 接口将压缩策略模块化，允许用户根据应用场景选择最合适的策略。

##### a. 核心数据结构 (`compaction_controller.hpp`)

**`CompactionTask`**: 这个结构体定义了一个压缩任务需要的所有信息。

```cpp
struct CompactionTask {
    std::vector<size_t> upper_level_sst_ids;
    std::vector<size_t> lower_level_sst_ids;
    size_t upper_level_id;
    size_t lower_level_id;
    bool is_valid() const {
        return upper_level_id != lower_level_id ||
               (!upper_level_sst_ids.empty() && !lower_level_sst_ids.empty());
    }
};
```
*   **`upper_level_sst_ids`**: 参与压缩的、来自上一层级（`upper_level_id`）的 SSTable ID 列表。
*   **`lower_level_sst_ids`**: 参与压缩的、来自下一层级（`lower_level_id`）的 SSTable ID 列表。
*   **`upper_level_id`, `lower_level_id`**: 定义了压缩发生在哪些层级之间。对于 L0->L1 的压缩，`upper_level_id` 是 0；对于 L1+ 的层内压缩，`upper_level_id` 和 `lower_level_id` 会不同。

**`CompactionController` 接口**:

```cpp
class CompactionController {
public:
    virtual CompactionTask GenerateCompactionTask(const LsmStorageState& snapshot) = 0;
    // ...
};
```
`GenerateCompactionTask` 是所有策略类必须实现的纯虚函数。它负责分析 `LSM-Tree` 的当前状态，并返回一个具体的 `CompactionTask`。

##### b. 分层压缩 (Leveled Compaction)

这是在 `simple_leveled_compaction_controller.cpp` 中实现的经典策略。

**决策逻辑源码 (`simple_leveled_compaction_controller.cpp`)**:
```cpp
CompactionTask SimpleLeveledCompactionController::GenerateCompactionTask(const LsmStorageState& snapshot) {
    // 1. 优先处理 L0 -> L1 的压缩
    if (snapshot.l0_sstables.size() >= options_.level0_file_num_compaction_trigger) {
        return CompactionTask{
            snapshot.l0_sstables,
            snapshot.GetOverlappingSsts(snapshot.l0_sstables, 1), // 计算与 L0 重叠的 L1 SSTs
            0, 1
        };
    }

    // 2. 检查 L1+ 各层是否需要压缩
    for (size_t i = 1; i < options_.num_levels - 1; ++i) {
        if (snapshot.levels[i-1].second.size() * options_.max_bytes_for_level_base > snapshot.levels[i].second.size()) {
             // 找到 L(i) 中需要被压缩的 SST
            size_t sst_id_to_compact = snapshot.FindSstToCompact(i);
            if (sst_id_to_compact != 0) {
                return CompactionTask{
                    {sst_id_to_compact},
                    snapshot.GetOverlappingSsts({sst_id_to_compact}, i + 1), // 计算与该 SST 重叠的 L(i+1) SSTs
                    i, i + 1
                };
            }
        }
    }
    return {}; // 无需压缩
}
```

**核心逻辑解读**:
1.  **L0 压缩检查**: 首先检查 L0 层的 SSTable 文件数量是否达到了触发阈值（`level0_file_num_compaction_trigger`）。如果达到，则生成一个将**所有** L0 的 SSTable 与 L1 中所有和它们键范围有重叠的 SSTable 进行合并的任务。这是最高优先级的任务。
2.  **L1+ 压缩检查**: 如果 L0 无需压缩，则从 L1 开始遍历每一层。它会比较 `L(i)` 层的总大小和 `L(i+1)` 层的总大小。如果 `Size(L(i)) * N > Size(L(i+1))`（其中 `N` 是 `max_bytes_for_level_base`，通常是 10），说明 `L(i)` 相对于下一层过于“膨胀”，需要将数据推向下一层。
3.  **选择 SSTable**: 一旦确定某一层需要压缩，`FindSstToCompact` 会从该层选择一个 SSTable（例如，选择最后一个，即键范围最大的那个）。然后生成一个将这个 SSTable 与 `L(i+1)` 中所有键范围重叠的 SSTable 进行合并的任务。

##### c. 分级/大小分级压缩 (Tiered Compaction)

这是在 `tiered_compaction_controller.cpp` 中实现的策略。

**决策逻辑源码 (`tiered_compaction_controller.cpp`)**:
```cpp
CompactionTask TieredCompactionController::GenerateCompactionTask(const LsmStorageState& snapshot) {
    // 1. 按 Tier 对 SSTable 进行分组
    std::vector<std::vector<size_t>> tiers = snapshot.GenerateTiers(options_.max_levels);

    // 2. 遍历每个 Tier，检查是否需要压缩
    for (size_t i = 0; i < tiers.size(); ++i) {
        if (tiers[i].size() >= options_.min_merge_width) {
            // 如果当前 Tier 的文件数达到阈值，则将其与下一级所有文件合并
            return CompactionTask{
                tiers[i],
                (i + 1 < tiers.size()) ? tiers[i+1] : std::vector<size_t>{},
                i, i + 1
            };
        }
    }
    return {}; // 无需压缩
}
```

**核心逻辑解读**:
1.  **生成 Tiers**: `GenerateTiers` 是分级压缩的核心。它会根据 SSTable 的大小，将所有 SSTable（不分层级）划分到不同的“级 (Tier)”中。例如，Tier 0 可能包含所有大小在 0-2MB 的 SSTable，Tier 1 包含 2-8MB 的，以此类推。
2.  **检查触发条件**: 策略会遍历这些 Tiers。如果发现某个 Tier 中的文件数量达到了 `min_merge_width`（最小合并宽度）的阈值，就会触发压缩。
3.  **生成任务**: 压缩任务会将触发合并的整个 Tier 的**所有** SSTable，与**下一整个 Tier** 的所有 SSTable 合并。这是一个大规模的合并操作，合并后会产生一个或多个新的、更大的 SSTable，这些新的 SSTable 会被放入更高的 Tier 中。

##### d. 策略对比总结

| 特性 | 分层压缩 (Leveled) | 分级压缩 (Tiered) |
| :--- | :--- | :--- |
| **读放大** | **低** | 高 |
| **写放大** | 高 | **低** |
| **空间放大** | **低** | 高 |
| **适用场景** | 读密集型、或对空间敏感的场景 | 写密集型、或存储成本低廉的场景 |


## 模块四：`MVCC` 与事务 - 实现隔离与并发控制

### 1. 概述

现代数据库系统需要处理并发读写，同时为用户提供一个一致性的数据视图。`mini-lsm-cpp` 通过实现多版本并发控制（MVCC）来达到这个目的。MVCC 的核心思想是“写操作不覆盖，而是创建新版本”，每个读操作都只访问特定时间点（版本）的数据快照。

本模块将深入剖析 `mini-lsm-cpp` 的 MVCC 实现，涵盖以下几个方面：

*   **带时间戳的 Key**: MVCC 的基石，如何将版本信息（时间戳）编码到 `LSM-Tree` 的 `Key` 中。
*   **事务 (`Txn`)**: 读写操作的载体，如何管理事务的读时间戳和写集合。
*   **读写路径**: 在 MVCC 下，`Get` 和 `Put` 操作的内部逻辑。
*   **提交与冲突检测**: 事务的提交过程，以及如何保证可串行化隔离级别。
*   **Watermark 与 GC**: 如何清理过时的旧版本数据，防止存储无限膨胀。

**实现说明**: 值得注意的是，为了最高效地实现上述功能，`mini-lsm-cpp` 的代码库中包含了一套与基础 LSM 组件平行的 **MVCC 专用组件** (例如 `MvccLsmStorage`, `MvccMergeIterator`, `mvcc_mem_table` 等)。本章后续的讲解将侧重于这些组件背后的通用逻辑与原理，但读者在阅读源码时会发现，这些原理被封装在专用的 `mvcc_*` 类中，以原生支持带时间戳的数据，并在 `Compaction` 时执行高效的垃圾回收。

---

### 2. `Key-Timestamp` 编码 (`key.hpp`, `key_ts.hpp`)

为了在 `LSM-Tree` 中存储多版本数据，`mini-lsm-cpp` 并没有改变 `SSTable` 和 `MemTable` 的基本结构，而是巧妙地将版本信息（一个 `u64` 类型的时间戳 `ts`）编码到了 `Key` 的末尾。

**物理编码格式**:

```
+--------------------------+------------------------------------+
|     User Key (bytes)     |  Inverted Timestamp (~ts) (8 bytes) |
+--------------------------+------------------------------------+
```

**核心源码 (`key.hpp`)**:
```cpp
inline std::vector<uint8_t> EncodeKey(const std::vector<uint8_t>& user_key, uint64_t ts) {
    std::vector<uint8_t> encoded_key;
    encoded_key.reserve(user_key.size() + sizeof(uint64_t));
    encoded_key.insert(encoded_key.end(), user_key.begin(), user_key.end());
    uint64_t inverted_ts = ~ts; // 按位取反
    // ... append inverted_ts to encoded_key in big-endian order ...
    return encoded_key;
}

inline KeyTs DecodeKey(const std::vector<uint8_t>& encoded_key) {
    // ... extract user_key part ...
    uint64_t inverted_ts = /* read last 8 bytes */;
    return {user_key, ~inverted_ts};
}
```
**设计解读**:
*   **按位取反**: `LSM-Tree` 的 `MergeIterator` 在合并数据时，如果 `Key` 相同，会优先选择来自较新组件（如 MemTable > L0 > L1）的数据。但对于 MVCC，我们需要的是 `Key` 最新版本的数据。通过将时间戳 `ts` 按位取反（`~ts`），一个更大的 `ts` 会得到一个更小的 `~ts`。这样，当 `(user_key, ~ts)` 被作为 `LSM-Tree` 的内部 `Key` 时，最新版本的数据（`ts` 最大，`~ts` 最小）会自然地排在最前面，可以直接被 `MergeIterator` 正确处理。
*   **大端序**: 时间戳使用大端序编码，这保证了 `memcmp` 可以直接按字节顺序比较两个编码后的 `Key`，其结果与先比较 `user_key` 再比较 `~ts` 的结果一致。

---

### 3. 事务上下文 `MvccTxn` (`mvcc_txn.hpp`)

`MvccTxn` 是用户与 MVCC 存储引擎交互的句柄，它封装了一个事务的所有状态。

**核心成员变量**:
```cpp
class MvccTxn {
private:
    uint64_t read_ts_; // 事务的读时间戳 (Snapshot)
    std::shared_ptr<MvccLsmStorage> storage_; // 存储引擎的引用
    std::unordered_map<std::vector<uint8_t>, std::vector<uint8_t>> write_set_; // 事务的写缓存
    bool committed_ = false;
};
```
*   **`read_ts_`**: 事务的“读时间戳”。在事务开始时，从 `MvccLsmStorage` 获取一个全局的最新时间戳。该事务的所有读操作，都只能看到**不晚于** `read_ts_` 的数据版本。这保证了可重复读（Repeatable Read）。
*   **`write_set_`**: 一个内存中的 `map`，缓存了当前事务内所有的写操作（`Put`）。这些修改在事务提交前对其他事务是不可见的。事务内的读操作会优先从 `write_set_` 中查找。

---

### 4. MVCC 下的读写操作

#### 4.1. 读取路径: `MvccTxn::Get`

**功能说明**: 在事务的快照（由 `read_ts_` 定义）中读取一个 `Key` 的 `Value`。

**源码片段** (`src/mvcc_txn.cpp`):
```cpp
std::optional<std::vector<uint8_t>> MvccTxn::Get(const std::vector<uint8_t>& key) {
    if (committed_) {
        throw std::runtime_error("cannot operate on a committed transaction");
    }
    // 1. 优先从事务自身的写集合中读取
    if (auto it = write_set_.find(key); it != write_set_.end()) {
        return it->second;
    }

    // 2. 构造用于查找的 Key-with-Timestamp
    std::vector<uint8_t> encoded_key = EncodeKey(key, read_ts_);

    // 3. 从 LSM-Tree 中查找
    auto iter = storage_->Scan(encoded_key, {}); // Scan from (key, read_ts) to infinity
    if (!iter->IsValid()) {
        return std::nullopt;
    }

    // 4. 解码并验证找到的 Key
    KeyTs decoded_key = DecodeKey(iter->Key());
    if (decoded_key.key == key) {
        if (iter->Value().empty()) { // 墓碑标记
            return std::nullopt;
        }
        return iter->Value();
    }
    
    return std::nullopt;
}
```
**逐行解读**:
1.  **查 `write_set_`**: 首先检查 `write_set_`。如果 `Key` 在当前事务中已经被写入或修改过，直接返回缓存的值。这保证了事务内的读写一致性。
2.  **构造 `Seek Key`**: 如果 `write_set_` 未命中，则需要从持久化存储中查找。使用 `EncodeKey` 将用户 `key` 和事务的 `read_ts_` 组合成一个 `LSM-Tree` 内部的 `Key`。
3.  **调用 `Scan`**: 以构造的 `Key` 为下界，调用 `storage_->Scan`。由于 `Key` 的排序机制，迭代器返回的第一个结果，将是该 `user_key` 所有版本中，时间戳**小于或等于** `read_ts_` 的那个最新版本。
4.  **解码验证**:
    *   `DecodeKey` 将迭代器返回的内部 `Key` 解码回 `user_key` 和 `ts`。
    *   必须再次比较 `decoded_key.key == key`，以防止 `Scan` 返回的是另一个前缀相同但更长的 `user_key`。
    *   如果 `Key` 匹配，检查其 `Value` 是否为空。空 `Value` 在 `LSM-Tree` 中是“墓碑”标记，代表该版本已被删除。
    *   如果一切正常，返回找到的 `Value`。

#### 4.2. 写入路径: `MvccTxn::Put`

`Put` 操作非常简单：它只是将键值对缓存在内存中的 `write_set_` 里，并不会立刻写入 `LSM-Tree`。真正的写入发生在事务提交阶段。

```cpp
void MvccTxn::Put(const std::vector<uint8_t>& key, const std::vector<uint8_t>& value) {
    if (committed_) {
        throw std::runtime_error("cannot modify a committed transaction");
    }
    write_set_[key] = value;
}
```

---

### 5. 事务提交与 `Watermark`

#### 5.1. 提交过程: `MvccTxn::Commit`

**源码片段** (`src/mvcc_txn.cpp`):
```cpp
void MvccTxn::Commit() {
    if (committed_) {
        throw std::runtime_error("cannot commit a committed transaction");
    }
    // 1. 调用存储层执行批量写入
    storage_->WriteBatch(write_set_);
    // 2. 标记为已提交
    committed_ = true;
}
```
`Commit` 方法本身是一个简单的封装，它将核心的提交流程委托给了 `MvccLsmStorage::WriteBatch`。

**核心提交逻辑** (`src/mvcc_lsm_storage.cpp`):
```cpp
void MvccLsmStorage::WriteBatch(const std::unordered_map<Key, Value>& batch) {
    // 1. 获取全局唯一的提交时间戳
    uint64_t commit_ts = latest_commit_ts_.fetch_add(1) + 1;

    // 2. 构造批量写入请求
    WriteBatchRecord record;
    for (const auto& [key, value] : batch) {
        record.AddPut(EncodeKey(key, commit_ts), value);
    }

    // 3. 写入底层 LSM 存储
    storage_->WriteBatch(record);

    // 4. 更新 Watermark
    // (此步骤通常由一个后台线程或协调器完成，以更新所有已提交事务的最低 read_ts)
    // For simplicity here, we can assume it's updated after the write.
    watermark_.store(commit_ts);
}
```
**核心逻辑步骤**:
1.  **获取 `commit_ts`**: 从一个原子计数器 `latest_commit_ts_` 中获取一个全新的、单调递增的时间戳。这个时间戳将作为本次事务所有修改的“版本号”。
2.  **构造 `WriteBatch`**: 遍历事务的 `write_set_`，对于每一个 `(key, value)`，使用刚刚获得的 `commit_ts` 将 `key` 编码为带时间戳的内部 `Key`，然后将这些 `(internal_key, value)` 对添加到一个 `WriteBatchRecord` 中。
3.  **原子写入**: 调用底层 `LsmStorage` 的 `WriteBatch` 方法。这个方法会将整个 `record` 中的所有操作原子地写入 `WAL`，然后再更新 `MemTable`。这保证了事务的原子性：要么全部成功，要么全部失败。
4.  **更新 `Watermark`**: 事务成功写入后，需要更新全局的 `Watermark`。`Watermark` 是一个重要的标记，表示所有时间戳早于它的事务都已经结束（提交或中止）。这个信息在垃圾回收（GC）时至关重要。

#### 5.2. `Watermark` 与垃圾回收 (GC)

垃圾回收并不是一个独立的进程，而是**在 `Compaction` 过程中顺带完成的**。

**GC 逻辑片段 (概念性，位于 `Compaction` 流程中)**:
```cpp
// Inside a Compaction process...
auto watermark = storage_->GetWatermark();
std::vector<uint8_t> current_user_key;
uint64_t latest_ts = 0;

// 遍历由 MergeIterator 产生的、合并后的有序数据流
for (auto iter = merge_iterator; iter->IsValid(); iter->Next()) {
    KeyTs decoded_key = DecodeKey(iter->Key());

    if (decoded_key.key != current_user_key) {
        // 遇到了一个新的 user_key，重置状态
        current_user_key = decoded_key.key;
        latest_ts = decoded_key.ts;
    }

    // 核心GC逻辑:
    // 只有每个 user_key 的最新版本，或者时间戳晚于 Watermark 的旧版本，才需要被保留
    if (decoded_key.ts == latest_ts || decoded_key.ts > watermark) {
        sstable_builder->Add(iter->Key(), iter->Value());
    } else {
        // 这个版本 (decoded_key.ts < watermark) 是一个过时的旧版本，
        // 可以安全地丢弃，不写入新的 SSTable。
    }
}
```
**逐行解读**:
1.  **获取 `Watermark`**: 在 `Compaction` 开始时，获取一次全局的 `Watermark`。
2.  **遍历与分组**: `Compaction` 会合并多个 `SSTable`，`MergeIterator` 会输出一个包含所有版本、按内部 `Key` 有序的数据流。代码会隐式地按 `user_key` 对这些版本进行分组。
3.  **决策保留或丢弃**:
    *   对于任何 `user_key`，它的**最新版本**（`ts` 最大，即 `decoded_key.ts == latest_ts`）必须被保留，无论其 `Value` 是否是墓碑。
    *   对于旧版本，检查其时间戳 `ts`。如果 `ts > watermark`，说明这个版本可能仍然对某个活跃的事务可见（该事务的 `read_ts` 可能介于 `watermark` 和 `latest_commit_ts` 之间），因此必须保留。
    *   如果一个旧版本的 `ts <= watermark`，则说明它对于任何当前或未来的事务都绝对不可见了。这个版本就是“垃圾”，可以直接被丢弃，无需写入到压缩后产生的新 `SSTable` 中。

通过这种方式，`Compaction` 在优化数据布局的同时，也高效地回收了过时的多版本数据，控制了存储空间的增长。
