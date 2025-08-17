#include "../include/lsm_storage.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>

#include <string>
#include <vector>

// POSIX for sync
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../include/compaction_controller.hpp"
#include "../include/manifest.hpp"
#include "../include/mem_table.hpp"
#include "../include/sstable.hpp"
#include "../include/sstable_builder.hpp"
#include "../include/bloom_filter.hpp"
#include "../include/sstable_iterator.hpp"
#include "../include/sst_concat_iterator.hpp"
#include "../include/simple_leveled_compaction_controller.hpp"
#include "../include/merge_iterator.hpp"
#include "../include/lsm_iterator.hpp"
#include "../include/mvcc.hpp"
#include "../include/mvcc_mem_table.hpp"
#include "../include/mvcc_sstable_iterator.hpp"
#include "../include/mvcc_merge_iterator.hpp"
#include "../include/mvcc_lsm_iterator.hpp"
#include "../include/crc32c.hpp"

namespace fs = std::filesystem;
using Path = fs::path;

namespace util {

bool LsmStorageInner::KeyPassesFilters(const ByteBuffer& key) const {
    std::lock_guard<std::mutex> lock(compaction_filters_mutex_);
    if (compaction_filters_.empty()) {
        return true; // no filters configured
    }
    for (const auto& f : compaction_filters_) {
        switch (f.GetType()) {
            case CompactionFilter::Type::kPrefix: {
                const ByteBuffer& prefix = f.GetValue();
                if (prefix.Size() <= key.Size() &&
                    std::memcmp(key.Data(), prefix.Data(), prefix.Size()) == 0) {
                    return false; // key matches prefix => filtered
                }
                break;
            }
            default:
                break;
        }
    }
    return true;
}

// Forward declarations for classes
class SsTable;
class BlockCache;

// -----------------------------------------------------------------------------
// Compaction task definition (minimal)
// -----------------------------------------------------------------------------
namespace {
struct CompactionTask {
    enum class Type {
        kForceFullCompaction
    } type;
    // IDs of SSTables to be compacted from L0 and L1
    std::vector<size_t> l0_sstables;
    std::vector<size_t> l1_sstables;
};
}

// Constructor implementation

// Implementation of LsmStorageInner methods
std::unique_ptr<LsmStorageInner> LsmStorageInner::Create(
    const Path& path,
    const LsmStorageOptions& options) {
    // Create directory if it doesn't exist
    if (!std::filesystem::exists(path)) {
        std::filesystem::create_directories(path);
    }

    // Create a new storage instance
    auto storage = std::unique_ptr<LsmStorageInner>(new LsmStorageInner(path, options));

    // Initialize manifest (same as Open method)
    Path manifest_path = path / "MANIFEST";
    storage->manifest_ = Manifest::Create(manifest_path);
    std::cout << "[DEBUG] Created manifest in Create method" << std::endl;

    // Initialize with a new state
    storage->state_ = std::make_shared<LsmStorageState>(1);

    // Create WAL segment if enabled
    if (options.enable_wal) {
        WalSegment::Options wal_options;
        wal_options.sync_on_write = options.sync_on_write;

        storage->wal_segment_ = WalSegment::Create(
            storage->WalSegmentDir(),
            storage->WalSegmentName(),
            wal_options);
        
        // CRITICAL FIX: Connect the WAL segment to the active memtable
        // This was missing and caused WAL writes to be ignored
        storage->state_->memtable->SetWalSegment(storage->wal_segment_);
    }

    return storage;
}

std::unique_ptr<LsmStorageInner> LsmStorageInner::Open(
    const Path& path,
    const LsmStorageOptions& options) {
    // 1. Ensure directory exists
    if (!std::filesystem::exists(path)) {
        std::filesystem::create_directories(path);
    }

    // 2. Instantiate storage object
    auto storage = std::unique_ptr<LsmStorageInner>(new LsmStorageInner(path, options));

    // 3. Open or create MANIFEST and rebuild in-memory state from SSTs
    Path manifest_path = path / "MANIFEST";
    std::cout << "[DEBUG] Manifest path: " << manifest_path << std::endl;
    std::cout << "[DEBUG] Manifest exists: " << fs::exists(manifest_path) << std::endl;
    if (fs::exists(manifest_path)) {
        storage->manifest_ = Manifest::Open(manifest_path);
        std::cout << "[DEBUG] Opened existing manifest" << std::endl;
    } else {
        storage->manifest_ = Manifest::Create(manifest_path);
        std::cout << "[DEBUG] Created new manifest" << std::endl;
    }

    const auto version = storage->manifest_->GetCurrentVersion();
    std::cout << "[DEBUG] Manifest version has " << version.size() << " levels" << std::endl;
    for (const auto& [level, ids] : version) {
        std::cout << "[DEBUG] Level " << level << " has " << ids.size() << " SSTs: ";
        for (size_t id : ids) {
            std::cout << id << " ";
        }
        std::cout << std::endl;
    }
    
    auto state = std::make_shared<LsmStorageState>(0); // Initial dummy memtable
    state->imm_memtables.clear();
    state->l0_sstables.clear();
    state->levels.clear();
    state->sstables.clear();

    size_t max_sst_id = 0;
    for (const auto& [level, ids] : version) {
        if (level == 0) {
            state->l0_sstables = ids;
        } else {
            state->levels.emplace_back(level, ids);
        }
        for (size_t id : ids) {
            max_sst_id = std::max(max_sst_id, id);
            auto sst_path = PathOfSstStatic(path, id);
            if (fs::exists(sst_path)) {
                try {
                    auto sst = SsTable::Open(
                        id,
                        storage->block_cache_,
                        FileObject::Open(sst_path.string()));
                    state->sstables.emplace(id, sst);
                } catch (const std::exception& e) {
                    // It's possible for a MANIFEST to exist but the SST to be gone if a crash happened
                    // during cleanup. We can log this but continue.
                    std::cerr << "Warning: could not open SST " << id << " from manifest: " << e.what() << std::endl;
                }
            }
        }
    }

    // 4. Recover memtable state from WAL
    size_t next_id = max_sst_id + 1;
    auto skiplist = std::make_shared<SkipList<ByteBuffer, ByteBuffer>>();
    if (options.enable_wal) {
        WalSegment::Options wal_options;
        wal_options.sync_on_write = options.sync_on_write;

        std::cout << "[DEBUG] Starting WAL recovery..." << std::endl;
        storage->wal_segment_ = WalSegment::Recover(
            storage->WalSegmentDir(),
            storage->WalSegmentName(),
            skiplist,
            wal_options);
        
        // Debug: check how many keys were recovered
        size_t recovered_keys = 0;
        skiplist->ForEach([&recovered_keys](const ByteBuffer& key, const ByteBuffer& value) {
            recovered_keys++;
            std::cout << "[DEBUG] Recovered key: " << key.ToString() << ", value: " << value.ToString() << std::endl;
        });
        std::cout << "[DEBUG] Total recovered keys: " << recovered_keys << std::endl;
    }

    // The recovered skiplist (even if empty) becomes the new active memtable.
    auto active_memtable = MemTable::CreateFromSkipList(next_id, skiplist);
    
    // Debug: verify memtable contents after creation
    std::cout << "[DEBUG] Active memtable created with approximate size: " << active_memtable->ApproximateSize() << std::endl;
    
    if (options.enable_wal) {
        if (!storage->wal_segment_) {
            WalSegment::Options wal_options;
            wal_options.sync_on_write = options.sync_on_write;
            storage->wal_segment_ = WalSegment::Create(storage->WalSegmentDir(), storage->WalSegmentName(), wal_options);
        }
        active_memtable->SetWalSegment(storage->wal_segment_);
    }
    state->memtable = std::move(active_memtable);
    
    // 6. Finalize storage object fields
    storage->state_ = state;
    storage->next_sst_id_.store(next_id + 1, std::memory_order_relaxed);

    (void)storage->SyncDir();

    return storage;
}

LsmStorageInner::LsmStorageInner(const Path& path, const LsmStorageOptions& options)
    : state_mutex_(std::make_shared<std::shared_mutex>()),
      path_(path),
      block_cache_(std::make_shared<BlockCache>()),
      next_sst_id_(1),
      options_(std::make_shared<LsmStorageOptions>(options)) {
    compaction_controller_ = CompactionController::Create(options.compaction_strategy, options_);
    if (options.enable_wal) {
        std::filesystem::create_directories(WalSegmentDir());
    }
}

LsmStorageInner::~LsmStorageInner() {
    (void)Flush();
}

Path LsmStorageInner::PathOfWal(size_t id) const {
    return path_ / ("wal-" + std::to_string(id) + ".log");
}

Path LsmStorageInner::WalSegmentDir() const {
    return path_ / "wal";
}

std::string LsmStorageInner::WalSegmentName() const {
    return "lsm";
}

Path LsmStorageInner::PathOfSstStatic(const Path& dir, size_t id) {
    return dir / (std::to_string(id) + ".sst");
}

Path LsmStorageInner::PathOfSst(size_t id) const {
    return PathOfSstStatic(path_, id);
}

bool LsmStorageInner::Sync() {
    // First sync the WAL segment if it exists
    if (wal_segment_) {
        if (!wal_segment_->Sync()) {
            return false;
        }
    }
    
#ifdef __linux__
    int fd = ::open(path_.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        return false;
    }
    if (::fsync(fd) != 0) {
        ::close(fd);
        return false;
    }
    ::close(fd);
    return true;
#else
    // For non-Linux platforms, this is a no-op.
    return true;
#endif
}

bool LsmStorageInner::Put(const util::ByteBuffer& key, const util::ByteBuffer& value) {
    std::lock_guard<std::mutex> lock(state_lock_mutex_);
    // Allow empty values for MVCC tombstone markers, but keys must not be empty
    assert(!key.Empty());
    (void)state_->memtable->Put(key, value);
    (void)TryFreeze(state_->memtable->ApproximateSize());
    return true;
}

bool LsmStorageInner::Delete(const ByteBuffer& key) {
    std::lock_guard<std::mutex> lock(state_lock_mutex_);
    assert(!key.Empty());
    (void)state_->memtable->Put(key, ByteBuffer());
    (void)TryFreeze(state_->memtable->ApproximateSize());
    return true;
}

bool LsmStorageInner::WriteBatch(const std::vector<std::pair<ByteBuffer, ByteBuffer>>& batch, uint64_t* commit_ts) {
    std::lock_guard<std::mutex> lock(state_lock_mutex_);
    
    // Generate a commit timestamp if MVCC is enabled
    uint64_t ts = 0;
    auto mvcc = GetMvcc();
    if (mvcc) {
        ts = mvcc->LatestCommitTs() + 1;
        mvcc->UpdateCommitTs(ts);
        
        // Return the commit timestamp if the pointer is provided
        if (commit_ts) {
            *commit_ts = ts;
        }
    }
    
    // Write the batch to the memtable
    for (const auto& [key, value] : batch) {
        assert(!key.Empty());
        (void)state_->memtable->Put(key, value);
    }
    
    // Check if we need to freeze the memtable
    (void)TryFreeze(state_->memtable->ApproximateSize());
    
    return true;
}

std::optional<ByteBuffer> LsmStorageInner::Get(const ByteBuffer& key) const {
    // If MVCC is enabled, use MVCC-aware read with current timestamp
    if (mvcc_) {
        uint64_t current_ts = mvcc_->GetNextTimestamp();
        return GetWithTs(key, current_ts);
    }
    
    // Original non-MVCC implementation for backward compatibility
    std::shared_ptr<LsmStorageState> state;
    {
        std::shared_lock<std::shared_mutex> lock(*state_mutex_);
        state = state_;
    }
    
    // Calculate key hash once for all bloom filter checks
    uint32_t key_hash = Crc32c::Compute(key.Data(), key.Size());

    // 1. Search active memtable
    auto result = state->memtable->Get(key);
    if (result.has_value()) {
        if (result->Empty()) {
            return std::nullopt; // Tombstone
        }
        return result;
    }

    // 2. Search immutable memtables (from newest to oldest)
    for (const auto& memtable : state->imm_memtables) {
        auto imm_result = memtable->Get(key);
        if (imm_result.has_value()) {
            if (imm_result->Empty()) {
                return std::nullopt; // Tombstone
            }
            return imm_result;
        }
    }

    // 3. Search L0 SSTs and level SSTs for plain keys (matching Rust get semantics)
    // Search from newest to oldest SST (forward iteration since newest is at begin())
    for (auto sst_iter = state->l0_sstables.begin(); sst_iter != state->l0_sstables.end(); ++sst_iter) {
        auto sst_id = *sst_iter;
        auto sst = state->sstables.at(sst_id);
        
        // Check if key is within SST range
        if (key < sst->FirstKey() || key > sst->LastKey()) {
            std::cout << "[DEBUG] Key out of range for SST " << sst_id << std::endl;
            continue;
        }
        
        // Check bloom filter
        if (sst->Bloom() && !sst->Bloom()->MayContain(key_hash)) {
            std::cout << "[DEBUG] Bloom filter rejected key in SST " << sst_id << std::endl;
            continue;
        }
        
        // Search in SST using plain key
        auto iter = SsTableIterator::CreateAndSeekToKey(sst, Bound::Included(key));
        if (iter.IsValid()) {
            auto found_key = iter.Key();
            if (found_key == key) {
                auto value = iter.Value();
                if (value.Empty()) {
                    return std::nullopt; // Tombstone
                }
                return value;
            }
        }
    }
    
    // Search level SSTs
    for (const auto& [level, sst_ids] : state->levels) {
        for (auto sst_id : sst_ids) {
            auto sst = state->sstables.at(sst_id);
            
            // Check if key is within SST range
            if (key < sst->FirstKey() || key > sst->LastKey()) {
                continue;
            }
            
            // Check bloom filter
            uint32_t key_hash = Crc32c::Compute(key.Data(), key.Size());
            if (sst->Bloom() && !sst->Bloom()->MayContain(key_hash)) {
                continue;
            }
            
            // Search in SST using plain key
            auto iter = SsTableIterator::CreateAndSeekToKey(sst, Bound::Included(key));
            if (iter.IsValid() && iter.Key() == key) {
                auto value = iter.Value();
                if (value.Empty()) {
                    return std::nullopt; // Tombstone
                }
                return value;
            }
        }
    }
    
    return std::nullopt;
}

std::optional<ByteBuffer> LsmStorageInner::GetWithTs(const ByteBuffer& key, uint64_t read_ts) const {
    std::cout << "[MVCC] GetWithTs: key=" << key.ToString() << ", read_ts=" << read_ts << std::endl;
    std::shared_ptr<LsmStorageState> state;
    {
        std::shared_lock<std::shared_mutex> lock(*state_mutex_);
        state = state_;
    }

    // 1. Search active memtable using MVCC-aware GetWithTs
    auto result = state->memtable->GetWithTs(key, read_ts);
    if (result.has_value()) {
        const auto& [value, timestamp] = result.value();
        if (value.Empty()) {
            return std::nullopt; // Tombstone
        }
        return std::optional(value);
    }

    // 2. Search immutable memtables (from newest to oldest)
    for (const auto& memtable : state->imm_memtables) {
        auto imm_result = memtable->GetWithTs(key, read_ts);
        if (imm_result.has_value()) {
            const auto& [value, timestamp] = imm_result.value();
            if (value.Empty()) {
                return std::nullopt; // Tombstone
            }
            return std::optional(value);
        }
    }

    // 3. Search L0 SSTables (from newest to oldest) for versioned keys
    uint32_t key_hash = Crc32c::Compute(key.Data(), key.Size());
    for (size_t sst_id : state->l0_sstables) {
        auto sst = state->sstables.at(sst_id);
        if (sst->Bloom() && !sst->Bloom()->MayContain(key_hash)) {
            continue;
        }
        
        // Search for versioned keys that match our user key
        auto iter = SsTableIterator::CreateAndSeekToKey(sst, Bound::Included(key));
        std::optional<ByteBuffer> best_value;
        uint64_t best_timestamp = 0;
        
        while (iter.IsValid()) {
            const ByteBuffer& versioned_key = iter.Key();
            
            // Check if this key matches our target key
            if (versioned_key.Size() >= key.Size() + sizeof(uint64_t)) {
                // This looks like a versioned key (MVCC scenario)
                // Compare the user key portion
                if (std::memcmp(versioned_key.Data(), key.Data(), key.Size()) == 0) {
                    // Extract timestamp from the versioned key
                    const uint8_t* ts_data = reinterpret_cast<const uint8_t*>(versioned_key.Data()) + key.Size();
                    uint64_t timestamp = 0;
                    for (int i = 0; i < 8; ++i) {
                        timestamp = (timestamp << 8) | ts_data[i];
                    }
                    
                    // Check if this version is visible to our read timestamp and is the best so far
                    if (timestamp <= read_ts && timestamp > best_timestamp) {
                        best_timestamp = timestamp;
                        best_value = iter.Value();
                    }
                } else {
                    // If the user key portion doesn't match, we've gone past our target key
                    break;
                }
            } else if (versioned_key.Size() == key.Size() && 
                       std::memcmp(versioned_key.Data(), key.Data(), key.Size()) == 0) {
                // This is a regular key (non-MVCC scenario) that matches exactly
                // For regular keys, we treat them as having timestamp 0 and always visible
                if (0 <= read_ts && 0 > best_timestamp) {
                    best_timestamp = 0;
                    best_value = iter.Value();
                }
                break; // Found exact match, no need to continue
            } else {
                // Key doesn't match, continue searching
                iter.Next();
                continue;
            }
            
            iter.Next();
        }
        
        if (best_value.has_value()) {
            if (best_value->Empty()) {
                return std::nullopt; // Tombstone
            }
            return best_value;
        }
    }

    // 4. Search leveled SSTables for versioned keys
    for (const auto& [level, sst_ids] : state->levels) {
        for (size_t sst_id : sst_ids) {
            auto sst_iter = state->sstables.find(sst_id);
            if (sst_iter == state->sstables.end()) continue;
            auto sst = sst_iter->second;

            // For versioned keys, we need to check if our user key could be in this SSTable
            // Since versioned keys are user_key + timestamp, we need to be more careful with bounds
            if (key < sst->FirstKey() || key > sst->LastKey()) {
                continue;
            }
            uint32_t key_hash = Crc32c::Compute(key.Data(), key.Size());
            if (sst->Bloom() && !sst->Bloom()->MayContain(key_hash)) {
                continue;
            }
            
            // Search for versioned keys that match our user key
            auto iter = SsTableIterator::CreateAndSeekToKey(sst, Bound::Included(key));
            std::optional<ByteBuffer> best_value;
            uint64_t best_timestamp = 0;
            
            while (iter.IsValid()) {
                const ByteBuffer& versioned_key = iter.Key();
                
                // Check if this key matches our target key
                if (versioned_key.Size() >= key.Size() + sizeof(uint64_t)) {
                    // This looks like a versioned key (MVCC scenario)
                    // Compare the user key portion
                    if (std::memcmp(versioned_key.Data(), key.Data(), key.Size()) == 0) {
                        // Extract timestamp from the versioned key
                        const uint8_t* ts_data = reinterpret_cast<const uint8_t*>(versioned_key.Data()) + key.Size();
                        uint64_t timestamp = 0;
                        for (int i = 0; i < 8; ++i) {
                            timestamp = (timestamp << 8) | ts_data[i];
                        }
                        
                        // Check if this version is visible to our read timestamp and is the best so far
                        if (timestamp <= read_ts && timestamp > best_timestamp) {
                            best_timestamp = timestamp;
                            best_value = iter.Value();
                        }
                    } else {
                        // If the user key portion doesn't match, we've gone past our target key
                        break;
                    }
                } else if (versioned_key.Size() == key.Size() && 
                           std::memcmp(versioned_key.Data(), key.Data(), key.Size()) == 0) {
                    // This is a regular key (non-MVCC scenario) that matches exactly
                    // For regular keys, we treat them as having timestamp 0 and always visible
                    if (0 <= read_ts && 0 > best_timestamp) {
                        best_timestamp = 0;
                        best_value = iter.Value();
                    }
                    break; // Found exact match, no need to continue
                } else {
                    // Key doesn't match, continue searching
                    iter.Next();
                    continue;
                }
                
                iter.Next();
            }
            
            if (best_value.has_value()) {
                if (best_value->Empty()) {
                    return std::nullopt; // Tombstone
                }
                return best_value;
            }
        }
    }

    return std::nullopt;
}

bool LsmStorageInner::SyncDir() {
#ifdef __linux__
    int fd = ::open(path_.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        return false;
    }
    if (::fsync(fd) != 0) {
        ::close(fd);
        return false;
    }
    ::close(fd);
    return true;
#else
    // For non-Linux platforms, this is a no-op.
    return true;
#endif
}

bool LsmStorageInner::Compact(const SimpleLeveledCompactionTask& task) {
    // Get a snapshot of the current state
    std::shared_ptr<LsmStorageState> snapshot;
    {
        std::shared_lock<std::shared_mutex> lock(*state_mutex_);
        snapshot = state_;
    }
    
    std::cerr << "[COMPACTION] Starting compaction: upper_level="
              << (task.upper_level ? std::to_string(*task.upper_level) : "L0")
              << ", lower_level=" << task.lower_level
              << ", upper_ssts=" << task.upper_level_sst_ids.size()
              << ", lower_ssts=" << task.lower_level_sst_ids.size() << std::endl;
    
    // Collect all SSTs that need to be merged
    std::vector<std::shared_ptr<SsTable>> ssts_to_merge;
    std::vector<size_t> all_sst_ids;
    
    // Add upper level SSTs
    for (size_t sst_id : task.upper_level_sst_ids) {
        auto iter = snapshot->sstables.find(sst_id);
        if (iter != snapshot->sstables.end()) {
            ssts_to_merge.push_back(iter->second);
            all_sst_ids.push_back(sst_id);
        }
    }
    
    // Add lower level SSTs
    for (size_t sst_id : task.lower_level_sst_ids) {
        auto iter = snapshot->sstables.find(sst_id);
        if (iter != snapshot->sstables.end()) {
            ssts_to_merge.push_back(iter->second);
            all_sst_ids.push_back(sst_id);
        }
    }
    
    if (ssts_to_merge.empty()) {
        std::cerr << "[COMPACTION] No SSTs to merge, skipping compaction" << std::endl;
        return true;
    }
    
    // Create iterators for all SSTs to be merged
    std::vector<std::unique_ptr<StorageIterator>> iterators;
    for (const auto& sst : ssts_to_merge) {
        auto iter = SsTableIterator::CreateAndSeekToFirst(sst);
        if (iter.IsValid()) {
            iterators.push_back(std::make_unique<SsTableIterator>(std::move(iter)));
        }
    }
    
    if (iterators.empty()) {
        std::cerr << "[COMPACTION] All SSTs are empty, skipping compaction" << std::endl;
        return true;
    }
    
    // Create a merge iterator to combine all SST iterators
    auto merge_iter = MergeIterator(std::move(iterators));
    
    // Generate new SST ID
    size_t new_sst_id = next_sst_id_.fetch_add(1, std::memory_order_relaxed);
    
    // Create new SST file
    auto sst_path = PathOfSst(new_sst_id);
    // Start with empty data - SST builder will write the actual content
    std::vector<uint8_t> initial_data;
    auto file_object = FileObject::Create(sst_path.string(), initial_data);
    
    // Create SST builder with default block size (4KB)
    SsTableBuilder sst_builder(4096);
    
    // Merge all data from the SSTs
    size_t merged_keys = 0;
    size_t filtered_keys = 0;
    
    while (merge_iter.IsValid()) {
        const auto& key = merge_iter.Key();
        const auto& value = merge_iter.Value();
        
        // Skip tombstones if this is the bottom level
        if (task.is_lower_level_bottom_level && value.Size() == 0) {
            filtered_keys++;
            merge_iter.Next();
            continue;
        }
        
        // Add the key-value pair to the new SST
        sst_builder.Add(key, value.CopyToBytes());
        merged_keys++;
        merge_iter.Next();
    }
    
    std::cout << "[COMPACTION] Merged " << merged_keys << " keys, filtered " 
              << filtered_keys << " keys" << std::endl;
    
    // Build the new SST
    auto new_sst = sst_builder.Build(new_sst_id, block_cache_, sst_path.string());
    if (!new_sst) {
        std::cerr << "[COMPACTION] Failed to build new SST" << std::endl;
        return false;
    }
    
    // File is already synced as part of the Build process
    // No explicit sync needed here
    
    std::cerr << "[COMPACTION] Created new SST " << new_sst_id 
              << " with size " << new_sst->FileSize() << " bytes" << std::endl;
    
    // Update the storage state atomically
    {
        std::unique_lock<std::shared_mutex> lock(*state_mutex_);
        
        // Create a new state based on the current state
        auto new_state = std::make_shared<LsmStorageState>(*state_);
        
        // Remove the old SSTs from the state
        for (size_t sst_id : all_sst_ids) {
            new_state->sstables.erase(sst_id);
            
            // Remove from L0 if present
            auto l0_iter = std::find(new_state->l0_sstables.begin(), 
                                   new_state->l0_sstables.end(), sst_id);
            if (l0_iter != new_state->l0_sstables.end()) {
                new_state->l0_sstables.erase(l0_iter);
            }
            
            // Remove from levels if present
            for (auto& [level, sst_ids] : new_state->levels) {
                auto level_iter = std::find(sst_ids.begin(), sst_ids.end(), sst_id);
                if (level_iter != sst_ids.end()) {
                    sst_ids.erase(level_iter);
                }
            }
        }
        
        // Add the new SST to the appropriate level
        new_state->sstables[new_sst_id] = new_sst;
        
        if (task.lower_level == 0) {
            // Add to L0
            new_state->l0_sstables.push_back(new_sst_id);
        } else {
            // Add to the specified level
            bool level_found = false;
            for (auto& [level, sst_ids] : new_state->levels) {
                if (level == task.lower_level) {
                    sst_ids.push_back(new_sst_id);
                    level_found = true;
                    break;
                }
            }
            if (!level_found) {
                // Create new level
                new_state->levels.emplace_back(task.lower_level, std::vector<size_t>{new_sst_id});
            }
        }
        
        // Update the state atomically
        state_ = new_state;
        
        std::cerr << "[COMPACTION] Updated storage state with new SST " << new_sst_id << std::endl;
    }
    
    // Update the manifest with the compaction result
    if (!manifest_) {
        std::cerr << "[COMPACTION] Warning: No manifest available for update" << std::endl;
    } else {
        // Create manifest record for compaction
        ManifestRecord record;
        record.tag = ManifestRecordTag::kCompaction;
        record.from_level = task.upper_level ? *task.upper_level : 0; // 0 for L0
        record.from_ids = all_sst_ids; // All SSTs being compacted
        record.to_level = task.lower_level;
        record.to_ids = {new_sst_id}; // New SST created from compaction
        
        if (!manifest_->AddRecord(record)) {
            std::cerr << "[COMPACTION] Warning: Failed to update manifest" << std::endl;
        }
    }
    
    // Clean up old SST files
    for (size_t sst_id : all_sst_ids) {
        auto old_sst_path = PathOfSst(sst_id);
        if (std::filesystem::exists(old_sst_path)) {
            try {
                std::filesystem::remove(old_sst_path);
                std::cerr << "[COMPACTION] Removed old SST file: " << old_sst_path << std::endl;
            } catch (const std::exception& e) {
                std::cerr << "[COMPACTION] Warning: Failed to remove old SST file " 
                          << old_sst_path << ": " << e.what() << std::endl;
            }
        }
    }
    
    // Sync directory to ensure metadata changes are persisted
    (void)SyncDir();
    
    std::cerr << "[COMPACTION] Compaction completed successfully" << std::endl;
    return true;
}

bool LsmStorageInner::Compact() {
    // Get a snapshot of the current state
    std::shared_ptr<LsmStorageState> snapshot;
    {
        std::shared_lock<std::shared_mutex> lock(*state_mutex_);
        snapshot = state_;
    }
    
    // Use the compaction controller to determine if compaction is needed
    if (!compaction_controller_) {
        return false; // No compaction controller configured
    }
    
    // Check if compaction is needed
    if (!compaction_controller_->NeedsCompaction(*snapshot)) {
        return false; // No compaction needed
    }
    
    // Generate compaction task
    auto task_opt = compaction_controller_->GenerateCompactionTask(*snapshot);
    if (!task_opt) {
        return false; // No task generated
    }
    
    // Execute the compaction task
    return Compact(*task_opt);
}

bool LsmStorageInner::ForceFullCompaction() {
    // Allow ForceFullCompaction with any compaction strategy
    // This is needed for testing and certain manual operations
    
    // Get a snapshot of the current state
    std::shared_ptr<LsmStorageState> snapshot;
    {
        std::shared_lock<std::shared_mutex> lock(*state_mutex_);
        snapshot = state_;
    }
    
    // Create compaction task with L0 and L1 SSTs
    std::vector<size_t> l0_sstables = snapshot->l0_sstables;
    std::vector<size_t> l1_sstables;
    if (!snapshot->levels.empty() && !snapshot->levels[0].second.empty()) {
        l1_sstables = snapshot->levels[0].second;
    }
    
    // If there's nothing to compact, return early
    if (l0_sstables.empty() && l1_sstables.empty()) {
        return true;
    }
    
    // Create our SimpleLeveledCompactionTask for full compaction
    SimpleLeveledCompactionTask compaction_task;
    compaction_task.upper_level = std::nullopt; // L0 is involved
    compaction_task.upper_level_sst_ids = l0_sstables;
    compaction_task.lower_level = 0; // Level 1 (in 0-indexed levels)
    compaction_task.lower_level_sst_ids = l1_sstables;
    compaction_task.is_lower_level_bottom_level = false;
    
    // Log compaction task information
    std::cerr << "Force full compaction: L0=" << l0_sstables.size() 
              << ", L1=" << l1_sstables.size() << " SSTs" << std::endl;
    
    // Load all SSTs that need to be merged
    std::vector<std::shared_ptr<SsTable>> ssts_to_merge;
    
    // Add L0 SSTs to merge
    for (size_t sst_id : l0_sstables) {
        auto iter = snapshot->sstables.find(sst_id);
        if (iter != snapshot->sstables.end()) {
            ssts_to_merge.push_back(iter->second);
        }
    }
    
    // Add L1 SSTs to merge
    for (size_t sst_id : l1_sstables) {
        auto iter = snapshot->sstables.find(sst_id);
        if (iter != snapshot->sstables.end()) {
            ssts_to_merge.push_back(iter->second);
        }
    }
    
    // If no SSTs to merge, return early
    if (ssts_to_merge.empty()) {
        return true;
    }
    
    // Create iterators for all SSTs
    std::vector<std::unique_ptr<StorageIterator>> iterators;
    for (size_t i = 0; i < ssts_to_merge.size(); ++i) {
        const auto& sst = ssts_to_merge[i];
        auto iter = SsTableIterator::CreateAndSeekToFirst(sst);
        iterators.push_back(std::make_unique<SsTableIterator>(std::move(iter)));
    }
    
    // Create a merged iterator over all the SSTs
    auto merged_iter = std::make_unique<MergeIterator>(std::move(iterators));
    
    // Build a new SST from the merged iterator
    size_t next_sst_id = next_sst_id_++;
    auto builder = std::make_unique<SsTableBuilder>(options_->block_size);
    
    // Add all data from the merged iterator
    ByteBuffer last_key;
    bool has_last_key = false;
    size_t total_keys = 0;
    
    while (merged_iter->IsValid()) {
        total_keys++;
        
        // Skip duplicate keys (keep only the most recent version)
        if (has_last_key && merged_iter->Key() == last_key) {
            merged_iter->Next();
            continue;
        }
        
        // Process the key-value pair
        ByteBuffer value = merged_iter->Value();
        std::vector<uint8_t> value_bytes = value.CopyToBytes();
        builder->Add(merged_iter->Key(), value_bytes);
        
        // Update last key
        last_key = merged_iter->Key();
        has_last_key = true;
        
        // Move to next key
        merged_iter->Next();
    }
    
    // Build and persist the SST
    auto new_sst_path = PathOfSst(next_sst_id);
    
    // Ensure the directory exists
    auto dir_path = std::filesystem::path(new_sst_path).parent_path();
    std::filesystem::create_directories(dir_path);
    
    // Build the SST file
    auto new_sst = builder->Build(next_sst_id, block_cache_, new_sst_path);
    if (!new_sst) {
        std::cerr << "Failed to build SST during compaction: " << new_sst_path << std::endl;
        return false;
    }
    
    // Collect the new SST IDs
    std::vector<size_t> new_sst_ids;
    new_sst_ids.push_back(next_sst_id);
    
    // Lock for state modification
    std::lock_guard<std::mutex> lock(state_lock_mutex_);
    
    // Create a new state based on the current state
    auto new_state = std::make_shared<LsmStorageState>(*snapshot);
    
    // Remove the old SSTs from the state
    for (size_t sst_id : l0_sstables) {
        new_state->sstables.erase(sst_id);
    }
    for (size_t sst_id : l1_sstables) {
        new_state->sstables.erase(sst_id);
    }
    
    // Add the new SST to the state
    new_state->sstables[next_sst_id] = new_sst;
    
    // Update L0 and L1 in the state
    new_state->l0_sstables.erase(
        std::remove_if(new_state->l0_sstables.begin(), new_state->l0_sstables.end(),
            [&](size_t id) { return std::find(l0_sstables.begin(), l0_sstables.end(), id) != l0_sstables.end(); }),
        new_state->l0_sstables.end());
    
    if (new_state->levels.empty()) {
        // Initialize levels if empty
        new_state->levels.resize(1);
        new_state->levels[0] = {1, {}}; // Level 1 with empty SST list
        // Initialized levels in state
    }
    
    // Add new SST to L1
    new_state->levels[0].second = new_sst_ids;
    // Updated L1 with new SST
    
    // Atomically swap the state
    {
        std::unique_lock<std::shared_mutex> state_lock(*state_mutex_);
        state_ = new_state;
    }
    
    // Sync directory to ensure state changes are persisted
    if (!SyncDir()) {
        return false;
    }
    
    // Update manifest with compaction record
    if (manifest_) {
        // Create a manifest record for the compaction
        ManifestRecord record;
        record.tag = ManifestRecordTag::kCompaction;
        record.from_level = 0; // L0
        record.from_ids = l0_sstables;
        record.to_level = 1; // L1
        record.to_ids = new_sst_ids;
        
        // Add the record to the manifest
        if (!manifest_->AddRecord(record)) {
            return false;
        }
    }
    
    // Remove the old SST files from disk
    for (size_t sst_id : l0_sstables) {
        std::filesystem::remove(PathOfSst(sst_id));
    }
    for (size_t sst_id : l1_sstables) {
        std::filesystem::remove(PathOfSst(sst_id));
    }
    
    return true;
}

std::shared_ptr<SsTable> LsmStorageInner::BuildSstFromMemtable(std::shared_ptr<MemTable> mem) {
    SsTableBuilder builder(options_->block_size);
    mem->ForEach([&](const ByteBuffer& key, const ByteBuffer& value) {
        builder.Add(key, value.CopyToBytes());
    });

    size_t sst_id = mem->Id();
    Path sst_path = PathOfSst(sst_id);
    return builder.Build(sst_id, block_cache_, sst_path.string());
}

bool LsmStorageInner::TriggerFlush() {
    std::unique_lock<std::shared_mutex> lock(*state_mutex_);
    if (state_->imm_memtables.empty()) {
        return true;
    }

    auto flush_memtable = state_->imm_memtables.back();

    // 1. Build an SST from the memtable.
    auto sst = BuildSstFromMemtable(flush_memtable);
    if (!sst) {
        return false; // SST build failed
    }

    // 2. Update the in-memory state.
    auto new_state = state_->Clone();
    new_state->imm_memtables.pop_back(); // remove the flushed memtable
    new_state->l0_sstables.insert(new_state->l0_sstables.begin(), sst->Id());
    new_state->sstables[sst->Id()] = sst;
    state_ = std::move(new_state);

    ManifestRecord rec;
    rec.tag = ManifestRecordTag::kFlush;
    rec.single_id = sst->Id();
    (void)manifest_->AddRecord(rec);

    (void)SyncDir();
    
    return true;
}

void LsmStorageInner::ForceFreezeMemtable() {
    // Allocate a fresh memtable ID for the new active memtable.
    const int memtable_id = static_cast<int>(
        next_sst_id_.fetch_add(1, std::memory_order_relaxed));

    // Create a new memtable. If WAL is enabled, hook up the current WAL
    std::unique_ptr<MemTable> new_memtable = MemTable::Create(memtable_id);

    if (options_->enable_wal) {
        WalSegment::Options wal_options;
        wal_options.sync_on_write = options_->sync_on_write;

        if (!wal_segment_) {
            // First time – create the initial segment.
            wal_segment_ = WalSegment::Create(
                WalSegmentDir(), WalSegmentName(), wal_options);
        } else {
            // Rotate WAL segment only if necessary. For now we reuse the same
            // segment – rotation policy can be added later once needed.
        }
        new_memtable->SetWalSegment(wal_segment_);
    }

    // Atomically swap the current active memtable with the newly created one,
    // pushing the old active memtable into the immutable list.
    (void)FreezeMemtableWithMemtable(std::move(new_memtable));
}

bool LsmStorageInner::FreezeMemtableWithMemtable(std::unique_ptr<MemTable> memtable) {
    // ------------------------------------------------------------------
    // 1. Swap the active memtable with the provided one under write lock.
    //    This mirrors Rust's copy-on-write snapshot update.
    // ------------------------------------------------------------------
    std::shared_ptr<MemTable> old_memtable;
    {
        std::unique_lock<std::shared_mutex> write_lock(*state_mutex_);
        // Clone current snapshot and mutate.
        LsmStorageState snapshot_copy = *state_;

        // Perform the swap: active -> immutable list, new memtable becomes active.
        old_memtable = std::move(snapshot_copy.memtable);
        snapshot_copy.memtable = std::shared_ptr<MemTable>(std::move(memtable));
        snapshot_copy.imm_memtables.insert(snapshot_copy.imm_memtables.begin(), old_memtable);

        // Publish new snapshot.
        state_ = std::make_shared<LsmStorageState>(std::move(snapshot_copy));
    }

    // ------------------------------------------------------------------
    // 2. Sync the WAL of the former active memtable *after* releasing lock
    //    to avoid holding global mutex while doing fsync which can be slow.
    // ------------------------------------------------------------------
    if (old_memtable) {
        (void)old_memtable->SyncWal();
    }
    return true;
}

std::unique_ptr<StorageIterator> LsmStorageInner::Scan(const Bound& lower,
                                                        const Bound& upper) const noexcept {
    std::shared_ptr<LsmStorageState> snapshot; 
    {
        std::shared_lock<std::shared_mutex> lock(*state_mutex_);
        snapshot = state_;
    }
    
    // ---- Build iterators ----

    const ByteBuffer lower_key = (lower.GetType() == Bound::Type::kUnbounded) ? ByteBuffer() : *lower.Key();
    const ByteBuffer upper_key = (upper.GetType() == Bound::Type::kUnbounded) ? ByteBuffer() : *upper.Key();

    // Collect all child iterators in priority order (lower index wins on duplicate key).
    std::vector<std::unique_ptr<StorageIterator>> iters;

        // Helper to create memtable iterator
    auto make_mem_iter = [&](const std::shared_ptr<MemTable>& mt) -> std::unique_ptr<StorageIterator> {
        auto iter = std::make_unique<MemTableIterator>(
            mt->NewIterator(lower, upper));
        // For lower Bound::Excluded, skip if equal
        if (lower.GetType() == Bound::Type::kExcluded && iter->IsValid() && iter->Key() == *lower.Key()) {
            iter->Next();
        }
        return std::unique_ptr<StorageIterator>(iter.release());
    };

    // Current memtable first (highest priority)
    iters.push_back(make_mem_iter(snapshot->memtable));
    // Immutable memtables (newest to oldest)
    for (const auto& mt : snapshot->imm_memtables) {
        iters.push_back(make_mem_iter(mt));
    }

    // ---- L0 SSTables ----

    for (size_t sst_id : snapshot->l0_sstables) {
        const auto& sst = snapshot->sstables.at(sst_id);
        std::unique_ptr<SsTableIterator> tbl_iter;
        switch (lower.GetType()) {
            case Bound::Type::kIncluded:
            case Bound::Type::kExcluded:
                tbl_iter = std::make_unique<SsTableIterator>(
                    SsTableIterator::CreateAndSeekToKey(sst, lower));
                if (lower.GetType() == Bound::Type::kExcluded && tbl_iter->IsValid() && tbl_iter->Key() == *lower.Key()) {
                    tbl_iter->Next();
                }
                break;
            case Bound::Type::kUnbounded:
                tbl_iter = std::make_unique<SsTableIterator>(
                    SsTableIterator::CreateAndSeekToFirst(sst));
                break;
        }
        iters.push_back(std::unique_ptr<StorageIterator>(std::move(tbl_iter)));
    }

    // ---- Leveled SSTables (L1+) ----

    for (const auto& level_pair : snapshot->levels) {
        const auto& sst_ids = level_pair.second;
        std::vector<std::shared_ptr<SsTable>> level_ssts;
        for (size_t id : sst_ids) {
            level_ssts.push_back(snapshot->sstables.at(id));
        }

        // Build concat iterator for this level according to the lower bound.
        SstConcatIterator concat_it = [&]() -> SstConcatIterator {
            switch (lower.GetType()) {
                case Bound::Type::kIncluded:
                case Bound::Type::kExcluded: {
                    auto it = SstConcatIterator::CreateAndSeekToKey(level_ssts, lower);
                    if (lower.GetType() == Bound::Type::kExcluded && it.IsValid() && it.Key() == lower_key) {
                        it.Next();
                    }
                    return it;
                }
                case Bound::Type::kUnbounded:
                default:
                    return SstConcatIterator::CreateAndSeekToFirst(level_ssts);
            }
        }();

        iters.push_back(std::make_unique<SstConcatIterator>(std::move(concat_it)));
    }

    // Build single MergeIterator over all sources (priority defined by insertion order above)
    auto merged = std::make_unique<MergeIterator>(std::move(iters));

    // Wrap with LsmIterator to enforce upper bound and tombstone filtering
    return std::make_unique<LsmIterator<MergeIterator>>(std::move(merged), upper);
}

std::unique_ptr<StorageIterator> LsmStorageInner::ScanWithTs(const Bound& lower,
                                                            const Bound& upper,
                                                            uint64_t read_ts) const noexcept {
    std::shared_ptr<LsmStorageState> snapshot; 
    {
        std::shared_lock<std::shared_mutex> lock(*state_mutex_);
        snapshot = state_;
    }
    
    // ---- Build MVCC iterators ----

    const ByteBuffer lower_key = (lower.GetType() == Bound::Type::kUnbounded) ? ByteBuffer() : *lower.Key();
    const ByteBuffer upper_key = (upper.GetType() == Bound::Type::kUnbounded) ? ByteBuffer() : *upper.Key();

    // Collect all child MVCC iterators in priority order (lower index wins on duplicate key).
    std::vector<std::unique_ptr<StorageIterator>> mvcc_iters;

    // Helper to create MVCC memtable iterator
    auto make_mvcc_mem_iter = [&](const std::shared_ptr<MemTable>& mt) -> std::unique_ptr<StorageIterator> {
        // Convert to MvccMemTable for timestamp-aware iteration
        auto mvcc_memtable = std::make_shared<MvccMemTable>(mt);
        auto iter = std::make_unique<MvccMemTableIterator>(
            mvcc_memtable->NewIterator(lower, upper, read_ts));
            
        // For lower Bound::Excluded, skip if equal
        if (lower.GetType() == Bound::Type::kExcluded && iter->IsValid() && iter->Key() == *lower.Key()) {
            iter->Next();
        }
        return std::unique_ptr<StorageIterator>(std::move(iter));
    };

    // Current memtable first (highest priority)
    mvcc_iters.push_back(make_mvcc_mem_iter(snapshot->memtable));
    
    // Immutable memtables (newest to oldest)
    for (const auto& mt : snapshot->imm_memtables) {
        mvcc_iters.push_back(make_mvcc_mem_iter(mt));
    }

    // ---- L0 SSTables ----

    for (size_t sst_id : snapshot->l0_sstables) {
        const auto& sst = snapshot->sstables.at(sst_id);
        
        // Create MVCC-aware SSTable iterator with timestamp filtering
        auto mvcc_iter = [&]() -> std::unique_ptr<StorageIterator> {
            auto iter = std::make_unique<MvccSsTableIterator>(
                MvccSsTableIterator::Create(sst, read_ts));
                
            switch (lower.GetType()) {
                case Bound::Type::kIncluded:
                case Bound::Type::kExcluded:
                    iter->Seek(*lower.Key());
                    if (lower.GetType() == Bound::Type::kExcluded && 
                        iter->IsValid() && iter->Key() == lower_key) {
                        iter->Next();
                    }
                    break;
                case Bound::Type::kUnbounded:
                    iter->SeekToFirst();
                    break;
            }
            return iter;
        }();
        
        mvcc_iters.push_back(std::move(mvcc_iter));
    }

    // ---- Leveled SSTables (L1+) ----

    for (const auto& level_pair : snapshot->levels) {
        const auto& sst_ids = level_pair.second;
        std::vector<std::shared_ptr<SsTable>> level_ssts;
        for (size_t id : sst_ids) {
            level_ssts.push_back(snapshot->sstables.at(id));
        }

        // Create a vector of MVCC iterators for each SST in this level
        std::vector<std::unique_ptr<StorageIterator>> level_mvcc_iters;
        for (const auto& sst : level_ssts) {
            auto iter = std::make_unique<MvccSsTableIterator>(MvccSsTableIterator::Create(sst, read_ts));
            level_mvcc_iters.push_back(std::move(iter));
        }
        
        // Create an MVCC merge iterator for this level
        auto level_merge_iter = std::make_unique<MvccMergeIterator>(std::move(level_mvcc_iters));
        
        // Position the iterator according to the lower bound
        switch (lower.GetType()) {
            case Bound::Type::kIncluded:
            case Bound::Type::kExcluded:
                level_merge_iter->Seek(*lower.Key());
                if (lower.GetType() == Bound::Type::kExcluded && 
                    level_merge_iter->IsValid() && level_merge_iter->Key() == lower_key) {
                    level_merge_iter->Next();
                }
                break;
            case Bound::Type::kUnbounded:
                level_merge_iter->SeekToFirst();
                break;
        }
        
        mvcc_iters.push_back(std::move(level_merge_iter));
    }

    // Build the final MVCC merge iterator over all sources
    auto mvcc_merged = std::make_unique<MvccMergeIterator>(std::move(mvcc_iters));
    
    // Create a wrapper iterator that enforces the upper bound and filters by timestamp
    class BoundedMvccIterator : public StorageIterator {
    public:
        BoundedMvccIterator(std::unique_ptr<StorageIterator> iter, const Bound& upper, uint64_t read_ts)
            : iter_(std::move(iter)), upper_(upper), read_ts_(read_ts) {
            // Skip any keys that are outside the bounds or not visible at read_ts
            SkipInvalidKeys();
        }
        
        bool IsValid() const noexcept override {
            return iter_ && iter_->IsValid() && 
                   (upper_.GetType() == Bound::Type::kUnbounded || 
                    (upper_.GetType() == Bound::Type::kIncluded && 
                     iter_->Key() <= *upper_.Key()) || 
                    (upper_.GetType() == Bound::Type::kExcluded && 
                     iter_->Key() < *upper_.Key()));
        }
        
        void Next() noexcept override {
            if (iter_) {
                iter_->Next();
                SkipInvalidKeys();
            }
        }
        
        ByteBuffer Key() const noexcept override {
            static const ByteBuffer kEmpty;
            return IsValid() ? iter_->Key() : kEmpty;
        }
        
        const ByteBuffer& Value() const noexcept override {
            static const ByteBuffer kEmpty;
            return IsValid() ? iter_->Value() : kEmpty;
        }
        
    private:
        void SkipInvalidKeys() {
            // Skip keys that are outside upper bound
            while (iter_ && iter_->IsValid()) {
                // Check if key is within upper bound
                bool within_upper = true;
                if (upper_.GetType() != Bound::Type::kUnbounded) {
                    const auto& key = iter_->Key();
                    if (upper_.GetType() == Bound::Type::kIncluded) {
                        within_upper = key <= *upper_.Key();
                    } else { // kExcluded
                        within_upper = key < *upper_.Key();
                    }
                }
                
                // If key is within bounds, keep it
                if (within_upper) {
                    break;
                }
                
                // Otherwise move to next key
                iter_->Next();
            }
        }
        
        std::unique_ptr<StorageIterator> iter_;
        Bound upper_;
        uint64_t read_ts_;
    };
    
    // Create a bounded MVCC iterator and wrap it with MvccLsmIterator
    auto bounded_iter = std::make_unique<BoundedMvccIterator>(std::move(mvcc_merged), upper, read_ts);
    return std::make_unique<MvccLsmIterator>(std::move(bounded_iter));
}


bool LsmStorageInner::ForceFlushNextImmMemtable() {
    std::lock_guard<std::mutex> state_lock(state_lock_mutex_);

    // Get the oldest immutable memtable
    std::shared_ptr<MemTable> flush_memtable;
    {
        std::shared_lock<std::shared_mutex> guard(*state_mutex_);
        if (state_->imm_memtables.empty()) {
            // No immutable memtables to flush
            return true;
        }
        
        flush_memtable = state_->imm_memtables.back();  // Oldest is at the back
    }

    // Create SSTable builder
    SsTableBuilder builder(options_->block_size);
    
    // Add all key-value pairs from memtable to the builder
    flush_memtable->ForEach([&builder](const ByteBuffer& key, const ByteBuffer& value) {
        std::vector<uint8_t> value_bytes(value.Data(), value.Data() + value.Size());
        builder.Add(key, value_bytes);
    });
    
    // Get SST ID from memtable
    size_t sst_id = flush_memtable->Id();
    
    // Build SSTable and write to disk
    std::shared_ptr<SsTable> sst = builder.Build(
        sst_id, 
        block_cache_, 
        PathOfSst(sst_id)
    );
    
    // Update state
    {
        std::unique_lock<std::shared_mutex> guard(*state_mutex_);
        auto snapshot = state_->Clone();
        
        // Remove memtable from imm_memtables
        snapshot->imm_memtables.pop_back();  // Remove oldest
        
        // Add L0 table
        // In C++ there's no CompactionController::FlushToL0 method. We need to follow the
        // default behavior based on the compaction strategy
        snapshot->l0_sstables.insert(snapshot->l0_sstables.begin(), sst_id);
        
        std::cout << "Flushed " << sst_id << ".sst" << std::endl;
        snapshot->sstables.emplace(sst_id, sst);
        
        // Update the state
        state_ = std::move(snapshot);
    }
    
    // Remove WAL file if enabled
    if (options_->enable_wal) {
        std::filesystem::remove(PathOfWal(sst_id));
    }
    
    // Update manifest
    if (manifest_) {
        std::cout << "[DEBUG] Writing manifest flush record for SST " << sst_id << std::endl;
        ManifestRecord rec;
        rec.tag = ManifestRecordTag::kFlush;
        rec.single_id = sst_id;
        bool success = manifest_->AddRecord(rec);
        std::cout << "[DEBUG] Manifest record write " << (success ? "succeeded" : "failed") << std::endl;
    } else {
        std::cout << "[DEBUG] No manifest available for writing flush record" << std::endl;
    }
    
    // Sync directory to ensure durability
    SyncDir();
    
    return true;
}

void LsmStorageInner::TryFreeze(size_t estimated_size) {
    if (estimated_size >= options_->target_sst_size) {
        std::lock_guard<std::mutex> state_lock(state_lock_mutex_);
        
        // Double-check memtable size after acquiring lock
        std::shared_lock<std::shared_mutex> guard(*state_mutex_);
        if (state_->memtable->ApproximateSize() >= options_->target_sst_size) {
            // Drop the shared lock before calling ForceFreezeMemtable
            guard.unlock();
            ForceFreezeMemtable();
        }
    }
}

bool LsmStorageInner::Flush() {
    // First, freeze current memtable if not empty
    {
        std::shared_lock<std::shared_mutex> guard(*state_mutex_);
        if (!state_->memtable->IsEmpty()) {
            guard.unlock();
            ForceFreezeMemtable();
        }
    }
    
    // Then flush all immutable memtables
    while (true) {
        {
            std::shared_lock<std::shared_mutex> guard(*state_mutex_);
            if (state_->imm_memtables.empty()) {
                break;
            }
        }
        
        if (!ForceFlushNextImmMemtable()) {
            return false;
        }
    }
    
    return true;
}

std::shared_ptr<LsmMvccInner> LsmStorageInner::GetMvcc() const noexcept {
    return mvcc_;
}

void LsmStorageInner::SetMvcc(std::shared_ptr<LsmMvccInner> mvcc) noexcept {
    mvcc_ = std::move(mvcc);
}

} // namespace util
