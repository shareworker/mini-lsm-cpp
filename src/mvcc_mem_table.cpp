#include "../include/mvcc_mem_table.hpp"

#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <utility>
#include <iostream>

#include "../include/byte_buffer.hpp"
#include "../include/mem_table.hpp"
#include "../include/mvcc_skiplist.hpp"
#include "../include/wal.hpp"
#include "../include/mvcc_wal.hpp"


std::unique_ptr<MvccMemTable> MvccMemTable::Create(size_t id) {
    return std::unique_ptr<MvccMemTable>(new MvccMemTable(id));
}

std::unique_ptr<MvccMemTable> MvccMemTable::CreateWithWal(
    size_t id, 
    std::filesystem::path path) {
    // Create with MVCC WAL for timestamp support
    auto mvcc_wal = MvccWal::Create(path);
    if (mvcc_wal) {
        return std::unique_ptr<MvccMemTable>(new MvccMemTable(id, std::move(mvcc_wal)));
    }
    
    // Fallback to legacy WAL for backward compatibility
    auto legacy_wal = Wal::Create(path);
    if (legacy_wal) {
        return std::unique_ptr<MvccMemTable>(new MvccMemTable(id, std::move(legacy_wal)));
    }
    
    return nullptr;
}

std::unique_ptr<MvccMemTable> MvccMemTable::RecoverFromWal(
    size_t id,
    const std::filesystem::path& path) {
    // For WAL recovery, we need to use a temporary standard SkipList
    // because the WAL system is designed for ByteBuffer keys and values
    auto temp_skiplist = std::make_shared<SkipList<ByteBuffer, ByteBuffer>>();
    
    // Recover WAL into the temporary skiplist
    auto wal = Wal::Recover(path, temp_skiplist);
    if (!wal) {
        return nullptr;  // Recovery failed
    }
    
    // Create a new MVCC MemTable with the recovered WAL
    auto mem_table = std::unique_ptr<MvccMemTable>(new MvccMemTable(id, std::move(wal)));
    
    // Now convert the temporary skiplist entries to the MVCC skiplist
    // Using timestamp 0 for all recovered entries as they represent the initial state
    size_t total_size = 0;
    temp_skiplist->ForEach([&](const ByteBuffer& key, const ByteBuffer& value) {
        // Add to the MVCC skiplist with timestamp 0 (initial state)
        mem_table->skiplist_->Put(key, value, 0);
        total_size += key.Size() + value.Size() + sizeof(uint64_t);
    });
    
    mem_table->approximate_size_.store(total_size, std::memory_order_relaxed);
    
    return mem_table;
}

std::unique_ptr<MvccMemTable> MvccMemTable::CreateFromSkipList(
    size_t id, 
    std::shared_ptr<MvccSkipList> skiplist) {
    auto mem_table = std::unique_ptr<MvccMemTable>(new MvccMemTable(id));
    mem_table->skiplist_ = skiplist;
    
    // Calculate approximate size based on all entries
    size_t total_size = 0;
    skiplist->ForEach([&total_size](const ByteBuffer& key, const ByteBuffer& value, uint64_t ts) {
        total_size += key.Size() + value.Size() + sizeof(ts);
    });
    mem_table->approximate_size_.store(total_size, std::memory_order_relaxed);
    
    return mem_table;
}

MvccMemTable::MvccMemTable(size_t id)
    : id_(id), skiplist_(std::make_shared<MvccSkipList>()), wal_(nullptr) {}

MvccMemTable::MvccMemTable(size_t id, std::unique_ptr<Wal> wal)
    : id_(id), skiplist_(std::make_shared<MvccSkipList>()), wal_(std::move(wal)) {}

MvccMemTable::MvccMemTable(size_t id, std::unique_ptr<MvccWal> mvcc_wal)
    : id_(id), skiplist_(std::make_shared<MvccSkipList>()), mvcc_wal_(std::move(mvcc_wal)), wal_(nullptr) {}
    
MvccMemTable::MvccMemTable(const std::shared_ptr<MemTable>& memtable)
    : id_(memtable->Id()), skiplist_(std::make_shared<MvccSkipList>()), wal_(nullptr) {
    
    // Convert the non-MVCC memtable data to MVCC format
    // For existing data, assume a default timestamp of 0 (earliest possible)
    // This allows any MVCC reader to see these values regardless of read timestamp
    const uint64_t kDefaultTs = 0;
    
    // Iterate through the memtable and add each item to our MVCC skiplist
    memtable->ForEach([this, kDefaultTs](const ByteBuffer& key, const ByteBuffer& value) {
        this->skiplist_->Put(key, value, kDefaultTs);
        this->approximate_size_.fetch_add(
            key.Size() + value.Size() + sizeof(kDefaultTs),
            std::memory_order_relaxed
        );
        return true; // Continue iteration
    });
}

std::unique_ptr<MvccMemTable> MvccMemTable::CreateFromMemTable(
    const std::shared_ptr<MemTable>& memtable) {
    return std::unique_ptr<MvccMemTable>(new MvccMemTable(memtable));
}

bool MvccMemTable::Put(const ByteBuffer& key, const ByteBuffer& value, uint64_t ts) {
    size_t entry_size = key.Size() + value.Size() + sizeof(ts);
    
    // First log the write to MVCC WAL if enabled
    if (mvcc_wal_) {
        // Use MVCC WAL that supports timestamp persistence
        if (!mvcc_wal_->PutWithTs(key, value, ts)) {
            std::cerr << "[MVCC-MEM] Failed to write to MVCC WAL" << std::endl;
            return false;
        }
    }
    // Fallback to legacy WAL for backward compatibility
    else if (wal_) {
        if (!wal_->Put(key, value)) {
            std::cerr << "[MVCC-MEM] Failed to write to legacy WAL" << std::endl;
            return false;
        }
    }
    
    // Then insert into skiplist
    bool result = skiplist_->Put(key, value, ts);
    
    // Update approximate size on success
    if (result) {
        approximate_size_.fetch_add(entry_size, std::memory_order_relaxed);
    }
    
    return result;
}

std::optional<ByteBuffer> MvccMemTable::GetWithTs(const ByteBuffer& key, uint64_t read_ts) {
    auto result = skiplist_->GetWithTs(key, read_ts);
    if (result) {
        return result->first;
    }
    return std::nullopt;
}

std::optional<std::pair<ByteBuffer, uint64_t>> MvccMemTable::GetWithTsInfo(
    const ByteBuffer& key, uint64_t read_ts) {
    return skiplist_->GetWithTs(key, read_ts);
}

bool MvccMemTable::Remove(const ByteBuffer& key, uint64_t ts) {
    size_t entry_size = key.Size() + sizeof(ts);
    
    // First log the removal to MVCC WAL if enabled
    if (mvcc_wal_) {
        // Use MVCC WAL that supports timestamp persistence for deletes
        if (!mvcc_wal_->DeleteWithTs(key, ts)) {
            std::cerr << "[MVCC-MEM] Failed to write delete to MVCC WAL" << std::endl;
            return false;
        }
    }
    // Fallback to legacy WAL for backward compatibility
    else if (wal_) {
        // For removals in legacy WAL, we insert a tombstone (empty value)
        // The WAL doesn't support timestamps directly, so this is an approximation
        ByteBuffer empty_value;
        if (!wal_->Put(key, empty_value)) {
            std::cerr << "[MVCC-MEM] Failed to write delete to legacy WAL" << std::endl;
            return false;
        }
    }
    
    // Then insert tombstone into skiplist
    bool result = skiplist_->Remove(key, ts);
    
    // Update approximate size on success
    if (result) {
        approximate_size_.fetch_add(entry_size, std::memory_order_relaxed);
    }
    
    return result;
}

size_t MvccMemTable::ApproximateSize() const {
    return approximate_size_.load(std::memory_order_relaxed);
}

size_t MvccMemTable::Id() const {
    return id_;
}

bool MvccMemTable::IsEmpty() const {
    return skiplist_->IsEmpty();
}

void MvccMemTable::SetWalSegment(const std::shared_ptr<WalSegment>& wal_segment) {
    wal_segment_ = wal_segment;
}

bool MvccMemTable::SyncWal() {
    if (wal_) {
        return wal_->Sync();
    }
    return true;  // No WAL, nothing to sync
}

// Implementation of MvccMemTableIterator
MvccMemTableIterator MvccMemTable::NewIterator(const Bound& lower_bound,
                                             const Bound& upper_bound,
                                             uint64_t read_ts) {
    return MvccMemTableIterator(skiplist_, lower_bound, upper_bound, read_ts);
}

MvccMemTableIterator::MvccMemTableIterator(
    std::shared_ptr<MvccSkipList> skiplist,
    const Bound& lower,
    const Bound& upper,
    uint64_t read_ts) noexcept
    : skiplist_(skiplist), items_(), index_(0) {
    
    // Track the latest visible version for each key
    std::map<ByteBuffer, std::pair<ByteBuffer, uint64_t>> latest_versions;
    
    // Collect all visible keys within the bounds at read_ts
    skiplist_->GetSkipList()->ForEach([&](const KeyTs& composite_key, const ByteBuffer& value) {
        const ByteBuffer& key = composite_key.Key();
        uint64_t ts = composite_key.Timestamp();
        
        // Skip if timestamp is not visible at read_ts
        if (ts > read_ts) {
            return;
        }
        
        // Skip if outside lower bound
        if (!lower.Contains(key)) {
            return;
        }
        
        // Stop if beyond upper bound
        if (!upper.Contains(key)) {
            return;
        }
        
        // Check if we've seen this key before and if this is a newer version (but still visible)
        auto it = latest_versions.find(key);
        if (it == latest_versions.end()) {
            // First time seeing this key
            if (!value.Empty()) {  // Skip tombstones
                latest_versions[key.Clone()] = {value.Clone(), ts};
            }
        } else if (ts > it->second.second) {
            // This is a newer version of a key we've already seen
            if (!value.Empty()) {  // Skip tombstones
                it->second = {value.Clone(), ts};
            } else {
                // This is a tombstone for this key, so remove it from our map
                latest_versions.erase(it);
            }
        }
    });
    
    // Convert the map of latest versions to the item vector
    for (const auto& [key, value_ts_pair] : latest_versions) {
        items_.emplace_back(key.Clone(), value_ts_pair.first.Clone());
    }
}

