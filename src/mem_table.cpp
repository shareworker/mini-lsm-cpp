#include "../include/mem_table.hpp"

#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <utility>

#include "../include/byte_buffer.hpp"
#include "../include/skiplist.hpp"
#include "../include/wal.hpp"



std::unique_ptr<MemTable> MemTable::Create(size_t id) {
    return std::unique_ptr<MemTable>(new MemTable(id));
}

std::unique_ptr<MemTable> MemTable::CreateWithWal(
    size_t id, 
    std::filesystem::path path) {
    return std::unique_ptr<MemTable>(new MemTable(id, Wal::Create(path)));
}

std::unique_ptr<MemTable> MemTable::RecoverFromWal(
    size_t id,
    const std::filesystem::path& path) {
    // Create a new skiplist to populate with recovered data
    auto skiplist = std::make_shared<SkipList<ByteBuffer, ByteBuffer>>();
    
    // Recover WAL and populate the skiplist
    auto wal = Wal::Recover(path, skiplist);
    if (!wal) {
        return nullptr;  // Recovery failed
    }
    
    // Create a new MemTable with the recovered WAL
    auto mem_table = std::unique_ptr<MemTable>(new MemTable(id, std::move(wal)));
    
    // Replace the default skiplist with our recovered one
    mem_table->skiplist_ = skiplist;
    
    // Calculate approximate size based on recovered data
    size_t total_size = 0;
    skiplist->ForEach([&total_size](const ByteBuffer& key, const ByteBuffer& value) {
        total_size += key.Size() + value.Size();
    });
    mem_table->approximate_size_.store(total_size, std::memory_order_relaxed);
    
    return mem_table;
}

std::unique_ptr<MemTable> MemTable::CreateFromSkipList(
    size_t id,
    std::shared_ptr<SkipList<ByteBuffer, ByteBuffer>> skiplist) {
    // Create a new MemTable with the given ID
    auto mem_table = std::unique_ptr<MemTable>(new MemTable(id));
    
    // Replace the default skiplist with the provided one
    mem_table->skiplist_ = skiplist;
    
    // Calculate approximate size based on the skiplist data
    size_t total_size = 0;
    skiplist->ForEach([&total_size](const ByteBuffer& key, const ByteBuffer& value) {
        total_size += key.Size() + value.Size();
    });
    mem_table->approximate_size_.store(total_size, std::memory_order_relaxed);
    
    return mem_table;
}

MemTable::MemTable(size_t id, std::unique_ptr<Wal> wal)
    : skiplist_(std::make_shared<SkipList<ByteBuffer, ByteBuffer>>()),
      wal_(std::move(wal)),
      wal_segment_(nullptr),
      id_(id),
      approximate_size_(0) {}

bool MemTable::Put(const ByteBuffer& key, const ByteBuffer& value) {
    // Write to WAL first (if enabled) for durability
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
    
    // Calculate size of key and value
    size_t entry_size = key.Size() + value.Size();
    
    // Insert into skiplist
    bool result = skiplist_->Insert(key, value);
    
    // Update approximate size if insertion was successful
    if (result) {
        approximate_size_.fetch_add(entry_size, std::memory_order_relaxed);
    }
    
    return result;
}

std::optional<ByteBuffer> MemTable::Get(const ByteBuffer& key) const {
    return skiplist_->Find(key);
}

std::optional<std::pair<ByteBuffer, uint64_t>> MemTable::GetWithTs(const ByteBuffer& key, uint64_t read_ts) const {
    // Search for versioned keys that match our user key
    // Versioned keys are stored as: user_key + 8-byte timestamp (big-endian)
    
    std::cout << "[DEBUG] MemTable::GetWithTs: key=" << std::string(reinterpret_cast<const char*>(key.Data()), key.Size()) 
              << ", read_ts=" << read_ts << std::endl;
    
    std::optional<ByteBuffer> best_value;
    uint64_t best_timestamp = 0;
    int total_keys = 0;
    int matching_keys = 0;
    
    // We need to scan through all keys to find versioned keys that match our user key
    skiplist_->ForEach([&](const ByteBuffer& versioned_key, const ByteBuffer& value) {
        total_keys++;
        
        // Debug: print all keys in the memtable
        std::cout << "[DEBUG] Checking versioned key: size=" << versioned_key.Size() << ", data=";
        for (size_t i = 0; i < std::min(versioned_key.Size(), size_t(20)); ++i) {
            std::cout << std::hex << static_cast<int>(reinterpret_cast<const uint8_t*>(versioned_key.Data())[i]) << " ";
        }
        std::cout << std::dec << std::endl;
        
        // Check if this versioned key matches our user key
        if (versioned_key.Size() >= key.Size() + sizeof(uint64_t)) {
            // Compare the user key portion
            if (std::memcmp(versioned_key.Data(), key.Data(), key.Size()) == 0) {
                matching_keys++;
                
                // Extract timestamp from the versioned key
                const uint8_t* ts_data = reinterpret_cast<const uint8_t*>(versioned_key.Data()) + key.Size();
                uint64_t inverted_timestamp = 0;
                for (int i = 0; i < 8; ++i) {
                    inverted_timestamp = (inverted_timestamp << 8) | ts_data[i];
                }
                
                // Decode the inverted timestamp back to original timestamp
                uint64_t timestamp = UINT64_MAX - inverted_timestamp;
                
                std::cout << "[DEBUG] Found matching key with inverted_timestamp=" << inverted_timestamp
                          << ", decoded_timestamp=" << timestamp << ", read_ts=" << read_ts << std::endl;
                
                // Check if this version is visible to our read timestamp and is the best so far
                if (timestamp <= read_ts && timestamp > best_timestamp) {
                    best_timestamp = timestamp;
                    best_value = value;
                    std::cout << "[DEBUG] Updated best match: timestamp=" << timestamp << std::endl;
                }
            }
        }
    });
    
    std::cout << "[DEBUG] MemTable scan complete: total_keys=" << total_keys << ", matching_keys=" << matching_keys 
              << ", best_timestamp=" << best_timestamp << std::endl;
    
    if (best_value.has_value()) {
        return std::make_pair(*best_value, best_timestamp);
    }
    
    return std::nullopt;
}

bool MemTable::Remove(const ByteBuffer& key) {
    // Check if key exists first
    auto value_opt = Get(key);
    if (!value_opt) {
        return false;
    }
    
    // Log tombstone (empty value) to WAL before removal so that deletes are
    // durable and can be replayed during recovery.
    if (wal_segment_) {
        // Use segmented WAL if available
        if (!wal_segment_->Put(key, ByteBuffer())) {
            return false;  // WAL failed – preserve consistency by aborting
        }
    } else if (wal_) {
        // Fall back to legacy WAL
        if (!wal_->Put(key, ByteBuffer())) {
            return false;  // WAL failed – preserve consistency by aborting
        }
    }

    // Remove from skiplist (or insert tombstone depending on design).  We keep
    // the simple remove semantics here because higher layers treat a missing
    // key as a tombstone.
    bool result = skiplist_->Remove(key);
    
    // Update approximate size if removal was successful
    if (result) {
        size_t entry_size = key.Size() + value_opt->Size();
        approximate_size_.fetch_sub(entry_size, std::memory_order_relaxed);
    }
    
    return result;
}

size_t MemTable::ApproximateSize() const {
    return approximate_size_.load(std::memory_order_relaxed);
}

size_t MemTable::Id() const {
    return id_;
}

bool MemTable::IsEmpty() const {
    return skiplist_->IsEmpty();
}

// -------------------- WAL Sync Implementation --------------------

void MemTable::SetWalSegment(const std::shared_ptr<WalSegment>& wal_segment) {
    wal_segment_ = wal_segment;
}

bool MemTable::SyncWal() {
    if (wal_segment_) {
        // Use segmented WAL if available
        return wal_segment_->Sync();
    } else if (wal_) {
        // Fall back to legacy WAL
        return wal_->Sync();
    }
    return true;  // WAL disabled – trivially successful.
}

// -------------------- Iterator Implementation --------------------

MemTableIterator::MemTableIterator(std::shared_ptr<SkipList<ByteBuffer, ByteBuffer>> map,
                                   const Bound& lower,
                                   const Bound& upper) noexcept
    : map_(std::move(map)), items_(), index_(0) {
    if (!map_) {
        return;
    }

    map_->ForEach([&](const ByteBuffer &key, const ByteBuffer &value) {
        bool in_lower = false;
        switch (lower.GetType()) {
            case Bound::Type::kUnbounded:
                in_lower = true;
                break;
            case Bound::Type::kIncluded:
                // key >= lower.Key()
                in_lower = !(key < *lower.Key());
                break;
            case Bound::Type::kExcluded:
                // key > lower.Key()
                in_lower = (*lower.Key() < key);
                break;
        }

        bool in_upper = false;
        switch (upper.GetType()) {
            case Bound::Type::kUnbounded:
                in_upper = true;
                break;
            case Bound::Type::kIncluded:
                // key <= upper.Key()
                in_upper = !(*upper.Key() < key);
                break;
            case Bound::Type::kExcluded:
                // key < upper.Key()
                in_upper = (key < *upper.Key());
                break;
        }

        if (in_lower && in_upper) {
            items_.emplace_back(key, value);
        }
    });
}

MemTableIterator MemTable::NewIterator(const Bound& lower_bound,
                                          const Bound& upper_bound) const {
    return MemTableIterator(skiplist_, lower_bound, upper_bound);
}

