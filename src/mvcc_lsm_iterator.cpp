#include "../include/mvcc_lsm_iterator.hpp"

#include "../include/mvcc_mem_table.hpp"
#include "../include/mvcc_sstable_iterator.hpp"
#include "../include/mvcc_merge_iterator.hpp"
#include "../include/sstable.hpp"

#include <memory>
#include <vector>
#include <utility>


MvccLsmIterator MvccLsmIterator::Create(
    std::shared_ptr<MvccMemTable> memtable,
    const std::vector<std::shared_ptr<MvccMemTable>>& imm_memtables,
    const std::vector<std::shared_ptr<SsTable>>& l0_sstables,
    const std::vector<std::shared_ptr<SsTable>>& leveled_sstables,
    const Bound& lower_bound,
    const Bound& upper_bound,
    uint64_t read_ts) {
    
    // Create iterators for all components
    auto iters = CreateComponentIterators(
        memtable, 
        imm_memtables, 
        l0_sstables, 
        leveled_sstables, 
        lower_bound, 
        upper_bound, 
        read_ts);
    
    // If no valid iterators, return an empty iterator
    if (iters.empty()) {
        return MvccLsmIterator(nullptr);
    }
    
    // If only one iterator, return it directly
    if (iters.size() == 1) {
        return MvccLsmIterator(std::move(iters[0]), read_ts);
    }
    
    // Otherwise, create a merge iterator for all components
    return MvccLsmIterator(std::make_unique<MvccMergeIterator>(
        MvccMergeIterator::Create(std::move(iters), read_ts)), read_ts);
}

MvccLsmIterator::MvccLsmIterator(std::unique_ptr<StorageIterator> iter, uint64_t read_ts)
    : iter_(std::move(iter)), read_ts_(read_ts) {
    // Clear any existing visited keys to ensure we start fresh
    visited_keys_.clear();
    
    // Find the first visible version
    SeekToVisibleVersion();
}

MvccLsmIterator::MvccLsmIterator(std::unique_ptr<StorageIterator> iter)
    : iter_(std::move(iter)), read_ts_(UINT64_MAX) {
    // Default constructor uses maximum timestamp to see all versions
    // Still clear the visited keys map to ensure we start fresh
    visited_keys_.clear();
}

bool MvccLsmIterator::IsValid() const noexcept {
    return iter_ && iter_->IsValid();
}

namespace {

// Helper method to extract the user key from a versioned key
// Format: [user_key_bytes][8_bytes_timestamp]
ByteBuffer ExtractUserKey(const ByteBuffer& versioned_key) {
    if (versioned_key.Size() < 8) {
        // Not a valid versioned key, return as is
        return versioned_key.Clone();
    }
    
    // Extract all bytes except the last 8 (timestamp)
    size_t user_key_size = versioned_key.Size() - 8;
    std::vector<uint8_t> user_key_data;
    user_key_data.reserve(user_key_size);
    
    const char* data = versioned_key.Data();
    for (size_t i = 0; i < user_key_size; i++) {
        user_key_data.push_back(static_cast<uint8_t>(data[i]));
    }
    
    return ByteBuffer(user_key_data);
}

// Helper method to extract the timestamp from a versioned key
// Format: [user_key_bytes][8_bytes_inverted_timestamp]
uint64_t ExtractTimestamp(const ByteBuffer& versioned_key) {
    if (versioned_key.Size() < 8) {
        // Not a valid versioned key, return max timestamp
        return UINT64_MAX;
    }
    
    // Extract the inverted timestamp from the last 8 bytes
    uint64_t inverted_ts = 0;
    const char* data = versioned_key.Data();
    size_t offset = versioned_key.Size() - 8;
    
    // Convert from big-endian
    // The timestamp was stored with most significant byte first
    for (size_t i = 0; i < 8; i++) {
        inverted_ts = (inverted_ts << 8) | static_cast<uint8_t>(data[offset + i]);
    }
    
    // Convert back from inverted timestamp to original timestamp
    uint64_t original_ts = UINT64_MAX - inverted_ts;
    return original_ts;
}

} // anonymous namespace

void MvccLsmIterator::SeekToVisibleVersion() {
    // Skip versions that are not visible to this transaction's read timestamp
    // or have already been processed for the same user key
    
    while (IsValid()) {
        // Extract the current key's timestamp
        const ByteBuffer& current_key = iter_->Key();
        uint64_t ts = ExtractTimestamp(current_key);
        
        // Extract the user key portion (without timestamp)
        ByteBuffer user_key = ExtractUserKey(current_key);
        
        // Check if this version is visible (ts <= read_ts_)
        // and if we haven't already processed this user key
        if (ts <= read_ts_ && visited_keys_.find(user_key) == visited_keys_.end()) {
            // This is a visible version we haven't seen yet
            // Mark this user key as visited
            visited_keys_[user_key] = true;
            // Found visible version, return immediately
            return;
        }
        
        // This version is not visible or we've already seen this user key
        // Move to the next key
        iter_->Next();
    }
}

void MvccLsmIterator::Next() noexcept {
    if (IsValid()) {
        // Remember the current user key before moving
        ByteBuffer current_user_key = ExtractUserKey(iter_->Key());
        
        // Add the current key to visited keys to avoid returning it again
        visited_keys_[current_user_key] = true;
        
        // Move to the next key
        iter_->Next();
        
        // Find the next visible version
        // We don't clear visited_keys_ here to ensure we don't see duplicate user keys
        // This is critical for MVCC semantics - we only want the latest version of each key
        SeekToVisibleVersion();
    }
}

ByteBuffer MvccLsmIterator::Key() const noexcept {
    if (!IsValid()) return empty_buffer_;
    
    // Get the versioned key from the underlying iterator
    ByteBuffer versioned_key = iter_->Key();
    
    // Extract user key (strip timestamp)
    // MVCC keys are stored as [user_key][8_byte_timestamp]
    if (versioned_key.Size() > sizeof(uint64_t)) {
        return ByteBuffer(versioned_key.Data(), versioned_key.Size() - sizeof(uint64_t));
    }
    
    // If key is too short to contain timestamp, return as-is
    return versioned_key;
}

const ByteBuffer& MvccLsmIterator::Value() const noexcept {
    return IsValid() ? iter_->Value() : empty_buffer_;
}

std::vector<std::unique_ptr<StorageIterator>> MvccLsmIterator::CreateComponentIterators(
    std::shared_ptr<MvccMemTable> memtable,
    const std::vector<std::shared_ptr<MvccMemTable>>& imm_memtables,
    const std::vector<std::shared_ptr<SsTable>>& l0_sstables,
    const std::vector<std::shared_ptr<SsTable>>& leveled_sstables,
    const Bound& lower_bound,
    const Bound& upper_bound,
    uint64_t read_ts) {
    
    std::vector<std::unique_ptr<StorageIterator>> iters;
    
    // 1. Add iterator for active memtable
    if (memtable) {
        auto mem_iter = std::make_unique<MvccMemTableIterator>(
            memtable->NewIterator(lower_bound, upper_bound, read_ts));
        if (mem_iter->IsValid()) {
            iters.push_back(std::move(mem_iter));
        }
    }
    
    // 2. Add iterators for immutable memtables (from newest to oldest)
    for (const auto& imm : imm_memtables) {
        auto imm_iter = std::make_unique<MvccMemTableIterator>(
            imm->NewIterator(lower_bound, upper_bound, read_ts));
        if (imm_iter->IsValid()) {
            iters.push_back(std::move(imm_iter));
        }
    }
    
    // 3. Add iterators for L0 SSTables (from newest to oldest)
    for (const auto& sst : l0_sstables) {
        auto sst_iter = std::make_unique<MvccSsTableIterator>(
            MvccSsTableIterator::CreateWithBounds(sst, lower_bound, upper_bound, read_ts));
        if (sst_iter->IsValid()) {
            iters.push_back(std::move(sst_iter));
        }
    }
    
    // 4. Add iterators for leveled SSTables
    for (const auto& sst : leveled_sstables) {
        // Only consider SSTables that may contain keys in our range
        if ((lower_bound.GetType() != Bound::Type::kUnbounded && sst->LastKey() < *lower_bound.Key()) ||
            (upper_bound.GetType() != Bound::Type::kUnbounded && sst->FirstKey() > *upper_bound.Key())) {
            continue;
        }
        
        auto sst_iter = std::make_unique<MvccSsTableIterator>(
            MvccSsTableIterator::CreateWithBounds(sst, lower_bound, upper_bound, read_ts));
        if (sst_iter->IsValid()) {
            iters.push_back(std::move(sst_iter));
        }
    }
    
    return iters;
}

