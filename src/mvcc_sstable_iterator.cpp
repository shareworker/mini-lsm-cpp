#include "../include/mvcc_sstable_iterator.hpp"

#include <memory>
#include <utility>
#include <cstdint>


MvccSsTableIterator MvccSsTableIterator::Create(
    std::shared_ptr<SsTable> sstable,
    uint64_t read_ts) {
    return MvccSsTableIterator(
        std::move(sstable),
        Bound::Unbounded(),
        Bound::Unbounded(),
        read_ts);
}

MvccSsTableIterator MvccSsTableIterator::CreateWithBounds(
    std::shared_ptr<SsTable> sstable,
    const Bound& lower_bound,
    const Bound& upper_bound,
    uint64_t read_ts) {
    return MvccSsTableIterator(
        std::move(sstable),
        lower_bound,
        upper_bound,
        read_ts);
}

MvccSsTableIterator::MvccSsTableIterator(
    std::shared_ptr<SsTable> sstable,
    const Bound& lower_bound,
    const Bound& upper_bound,
    uint64_t read_ts)
    : sstable_(std::move(sstable)),
      read_ts_(read_ts),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound) {
    
    // If we have no blocks, iterator is invalid
    if (sstable_->NumOfBlocks() == 0) {
        return;
    }
    
    // Start with the first block
    current_block_idx_ = 0;
    current_block_ = sstable_->ReadBlockCached(current_block_idx_);
    
    // If lower bound is unbounded, start at beginning of first block
    if (lower_bound_.GetType() == Bound::Type::kUnbounded) {
        block_iter_ = BlockIterator::CreateAndSeekToFirst(current_block_);
    } else {
        // Otherwise, seek to the lower bound
        // We need to include the timestamp in the search key for proper ordering
        // We use UINT64_MAX to find the newest version of the key
        block_iter_ = BlockIterator::CreateAndSeekToKey(
            current_block_,
            lower_bound_.Key().value());
    }
    
    // Find the first valid entry (respecting timestamp)
    FindNextValidEntry();
}

bool MvccSsTableIterator::IsValid() const noexcept {
    return !current_key_.Empty();
}

void MvccSsTableIterator::Next() noexcept {
    // Skip to next key-timestamp pair
    block_iter_.Next();
    
    // Find the next valid entry
    FindNextValidEntry();
}

ByteBuffer MvccSsTableIterator::Key() const noexcept {
    return IsValid() ? current_key_ : empty_buffer_;
}

const ByteBuffer& MvccSsTableIterator::Value() const noexcept {
    return IsValid() ? current_value_ : empty_buffer_;
}

KeyTs MvccSsTableIterator::ExtractKeyTs(const ByteBuffer& raw_key) const {
    // Format: <user_key><timestamp>
    // Extract the timestamp (last 8 bytes)
    size_t key_size = raw_key.Size();
    if (key_size < sizeof(uint64_t)) {
        // Malformed key, return empty KeyTs
        return KeyTs();
    }
    
    // Extract user key (everything except the last 8 bytes)
    size_t user_key_size = key_size - sizeof(uint64_t);
    ByteBuffer user_key(raw_key.Data(), user_key_size);
    
    // Extract timestamp (last 8 bytes)
    uint64_t ts = 0;
    const auto* ts_ptr = raw_key.Data() + user_key_size;
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        ts = (ts << 8) | ts_ptr[i];
    }
    
    return KeyTs(std::move(user_key), ts);
}

ByteBuffer MvccSsTableIterator::ExtractUserKey(const ByteBuffer& raw_key) const {
    // Format: <user_key><timestamp>
    size_t key_size = raw_key.Size();
    if (key_size < sizeof(uint64_t)) {
        return raw_key.Clone();  // Malformed key, return as is
    }
    
    // Extract user key (everything except the last 8 bytes)
    size_t user_key_size = key_size - sizeof(uint64_t);
    return ByteBuffer(raw_key.Data(), user_key_size);
}

void MvccSsTableIterator::FindNextValidEntry() noexcept {
    // Clear current state
    current_key_ = ByteBuffer();
    current_value_ = ByteBuffer();
    
    // Loop until we find a valid entry or exhaust all blocks
    while (true) {
        // Check if current block iterator is valid
        if (!block_iter_.IsValid()) {
            // Move to the next block if available
            if (current_block_idx_ + 1 < sstable_->NumOfBlocks()) {
                current_block_idx_++;
                current_block_ = sstable_->ReadBlockCached(current_block_idx_);
                block_iter_ = BlockIterator::CreateAndSeekToFirst(current_block_);
                continue;
            } else {
                // No more blocks
                return;
            }
        }
        
        // Extract key and timestamp from raw key
        const ByteBuffer& raw_key = block_iter_.Key();
        KeyTs key_ts = ExtractKeyTs(raw_key);
        ByteBuffer user_key = key_ts.Key();
        uint64_t ts = key_ts.Timestamp();
        
        // Check if we've moved past the upper bound
        if (upper_bound_.GetType() != Bound::Type::kUnbounded && user_key > upper_bound_.Key().value()) {
            return;  // Beyond upper bound, stop iteration
        }
        
        // Apply MVCC timestamp filtering - only versions <= read_ts are visible
        if (ts <= read_ts_) {
            // For a new key or if this is the first key we've seen
            if (!has_last_key_ || user_key != last_user_key_) {
                // This is the newest version of this key that's visible to us
                current_key_ = user_key.Clone();
                current_value_ = block_iter_.Value().Clone();
                
                // Remember this key so we skip older versions
                last_user_key_ = user_key.Clone();
                has_last_key_ = true;
                return;
            }
            // Skip older versions of the same key - we already found the newest visible version
        }
        
        // Move to the next entry in the current block
        block_iter_.Next();
    }
}

void MvccSsTableIterator::Seek(const ByteBuffer& key) {
    // Reset current state
    current_key_ = ByteBuffer();
    current_value_ = ByteBuffer();
    has_last_key_ = false;
    last_user_key_ = ByteBuffer();
    
    // If the SSTable has no blocks, iterator becomes invalid
    if (sstable_->NumOfBlocks() == 0) {
        return;
    }
    
    // Perform binary search to find the block that may contain the key
    size_t block_idx = sstable_->FindBlockIdx(key);
    
    // Load the target block
    current_block_idx_ = block_idx;
    current_block_ = sstable_->ReadBlockCached(current_block_idx_);
    
    // Seek within the block to find the key
    block_iter_ = BlockIterator::CreateAndSeekToKey(current_block_, key);
    
    // Find the first valid entry respecting MVCC timestamp rules
    FindNextValidEntry();
}

void MvccSsTableIterator::SeekToFirst() {
    // Reset current state
    current_key_ = ByteBuffer();
    current_value_ = ByteBuffer();
    has_last_key_ = false;
    last_user_key_ = ByteBuffer();
    
    // If the SSTable has no blocks, iterator becomes invalid
    if (sstable_->NumOfBlocks() == 0) {
        return;
    }
    
    // Start with the first block
    current_block_idx_ = 0;
    current_block_ = sstable_->ReadBlockCached(current_block_idx_);
    
    // Position at the beginning of the first block
    block_iter_ = BlockIterator::CreateAndSeekToFirst(current_block_);
    
    // Find the first valid entry respecting MVCC timestamp rules
    FindNextValidEntry();
}

uint64_t MvccSsTableIterator::Timestamp() const noexcept {
    if (!IsValid()) {
        return 0;  // Invalid iterator
    }
    
    // The current raw key in the block iterator contains the timestamp
    const ByteBuffer& raw_key = block_iter_.Key();
    KeyTs key_ts = ExtractKeyTs(raw_key);
    return key_ts.Timestamp();
}

