#include "../include/mvcc_two_merge_iterator.hpp"
#include <cstdint>
#include <memory>
#include <utility>

namespace util {

MvccTwoMergeIterator MvccTwoMergeIterator::Create(
    std::unique_ptr<StorageIterator> iter1,
    std::unique_ptr<StorageIterator> iter2,
    uint64_t read_ts) {
    return MvccTwoMergeIterator(std::move(iter1), std::move(iter2), read_ts);
}

MvccTwoMergeIterator::MvccTwoMergeIterator(
    std::unique_ptr<StorageIterator> iter1,
    std::unique_ptr<StorageIterator> iter2,
    uint64_t read_ts)
    : iter1_(std::move(iter1)),
      iter2_(std::move(iter2)),
      read_ts_(read_ts),
      current_(0) {
    // Initialize by selecting the correct iterator
    UpdateCurrent();
}

bool MvccTwoMergeIterator::IsValid() const noexcept {
    return current_ != 0;
}

void MvccTwoMergeIterator::Next() noexcept {
    if (!IsValid()) {
        return;
    }
    
    // Remember the current key to avoid duplicates
    ByteBuffer current_user_key;
    if (current_ == 1) {
        current_user_key = ExtractUserKey(iter1_->Key());
    } else {
        current_user_key = ExtractUserKey(iter2_->Key());
    }
    
    // Move the current iterator forward
    ForwardCurrent();
    
    // Skip any duplicates of the same key (lower timestamp versions)
    while (IsValid()) {
        ByteBuffer next_user_key;
        if (current_ == 1) {
            next_user_key = ExtractUserKey(iter1_->Key());
        } else {
            next_user_key = ExtractUserKey(iter2_->Key());
        }
        
        // If we encounter a different key, we're done
        if (next_user_key != current_user_key) {
            break;
        }
        
        // Otherwise, skip this duplicate key (older version)
        ForwardCurrent();
    }
    
    // Update our last seen key
    if (IsValid()) {
        ByteBuffer new_key;
        if (current_ == 1) {
            new_key = ExtractUserKey(iter1_->Key());
        } else {
            new_key = ExtractUserKey(iter2_->Key());
        }
        last_key_ = std::move(new_key);
        has_last_key_ = true;
    } else {
        has_last_key_ = false;
    }
}

ByteBuffer MvccTwoMergeIterator::Key() const noexcept {
    static const ByteBuffer kEmpty;
    if (current_ == 1) {
        return ExtractUserKey(iter1_->Key());
    } else if (current_ == 2) {
        return ExtractUserKey(iter2_->Key());
    }
    return kEmpty;
}

const ByteBuffer& MvccTwoMergeIterator::Value() const noexcept {
    static const ByteBuffer kEmpty;
    if (current_ == 1) {
        return iter1_->Value();
    } else if (current_ == 2) {
        return iter2_->Value();
    }
    return kEmpty;
}

void MvccTwoMergeIterator::ForwardCurrent() noexcept {
    if (current_ == 1) {
        iter1_->Next();
    } else if (current_ == 2) {
        iter2_->Next();
    }
    UpdateCurrent();
}

uint64_t MvccTwoMergeIterator::ExtractTimestamp(const StorageIterator& iter) const noexcept {
    // Format: <user_key><timestamp>
    // Extract the timestamp (last 8 bytes)
    const ByteBuffer& key = iter.Key();
    size_t key_size = key.Size();
    
    // Ensure the key is large enough to contain a timestamp
    if (key_size < sizeof(uint64_t)) {
        return 0;  // Invalid timestamp, return 0
    }
    
    // Extract timestamp (last 8 bytes)
    uint64_t ts = 0;
    const uint8_t* ts_ptr = reinterpret_cast<const uint8_t*>(key.Data()) + (key_size - sizeof(uint64_t));
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        ts = (ts << 8) | ts_ptr[i];
    }
    
    return ts;
}

ByteBuffer MvccTwoMergeIterator::ExtractUserKey(const ByteBuffer& composite_key) const noexcept {
    // Format: <user_key><timestamp>
    size_t key_size = composite_key.Size();
    
    // Ensure the key is large enough to contain a timestamp
    if (key_size < sizeof(uint64_t)) {
        return composite_key.Clone();  // Invalid format, return as is
    }
    
    // Extract user key (everything except the last 8 bytes)
    size_t user_key_size = key_size - sizeof(uint64_t);
    return ByteBuffer(composite_key.Data(), user_key_size);
}

void MvccTwoMergeIterator::UpdateCurrent() noexcept {
    // Reset current selection
    current_ = 0;
    
    bool valid1 = iter1_->IsValid();
    bool valid2 = iter2_->IsValid();
    
    // If both are invalid, we're done
    if (!valid1 && !valid2) {
        return;
    }
    
    // If only one is valid, select it
    if (valid1 && !valid2) {
        current_ = 1;
        return;
    }
    
    if (!valid1 && valid2) {
        current_ = 2;
        return;
    }
    
    // Both are valid, need to compare keys and timestamps
    ByteBuffer user_key1 = ExtractUserKey(iter1_->Key());
    ByteBuffer user_key2 = ExtractUserKey(iter2_->Key());
    
    // First check if they're the same user key
    if (user_key1 == user_key2) {
        // Same key, select based on timestamp (newer first)
        uint64_t ts1 = ExtractTimestamp(*iter1_);
        uint64_t ts2 = ExtractTimestamp(*iter2_);
        
        // Skip versions newer than read_ts (not visible)
        if (ts1 > read_ts_ && ts2 > read_ts_) {
            // Both are too new, skip both
            iter1_->Next();
            iter2_->Next();
            UpdateCurrent();
            return;
        } else if (ts1 > read_ts_) {
            // Only iter1 is too new, skip it
            iter1_->Next();
            UpdateCurrent();
            return;
        } else if (ts2 > read_ts_) {
            // Only iter2 is too new, skip it
            iter2_->Next();
            UpdateCurrent();
            return;
        }
        
        // Both are visible, choose the newer one
        if (ts1 >= ts2) {
            current_ = 1;
        } else {
            current_ = 2;
        }
    } else {
        // Different keys, select the smaller one
        current_ = (user_key1 < user_key2) ? 1 : 2;
    }
    
    // Skip if we've already returned this key
    if (has_last_key_) {
        ByteBuffer current_key = (current_ == 1) ? user_key1 : user_key2;
        if (current_key == last_key_) {
            ForwardCurrent();
        }
    }
}

} // namespace util
