#pragma once

#include <cstdint>
#include <memory>
#include <vector>
#include <utility>
#include "block.hpp"
#include "byte_buffer.hpp"

namespace util {

class BlockIterator {
public:
    BlockIterator() = default;

    using BlockPtr = std::shared_ptr<const Block>;

    static BlockIterator CreateAndSeekToFirst(BlockPtr block) noexcept {
        BlockIterator iter(std::move(block));
        iter.SeekToFirst();
        return iter;
    }

    static BlockIterator CreateAndSeekToKey(BlockPtr block, const ByteBuffer &key) noexcept {
        BlockIterator iter(std::move(block));
        iter.SeekToKey(key);
        return iter;
    }

    bool IsValid() const noexcept { return !key_.Empty(); }

    const ByteBuffer &Key() const noexcept { return key_; }

    ByteBuffer Value() const noexcept {
        if (!IsValid()) {
            static ByteBuffer kEmpty;
            return kEmpty;
        }
        return ByteBuffer(block_->Data().data() + value_range_.first,
                           value_range_.second - value_range_.first);
    }

    void Next() noexcept {
        ++idx_;
        SeekTo(idx_);
    }

private:
    explicit BlockIterator(BlockPtr block)
        : block_(std::move(block)), idx_(0) {
        first_key_ = ExtractFirstKey();
    }

    void SeekToFirst() noexcept { SeekTo(0); }

    void SeekTo(size_t idx) noexcept {
        if (idx >= block_->Offsets().size()) {
            key_.Clear();
            value_range_ = {0, 0};
            return;
        }
        size_t offset = block_->Offsets()[idx];
        SeekToOffset(offset);
        idx_ = idx;
    }

    void NextOffset(std::size_t offset) noexcept { SeekToOffset(offset); }

    void SeekToOffset(size_t offset) noexcept {
        const std::vector<uint8_t> &data = block_->Data();
        const uint8_t *ptr = data.data() + offset;
        uint16_t overlap = ReadU16(ptr);
        uint16_t key_len = ReadU16(ptr + 2);
        const uint8_t *key_ptr = ptr + 4;
        // reconstruct key
        std::vector<uint8_t> key_vec;
        key_vec.reserve(overlap + key_len);
        key_vec.insert(key_vec.end(), first_key_.Data(), first_key_.Data() + overlap);
        key_vec.insert(key_vec.end(), key_ptr, key_ptr + key_len);
        key_ = ByteBuffer(std::move(key_vec));
        const uint8_t *val_len_ptr = key_ptr + key_len;
        uint16_t val_len = ReadU16(val_len_ptr);
        size_t value_begin = (val_len_ptr + 2) - data.data();
        value_range_ = {value_begin, value_begin + val_len};
    }

    void SeekToKey(const ByteBuffer &target) noexcept {
        size_t low = 0;
        size_t high = block_->Offsets().size();
        while (low < high) {
            size_t mid = low + (high - low) / 2;
            SeekTo(mid);
            if (!IsValid()) break;
            if (Key() < target) {
                low = mid + 1;
            } else if (Key() > target) {
                high = mid;
            } else {
                return;
            }
        }
        SeekTo(low);
    }

    ByteBuffer ExtractFirstKey() const {
        const std::vector<uint8_t> &data = block_->Data();
        uint16_t overlap = ReadU16(data.data()); // should be 0 for first
        (void)overlap;
        uint16_t key_len = ReadU16(data.data() + 2);
        ByteBuffer key(data.data() + 4, key_len);
        return key;
    }

    static uint16_t ReadU16(const uint8_t *ptr) noexcept {
        return static_cast<uint16_t>(ptr[0]) | (static_cast<uint16_t>(ptr[1]) << 8);
    }

    BlockPtr block_;
    ByteBuffer key_;
    std::pair<size_t, size_t> value_range_{0, 0};
    size_t idx_{0};
    ByteBuffer first_key_;
};

}  // namespace util
