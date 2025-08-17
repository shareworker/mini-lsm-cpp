#pragma once

#include <cstdint>
#include <vector>
#include <cassert>
#include <algorithm>
#include <stdexcept>
#include "byte_buffer.hpp"

namespace util {

// Size of an unsigned 16-bit integer in bytes.
inline constexpr size_t kSizeOfU16 = 2;

/**
 * A Block stores a sequence of key-value pairs in a compact encoded form.
 * Layout (same as Rust impl):
 *   [entry0 .. entryN] | [offset0 .. offsetN] | entry_count(u16)
 * Each `offset` is the starting position of an entry relative to the
 * beginning of the block.
 * Each entry layout: <key_overlap:u16><key_len:u16><key_bytes><val_len:u16><val_bytes>
 */
class Block {
public:
    Block() = default;

    Block(std::vector<uint8_t> data, std::vector<uint16_t> offsets)
        : data_(std::move(data)), offsets_(std::move(offsets)) {}

    const std::vector<uint8_t>& Data() const noexcept { return data_; }
    const std::vector<uint16_t>& Offsets() const noexcept { return offsets_; }
    size_t NumEntries() const noexcept { return offsets_.size(); }

    /**
     * Serialize the block into raw bytes.
     */
    std::vector<uint8_t> Encode() const {
        std::vector<uint8_t> buf;
        buf.reserve(data_.size() + offsets_.size() * kSizeOfU16 + kSizeOfU16);
        // 1. raw kv data
        buf.insert(buf.end(), data_.begin(), data_.end());
        // 2. offsets (u16 little-endian)
        for (uint16_t off : offsets_) {
            buf.push_back(static_cast<uint8_t>(off & 0xff));
            buf.push_back(static_cast<uint8_t>((off >> 8) & 0xff));
        }
        // 3. number of entries (u16)
        uint16_t cnt = static_cast<uint16_t>(offsets_.size());
        buf.push_back(static_cast<uint8_t>(cnt & 0xff));
        buf.push_back(static_cast<uint8_t>((cnt >> 8) & 0xff));
        return buf;
    }

    /**
     * Decode a block from raw memory [data, data+len).
     */
    static Block Decode(const uint8_t* data, size_t len) {
        assert(len >= kSizeOfU16);
        // read entry count (last u16, little-endian)
        size_t idx_cnt_lo = len - kSizeOfU16;
        uint16_t cnt = static_cast<uint16_t>(data[idx_cnt_lo]) |
                       (static_cast<uint16_t>(data[idx_cnt_lo + 1]) << 8);
        size_t offsets_bytes = cnt * kSizeOfU16;
        assert(len >= kSizeOfU16 + offsets_bytes);
        size_t data_end = len - kSizeOfU16 - offsets_bytes;
        const uint8_t* offsets_raw = data + data_end;

        std::vector<uint16_t> offsets;
        offsets.reserve(cnt);
        for (size_t i = 0; i < cnt; ++i) {
            size_t base = i * kSizeOfU16;
            uint16_t off = static_cast<uint16_t>(offsets_raw[base]) |
                           (static_cast<uint16_t>(offsets_raw[base + 1]) << 8);
            offsets.push_back(off);
        }
        std::vector<uint8_t> kv(data, data + data_end);
        return Block{std::move(kv), std::move(offsets)};
    }

private:
    std::vector<uint8_t> data_;
    std::vector<uint16_t> offsets_;
};

//////////////////////////////////////////////////////////////////
// BlockBuilder
//////////////////////////////////////////////////////////////////

/**
 * Helper class to build a Block incrementally with prefix-compression to the
 * first key.
 */
class BlockBuilder {
public:
    explicit BlockBuilder(size_t block_size)
        : block_size_(block_size), first_key_() {}

    bool IsEmpty() const noexcept { return offsets_.empty(); }

    /**
     * Estimate current encoded size of the block.
     */
    size_t EstimatedSize() const noexcept {
        return kSizeOfU16 /*cnt*/ + offsets_.size() * kSizeOfU16 + data_.size();
    }

    /**
     * Add a key-value pair. Return false if adding would exceed `block_size_`
     * (and the block already has at least one entry).
     */
    bool Add(const ByteBuffer& key, const std::vector<uint8_t>& value) {
        assert(!key.Empty());
        size_t extra = key.Size() + value.size() + 3 * kSizeOfU16; // key_len, val_len, offset + overlap u16
        if (EstimatedSize() + extra > block_size_ && !IsEmpty()) {
            return false;
        }
        // push offset of this entry
        offsets_.push_back(static_cast<uint16_t>(data_.size()));

        size_t overlap = ComputeOverlap(first_key_, key);
        // encode key_overlap
        PutU16(static_cast<uint16_t>(overlap));
        // encode key_len (remaining part)
        PutU16(static_cast<uint16_t>(key.Size() - overlap));
        // key bytes
        data_.insert(data_.end(), key.Data() + overlap, key.Data() + key.Size());
        // value len
        PutU16(static_cast<uint16_t>(value.size()));
        // value bytes
        data_.insert(data_.end(), value.begin(), value.end());

        if (first_key_.Empty()) {
            first_key_ = key; // copy (ByteBuffer ref-counts underlying data)
        }
        return true;
    }

    Block Build() {
        if (IsEmpty()) {
            throw std::runtime_error("block should not be empty");
        }
        return Block{std::move(data_), std::move(offsets_)};
    }

private:
    void PutU16(uint16_t v) { data_.push_back(v & 0xff); data_.push_back((v >> 8) & 0xff); }

    static size_t ComputeOverlap(const ByteBuffer& first_key, const ByteBuffer& key) noexcept {
        size_t i = 0;
        size_t min_len = std::min(first_key.Size(), key.Size());
        while (i < min_len && first_key.Data()[i] == key.Data()[i]) { ++i; }
        return i;
    }

    std::vector<uint16_t> offsets_;
    std::vector<uint8_t> data_;
    size_t block_size_;
    ByteBuffer first_key_;
};

}  // namespace util
