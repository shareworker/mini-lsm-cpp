#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <utility>
#include <cassert>

#include "block.hpp"  // provides Block and BlockBuilder
#include "byte_buffer.hpp"
#include "sstable.hpp"  // for BlockMeta definition

namespace util {

// Forward declarations to avoid heavy dependencies for now.
struct BlockMeta;              // defined in sstable.hpp later
class SsTable;                 // forward
class BlockCache;              // forward
class FileObject;              // forward
class Bloom;                   // forward

class SsTableBuilder {
public:
    explicit SsTableBuilder(size_t block_size)
        : builder_(block_size), block_size_(block_size) {}

    // Disallow copy
    SsTableBuilder(const SsTableBuilder&) = delete;
    SsTableBuilder& operator=(const SsTableBuilder&) = delete;

    // Allow move
    SsTableBuilder(SsTableBuilder&&) noexcept = default;
    SsTableBuilder& operator=(SsTableBuilder&&) noexcept = default;

    // Adds a key-value pair into the SSTable being built.
    void Add(const ByteBuffer& key, const std::vector<uint8_t>& value) {
        if (first_key_.Empty()) {
            first_key_ = key;
        }
        key_hashes_.push_back(Fingerprint32(key));

        if (builder_.Add(key, value)) {
            last_key_ = key;
            return;
        }
        // Current block is full – finalize it and start a new one.
        FinishBlock();
        bool ok = builder_.Add(key, value);
        assert(ok);
        first_key_ = last_key_ = key;
    }

    // Approximate size so far (without yet-to-be-flushed block)
    size_t EstimatedSize() const noexcept { return data_.size(); }

    // Build the final SsTable and persist to `file_path`.
    // NOTE: Implementation requires FileObject, Bloom, SsTable definitions. Placeholder for now.
    std::shared_ptr<SsTable> Build(
        size_t id,
        std::shared_ptr<BlockCache> block_cache,
        const std::string& file_path);

private:
    static uint32_t Fingerprint32(const ByteBuffer& key);
    static uint32_t Crc32(const std::vector<uint8_t>& buf);

    void FinishBlock() {
        Block block = builder_.Build();
        std::vector<uint8_t> encoded = block.Encode();
        meta_.push_back(BlockMeta{
            /*offset=*/data_.size(),
            /*first_key=*/first_key_.CopyToBytes(),
            /*last_key=*/last_key_.CopyToBytes()});
        uint32_t checksum = Crc32(encoded);
        data_.insert(data_.end(), encoded.begin(), encoded.end());
        PutU32(checksum);
        // reset builder for next block
        builder_ = BlockBuilder(block_size_);
        first_key_.Clear();
        last_key_.Clear();
    }

    void PutU32(uint32_t v) {
        data_.push_back(static_cast<uint8_t>(v & 0xff));
        data_.push_back(static_cast<uint8_t>((v >> 8) & 0xff));
        data_.push_back(static_cast<uint8_t>((v >> 16) & 0xff));
        data_.push_back(static_cast<uint8_t>((v >> 24) & 0xff));
    }

    BlockBuilder builder_;
    ByteBuffer first_key_;
    ByteBuffer last_key_;
    std::vector<uint8_t> data_;
    std::vector<BlockMeta> meta_;
    size_t block_size_;
    std::vector<uint32_t> key_hashes_;
};

} // namespace util
