#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>
#include "block.hpp"
#include "byte_buffer.hpp"
#include "file_object.hpp"


class BlockCache; // forward
class BloomFilter; // forward

struct BlockMeta {
    size_t offset{};
    std::vector<uint8_t> first_key;
    std::vector<uint8_t> last_key;

    // Encode/decode helper compatible with Rust implementation
    static void EncodeBlockMeta(const std::vector<BlockMeta>& metas, std::vector<uint8_t>& buf);
    static std::vector<BlockMeta> DecodeBlockMeta(const uint8_t* data, size_t len);
};

class SsTable {
public:
    using BlockPtr = std::shared_ptr<const Block>;

    // Disable copy; allow move.
    SsTable(const SsTable&) = delete;
    SsTable& operator=(const SsTable&) = delete;
    SsTable(SsTable&&) noexcept = delete;
    SsTable& operator=(SsTable&&) noexcept = delete;

    size_t NumOfBlocks() const noexcept { return metas_.size(); }

    BlockPtr ReadBlockCached(size_t idx) const noexcept;

    size_t FindBlockIdx(const ByteBuffer& target) const noexcept;

    ByteBuffer FirstKey() const noexcept { return ByteBuffer(first_key_); }
    ByteBuffer LastKey() const noexcept { return ByteBuffer(last_key_); }

    /**
     * @brief Return SST ID.
     */
    size_t Id() const noexcept { return id_; }

    /**
     * @brief Access the bloom filter of the table (may be nullptr).
     */
    const std::shared_ptr<BloomFilter>& Bloom() const noexcept { return bloom_; }

    /**
     * @brief Get the actual file size in bytes.
     */
    uint64_t FileSize() const noexcept { return file_.Size(); }

    static std::shared_ptr<SsTable> Open(size_t id,
                                         std::shared_ptr<BlockCache> block_cache,
                                         FileObject file);

private:
    SsTable() = default;

    size_t id_{};
    FileObject file_;
    uint32_t block_meta_offset_{};
    std::vector<BlockMeta> metas_;
    std::shared_ptr<BloomFilter> bloom_;
    std::shared_ptr<BlockCache> block_cache_;
    std::vector<uint8_t> first_key_;
    std::vector<uint8_t> last_key_;
};

