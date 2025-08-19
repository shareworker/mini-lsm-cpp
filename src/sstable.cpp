#include "../include/sstable.hpp"

#include "../include/bloom_filter.hpp"
#include "../include/block.hpp"
#include "../include/file_object.hpp"
#include "../include/block_cache.hpp"
#include "../include/crc32c.hpp"

#include <zlib.h>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <algorithm>  // for binary_search
#include <system_error>


/**************** BlockMeta encode/decode ****************/ 
static void PutU32(std::vector<uint8_t>& buf, uint32_t v) {
    // Little-endian format (least significant byte first)
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
}

static void PutU16(std::vector<uint8_t>& buf, uint16_t v) {
    // Little-endian format (least significant byte first)
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
}

void BlockMeta::EncodeBlockMeta(const std::vector<BlockMeta>& metas, std::vector<uint8_t>& buf) {
    size_t original_len = buf.size();
    PutU32(buf, static_cast<uint32_t>(metas.size()));
    for (const auto& m : metas) {
        PutU32(buf, static_cast<uint32_t>(m.offset));
        PutU16(buf, static_cast<uint16_t>(m.first_key.size()));
        buf.insert(buf.end(), m.first_key.begin(), m.first_key.end());
        PutU16(buf, static_cast<uint16_t>(m.last_key.size()));
        buf.insert(buf.end(), m.last_key.begin(), m.last_key.end());
    }
    // checksum over bytes after the first 4 (count) bytes
    const uint8_t* checksum_start = buf.data() + original_len + 4;
    uint32_t crc = ::crc32(0U, checksum_start, static_cast<uInt>(buf.size() - (original_len + 4)));
    PutU32(buf, crc);
}

std::vector<BlockMeta> BlockMeta::DecodeBlockMeta(const uint8_t* data, size_t len) {
    if (len < 8) {
        throw std::invalid_argument("meta buffer too small");
    }
    const uint8_t* p = data;
    auto ReadU32 = [&p]() {
        // Little-endian format (least significant byte first)
        uint32_t v = static_cast<uint32_t>(p[0]) |
                     (static_cast<uint32_t>(p[1]) << 8) |
                     (static_cast<uint32_t>(p[2]) << 16) |
                     (static_cast<uint32_t>(p[3]) << 24);
        p += 4;
        return v;
    };
    auto ReadU16 = [&p]() {
        // Little-endian format (least significant byte first)
        uint16_t v = static_cast<uint16_t>(p[0]) | (static_cast<uint16_t>(p[1]) << 8);
        p += 2;
        return v;
    };

    uint32_t num = ReadU32();
    std::vector<BlockMeta> metas;
    metas.reserve(num);
    const uint8_t* content_start = p; // start for checksum
    for (uint32_t i = 0; i < num; ++i) {
        uint32_t offset = ReadU32();
        uint16_t first_len = ReadU16();
        std::vector<uint8_t> first_key(p, p + first_len);
        p += first_len;
        uint16_t last_len = ReadU16();
        std::vector<uint8_t> last_key(p, p + last_len);
        p += last_len;
        metas.push_back(BlockMeta{offset, std::move(first_key), std::move(last_key)});
    }
    if (static_cast<size_t>(p - data + 4) != len) {
        // ensure checksum present
    }
    uint32_t crc_read = ReadU32();
    uint32_t crc_calc = ::crc32(0U, content_start, static_cast<uInt>(p - content_start - 4));
    if (crc_calc != crc_read) {
        throw std::invalid_argument("meta checksum mismatch");
    }
    return metas;
}

/**************** SsTable open ****************/
std::shared_ptr<SsTable> SsTable::Open(size_t id,
                                       std::shared_ptr<BlockCache> block_cache,
                                       FileObject file) {
    uint64_t len = file.Size();
    if (len < 8) {
        throw std::invalid_argument("sstable file too small");
    }
    // read last 8 bytes: meta_offset(u32) | bloom_offset(u32) - matching order written by SsTableBuilder
    std::vector<uint8_t> footer = file.Read(len - 8, 8);
    
    // Debug: print raw footer bytes
    std::cout << "Footer bytes: ";
    for (int i = 0; i < 8; ++i) {
        std::cout << static_cast<int>(footer[i]) << " ";
    }
    std::cout << std::endl;
    
    // Read using little-endian format (least significant byte first)
    uint32_t meta_offset = footer[0] | (footer[1] << 8) | (footer[2] << 16) | (footer[3] << 24);
    uint32_t bloom_offset = footer[4] | (footer[5] << 8) | (footer[6] << 16) | (footer[7] << 24);
    
    // Debug: print file size and offsets
    std::cout << "File size: " << len << ", meta_offset: " << meta_offset 
              << ", bloom_offset: " << bloom_offset << std::endl;
    
    // Validate offsets to prevent invalid reads
    if (bloom_offset >= len - 8) {
        throw std::invalid_argument("invalid bloom filter offset: " + 
                                 std::to_string(bloom_offset) + 
                                 ", file size: " + std::to_string(len));
    }
    if (meta_offset >= bloom_offset) {
        throw std::invalid_argument("invalid metadata offset: " + 
                                 std::to_string(meta_offset) + 
                                 ", bloom offset: " + std::to_string(bloom_offset));
    }
    
    // Calculate lengths safely
    uint64_t bloom_len = len - 8 - bloom_offset;
    uint64_t meta_len = bloom_offset - meta_offset;
    
    // Additional sanity checks for read lengths
    const uint64_t kMaxReadSize = 100 * 1024 * 1024; // 100 MB
    if (bloom_len > kMaxReadSize || meta_len > kMaxReadSize) {
        throw std::invalid_argument("read size too large: bloom_len=" + 
                                 std::to_string(bloom_len) + 
                                 ", meta_len=" + std::to_string(meta_len));
    }

    // read bloom filter
    std::vector<uint8_t> bloom_buf = file.Read(bloom_offset, bloom_len);
    BloomFilter bloom = BloomFilter::Decode(bloom_buf);

    // read block meta
    std::vector<uint8_t> meta_buf = file.Read(meta_offset, meta_len);
    std::vector<BlockMeta> metas = BlockMeta::DecodeBlockMeta(meta_buf.data(), meta_buf.size());

    auto table = std::shared_ptr<SsTable>(new SsTable());
    table->id_ = id;
    table->metas_ = std::move(metas);
    table->file_ = std::move(file);
    table->block_cache_ = std::move(block_cache);
    table->block_meta_offset_ = meta_offset;
    table->bloom_ = std::make_shared<BloomFilter>(std::move(bloom));
    table->first_key_ = table->metas_.front().first_key;
    table->last_key_ = table->metas_.back().last_key;
    return table;
}

SsTable::BlockPtr SsTable::ReadBlockCached(size_t idx) const noexcept {
    if (idx >= metas_.size()) return nullptr;
    // Try cache first if available.
    if (block_cache_) {
        size_t cache_key = (id_ << 32) | idx;
        auto cached = block_cache_->Lookup(cache_key);
        if (cached) {
            return cached;
        }
    }

    size_t offset = metas_[idx].offset;
    size_t offset_end = (idx + 1 < metas_.size()) ? metas_[idx + 1].offset : block_meta_offset_;
    size_t block_len = offset_end - offset - 4;
    try {
        std::vector<uint8_t> data = file_.Read(offset, offset_end - offset);
        // Read checksum in little-endian format to match PutU32LE used during writing
        uint32_t checksum_read = data[block_len] | (data[block_len + 1] << 8) |
                                 (data[block_len + 2] << 16) | (data[block_len + 3] << 24);
        std::cout << "[DEBUG] Block checksum - read: " << checksum_read << ", calculated: ";
        uint32_t checksum_calc = Crc32c::Compute(data.data(), block_len);
        std::cout << checksum_calc << std::endl;
        if (checksum_calc != checksum_read) {
            return nullptr;
        }
        // decode Block
        std::vector<uint8_t> block_data(data.begin(), data.begin() + block_len);
        auto blk = std::make_shared<Block>(Block::Decode(block_data.data(), block_data.size()));
        // Put into cache.
        if (block_cache_) {
            size_t cache_key = (id_ << 32) | idx;
            block_cache_->Insert(cache_key, blk);
        }
        return blk;
    } catch (...) {
        return nullptr;
    }
}

size_t SsTable::FindBlockIdx(const ByteBuffer& target) const noexcept {
    if (metas_.empty()) return 0;
    
    // Implement Rust's partition_point logic: find first block where first_key > target
    size_t left = 0;
    size_t right = metas_.size();
    while (left < right) {
        size_t mid = left + (right - left) / 2;
        ByteBuffer first_key(metas_[mid].first_key);
        if (first_key <= target) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    // Return the previous block (saturating_sub(1) in Rust)
    return left == 0 ? 0 : left - 1;
}

