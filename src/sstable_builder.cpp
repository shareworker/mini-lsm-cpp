#include "sstable_builder.hpp"

#include "sstable.hpp"
#include "file_object.hpp"
#include "bloom_filter.hpp"
#include "crc32c.hpp"

#include <filesystem>
#include <iostream>
#include <system_error>

namespace util {

namespace {
inline void PutU32LE(std::vector<uint8_t>& buf, uint32_t v) {
    // Truly little-endian format (least significant byte first)
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
}
}  // namespace

/********************** static helpers ************************/
uint32_t SsTableBuilder::Fingerprint32(const ByteBuffer& key) {
    return Crc32c::Compute(key.Data(), key.Size());
}

uint32_t SsTableBuilder::Crc32(const std::vector<uint8_t>& buf) {
    return Crc32c::Compute(buf.data(), buf.size());
}

/********************** Build ************************/
std::shared_ptr<SsTable> SsTableBuilder::Build(size_t id,
                                              std::shared_ptr<BlockCache> block_cache,
                                              const std::string& file_path) {
    // Flush the current block if it has pending entries.
    if (!builder_.IsEmpty()) {
        FinishBlock();
    }

    // 1. Move the accumulated raw data out so that we can continue appending.
    std::vector<uint8_t> buf = std::move(data_);

    // 2. Append block meta.
    const uint32_t meta_offset = static_cast<uint32_t>(buf.size());
    BlockMeta::EncodeBlockMeta(meta_, buf);
    
    // 3. Build bloom filter and append.
    const double kTargetFpr = 0.01;  // same as Rust impl
    size_t bits_per_key = BloomFilter::BitsPerKey(key_hashes_.size(), kTargetFpr);
    BloomFilter bloom = BloomFilter::BuildFromKeyHashes(key_hashes_, bits_per_key);

    const uint32_t bloom_offset = static_cast<uint32_t>(buf.size());
    bloom.Encode(buf);
    
    // 4. Add footer with meta and bloom offsets
    // Create a separate footer buffer and append it to the main buffer
    std::vector<uint8_t> footer;
    PutU32LE(footer, meta_offset);
    PutU32LE(footer, bloom_offset);
    
    // Append footer to main buffer
    buf.insert(buf.end(), footer.begin(), footer.end());
    
    // Debug print
    std::cout << "Writing footer - meta_offset: " << meta_offset 
              << ", bloom_offset: " << bloom_offset
              << ", file size: " << buf.size() << std::endl;

    // 4. Persist to disk as a new file.
    // Ensure parent directory exists
    std::filesystem::path p{file_path};
    std::error_code ec;
    std::filesystem::create_directories(p.parent_path(), ec);
    FileObject file = FileObject::Create(file_path, buf);

    // 5. Open the SsTable object from the created file so that all in-memory
    //    metadata is properly initialized.
    return SsTable::Open(id, std::move(block_cache), std::move(file));
}

}  // namespace util
