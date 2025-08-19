#include "wal.hpp"
#include "crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <ios>
#include <stdexcept>
#include <system_error>
#include <filesystem>
#include <memory>
#include <utility>
#include <vector>
#include <fstream>
#include "byte_buffer.hpp"
#include "skiplist.hpp"

#ifdef __linux__
#include <unistd.h>  // for fsync
#include <fcntl.h>   // for open/close
#endif


namespace {

// CRC32 checksum calculation for data integrity
uint32_t CalculateChecksum(const std::vector<uint8_t>& data) {
    // Make sure tables are initialized
    static bool initialized = false;
    if (!initialized) {
        Crc32c::Initialize();
        initialized = true;
    }
    return Crc32c::Compute(data);
}

// Utility functions for byte manipulation
void AppendUint16(std::vector<uint8_t>& buffer, uint16_t value) {
    buffer.push_back(static_cast<uint8_t>(value & 0xFF));
    buffer.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
}

void AppendBuffer(std::vector<uint8_t>& buffer, const ByteBuffer& data) {
    const uint8_t* data_ptr = reinterpret_cast<const uint8_t*>(data.Data());
    buffer.insert(buffer.end(), data_ptr, data_ptr + data.Size());
}

void AppendUint32(std::vector<uint8_t>& buffer, uint32_t value) {
    buffer.push_back(static_cast<uint8_t>(value & 0xFF));
    buffer.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
    buffer.push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
    buffer.push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
}

uint16_t ReadUint16(const uint8_t* data) {
    return static_cast<uint16_t>(data[0]) | 
           (static_cast<uint16_t>(data[1]) << 8);
}

uint32_t ReadUint32(const uint8_t* data) {
    return static_cast<uint32_t>(data[0]) | 
           (static_cast<uint32_t>(data[1]) << 8) |
           (static_cast<uint32_t>(data[2]) << 16) |
           (static_cast<uint32_t>(data[3]) << 24);
}

}  // anonymous namespace

Wal::Wal(std::unique_ptr<std::fstream> file, const std::filesystem::path& path)
    : file_(std::move(file)),
      file_path_(path) {
}

std::unique_ptr<Wal> Wal::Create(const std::filesystem::path& path) {
    try {
        auto file = std::make_unique<std::fstream>();
        file->exceptions(std::ios::failbit | std::ios::badbit);
        file->open(path, std::ios::out | std::ios::binary | std::ios::trunc);
        
        return std::unique_ptr<Wal>(new Wal(std::move(file), path));
    } catch (const std::exception& e) {
        // Log error and return nullptr on failure
        return nullptr;
    }
}

std::unique_ptr<Wal> Wal::Recover(
    const std::filesystem::path& path,
    std::shared_ptr<SkipList<ByteBuffer, ByteBuffer>> skiplist) {
    // Robust recovery: tolerate partial writes or corrupted tail.
    // We parse record-by-record; on first sign of corruption we truncate the
    // WAL to the last verified-good byte offset so future appends work.
    // Format: [u16 key_len][key][u16 value_len][value][u32 checksum]
    // Checksum covers everything except the checksum field itself.
    try {
        auto file = std::make_unique<std::fstream>();
        file->exceptions(std::ios::failbit | std::ios::badbit);
        // open with read/write/append so we can later truncate
        file->open(path, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);

        // Load entire file into memory
        file->seekg(0, std::ios::end);
        size_t file_size = static_cast<size_t>(file->tellg());
        file->seekg(0, std::ios::beg);

        std::vector<uint8_t> buffer(file_size);
        if (file_size > 0) {
            file->read(reinterpret_cast<char*>(buffer.data()), file_size);
        }

        size_t pos = 0;
        size_t last_good_pos = 0;
        std::vector<uint8_t> checksum_data;

        while (pos + 8 /* minimal record size */ <= file_size) {
            checksum_data.clear();

            // --- key length ---
            uint16_t key_len = ReadUint16(&buffer[pos]);
            checksum_data.push_back(buffer[pos]);
            checksum_data.push_back(buffer[pos + 1]);
            pos += 2;

            if (pos + key_len + 2 + 4 > file_size) {
                // Incomplete record (likely partial write)
                break;
            }

            // --- key ---
            ByteBuffer key(reinterpret_cast<const char*>(&buffer[pos]), key_len);
            checksum_data.insert(checksum_data.end(), buffer.begin() + pos, buffer.begin() + pos + key_len);
            pos += key_len;

            // --- value length ---
            uint16_t value_len = ReadUint16(&buffer[pos]);
            checksum_data.push_back(buffer[pos]);
            checksum_data.push_back(buffer[pos + 1]);
            pos += 2;

            if (pos + value_len + 4 > file_size) {
                // Incomplete value / checksum
                break;
            }

            // --- value ---
            ByteBuffer value(reinterpret_cast<const char*>(&buffer[pos]), value_len);
            checksum_data.insert(checksum_data.end(), buffer.begin() + pos, buffer.begin() + pos + value_len);
            pos += value_len;

            // --- checksum ---
            uint32_t stored_checksum = ReadUint32(&buffer[pos]);
            pos += 4;

            uint32_t computed_checksum = CalculateChecksum(checksum_data);
            if (computed_checksum != stored_checksum) {
                // Corrupted record
                break;
            }

            // Valid record => restore into skiplist
            skiplist->Insert(key, value);
            last_good_pos = pos;
        }

        if (last_good_pos < file_size) {
            // Truncate corrupted tail so future appends are valid
            try {
                std::filesystem::resize_file(path, last_good_pos);
            } catch (...) {
                // Best-effort; ignore failure
            }
            // Position write pointer at end of truncated file
            file->seekp(0, std::ios::end);
        }

        return std::unique_ptr<Wal>(new Wal(std::move(file), path));
    } catch (...) {
        return nullptr;
    }
}

#if 0  // duplicate recovery block disabled
    try {
        auto file = std::make_unique<std::fstream>();
        file->exceptions(std::ios::failbit | std::ios::badbit);
        
        // Open for reading and appending
        file->open(path, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
        
        // Determine file size
        file->seekg(0, std::ios::end);
        size_t file_size = file->tellg();
        file->seekg(0, std::ios::beg);
        
        // Read the entire file
        std::vector<uint8_t> buffer(file_size);
        if (file_size > 0) {
            file->read(reinterpret_cast<char*>(buffer.data()), file_size);
        }
        
        // Process the buffer and recover data
        size_t pos = 0;
        std::vector<uint8_t> checksum_data;
        
        while (pos + 4 < file_size) {  // Minimum record: 2 bytes key_len + 2 bytes value_len + 4 bytes checksum
            checksum_data.clear();
            
            // Read key length (2 bytes)
            uint16_t key_len = ReadUint16(&buffer[pos]);
            checksum_data.push_back(buffer[pos]);
            checksum_data.push_back(buffer[pos + 1]);
            pos += 2;
            
            if (pos + key_len + 2 + 4 > file_size) {
                throw std::runtime_error("Corrupted WAL: incomplete record");
            }
            
            // Read key
            ByteBuffer key(reinterpret_cast<const char*>(&buffer[pos]), key_len);
            checksum_data.insert(checksum_data.end(), buffer.begin() + pos, buffer.begin() + pos + key_len);
            pos += key_len;
            
            // Read value length (2 bytes)
            uint16_t value_len = ReadUint16(&buffer[pos]);
            checksum_data.push_back(buffer[pos]);
            checksum_data.push_back(buffer[pos + 1]);
            pos += 2;
            
            if (pos + value_len + 4 > file_size) {
                throw std::runtime_error("Corrupted WAL: incomplete record");
            }
            
            // Read value
            ByteBuffer value(reinterpret_cast<const char*>(&buffer[pos]), value_len);
            checksum_data.insert(checksum_data.end(), buffer.begin() + pos, buffer.begin() + pos + value_len);
            pos += value_len;
            
            // Read checksum (4 bytes)
            uint32_t stored_checksum = ReadUint32(&buffer[pos]);
            pos += 4;
            
            // Verify checksum
            uint32_t computed_checksum = CalculateChecksum(checksum_data);
            if (computed_checksum != stored_checksum) {
                throw std::runtime_error("Checksum mismatch");
            }
            
            // Insert into skiplist
            skiplist->Insert(key, value);
        }
        
        // Process the entire WAL file
        std::unique_ptr<Wal> wal(new Wal(std::move(file), path));
        return wal;
    } catch (const std::exception& e) {
        // Log error and return nullptr on failure
        return nullptr;
    }
#endif  // end duplicate Recover block

bool Wal::Put(const ByteBuffer& key, const ByteBuffer& value) {
    try {
        std::lock_guard<std::mutex> lock(file_mutex_);
        
        std::vector<uint8_t> buffer;
        buffer.reserve(2 + key.Size() + 2 + value.Size() + 4); // Pre-allocate to avoid reallocations
        
        // Prepare data and calculate checksum
        AppendUint16(buffer, static_cast<uint16_t>(key.Size()));
        AppendBuffer(buffer, key);
        AppendUint16(buffer, static_cast<uint16_t>(value.Size()));
        AppendBuffer(buffer, value);
        
        // Calculate checksum of all data so far
        uint32_t checksum = CalculateChecksum(buffer);
        AppendUint32(buffer, checksum);
        
        // Write to file
        file_->write(reinterpret_cast<const char*>(buffer.data()), buffer.size());
        
        return true;
    } catch (const std::exception& e) {
        // Log error and return false on failure
        return false;
    }
}

bool Wal::PutBatch(const std::vector<std::pair<ByteBuffer, ByteBuffer>>& data) {
    try {
        std::lock_guard<std::mutex> lock(file_mutex_);
        
        // Calculate total buffer size needed
        size_t total_size = 0;
        for (const auto& [key, value] : data) {
            total_size += 2 + key.Size() + 2 + value.Size() + 4; // key_len + key + value_len + value + checksum
        }
        
        std::vector<uint8_t> buffer;
        buffer.reserve(total_size);
        
        // Process each key-value pair
        for (const auto& [key, value] : data) {
            std::vector<uint8_t> entry_buffer;
            entry_buffer.reserve(2 + key.Size() + 2 + value.Size());
            
            AppendUint16(entry_buffer, static_cast<uint16_t>(key.Size()));
            AppendBuffer(entry_buffer, key);
            AppendUint16(entry_buffer, static_cast<uint16_t>(value.Size()));
            AppendBuffer(entry_buffer, value);
            
            // Calculate checksum for this entry
            uint32_t checksum = CalculateChecksum(entry_buffer);
            
            // Add to main buffer
            buffer.insert(buffer.end(), entry_buffer.begin(), entry_buffer.end());
            AppendUint32(buffer, checksum);
        }
        
        // Write entire batch to file
        file_->write(reinterpret_cast<const char*>(buffer.data()), buffer.size());
        
        return true;
    } catch (const std::exception& e) {
        // Log error and return false on failure
        return false;
    }
}

bool Wal::Sync() {
    std::lock_guard<std::mutex> lock(file_mutex_);
    try {
        file_->flush();
#ifndef _WIN32
        bool ok = true;
        if (!file_path_.empty()) {
            int fd = ::open(file_path_.c_str(), O_RDONLY);
            if (fd != -1) {
                if (::fsync(fd) == -1) {
                    ok = false;
                }
                ::close(fd);
            } else {
                ok = false;
            }
        }
        return ok;
#else
        // On Windows flush is the best we can do
        return true;
#endif
    } catch (...) {
        return false;
    }
}

