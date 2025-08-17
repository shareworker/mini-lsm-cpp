#include "../include/mvcc_wal.hpp"

#include <iostream>
#include <cstring>
#include "../include/crc32c.hpp"

namespace util {

MvccWal::MvccWal(std::unique_ptr<std::fstream> file, const std::filesystem::path& path)
    : file_(std::move(file)), file_path_(path) {}

std::unique_ptr<MvccWal> MvccWal::Create(const std::filesystem::path& path) {
    // Create parent directories if they don't exist
    auto parent = path.parent_path();
    if (!parent.empty() && !std::filesystem::exists(parent)) {
        std::filesystem::create_directories(parent);
    }
    
    // Create and open the file for read/write
    auto file = std::make_unique<std::fstream>(
        path, std::ios::binary | std::ios::in | std::ios::out | std::ios::trunc);
    
    if (!file->is_open()) {
        std::cerr << "[MVCC-WAL] Failed to create WAL file: " << path << std::endl;
        return nullptr;
    }
    
    std::cout << "[MVCC-WAL] Created new WAL file: " << path << std::endl;
    return std::unique_ptr<MvccWal>(new MvccWal(std::move(file), path));
}

std::unique_ptr<MvccWal> MvccWal::Recover(
    const std::filesystem::path& path,
    std::shared_ptr<MvccSkipList> skiplist) {
    
    if (!std::filesystem::exists(path)) {
        std::cerr << "[MVCC-WAL] WAL file does not exist: " << path << std::endl;
        return nullptr;
    }
    
    // Open the file for read/write
    auto file = std::make_unique<std::fstream>(
        path, std::ios::binary | std::ios::in | std::ios::out);
    
    if (!file->is_open()) {
        std::cerr << "[MVCC-WAL] Failed to open WAL file for recovery: " << path << std::endl;
        return nullptr;
    }
    
    auto wal = std::unique_ptr<MvccWal>(new MvccWal(std::move(file), path));
    
    // Recover entries from the WAL
    std::cout << "[MVCC-WAL] Starting recovery from: " << path << std::endl;
    
    size_t recovered_entries = 0;
    ByteBuffer key, value;
    uint64_t ts;
    uint8_t entry_type;
    
    // Read entries from the beginning of the file
    wal->file_->seekg(0, std::ios::beg);
    
    while (wal->ReadEntry(key, value, ts, entry_type)) {
        switch (entry_type) {
            case kEntryTypePut:
                if (skiplist->Put(key, value, ts)) {
                    recovered_entries++;
                }
                break;
            case kEntryTypeDelete:
                if (skiplist->Remove(key, ts)) {
                    recovered_entries++;
                }
                break;
            default:
                std::cerr << "[MVCC-WAL] Unknown entry type: " << static_cast<int>(entry_type) << std::endl;
                break;
        }
    }
    
    std::cout << "[MVCC-WAL] Recovery complete. Recovered " << recovered_entries 
              << " entries from: " << path << std::endl;
    
    // Position file at end for future writes
    wal->file_->clear();
    wal->file_->seekp(0, std::ios::end);
    
    return wal;
}

bool MvccWal::PutWithTs(const ByteBuffer& key, const ByteBuffer& value, uint64_t ts) {
    std::lock_guard<std::mutex> lock(file_mutex_);
    return WriteEntry(kEntryTypePut, key, value, ts);
}

bool MvccWal::PutBatchWithTs(
    const std::vector<std::tuple<ByteBuffer, ByteBuffer, uint64_t>>& data) {
    
    std::lock_guard<std::mutex> lock(file_mutex_);
    
    for (const auto& [key, value, ts] : data) {
        if (!WriteEntry(kEntryTypePut, key, value, ts)) {
            return false;
        }
    }
    
    return true;
}

bool MvccWal::DeleteWithTs(const ByteBuffer& key, uint64_t ts) {
    std::lock_guard<std::mutex> lock(file_mutex_);
    ByteBuffer empty_value; // Empty value for delete
    return WriteEntry(kEntryTypeDelete, key, empty_value, ts);
}

bool MvccWal::Sync() {
    std::lock_guard<std::mutex> lock(file_mutex_);
    
    if (!file_->flush()) {
        std::cerr << "[MVCC-WAL] Failed to flush WAL file" << std::endl;
        return false;
    }
    
    // Force synchronization to disk
    if (!file_->good()) {
        std::cerr << "[MVCC-WAL] WAL file is in bad state" << std::endl;
        return false;
    }
    
    return true;
}

bool MvccWal::WriteEntry(uint8_t entry_type, const ByteBuffer& key, 
                        const ByteBuffer& value, uint64_t ts) {
    
    if (!file_->good()) {
        std::cerr << "[MVCC-WAL] File stream is in bad state" << std::endl;
        return false;
    }
    
    // Calculate total entry size
    size_t key_len = key.Size();
    size_t value_len = value.Size();
    uint32_t key_len_u32 = static_cast<uint32_t>(key_len);
    uint32_t value_len_u32 = static_cast<uint32_t>(value_len);
    
    // Write entry header
    file_->write(reinterpret_cast<const char*>(&entry_type), sizeof(entry_type));
    file_->write(reinterpret_cast<const char*>(&key_len_u32), sizeof(key_len_u32));
    file_->write(reinterpret_cast<const char*>(&value_len_u32), sizeof(value_len_u32));
    file_->write(reinterpret_cast<const char*>(&ts), sizeof(ts));
    
    // Write key data
    if (key_len > 0) {
        file_->write(key.Data(), key_len);
    }
    
    // Write value data
    if (value_len > 0) {
        file_->write(value.Data(), value_len);
    }
    
    // Calculate checksum over all written data
    std::vector<uint8_t> checksum_data;
    checksum_data.reserve(sizeof(entry_type) + sizeof(key_len_u32) + sizeof(value_len_u32) + sizeof(ts) + key_len + value_len);
    
    // Add header fields to checksum data
    checksum_data.insert(checksum_data.end(), 
                        reinterpret_cast<const uint8_t*>(&entry_type), 
                        reinterpret_cast<const uint8_t*>(&entry_type) + sizeof(entry_type));
    checksum_data.insert(checksum_data.end(), 
                        reinterpret_cast<const uint8_t*>(&key_len_u32), 
                        reinterpret_cast<const uint8_t*>(&key_len_u32) + sizeof(key_len_u32));
    checksum_data.insert(checksum_data.end(), 
                        reinterpret_cast<const uint8_t*>(&value_len_u32), 
                        reinterpret_cast<const uint8_t*>(&value_len_u32) + sizeof(value_len_u32));
    checksum_data.insert(checksum_data.end(), 
                        reinterpret_cast<const uint8_t*>(&ts), 
                        reinterpret_cast<const uint8_t*>(&ts) + sizeof(ts));
    
    // Add key and value data
    if (key_len > 0) {
        checksum_data.insert(checksum_data.end(), 
                            reinterpret_cast<const uint8_t*>(key.Data()), 
                            reinterpret_cast<const uint8_t*>(key.Data()) + key_len);
    }
    if (value_len > 0) {
        checksum_data.insert(checksum_data.end(), 
                            reinterpret_cast<const uint8_t*>(value.Data()), 
                            reinterpret_cast<const uint8_t*>(value.Data()) + value_len);
    }
    
    uint32_t checksum = Crc32c::Compute(checksum_data);
    
    file_->write(reinterpret_cast<const char*>(&checksum), sizeof(checksum));
    
    if (!file_->good()) {
        std::cerr << "[MVCC-WAL] Failed to write entry to WAL" << std::endl;
        return false;
    }
    
    return true;
}

bool MvccWal::ReadEntry(ByteBuffer& key, ByteBuffer& value, 
                       uint64_t& ts, uint8_t& entry_type) {
    
    if (!file_->good()) {
        return false;
    }
    
    // Read entry header
    if (!file_->read(reinterpret_cast<char*>(&entry_type), sizeof(entry_type))) {
        return false; // EOF or error
    }
    
    uint32_t key_len_u32, value_len_u32;
    if (!file_->read(reinterpret_cast<char*>(&key_len_u32), sizeof(key_len_u32)) ||
        !file_->read(reinterpret_cast<char*>(&value_len_u32), sizeof(value_len_u32)) ||
        !file_->read(reinterpret_cast<char*>(&ts), sizeof(ts))) {
        std::cerr << "[MVCC-WAL] Failed to read entry header" << std::endl;
        return false;
    }
    
    size_t key_len = static_cast<size_t>(key_len_u32);
    size_t value_len = static_cast<size_t>(value_len_u32);
    
    // Read key data
    std::vector<char> key_data(key_len);
    if (key_len > 0 && !file_->read(key_data.data(), key_len)) {
        std::cerr << "[MVCC-WAL] Failed to read key data" << std::endl;
        return false;
    }
    
    // Read value data
    std::vector<char> value_data(value_len);
    if (value_len > 0 && !file_->read(value_data.data(), value_len)) {
        std::cerr << "[MVCC-WAL] Failed to read value data" << std::endl;
        return false;
    }
    
    // Read and verify checksum
    uint32_t stored_checksum;
    if (!file_->read(reinterpret_cast<char*>(&stored_checksum), sizeof(stored_checksum))) {
        std::cerr << "[MVCC-WAL] Failed to read checksum" << std::endl;
        return false;
    }
    
    // Calculate expected checksum
    std::vector<uint8_t> checksum_data;
    checksum_data.reserve(sizeof(entry_type) + sizeof(key_len_u32) + sizeof(value_len_u32) + sizeof(ts) + key_len + value_len);
    
    // Add header fields to checksum data
    checksum_data.insert(checksum_data.end(), 
                        reinterpret_cast<const uint8_t*>(&entry_type), 
                        reinterpret_cast<const uint8_t*>(&entry_type) + sizeof(entry_type));
    checksum_data.insert(checksum_data.end(), 
                        reinterpret_cast<const uint8_t*>(&key_len_u32), 
                        reinterpret_cast<const uint8_t*>(&key_len_u32) + sizeof(key_len_u32));
    checksum_data.insert(checksum_data.end(), 
                        reinterpret_cast<const uint8_t*>(&value_len_u32), 
                        reinterpret_cast<const uint8_t*>(&value_len_u32) + sizeof(value_len_u32));
    checksum_data.insert(checksum_data.end(), 
                        reinterpret_cast<const uint8_t*>(&ts), 
                        reinterpret_cast<const uint8_t*>(&ts) + sizeof(ts));
    
    // Add key and value data
    if (key_len > 0) {
        checksum_data.insert(checksum_data.end(), 
                            reinterpret_cast<const uint8_t*>(key_data.data()), 
                            reinterpret_cast<const uint8_t*>(key_data.data()) + key_len);
    }
    if (value_len > 0) {
        checksum_data.insert(checksum_data.end(), 
                            reinterpret_cast<const uint8_t*>(value_data.data()), 
                            reinterpret_cast<const uint8_t*>(value_data.data()) + value_len);
    }
    
    uint32_t calculated_checksum = Crc32c::Compute(checksum_data);
    
    if (stored_checksum != calculated_checksum) {
        std::cerr << "[MVCC-WAL] Checksum mismatch. Expected: " << calculated_checksum 
                  << ", Got: " << stored_checksum << std::endl;
        return false;
    }
    
    // Create ByteBuffer objects
    key = ByteBuffer(std::string_view(key_data.data(), key_len));
    value = ByteBuffer(std::string_view(value_data.data(), value_len));
    
    return true;
}

} // namespace util
