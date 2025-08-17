#include "../include/manifest.hpp"

#include <algorithm>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>
#include <cstdint>
#include <fcntl.h>
#include <unistd.h>
#include <memory>
#include <filesystem>
#include <endian.h>

namespace util {

std::unique_ptr<Manifest> Manifest::Create(const std::filesystem::path& path) {
    return std::unique_ptr<Manifest>(new Manifest(path, true));
}

std::unique_ptr<Manifest> Manifest::Open(const std::filesystem::path& path) {
    if (!std::filesystem::exists(path)) {
        throw std::runtime_error("Manifest file does not exist: " + path.string());
    }
    return std::unique_ptr<Manifest>(new Manifest(path, false));
}

Manifest::Manifest(const std::filesystem::path& path, bool create) 
    : path_(path) {
    
    if (create) {
        // Create directory if it doesn't exist
        std::filesystem::create_directories(path.parent_path());
        
        // Initialize an empty manifest file
        std::ofstream file(path, std::ios::binary | std::ios::trunc);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to create manifest file: " + path.string());
        }
        
        // Initial version is empty
        current_version_ = {};
        
        // Write initial empty version to file
        (void)Flush();
    } else {
        // Open existing manifest file in append mode so we can write later.
        std::ifstream file(path, std::ios::binary);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open manifest file: " + path.string());
        }

        // Parse append-log records (length-prefixed JSON)
        current_version_.clear();

        auto apply_record = [this](const ManifestRecord& rec) {
            switch (rec.tag) {
                case ManifestRecordTag::kFlush: {
                    // Flush always produces an L0 file (newest first)
                    if (current_version_.empty() || current_version_[0].first != 0) {
                        current_version_.insert(current_version_.begin(), {0, {}});
                    }
                    current_version_[0].second.insert(current_version_[0].second.begin(), rec.single_id);
                    break;
                }
                case ManifestRecordTag::kNewMemtable:
                    // Not persisted on disk; ignore for version building
                    break;
                case ManifestRecordTag::kCompaction: {
                    // Remove from_ids from from_level
                    auto it_from = std::find_if(current_version_.begin(), current_version_.end(),
                                                [&](auto& p) { return p.first == rec.from_level; });
                    if (it_from != current_version_.end()) {
                        for (size_t id : rec.from_ids) {
                            auto& vec = it_from->second;
                            vec.erase(std::remove(vec.begin(), vec.end(), id), vec.end());
                        }
                    }
                    // Add to_ids to to_level (create level if absent)
                    auto it_to = std::find_if(current_version_.begin(), current_version_.end(),
                                              [&](auto& p) { return p.first == rec.to_level; });
                    if (it_to == current_version_.end()) {
                        current_version_.emplace_back(rec.to_level, rec.to_ids);
                    } else {
                        it_to->second.insert(it_to->second.end(), rec.to_ids.begin(), rec.to_ids.end());
                    }
                    // Keep levels sorted
                    std::sort(current_version_.begin(), current_version_.end(),
                              [](auto& a, auto& b) { return a.first < b.first; });
                    break;
                }
            }
        };

        while (true) {
            uint64_t len_be;
            if (!file.read(reinterpret_cast<char*>(&len_be), sizeof(uint64_t))) {
                break; // EOF
            }
            uint64_t len = be64toh(len_be);
            std::string json_str(len, '\0');
            if (!file.read(json_str.data(), len)) {
                throw std::runtime_error("Manifest truncated or corrupted");
            }
            nlohmann::json j = nlohmann::json::parse(json_str);
            ManifestRecord rec;
            std::string type = j.at("type").get<std::string>();
            if (type == "flush") {
                rec.tag = ManifestRecordTag::kFlush;
                rec.single_id = j.at("id").get<size_t>();
            } else if (type == "new_memtable") {
                rec.tag = ManifestRecordTag::kNewMemtable;
                rec.single_id = j.at("id").get<size_t>();
            } else if (type == "compaction") {
                rec.tag = ManifestRecordTag::kCompaction;
                rec.from_level = j.at("from_level").get<size_t>();
                rec.from_ids = j.at("from_ids").get<std::vector<size_t>>();
                rec.to_level = j.at("to_level").get<size_t>();
                rec.to_ids = j.at("to_ids").get<std::vector<size_t>>();
            } else {
                throw std::runtime_error("Unknown manifest record type");
            }
            apply_record(rec);
        }
    }
}

bool Manifest::AddSst(size_t id, size_t level) {
    // Find the level in the current version
    auto it = std::find_if(
        current_version_.begin(),
        current_version_.end(),
        [level](const auto& l) { return l.first == level; }
    );
    
    if (it == current_version_.end()) {
        // Level doesn't exist yet, add it
        current_version_.emplace_back(level, std::vector<size_t>{id});
        
        // Sort by level number
        std::sort(
            current_version_.begin(),
            current_version_.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; }
        );
    } else {
        // Add SST ID to existing level
        it->second.push_back(id);
    }
    
    // Persist flush record for this SST creation
    ManifestRecord rec;
    rec.tag = ManifestRecordTag::kFlush;
    rec.single_id = id;
    return AddRecord(rec);
}

bool Manifest::MoveSsts(
    size_t from_level,
    const std::vector<size_t>& from_ids,
    size_t to_level,
    const std::vector<size_t>& to_ids) {
    
    // Remove SSTs from source level
    auto from_it = std::find_if(
        current_version_.begin(),
        current_version_.end(),
        [from_level](const auto& l) { return l.first == from_level; }
    );
    
    if (from_it != current_version_.end()) {
        for (auto id : from_ids) {
            auto& sst_ids = from_it->second;
            sst_ids.erase(
                std::remove(sst_ids.begin(), sst_ids.end(), id),
                sst_ids.end()
            );
        }
    }
    
    // Add SSTs to target level
    auto to_it = std::find_if(
        current_version_.begin(),
        current_version_.end(),
        [to_level](const auto& l) { return l.first == to_level; }
    );
    
    if (to_it == current_version_.end()) {
        // Target level doesn't exist, create it
        current_version_.emplace_back(to_level, to_ids);
        
        // Sort by level number
        std::sort(
            current_version_.begin(),
            current_version_.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; }
        );
    } else {
        // Add SST IDs to existing level
        to_it->second.insert(to_it->second.end(), to_ids.begin(), to_ids.end());
    }
    
    // Persist compaction movement record
    ManifestRecord rec;
    rec.tag = ManifestRecordTag::kCompaction;
    rec.from_level = from_level;
    rec.from_ids = from_ids;
    rec.to_level = to_level;
    rec.to_ids = to_ids;
    return AddRecord(rec);
}

std::vector<std::pair<size_t, std::vector<size_t>>> Manifest::GetCurrentVersion() const {
    return current_version_;
}

std::filesystem::path Manifest::GetPath() const {
    return path_;
}



// -----------------------------------------------------------------------------
// Append a manifest record to the file (length + json + optional checksum)
// -----------------------------------------------------------------------------
bool Manifest::AddRecord(const ManifestRecord& rec) {
    try {
        std::ofstream file(path_, std::ios::binary | std::ios::app);
        if (!file.is_open()) {
            return false;
        }
        // Serialize to json
        nlohmann::json j;
        switch (rec.tag) {
            case ManifestRecordTag::kFlush:
                j = {
                    {"type", "flush"},
                    {"id", rec.single_id}
                };
                break;
            case ManifestRecordTag::kNewMemtable:
                j = {
                    {"type", "new_memtable"},
                    {"id", rec.single_id}
                };
                break;
            case ManifestRecordTag::kCompaction:
                j = {
                    {"type", "compaction"},
                    {"from_level", rec.from_level},
                    {"from_ids", rec.from_ids},
                    {"to_level", rec.to_level},
                    {"to_ids", rec.to_ids}
                };
                break;
        }
        std::string json_str = j.dump();
        uint64_t len_be = htobe64(static_cast<uint64_t>(json_str.size()));
        file.write(reinterpret_cast<const char*>(&len_be), sizeof(uint64_t));
        file.write(json_str.data(), json_str.size());
        file.flush();
        // fsync
        int fd = ::open(path_.c_str(), O_RDWR);
        if (fd >= 0) {
            ::fsync(fd);
            ::close(fd);
        }
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace util
