// Fixed implementations based on C++ API and Rust reference

#include "../include/lsm_storage.hpp"
#include "sstable_builder.hpp"

namespace util {

bool LsmStorageInner::ForceFlushNextImmMemtable() {
    std::lock_guard<std::mutex> state_lock(state_lock_mutex_);

    // Get the oldest immutable memtable
    std::shared_ptr<MemTable> flush_memtable;
    {
        std::shared_lock<std::shared_mutex> guard(*state_mutex_);
        if (state_->imm_memtables.empty()) {
            // No immutable memtables to flush
            return true;
        }
        
        flush_memtable = state_->imm_memtables.back();  // Oldest is at the back
    }

    // Create SSTable builder
    SsTableBuilder builder(options_->block_size);
    
    // Add all key-value pairs from memtable to the builder
    flush_memtable->ForEach([&builder](const ByteBuffer& key, const ByteBuffer& value) {
        std::vector<uint8_t> value_bytes(value.Data(), value.Data() + value.Size());
        builder.Add(key, value_bytes);
    });
    
    // Get SST ID from memtable
    size_t sst_id = flush_memtable->Id();
    
    // Build SSTable and write to disk
    std::shared_ptr<SsTable> sst = builder.Build(
        sst_id, 
        block_cache_, 
        PathOfSst(sst_id)
    );
    
    // Update state
    {
        std::unique_lock<std::shared_mutex> guard(*state_mutex_);
        auto snapshot = state_->Clone();
        
        // Remove memtable from imm_memtables
        snapshot->imm_memtables.pop_back();  // Remove oldest
        
        // Add L0 table
        // In C++ there's no CompactionController::FlushToL0 method. We need to follow the
        // default behavior based on the compaction strategy
        snapshot->l0_sstables.insert(snapshot->l0_sstables.begin(), sst_id);
        
        std::cout << "Flushed " << sst_id << ".sst" << std::endl;
        snapshot->sstables.emplace(sst_id, sst);
        
        // Update the state
        state_ = std::move(snapshot);
    }
    
    // Remove WAL file if enabled
    if (options_->enable_wal) {
        std::filesystem::remove(PathOfWal(sst_id));
    }
    
    // Update manifest
    if (manifest_) {
        ManifestRecord rec;
        rec.tag = ManifestRecordTag::kFlush;
        rec.single_id = sst_id;
        manifest_->AddRecord(rec);
    }
    
    // Sync directory to ensure durability
    SyncDir();
    
    return true;
}

void LsmStorageInner::TryFreeze(size_t estimated_size) {
    if (estimated_size >= options_->target_sst_size) {
        std::lock_guard<std::mutex> state_lock(state_lock_mutex_);
        
        // Double-check memtable size after acquiring lock
        std::shared_lock<std::shared_mutex> guard(*state_mutex_);
        if (state_->memtable->ApproximateSize() >= options_->target_sst_size) {
            // Drop the shared lock before calling ForceFreezeMemtable
            guard.unlock();
            ForceFreezeMemtable();
        }
    }
}

bool LsmStorageInner::Flush() {
    // First, freeze current memtable if not empty
    {
        std::shared_lock<std::shared_mutex> guard(*state_mutex_);
        if (!state_->memtable->IsEmpty()) {
            guard.unlock();
            ForceFreezeMemtable();
        }
    }
    
    // Then flush all immutable memtables
    while (true) {
        {
            std::shared_lock<std::shared_mutex> guard(*state_mutex_);
            if (state_->imm_memtables.empty()) {
                break;
            }
        }
        
        if (!ForceFlushNextImmMemtable()) {
            return false;
        }
    }
    
    return true;
}

} // namespace util
