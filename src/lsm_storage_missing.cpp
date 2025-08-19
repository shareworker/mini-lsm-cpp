#include "../include/lsm_storage.hpp"


bool LsmStorageInner::ForceFlushNextImmMemtable() {
    // Acquire state lock for atomic operations
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
    
    // Flush memtable to builder
    flush_memtable->Flush(builder);
    
    // Get SST ID from memtable
    size_t sst_id = flush_memtable->Id();
    
    // Build SSTable
    std::shared_ptr<SsTable> sst = std::make_shared<SsTable>(
        builder.Build(sst_id, block_cache_, PathOfSst(sst_id)));
    
    // Update state
    {
        std::unique_lock<std::shared_mutex> guard(*state_mutex_);
        auto snapshot = state_->Clone();
        
        // Remove memtable from imm_memtables
        snapshot->imm_memtables.pop_back();  // Remove oldest
        
        // Add L0 table
        if (compaction_controller_->FlushToL0()) {
            // In leveled compaction or no compaction, simply flush to L0
            snapshot->l0_sstables.insert(snapshot->l0_sstables.begin(), sst_id);
        } else {
            // In tiered compaction, create a new tier
            snapshot->levels.insert(snapshot->levels.begin(), 
                                   std::make_pair(0, std::vector<size_t>{sst_id}));
        }
        
        std::cout << "Flushed " << sst_id << ".sst with size=" << sst->TableSize() << std::endl;
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
        manifest_->AddRecord(state_lock_mutex_, ManifestRecord::Flush(sst_id));
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

