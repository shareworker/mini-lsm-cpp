#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <thread>
#include <chrono>

#include "../include/mini_lsm_mvcc.hpp"
#include "../include/mvcc_transaction.hpp"
#include "../include/mvcc_lsm_storage.hpp"


class MvccGcTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a unique temporary directory for each test with better uniqueness
        const ::testing::TestInfo* test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        auto now = std::chrono::high_resolution_clock::now();
        auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
        
        test_dir_ = std::filesystem::temp_directory_path() / 
                   ("mvcc_gc_test_" + std::to_string(nanoseconds) + "_" + 
                    test_info->test_suite_name() + "_" + test_info->name());
        
        // Ensure clean state
        std::filesystem::remove_all(test_dir_);
        std::filesystem::create_directories(test_dir_);
        
        // Create the MVCC LSM storage
        LsmStorageOptions options;
        options.target_sst_size = 1024;  // Small SST size to trigger frequent compactions
        options.block_size = 128;        // Small block size for testing
        
        mvcc_lsm_ = MiniLsmMvcc::Create(options, test_dir_);
        ASSERT_NE(mvcc_lsm_, nullptr);
    }

    void TearDown() override {
        if (mvcc_lsm_) {
            try {
                mvcc_lsm_->Close();
            } catch (...) {
                // Ignore close errors during cleanup
            }
            mvcc_lsm_.reset();
        }
        
        // Clean up the test directory with retry mechanism
        for (int retry = 0; retry < 3; ++retry) {
            std::error_code ec;
            std::filesystem::remove_all(test_dir_, ec);
            if (!ec) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        
        // Small delay to ensure file system operations complete
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    std::filesystem::path test_dir_;
    std::unique_ptr<MiniLsmMvcc> mvcc_lsm_;
};

TEST_F(MvccGcTest, BasicGarbageCollection) {
    // Test basic garbage collection functionality
    
    // Step 1: Create multiple versions of the same key
    std::string key = "gc_test_key";
    
    // Version 1: initial value
    {
        auto txn = mvcc_lsm_->NewTransaction();
        ASSERT_NE(txn, nullptr);
        
        ASSERT_TRUE(txn->Put(ByteBuffer(key), ByteBuffer("value_v1")));
        ASSERT_TRUE(txn->Commit());
    }
    
    // Version 2: updated value
    {
        auto txn = mvcc_lsm_->NewTransaction();
        ASSERT_NE(txn, nullptr);
        
        ASSERT_TRUE(txn->Put(ByteBuffer(key), ByteBuffer("value_v2")));
        ASSERT_TRUE(txn->Commit());
    }
    
    // Version 3: final value
    {
        auto txn = mvcc_lsm_->NewTransaction();
        ASSERT_NE(txn, nullptr);
        
        ASSERT_TRUE(txn->Put(ByteBuffer(key), ByteBuffer("value_v3")));
        ASSERT_TRUE(txn->Commit());
    }
    
    // Step 2: Verify we can read the latest version
    {
        auto txn = mvcc_lsm_->NewTransaction();
        ASSERT_NE(txn, nullptr);
        
        auto value = txn->Get(ByteBuffer(key));
        ASSERT_FALSE(value.Empty());
        ASSERT_EQ(value.ToString(), "value_v3");
    }
    
    // Step 3: Trigger garbage collection
    auto mvcc_storage = mvcc_lsm_->GetMvccStorage();
    ASSERT_NE(mvcc_storage, nullptr);
    
    printf("[TEST] Triggering garbage collection...\n");
    size_t removed_versions = mvcc_storage->GarbageCollect();
    printf("[TEST] Garbage collection removed %zu versions\n", removed_versions);
    
    // Step 4: Verify data is still accessible after GC
    {
        auto txn = mvcc_lsm_->NewTransaction();
        ASSERT_NE(txn, nullptr);
        
        auto value = txn->Get(ByteBuffer(key));
        ASSERT_FALSE(value.Empty());
        ASSERT_EQ(value.ToString(), "value_v3");
    }
}

TEST_F(MvccGcTest, GarbageCollectionWithActiveTransactions) {
    // Test that GC respects active transactions by not removing versions they might need
    
    std::string key = "gc_active_txn_key";
    
    // Step 1: Create initial version
    {
        auto txn = mvcc_lsm_->NewTransaction();
        ASSERT_NE(txn, nullptr);
        
        ASSERT_TRUE(txn->Put(ByteBuffer(key), ByteBuffer("initial_value")));
        ASSERT_TRUE(txn->Commit());
    }
    
    // Step 2: Start a long-running transaction that will read the initial version
    auto long_running_txn = mvcc_lsm_->NewTransaction();
    ASSERT_NE(long_running_txn, nullptr);
    
    // Read the value in the long-running transaction
    auto initial_value = long_running_txn->Get(ByteBuffer(key));
    ASSERT_FALSE(initial_value.Empty());
    ASSERT_EQ(initial_value.ToString(), "initial_value");
    
    // Step 3: Create newer versions of the same key
    {
        auto txn = mvcc_lsm_->NewTransaction();
        ASSERT_NE(txn, nullptr);
        
        ASSERT_TRUE(txn->Put(ByteBuffer(key), ByteBuffer("newer_value")));
        ASSERT_TRUE(txn->Commit());
    }
    
    // Step 4: Trigger garbage collection while the long-running transaction is active
    auto mvcc_storage = mvcc_lsm_->GetMvccStorage();
    ASSERT_NE(mvcc_storage, nullptr);
    
    printf("[TEST] Triggering garbage collection with active transaction...\n");
    size_t removed_versions = mvcc_storage->GarbageCollect();
    printf("[TEST] Garbage collection removed %zu versions with active txn\n", removed_versions);
    
    // Step 5: Verify the long-running transaction can still read its snapshot
    auto still_initial_value = long_running_txn->Get(ByteBuffer(key));
    ASSERT_FALSE(still_initial_value.Empty());
    ASSERT_EQ(still_initial_value.ToString(), "initial_value");
    
    // Step 6: Verify new transactions see the latest value
    {
        auto new_txn = mvcc_lsm_->NewTransaction();
        ASSERT_NE(new_txn, nullptr);
        
        auto latest_value = new_txn->Get(ByteBuffer(key));
        ASSERT_FALSE(latest_value.Empty());
        ASSERT_EQ(latest_value.ToString(), "newer_value");
    }
    
    // Step 7: Commit the long-running transaction
    long_running_txn->Commit();
    
    // Step 8: Now trigger GC again - should be able to remove more versions
    printf("[TEST] Triggering garbage collection after long-running txn commit...\n");
    size_t additional_removed = mvcc_storage->GarbageCollect();
    printf("[TEST] Additional garbage collection removed %zu versions\n", additional_removed);
}

TEST_F(MvccGcTest, MultipleKeysGarbageCollection) {
    // Test garbage collection with multiple keys having multiple versions
    
    const int num_keys = 5;
    const int versions_per_key = 3;
    
    // Step 1: Create multiple versions for multiple keys
    for (int key_idx = 0; key_idx < num_keys; ++key_idx) {
        for (int version = 0; version < versions_per_key; ++version) {
            auto txn = mvcc_lsm_->NewTransaction();
            ASSERT_NE(txn, nullptr);
            
            std::string key = "multi_key_" + std::to_string(key_idx);
            std::string value = "value_" + std::to_string(version) + "_for_key_" + std::to_string(key_idx);
            
            ASSERT_TRUE(txn->Put(ByteBuffer(key), ByteBuffer(value)));
            ASSERT_TRUE(txn->Commit());
        }
    }
    
    // Step 2: Verify latest versions are accessible
    for (int key_idx = 0; key_idx < num_keys; ++key_idx) {
        auto txn = mvcc_lsm_->NewTransaction();
        ASSERT_NE(txn, nullptr);
        
        std::string key = "multi_key_" + std::to_string(key_idx);
        std::string expected_value = "value_" + std::to_string(versions_per_key - 1) + "_for_key_" + std::to_string(key_idx);
        
        auto value = txn->Get(ByteBuffer(key));
        ASSERT_FALSE(value.Empty());
        ASSERT_EQ(value.ToString(), expected_value);
    }
    
    // Step 3: Trigger garbage collection
    auto mvcc_storage = mvcc_lsm_->GetMvccStorage();
    ASSERT_NE(mvcc_storage, nullptr);
    
    printf("[TEST] Triggering garbage collection for multiple keys...\n");
    size_t removed_versions = mvcc_storage->GarbageCollect();
    printf("[TEST] Garbage collection removed %zu versions from multiple keys\n", removed_versions);
    
    // Step 4: Verify all keys still have their latest values accessible
    for (int key_idx = 0; key_idx < num_keys; ++key_idx) {
        auto txn = mvcc_lsm_->NewTransaction();
        ASSERT_NE(txn, nullptr);
        
        std::string key = "multi_key_" + std::to_string(key_idx);
        std::string expected_value = "value_" + std::to_string(versions_per_key - 1) + "_for_key_" + std::to_string(key_idx);
        
        auto value = txn->Get(ByteBuffer(key));
        ASSERT_FALSE(value.Empty());
        ASSERT_EQ(value.ToString(), expected_value);
    }
}


int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
