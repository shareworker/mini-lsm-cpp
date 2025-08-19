#include <gtest/gtest.h>
#include <filesystem>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "mini_lsm.hpp"
#include "mini_lsm_mvcc.hpp"
#include "block_cache.hpp"
#include "bloom_filter.hpp"
#include "bound.hpp"
#include "byte_buffer.hpp"
#include "key.hpp"
#include "lsm_storage.hpp"
#include "mem_table.hpp"
#include "mvcc_txn.hpp"
#include "storage_iterator.hpp"


class MiniLsmComprehensiveTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a unique test directory based on test name and timestamp
        const ::testing::TestInfo* test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        std::string unique_name = std::string("mini_lsm_test_") +
                                 test_info->test_suite_name() + "_" +
                                 test_info->name() + "_" +
                                 std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                     std::chrono::high_resolution_clock::now().time_since_epoch()).count());
        
        test_dir_ = std::filesystem::temp_directory_path() / unique_name;
        std::filesystem::create_directories(test_dir_);
        
        // No need to clean up existing files since directory is unique
        
        // Create a new LSM instance
        LsmStorageOptions options;
        options.block_size = 4096;
        options.target_sst_size = 1024 * 1024; // 1MB
        options.num_memtable_limit = 3;
        options.enable_wal = true;
        
        lsm_ = MiniLsm::Open(test_dir_, options);
        
        // Only create MVCC instance for MVCC-specific tests to avoid interference
        if (std::string(test_info->name()).find("Mvcc") != std::string::npos) {
            options.serializable = true;
            mvcc_lsm_ = std::make_shared<MiniLsmMvcc>(test_dir_ / "mvcc", options);
        }
    }
    
    void TearDown() override {
        // Close the LSM instances with error handling
        if (lsm_) {
            try {
                lsm_->Close();
            } catch (...) {
                // Ignore close errors during cleanup
            }
            lsm_.reset();
        }
        
        if (mvcc_lsm_) {
            try {
                mvcc_lsm_->Close();
            } catch (...) {
                // Ignore close errors during cleanup
            }
            mvcc_lsm_.reset();
        }
        
        // Force cleanup of test directory with retry
        for (int retry = 0; retry < 3; ++retry) {
            try {
                std::filesystem::remove_all(test_dir_);
                break;
            } catch (...) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }
        
        // Small delay to ensure file system operations complete
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
    std::filesystem::path test_dir_;
    MiniLsm::Ptr lsm_;
    std::shared_ptr<MiniLsmMvcc> mvcc_lsm_;
};

// Test basic put/get operations
TEST_F(MiniLsmComprehensiveTest, BasicPutGet) {
    // Put some data
    for (int i = 0; i < 10; ++i) {
        std::string key_str = "key" + std::to_string(i);
        std::string value_str = "value" + std::to_string(i);
        
        ByteBuffer key(reinterpret_cast<const uint8_t*>(key_str.c_str()), key_str.size());
        ByteBuffer value(reinterpret_cast<const uint8_t*>(value_str.c_str()), value_str.size());
        
        ASSERT_TRUE(lsm_->Put(key, value));
    }
    
    // Get the data back
    for (int i = 0; i < 10; ++i) {
        std::string key_str = "key" + std::to_string(i);
        std::string expected_value = "value" + std::to_string(i);
        
        ByteBuffer key(reinterpret_cast<const uint8_t*>(key_str.c_str()), key_str.size());
        auto result = lsm_->Get(key);
        
        ASSERT_TRUE(result.has_value()) << "Missing key: " << key_str;
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(result->Data()), result->Size()), expected_value);
    }
    
    // Delete some data
    for (int i = 0; i < 5; ++i) {
        std::string key_str = "key" + std::to_string(i);
        ByteBuffer key(reinterpret_cast<const uint8_t*>(key_str.c_str()), key_str.size());
        ASSERT_TRUE(lsm_->Delete(key));
    }
    
    // Force flush to ensure tombstones are written to SST files
    ASSERT_TRUE(lsm_->GetInner()->Flush());
    
    // Verify deleted data is gone
    for (int i = 0; i < 5; ++i) {
        std::string key_str = "key" + std::to_string(i);
        ByteBuffer key(reinterpret_cast<const uint8_t*>(key_str.c_str()), key_str.size());
        auto result = lsm_->Get(key);
        EXPECT_FALSE(result.has_value()) << "Key should be deleted: " << key_str;
    }
    
    // Verify remaining data is intact
    for (int i = 5; i < 10; ++i) {
        std::string key_str = "key" + std::to_string(i);
        std::string expected_value = "value" + std::to_string(i);
        
        ByteBuffer key(reinterpret_cast<const uint8_t*>(key_str.c_str()), key_str.size());
        auto result = lsm_->Get(key);
        
        ASSERT_TRUE(result.has_value()) << "Missing key: " << key_str;
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(result->Data()), result->Size()), expected_value);
    }
    
    // Scan a range
    ByteBuffer lower_key(reinterpret_cast<const uint8_t*>("key3"), 4);
    ByteBuffer upper_key(reinterpret_cast<const uint8_t*>("key7"), 4);
    
    Bound lower = Bound::Included(lower_key);
    Bound upper = Bound::Excluded(upper_key);
    
    auto iter = lsm_->GetInner()->Scan(lower, upper);
    
    // Verify scan results - only non-deleted keys should appear
    int count = 0;
    std::vector<std::string> expected_keys = {"key5", "key6"};
    
    while (iter->IsValid()) {
        std::string key_str(reinterpret_cast<const char*>(iter->Key().Data()), iter->Key().Size());
        std::string value_str(reinterpret_cast<const char*>(iter->Value().Data()), iter->Value().Size());
        
        ASSERT_LT(count, expected_keys.size()) << "Too many keys in scan result";
        EXPECT_EQ(key_str, expected_keys[count]);
        EXPECT_EQ(value_str, "value" + expected_keys[count].substr(3));
        
        iter->Next();
        count++;
    }
    
    EXPECT_EQ(count, expected_keys.size()) << "Not enough keys in scan result";
}

// Test LSM flush and compaction
TEST_F(MiniLsmComprehensiveTest, LsmFlushAndCompaction) {
    // Put a lot of data to trigger flush
    for (int i = 0; i < 2000; ++i) {
        std::string key_str = "key" + std::to_string(i);
        std::string value_str = "value" + std::to_string(i);
        
        ByteBuffer key(reinterpret_cast<const uint8_t*>(key_str.c_str()), key_str.size());
        ByteBuffer value(reinterpret_cast<const uint8_t*>(value_str.c_str()), value_str.size());
        
        ASSERT_TRUE(lsm_->Put(key, value));
    }
    
    // Manually trigger flush and compaction
    lsm_->GetInner()->Flush();
    lsm_->GetInner()->ForceFullCompaction();
    
    // Verify all data is still accessible
    for (int i = 0; i < 2000; i += 200) {
        std::string key_str = "key" + std::to_string(i);
        std::string expected_value = "value" + std::to_string(i);
        
        ByteBuffer key(reinterpret_cast<const uint8_t*>(key_str.c_str()), key_str.size());
        auto result = lsm_->Get(key);
        
        ASSERT_TRUE(result.has_value()) << "Missing key: " << key_str;
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(result->Data()), result->Size()), expected_value);
    }
}

// Test WAL recovery
TEST_F(MiniLsmComprehensiveTest, WalRecovery) {
    // Put some data
    for (int i = 0; i < 100; ++i) {
        std::string key_str = "key" + std::to_string(i);
        std::string value_str = "value" + std::to_string(i);
        
        ByteBuffer key(reinterpret_cast<const uint8_t*>(key_str.c_str()), key_str.size());
        ByteBuffer value(reinterpret_cast<const uint8_t*>(value_str.c_str()), value_str.size());
        
        ASSERT_TRUE(lsm_->Put(key, value));
    }
    
    // Close the LSM
    lsm_->Close();
    
    // Reopen the LSM
    LsmStorageOptions options;
    options.enable_wal = true;
    
    lsm_ = MiniLsm::Open(test_dir_, options);
    
    // Verify data is recovered
    for (int i = 0; i < 100; i += 10) {
        std::string key_str = "key" + std::to_string(i);
        std::string expected_value = "value" + std::to_string(i);
        
        ByteBuffer key(reinterpret_cast<const uint8_t*>(key_str.c_str()), key_str.size());
        auto result = lsm_->Get(key);
        
        ASSERT_TRUE(result.has_value()) << "Missing key after recovery: " << key_str;
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(result->Data()), result->Size()), expected_value);
    }
}

// Test MVCC basic operations
TEST_F(MiniLsmComprehensiveTest, MvccBasicOperations) {
    // Create a transaction
    auto txn = mvcc_lsm_->NewTransaction();
    ASSERT_TRUE(txn != nullptr);
    
    // Put some data
    ByteBuffer key1(reinterpret_cast<const uint8_t*>("key1"), 4);
    ByteBuffer value1(reinterpret_cast<const uint8_t*>("value1"), 6);
    txn->Put(key1, value1);
    
    // Get should return the value we just put
    auto result = txn->Get(key1);
    ASSERT_FALSE(result.Empty());
    EXPECT_EQ(result, value1);
    
    // Delete the key
    txn->Delete(key1);
    
    // Get should now return an empty ByteBuffer
    result = txn->Get(key1);
    ASSERT_TRUE(result.Empty());
    
    // Commit the transaction
    bool commit_result = txn->Commit();
    EXPECT_TRUE(commit_result);
}

// Test MVCC snapshot isolation
TEST_F(MiniLsmComprehensiveTest, MvccSnapshotIsolation) {
    // Create and commit a transaction with initial data
    {
        auto txn = mvcc_lsm_->NewTransaction();
        ByteBuffer key(reinterpret_cast<const uint8_t*>("key"), 3);
        ByteBuffer value1(reinterpret_cast<const uint8_t*>("value1"), 6);
        txn->Put(key, value1);
        ASSERT_TRUE(txn->Commit());
    }
    
    // Start a transaction that will read the data
    auto txn1 = mvcc_lsm_->NewTransaction();
    ByteBuffer key(reinterpret_cast<const uint8_t*>("key"), 3);
    auto result1 = txn1->Get(key);
    ASSERT_FALSE(result1.Empty());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(result1.Data()), result1.Size()), "value1");
    
    // Start another transaction that will update the data
    auto txn2 = mvcc_lsm_->NewTransaction();
    ByteBuffer value2(reinterpret_cast<const uint8_t*>("value2"), 6);
    txn2->Put(key, value2);
    ASSERT_TRUE(txn2->Commit());
    
    // txn1 should still see the old value due to snapshot isolation
    auto result1_again = txn1->Get(key);
    ASSERT_FALSE(result1_again.Empty());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(result1_again.Data()), result1_again.Size()), "value1");
    
    // A new transaction should see the new value
    auto txn3 = mvcc_lsm_->NewTransaction();
    auto result3 = txn3->Get(key);
    ASSERT_FALSE(result3.Empty());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(result3.Data()), result3.Size()), "value2");
}

// Test MVCC scan functionality
TEST_F(MiniLsmComprehensiveTest, MvccScanOperations) {
    // Create initial data
    {
        auto txn = mvcc_lsm_->NewTransaction();
        for (int i = 0; i < 10; ++i) {
            std::string key_str = "key" + std::to_string(i);
            std::string value_str = "value" + std::to_string(i);
            
            ByteBuffer key(reinterpret_cast<const uint8_t*>(key_str.c_str()), key_str.size());
            ByteBuffer value(reinterpret_cast<const uint8_t*>(value_str.c_str()), value_str.size());
            
            txn->Put(key, value);
        }
        ASSERT_TRUE(txn->Commit());
    }
    
    // Create a transaction and scan a range
    auto txn = mvcc_lsm_->NewTransaction();
    
    ByteBuffer lower_key(reinterpret_cast<const uint8_t*>("key3"), 4);
    ByteBuffer upper_key(reinterpret_cast<const uint8_t*>("key7"), 4);
    
    Bound lower = Bound::Included(lower_key);
    Bound upper = Bound::Excluded(upper_key);
    
    auto iter = txn->Scan(lower, upper);
    
    // Verify scan results
    int count = 0;
    std::vector<std::string> expected_keys = {"key3", "key4", "key5", "key6"};
    
    while (iter->IsValid()) {
        std::string key_str(reinterpret_cast<const char*>(iter->Key().Data()), iter->Key().Size());
        std::string value_str(reinterpret_cast<const char*>(iter->Value().Data()), iter->Value().Size());
        
        ASSERT_LT(count, expected_keys.size()) << "Too many keys in scan result";
        EXPECT_EQ(key_str, expected_keys[count]);
        EXPECT_EQ(value_str, "value" + expected_keys[count].substr(3));
        
        iter->Next();
        count++;
    }
    
    EXPECT_EQ(count, expected_keys.size()) << "Not enough keys in scan result";
}

// Test MVCC serializable transactions
TEST_F(MiniLsmComprehensiveTest, MvccSerializableTransactions) {
    // Create initial data
    {
        auto txn = mvcc_lsm_->NewTransaction();
        ByteBuffer key1(reinterpret_cast<const uint8_t*>("key1"), 4);
        ByteBuffer key2(reinterpret_cast<const uint8_t*>("key2"), 4);
        ByteBuffer value1(reinterpret_cast<const uint8_t*>("value1"), 6);
        ByteBuffer value2(reinterpret_cast<const uint8_t*>("value2"), 6);
        txn->Put(key1, value1);
        txn->Put(key2, value2);
        ASSERT_TRUE(txn->Commit());
    }
    
    // Start two serializable transactions
    auto txn1 = mvcc_lsm_->NewSerializableTransaction();
    auto txn2 = mvcc_lsm_->NewSerializableTransaction();
    
    // Both read key1 and key2
    ByteBuffer key1(reinterpret_cast<const uint8_t*>("key1"), 4);
    ByteBuffer key2(reinterpret_cast<const uint8_t*>("key2"), 4);
    
    auto result1_1 = txn1->Get(key1);
    auto result1_2 = txn1->Get(key2);
    
    auto result2_1 = txn2->Get(key1);
    auto result2_2 = txn2->Get(key2);
    
    // txn1 updates key1
    ByteBuffer new_value1(reinterpret_cast<const uint8_t*>("new1"), 4);
    txn1->Put(key1, new_value1);
    
    // txn2 updates key2
    ByteBuffer new_value2(reinterpret_cast<const uint8_t*>("new2"), 4);
    txn2->Put(key2, new_value2);
    
    // txn1 commits first
    ASSERT_TRUE(txn1->Commit());
    
    // txn2 should still be able to commit since there's no conflict
    ASSERT_TRUE(txn2->Commit());
    
    // Verify final state
    auto verify_txn = mvcc_lsm_->NewTransaction();
    auto final_result1 = verify_txn->Get(key1);
    auto final_result2 = verify_txn->Get(key2);
    
    ASSERT_FALSE(final_result1.Empty());
    ASSERT_FALSE(final_result2.Empty());
    
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(final_result1.Data()), final_result1.Size()), "new1");
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(final_result2.Data()), final_result2.Size()), "new2");
}

// Main function required for Google Test
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
