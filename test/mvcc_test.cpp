#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "../include/mini_lsm_mvcc.hpp"
#include "../include/bound.hpp"
#include "../include/mvcc_transaction.hpp"

#include "test_util.hpp"

namespace test {

class MvccTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a temporary directory for testing with test-specific naming
        const ::testing::TestInfo* test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        test_dir_ = CreateTestDir() + "_" + test_info->test_suite_name() + "_" + test_info->name();
        
        // Ensure directory is completely clean
        std::filesystem::remove_all(test_dir_);
        std::filesystem::create_directories(test_dir_);
        
        // Create options with WAL enabled
        MiniLsmMvcc::Options options;
        options.enable_wal = true;
        
        // Create MiniLsmMvcc instance
        mvcc_lsm_ = std::make_unique<MiniLsmMvcc>(test_dir_, options);
    }

    void TearDown() override {
        // Close and cleanup thoroughly
        if (mvcc_lsm_) {
            try {
                mvcc_lsm_->Close();
            } catch (...) {
                // Ignore close errors during cleanup
            }
            mvcc_lsm_.reset();
        }
        
        // Force cleanup of test directory
        try {
            std::filesystem::remove_all(test_dir_);
        } catch (...) {
            // Ignore cleanup errors
        }
        
        // Small delay to ensure file system operations complete
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    std::string test_dir_;
    std::unique_ptr<MiniLsmMvcc> mvcc_lsm_;
};

// Test basic transaction operations
TEST_F(MvccTest, BasicTransactionOperations) {
    printf("[TEST] BasicTransactionOperations: Creating transaction\n");
    fflush(stdout);
    
    // Create a transaction
    auto txn = mvcc_lsm_->NewTransaction();
    ASSERT_TRUE(txn != nullptr);
    
    printf("[TEST] BasicTransactionOperations: Transaction created, type: %s\n", 
           typeid(*txn).name());
    fflush(stdout);
    
    // Put some data
    ByteBuffer key1(reinterpret_cast<const uint8_t*>("key1"), 4);
    ByteBuffer value1(reinterpret_cast<const uint8_t*>("value1"), 6);
    txn->Put(key1, value1);
    
    // Get should return the value we just put
    auto result = txn->Get(key1);
    ASSERT_FALSE(result.IsEmpty());
    EXPECT_EQ(result, value1);
    
    // Delete the key
    txn->Delete(key1);
    
    // Get should now return an empty ByteBuffer
    result = txn->Get(key1);
    ASSERT_TRUE(result.IsEmpty());
    
    // Commit the transaction
    bool commit_result = txn->Commit();
    EXPECT_TRUE(commit_result);
}

// Test snapshot isolation
TEST_F(MvccTest, SnapshotIsolation) {
    printf("[TEST] SnapshotIsolation test starting\n");
    fflush(stdout);
    // Create and commit a transaction with initial data
    {
        printf("[TEST] Creating first transaction\n");
        auto txn = mvcc_lsm_->NewTransaction();
        ByteBuffer key(reinterpret_cast<const uint8_t*>("key"), 3);
        ByteBuffer value1(reinterpret_cast<const uint8_t*>("value1"), 6);
        printf("[TEST] Putting key=key, value=value1\n");
        txn->Put(key, value1);
        printf("[TEST] Committing first transaction\n");
        bool commit_result = txn->Commit();
        printf("[TEST] First transaction commit result: %s\n", commit_result ? "true" : "false");
        ASSERT_TRUE(commit_result);
        printf("[TEST] First transaction committed successfully\n");
    }
    
    // Start a transaction that will read the data
    auto txn1 = mvcc_lsm_->NewTransaction();
    ByteBuffer key(reinterpret_cast<const uint8_t*>("key"), 3);
    auto result1 = txn1->Get(key);
    ASSERT_FALSE(result1.IsEmpty());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(result1.Data()), result1.Size()), "value1");
    
    // Start another transaction that will update the data
    auto txn2 = mvcc_lsm_->NewTransaction();
    ByteBuffer value2(reinterpret_cast<const uint8_t*>("value2"), 6);
    txn2->Put(key, value2);
    ASSERT_TRUE(txn2->Commit());
    
    // txn1 should still see the old value due to snapshot isolation
    auto result1_again = txn1->Get(key);
    ASSERT_FALSE(result1_again.IsEmpty());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(result1_again.Data()), result1_again.Size()), "value1");
    
    // A new transaction should see the new value
    auto txn3 = mvcc_lsm_->NewTransaction();
    auto result3 = txn3->Get(key);
    ASSERT_FALSE(result3.IsEmpty());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(result3.Data()), result3.Size()), "value2");
}

// Test concurrent transactions
TEST_F(MvccTest, ConcurrentTransactions) {
    constexpr int kNumThreads = 4;
    constexpr int kOpsPerThread = 100;
    
    std::vector<std::thread> threads;
    
    // Launch multiple threads to perform operations concurrently
    for (int t = 0; t < kNumThreads; ++t) {
        threads.emplace_back([this, t]() {
            for (int i = 0; i < kOpsPerThread; ++i) {
                // Create a unique key for this thread and operation
                std::string key_str = "key_" + std::to_string(t) + "_" + std::to_string(i);
                std::string value_str = "value_" + std::to_string(t) + "_" + std::to_string(i);
                
                ByteBuffer key(reinterpret_cast<const uint8_t*>(key_str.c_str()), key_str.size());
                ByteBuffer value(reinterpret_cast<const uint8_t*>(value_str.c_str()), value_str.size());
                
                // Create a transaction and perform operations
                auto txn = mvcc_lsm_->NewTransaction();
                txn->Put(key, value);
                ASSERT_TRUE(txn->Commit());
            }
        });
    }
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Verify all values were written correctly
    for (int t = 0; t < kNumThreads; ++t) {
        for (int i = 0; i < kOpsPerThread; ++i) {
            std::string key_str = "key_" + std::to_string(t) + "_" + std::to_string(i);
            std::string expected_value = "value_" + std::to_string(t) + "_" + std::to_string(i);
            
            ByteBuffer key(reinterpret_cast<const uint8_t*>(key_str.c_str()), key_str.size());
            
            auto txn = mvcc_lsm_->NewTransaction();
            auto result = txn->Get(key);
            
            ASSERT_FALSE(result.IsEmpty()) << "Missing key: " << key_str;
            EXPECT_EQ(std::string(reinterpret_cast<const char*>(result.Data()), result.Size()), expected_value);
        }
    }
}

// Test serializable transactions
TEST_F(MvccTest, SerializableTransactions) {
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
    
    ASSERT_FALSE(final_result1.IsEmpty());
    ASSERT_FALSE(final_result2.IsEmpty());
    
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(final_result1.Data()), final_result1.Size()), "new1");
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(final_result2.Data()), final_result2.Size()), "new2");
}

// Test scan functionality with transactions
TEST_F(MvccTest, TransactionScan) {
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
    std::cout << "[DEBUG] Created transaction for scan" << std::endl;
    
    ByteBuffer lower_key(reinterpret_cast<const uint8_t*>("key3"), 4);
    ByteBuffer upper_key(reinterpret_cast<const uint8_t*>("key7"), 4);
    
    auto lower = Bound::Included(lower_key);
    auto upper = Bound::Excluded(upper_key);
    
    std::cout << "[DEBUG] About to call Scan" << std::endl;
    auto iter = txn->Scan(lower, upper);
    std::cout << "[DEBUG] Scan returned" << std::endl;
    
    // Debug: Check if iterator is valid initially
    std::cout << "[DEBUG] Iterator initially valid: " << (iter->IsValid() ? "true" : "false") << std::endl;
    
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

} // namespace test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
