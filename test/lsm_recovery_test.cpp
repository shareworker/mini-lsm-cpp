#include <gtest/gtest.h>
#include "lsm_storage.hpp"
#include "byte_buffer.hpp"

#include <filesystem>
#include <cstdlib>
#include <ctime>

using namespace util;
namespace fs = std::filesystem;

namespace {
// Helper to create a unique temporary directory for each test run.
fs::path CreateTempDir(const std::string &prefix) {
    std::srand(static_cast<unsigned>(std::time(nullptr)));
    fs::path dir = fs::temp_directory_path() / (prefix + "_" + std::to_string(std::rand()));
    fs::create_directories(dir);
    return dir;
}
}  // namespace

// -----------------------------------------------------------------------------
// Manifest replay should restore SST and memtable state without WAL.
// -----------------------------------------------------------------------------
TEST(LsmRecoveryTest, ManifestReplay) {
    auto dir = CreateTempDir("manifest_replay");

    LsmStorageOptions options;
    options.enable_wal = false;  // Rely solely on manifest + SSTs.

    // 1. Create a brand-new storage and ingest some data.
    {
        auto storage = LsmStorageInner::Create(dir, options);
        ASSERT_TRUE(storage);

        ByteBuffer key("k1");
        ByteBuffer value("v1");
        ASSERT_TRUE(storage->Put(key, value));

        // Force flush so that the data goes into an SST and a MANIFEST record.
        storage->ForceFlushNextImmMemtable();
    }  // storage goes out of scope – destructor triggers final Flush().

    // 2. Re-open the storage and verify that the data is still present.
    auto reopened = LsmStorageInner::Open(dir, options);
    ASSERT_TRUE(reopened);
    auto result = reopened->Get(ByteBuffer("k1"));
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->ToString(), "v1");
}

// -----------------------------------------------------------------------------
// WAL replay should restore unflushed memtable contents after a crash.
// -----------------------------------------------------------------------------
TEST(LsmRecoveryTest, WalReplay) {
    auto dir = CreateTempDir("wal_replay");

    LsmStorageOptions options;
    options.enable_wal = true;

    // 1. Create storage and write a key, ensuring it is persisted only to WAL.
    //    We purposefully leak the instance to simulate a crash before Flush().
    {
        auto storage_unique = LsmStorageInner::Create(dir, options);
        ASSERT_TRUE(storage_unique);

        ByteBuffer key("k2");
        ByteBuffer value("v2");
        ASSERT_TRUE(storage_unique->Put(key, value));

        // fsync WAL so that the record is durable on disk.
        ASSERT_TRUE(storage_unique->Sync());

        // Crash simulation: release without destructor to avoid automatic flush.
        storage_unique.release();
    }

    // 2. Re-open the storage and expect the key to be recovered from the WAL.
    auto reopened = LsmStorageInner::Open(dir, options);
    ASSERT_TRUE(reopened);
    auto result = reopened->Get(ByteBuffer("k2"));
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->ToString(), "v2");
}

// -----------------------------------------------------------------------------
// main() entry for standalone execution (optional when using gtest runner).
// -----------------------------------------------------------------------------
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
