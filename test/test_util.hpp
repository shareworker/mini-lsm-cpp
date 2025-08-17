#pragma once

#include <filesystem>
#include <random>
#include <string>

namespace util {
namespace test {

/**
 * @brief Create a temporary directory for testing
 * 
 * @return std::string Path to the temporary directory
 */
inline std::string CreateTestDir() {
    // Create a random directory name
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 999999);
    std::string dir_name = "/tmp/mini_lsm_test_" + std::to_string(dis(gen));
    
    // Create the directory
    std::filesystem::create_directories(dir_name);
    
    return dir_name;
}

/**
 * @brief Remove a test directory
 * 
 * @param dir_path Path to the directory
 */
inline void RemoveTestDir(const std::string& dir_path) {
    std::filesystem::remove_all(dir_path);
}

} // namespace test
} // namespace util
