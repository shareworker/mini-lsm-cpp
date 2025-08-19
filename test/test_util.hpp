#pragma once

#include <filesystem>
#include <random>
#include <string>
#include <chrono>
#include <thread>

namespace test {

/**
 * @brief Create a temporary directory for testing
 * 
 * @return std::string Path to the temporary directory
 */
inline std::string CreateTestDir() {
    // Create a unique directory name using high resolution clock and thread ID
    auto now = std::chrono::high_resolution_clock::now();
    auto duration = now.time_since_epoch();
    auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 999999);
    
    std::string dir_name = "/tmp/mini_lsm_test_" + std::to_string(nanoseconds) + 
                          "_" + std::to_string(dis(gen)) + 
                          "_" + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id()));
    
    // Ensure any existing directory is removed first
    std::filesystem::remove_all(dir_name);
    
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
