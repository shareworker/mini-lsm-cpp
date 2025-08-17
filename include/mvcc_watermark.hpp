#pragma once

// MVCC Watermark implementation - C++ port of Rust watermark.rs
// Copyright (c) 2025
// Licensed under Apache 2.0.
//
// This header defines the Watermark class for MVCC garbage collection.

#include <cstdint>
#include <map>
#include <optional>

namespace util {

/**
 * @brief Watermark tracking for MVCC garbage collection
 * 
 * Tracks active reader timestamps and provides a watermark below which
 * data can be safely garbage collected.
 */
class Watermark {
public:
    /**
     * @brief Construct a new Watermark object
     */
    Watermark();

    /**
     * @brief Add a reader at the specified timestamp
     * 
     * @param ts Reader timestamp
     */
    void AddReader(uint64_t ts);

    /**
     * @brief Remove a reader at the specified timestamp
     * 
     * @param ts Reader timestamp
     */
    void RemoveReader(uint64_t ts);

    /**
     * @brief Get the current watermark
     * 
     * @return std::optional<uint64_t> Watermark timestamp if available, nullopt otherwise
     */
    std::optional<uint64_t> GetWatermark() const;

private:
    // Maps timestamp to count of readers at that timestamp
    std::map<uint64_t, size_t> readers_;
};

} // namespace util
