add_rules("mode.debug", "mode.release")
add_requires("gtest", "nlohmann_json", "zlib")

set_languages("cxx20")

-- Add common compiler flags
add_cxxflags("-Wall", "-Wextra")
add_cxxflags("-fstack-protector-strong", "-D_FORTIFY_SOURCE=2", "-Wformat-security")

-- Debug flags (ASAN disabled)
add_cxxflags("-fno-omit-frame-pointer", "-g", "-O1", {mode = "debug"})

-- Configure test execution to prevent race conditions

-- Release flags  
add_cxxflags("-O2", "-DNDEBUG", {mode = "release"})

-- Include directories
add_includedirs("include")

-- Core Mini-LSM library
target("mini_lsm")
    set_kind("static")
    add_files("src/*.cpp")
    remove_files("src/lsm_test.cpp", "src/lsm_storage_*.cpp")
    add_packages("nlohmann_json", "zlib")

-- LSM Recovery Test
target("lsm_recovery_test")
    set_kind("binary")
    add_deps("mini_lsm")
    add_files("test/lsm_recovery_test.cpp")
    add_packages("gtest")
    add_tests("default")

-- Basic Iterator Safety Test
target("basic_iterator_safety_test")
    set_kind("binary")
    add_deps("mini_lsm")
    add_files("test/basic_iterator_safety_test.cpp")
    add_packages("gtest")
    add_tests("default")

-- Iterator Safety Test
target("iterator_safety_test")
    set_kind("binary")
    add_deps("mini_lsm")
    add_files("test/iterator_safety_test.cpp")
    add_packages("gtest")
    add_tests("default")
    
-- Compaction Test
target("compaction_test")
    set_kind("binary")
    add_deps("mini_lsm")
    add_files("test/compaction_test.cpp")
    add_packages("gtest")
    add_tests("default")

-- MVCC Test
target("mvcc_test")
    set_kind("binary")
    add_deps("mini_lsm")
    add_files("test/mvcc_test.cpp")
    add_packages("gtest")
    add_tests("default")

-- MVCC Transaction Test
target("mvcc_transaction_test")
    set_kind("binary")
    add_deps("mini_lsm")
    add_files("test/mvcc_transaction_test.cpp")
    add_packages("gtest")
    add_tests("default")

-- MVCC Enhanced Transaction Test
target("mvcc_enhanced_transaction_test")
    set_kind("binary")
    add_deps("mini_lsm")
    add_files("test/mvcc_enhanced_transaction_test.cpp")
    add_packages("gtest")
    add_tests("default")

-- MVCC GC Test
target("mvcc_gc_test")
    set_kind("binary")
    add_deps("mini_lsm")
    add_files("test/mvcc_gc_test.cpp")
    add_packages("gtest")
    add_tests("default")

-- Mini LSM Comprehensive Test
target("mini_lsm_comprehensive_test")
    set_kind("binary")
    add_deps("mini_lsm")
    add_files("test/mini_lsm_comprehensive_test.cpp")
    add_packages("gtest")
    add_tests("default")
