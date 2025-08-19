#pragma once

#include <string>
#include <fstream>
#include <memory>
#include <filesystem>
#include <chrono>
#include <ctime>
#include <unistd.h>
#include <mutex>
#include <sstream>
#include <iostream>
#include <queue>
#include <thread>
#include <condition_variable>
#include <atomic>
#include <cstdarg>
#include <cstdio>

namespace logger {

enum class Level {
    DEBUG,
    INFO,
    WARNING,
    ERROR
};

struct LogConfig {
    std::string log_dir = "/tmp/.util_log";
    bool use_stdout = false;
    Level min_level = Level::DEBUG;
    size_t max_file_size = 10 * 1024 * 1024;  // 10MB
    size_t max_files = 5;
    bool async_mode = true;
    size_t flush_interval_ms = 1000;
};

class Logger {
public:
    static Logger& instance() {
        static Logger instance;
        return instance;
    }

    void configure(const LogConfig& config) {
        std::lock_guard<std::mutex> lock(mutex_);
        config_ = config;
        init();
    }

    void log(Level level, const char* file, const char* func, int line, const char* fmt, ...) {
        if (level < config_.min_level) return;
        if (!init_success_) return;

        char buffer[256];
        va_list args;
        va_start(args, fmt);
        vsnprintf(buffer, sizeof(buffer), fmt, args);
        va_end(args);

        auto log_entry = format_log(level, file, func, line, buffer);
        
        if (config_.async_mode) {
            enqueue_log(log_entry);
        } else {
            write_log(log_entry);
        }
    }

    ~Logger() {
        if (config_.async_mode) {
            stop_async_thread();
        }
    }

private:
    Logger() : config_(), stop_flag_(false) {
        init();
        if (config_.async_mode) {
            start_async_thread();
        }
    }

    void init() {
        namespace fs = std::filesystem;
        
        try {
            if (config_.use_stdout) {
                init_success_ = true;
                return;
            }

            if (!fs::exists(config_.log_dir)) {
                fs::create_directories(config_.log_dir);
            }

            rotate_log_files();
            open_new_log_file();

        } catch (const std::exception& e) {
            init_success_ = false;
            std::cerr << "Logger initialization failed: " << e.what() << std::endl;
        }
    }

    void rotate_log_files() {
        namespace fs = std::filesystem;
        
        if (!file_stream_) return;
        
        file_stream_->flush();
        if (fs::file_size(current_log_path_) < config_.max_file_size) {
            return;
        }

        // Rotate existing files
        for (int i = config_.max_files - 1; i >= 0; --i) {
            auto old_path = get_log_path(i);
            auto new_path = get_log_path(i + 1);
            if (fs::exists(old_path)) {
                if (i == static_cast<int>(config_.max_files) - 1) {
                    fs::remove(old_path);
                } else {
                    fs::rename(old_path, new_path);
                }
            }
        }

        file_stream_->close();
        open_new_log_file();
    }

    std::filesystem::path get_log_path(int index) {
        namespace fs = std::filesystem;
        auto base_name = std::to_string(getpid()) + ".log";
        if (index == 0) return fs::path(config_.log_dir) / base_name;
        return fs::path(config_.log_dir) / (base_name + "." + std::to_string(index));
    }

    void open_new_log_file() {
        current_log_path_ = get_log_path(0);
        file_stream_ = std::make_unique<std::ofstream>(
            current_log_path_, std::ios::out | std::ios::app);
        init_success_ = file_stream_->is_open();
    }

    std::string format_log(Level level, const char* file, const char* func, int line, const std::string& message) {
        auto now = std::chrono::system_clock::now();
        std::time_t t = std::chrono::system_clock::to_time_t(now);
        char time_str[20];
        std::strftime(time_str, sizeof(time_str), "%Y%m%d%H%M%S", std::localtime(&t));

        std::string level_str;
        switch(level) {
            case Level::DEBUG: level_str = "DEBUG"; break;
            case Level::INFO: level_str = "INFO"; break;
            case Level::WARNING: level_str = "WARNING"; break;
            case Level::ERROR: level_str = "ERROR"; break;
        }

        std::ostringstream oss;
        oss << "[" << time_str << "] "
            << "[" << level_str << "] "
            << "[" << getpid() << "] "
            << "[" << file << ":" << func << ":" << line << "] "
            << message << "\n";
        return oss.str();
    }

    void write_log(const std::string& log_entry) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        if (config_.use_stdout) {
            std::cout << log_entry;
            return;
        }

        if (file_stream_ && file_stream_->is_open()) {
            *file_stream_ << log_entry;
            file_stream_->flush();
            
            namespace fs = std::filesystem;
            if (fs::file_size(current_log_path_) >= config_.max_file_size) {
                rotate_log_files();
            }
        }
    }

    void enqueue_log(const std::string& log_entry) {
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            log_queue_.push(log_entry);
        }
        queue_cv_.notify_one();
    }

    void start_async_thread() {
        async_thread_ = std::thread([this] { async_logging_thread(); });
    }

    void stop_async_thread() {
        stop_flag_ = true;
        queue_cv_.notify_one();
        if (async_thread_.joinable()) {
            async_thread_.join();
        }
    }

    void async_logging_thread() {
        while (!stop_flag_) {
            std::vector<std::string> batch;
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                if (log_queue_.empty()) {
                    queue_cv_.wait_for(lock, 
                        std::chrono::milliseconds(config_.flush_interval_ms),
                        [this] { return !log_queue_.empty() || stop_flag_; });
                }
                
                while (!log_queue_.empty()) {
                    batch.push_back(std::move(log_queue_.front()));
                    log_queue_.pop();
                }
            }

            for (const auto& entry : batch) {
                write_log(entry);
            }
        }
    }

    LogConfig config_;
    std::unique_ptr<std::ofstream> file_stream_;
    bool init_success_ = false;
    std::mutex mutex_;
    std::filesystem::path current_log_path_;

    // Async logging members
    std::queue<std::string> log_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::thread async_thread_;
    std::atomic<bool> stop_flag_;
};

} // namespace logger

#define LOG_DEBUG(fmt, ...) \
    logger::Logger::instance().log(logger::Level::DEBUG, __FILE__, __FUNCTION__, __LINE__, fmt, ## __VA_ARGS__)

#define LOG_INFO(fmt, ...) \
    logger::Logger::instance().log(logger::Level::INFO, __FILE__, __FUNCTION__, __LINE__, fmt, ## __VA_ARGS__)

#define LOG_WARNING(fmt, ...) \
    logger::Logger::instance().log(logger::Level::WARNING, __FILE__, __FUNCTION__, __LINE__, fmt, ## __VA_ARGS__)

#define LOG_ERROR(fmt, ...) \
    logger::Logger::instance().log(logger::Level::ERROR, __FILE__, __FUNCTION__, __LINE__, fmt, ## __VA_ARGS__)