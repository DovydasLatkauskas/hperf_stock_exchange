#pragma once

#include <string>
#include <fstream>
#include <queue>
#include <thread>
#include <mutex>
#include <atomic>
#include <chrono>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <condition_variable>

class ExchangeLogger {
public:
    ExchangeLogger(const ExchangeLogger&) = delete;
    ExchangeLogger& operator=(const ExchangeLogger&) = delete;
    ExchangeLogger(ExchangeLogger&&) = delete;
    ExchangeLogger& operator=(ExchangeLogger&&) = delete;

    enum class LogLevel {
        DEBUG,
        INFO,
        WARNING,
        ERROR
    };

    struct ExchangeLogMessage {
        LogLevel level;
        std::string timestamp;
        std::string message;
        std::array<uint8_t, 16> orderId{};
        std::array<uint8_t, 16> secondaryId{};
        std::string symbol;
        int price = 0;
        int quantity = 0;
    };

    explicit ExchangeLogger(const std::string& filename) : filename_(filename) {
        outfile_.open(filename, std::ios::app);
        if (!outfile_.is_open()) {
            throw std::runtime_error("Failed to open log file: " + filename);
        }
        
        logger_thread_ = std::thread(&ExchangeLogger::process_logs, this);
    }

    ~ExchangeLogger() {
        running_ = false;
        if (logger_thread_.joinable()) {
            logger_thread_.join();
        }
        outfile_.close();
    }

    void log_order(const LogLevel level, const std::array<uint8_t, 16>& orderId,
                  const std::string& symbol, const int price, const int quantity) {
        ExchangeLogMessage msg{
            .level = level,
            .timestamp = get_current_time(),
            .message = "New Order",
            .symbol = symbol,
            .price = price,
            .quantity = quantity
        };
        msg.orderId = orderId;
        enqueue_message(msg);
    }

    void log_trade(const std::array<uint8_t, 16>& buyOrderId,
                   const std::array<uint8_t, 16>& sellOrderId,
                   const std::string& symbol, const int price, const int quantity) {
        ExchangeLogMessage msg{
            .level = LogLevel::INFO,
            .timestamp = get_current_time(),
            .message = "Trade Executed",
            .symbol = symbol,
            .price = price,
            .quantity = quantity
        };
        msg.orderId = buyOrderId;
        msg.secondaryId = sellOrderId;
        enqueue_message(msg);
    }

    void log_system(const LogLevel level, const std::string& message) {
        ExchangeLogMessage msg{
            .level = level,
            .timestamp = get_current_time(),
            .message = message
        };
        enqueue_message(msg);
    }

private:
    std::string filename_;
    std::ofstream outfile_;
    std::queue<ExchangeLogMessage> message_queue_;
    std::mutex queue_mutex_;
    std::atomic<bool> running_{true};
    std::thread logger_thread_;
    std::condition_variable cv_;
    std::chrono::milliseconds flush_interval_{1000};
    std::chrono::steady_clock::time_point last_flush_;

    static std::string get_current_time() {
        const auto now = std::chrono::system_clock::now();
        const auto now_time = std::chrono::system_clock::to_time_t(now);
        const auto now_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
        
        std::stringstream ss;
        ss << std::put_time(std::localtime(&now_time), "%Y-%m-%d %H:%M:%S")
           << '.' << std::setfill('0') << std::setw(3) << now_ms.count();
        return ss.str();
    }

    static std::string level_to_string(const LogLevel level) {
        switch (level) {
            case LogLevel::DEBUG:   return "DEBUG";
            case LogLevel::INFO:    return "INFO";
            case LogLevel::WARNING: return "WARNING";
            case LogLevel::ERROR:   return "ERROR";
            default:                return "UNKNOWN";
        }
    }

    static std::string order_id_to_string(const std::array<uint8_t, 16>& orderId) {
        std::stringstream ss;
        for (const auto& byte : orderId) {
            ss << std::hex << std::setw(2) << std::setfill('0')
               << static_cast<int>(byte);
        }
        return ss.str();
    }

    static bool has_order_id(const std::array<uint8_t, 16>& orderId) {
        return std::any_of(orderId.begin(), orderId.end(),
                          [](const uint8_t b) { return b != 0; });
    }

    void maybe_flush() {
        auto now = std::chrono::steady_clock::now();
        if (now - last_flush_ > flush_interval_) {
            outfile_.flush();
            last_flush_ = now;
        }
    }

    void enqueue_message(const ExchangeLogMessage& msg) {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        message_queue_.push(msg);
        cv_.notify_one();
    }

    void process_logs() {
        while (running_) {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            cv_.wait(lock, [this]() {
                return !message_queue_.empty() || !running_;
            });
            
            while (!message_queue_.empty()) {
                const ExchangeLogMessage& msg = message_queue_.front();
                
                outfile_ << msg.timestamp << " ["
                        << level_to_string(msg.level) << "] "
                        << msg.message;

                // Check primary order ID
                if (has_order_id(msg.orderId)) {
                    outfile_ << " OrderID:" << order_id_to_string(msg.orderId);
                }

                // Check secondary order ID for trades
                if (has_order_id(msg.secondaryId)) {
                    outfile_ << " MatchedWith:" << order_id_to_string(msg.secondaryId);
                }
                
                if (!msg.symbol.empty()) {
                    outfile_ << " Symbol:" << msg.symbol;
                }
                if (msg.price > 0) {
                    outfile_ << " Price:" << msg.price;
                }
                if (msg.quantity > 0) {
                    outfile_ << " Qty:" << msg.quantity;
                }
                
                outfile_ << std::endl;
                message_queue_.pop();
            }

            maybe_flush();
        }
    }
};
