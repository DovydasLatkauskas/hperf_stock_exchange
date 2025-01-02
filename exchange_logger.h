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
#include <cstring>
#include <condition_variable>

class ExchangeLogger {
public:
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
        uint8_t orderId[16];
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

    void log_order(LogLevel level, const uint8_t* orderId, 
                  const std::string& symbol, int price, int quantity) {
        ExchangeLogMessage msg{
            .level = level,
            .timestamp = get_current_time(),
            .message = "New Order",
            .symbol = symbol,
            .price = price,
            .quantity = quantity
        };
        std::memcpy(msg.orderId, orderId, 16);
        enqueue_message(msg);
    }

    void log_trade(const uint8_t* buyOrderId, const uint8_t* sellOrderId,
                  const std::string& symbol, int price, int quantity) {
        ExchangeLogMessage msg{
            .level = LogLevel::INFO,
            .timestamp = get_current_time(),
            .message = "Trade Executed",
            .symbol = symbol,
            .price = price,
            .quantity = quantity
        };

        std::memcpy(msg.orderId, buyOrderId, 16);
        enqueue_message(msg);
    }

    void log_system(LogLevel level, const std::string& message) {
        ExchangeLogMessage msg{
            .level = level,
            .timestamp = get_current_time(),
            .message = message
        };
        std::memset(msg.orderId, 0, 16);
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

    static std::string get_current_time() {
        auto now = std::chrono::system_clock::now();
        auto now_time = std::chrono::system_clock::to_time_t(now);
        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()) % 1000;
        
        std::stringstream ss;
        ss << std::put_time(std::localtime(&now_time), "%Y-%m-%d %H:%M:%S")
           << '.' << std::setfill('0') << std::setw(3) << now_ms.count();
        return ss.str();
    }

    static std::string level_to_string(LogLevel level) {
        switch (level) {
            case LogLevel::DEBUG:   return "DEBUG";
            case LogLevel::INFO:    return "INFO";
            case LogLevel::WARNING: return "WARNING";
            case LogLevel::ERROR:   return "ERROR";
            default:                return "UNKNOWN";
        }
    }

    static std::string orderid_to_string(const uint8_t* orderId) {
        std::stringstream ss;
        for (int i = 0; i < 16; ++i) {
            ss << std::hex << std::setw(2) << std::setfill('0') 
               << static_cast<int>(orderId[i]);
        }
        return ss.str();
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
                const auto& msg = message_queue_.front();
                
                outfile_ << msg.timestamp << " ["
                        << level_to_string(msg.level) << "] "
                        << msg.message;

                bool hasOrderId = false;
                for (int i = 0; i < 16; i++) {
                    if (msg.orderId[i] != 0) {
                        hasOrderId = true;
                        break;
                    }
                }
                if (hasOrderId) {
                    outfile_ << " OrderID:" << orderid_to_string(msg.orderId);
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
            
            outfile_.flush();
            lock.unlock();
            
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
};
