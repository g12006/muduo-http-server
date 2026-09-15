// async_log.hpp —— 异步日志模块
//   前端（业务线程）：互斥锁 + memcpy 进当前缓冲，写满则把缓冲交给待写队列，
//                     换用备用缓冲，并 write(eventfd) 唤醒后端线程 —— 全程不做磁盘 IO
//   后端（专用线程）：poll(eventfd, timeout=flush_interval)，整块 fwrite 落盘，
//                     写完把缓冲归还空闲池
//   双缓冲交换：cur_ / next_ 两块 4MB 缓冲轮换，保证前端任何时刻都有可写缓冲
#pragma once

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace bite {

enum class LogLevel : int { TRACE = 0, DEBUG, INFO, WARN, ERROR, FATAL };

const char* LevelName(LogLevel lv);

// ---------------------------------------------------------------------------
// 固定容量缓冲：前端只做 memcpy，不产生任何系统调用
// ---------------------------------------------------------------------------
class FixedBuffer {
public:
    static const size_t kCapacity = 4 * 1024 * 1024;  // 4MB

    FixedBuffer() : cur_(data_) {}

    const char* Data()   const { return data_; }
    size_t      Length() const { return static_cast<size_t>(cur_ - data_); }
    size_t      Avail()  const { return kCapacity - Length(); }
    bool        Empty()  const { return cur_ == data_; }

    void Reset() { cur_ = data_; }

    // 预触所有页：4MB 缓冲首次写入会触发约 1024 次缺页中断（实测单次可卡 1.8ms），
    // 在分配时一次性 memset 掉，把代价挪出业务线程的热路径
    void Touch() { std::memset(data_, 0, sizeof data_); }

    void Append(const char* p, size_t n) {
        if (n <= Avail()) {
            std::memcpy(cur_, p, n);
            cur_ += n;
        }
    }

private:
    char  data_[kCapacity];
    char* cur_;
};

// ---------------------------------------------------------------------------
// 无符号/有符号整数 → 十进制：手写转换，比 snprintf 快 5~8 倍（日志热路径）
// ---------------------------------------------------------------------------
namespace detail {

inline const char* DigitsEnd() {
    static const char kDigits[] = "9876543210123456789";
    return kDigits + 9;  // 指向 '0'
}

template <typename T>
inline size_t ConvertUInt(char* buf, T value) {
    const char* zero = DigitsEnd();
    char tmp[24];
    int  i = 0;
    do {
        int lsd = static_cast<int>(value % 10);
        value /= 10;
        tmp[i++] = zero[lsd];
    } while (value != 0);
    char* p = buf;
    while (i > 0) *p++ = tmp[--i];
    return static_cast<size_t>(p - buf);
}

template <typename T>
inline size_t ConvertInt(char* buf, T value) {
    char* p = buf;
    unsigned long long uv;
    if (value < 0) {
        *p++ = '-';
        uv = 0ull - static_cast<unsigned long long>(value);  // 补码取绝对值，避免 -INT64_MIN 溢出
    } else {
        uv = static_cast<unsigned long long>(value);
    }
    return static_cast<size_t>(p - buf) + ConvertUInt(p, uv);
}

}  // namespace detail

// ---------------------------------------------------------------------------
// 日志流：把一条日志拼进栈上 4KB 缓冲，最后一次性交给 AsyncLogger
// ---------------------------------------------------------------------------
class LogStream {
public:
    static const int kMaxLine = 4096;

    LogStream() : cur_(buf_), end_(buf_ + kMaxLine) {}

    const char* Data()   const { return buf_; }
    size_t      Length() const { return static_cast<size_t>(cur_ - buf_); }

    // 追加裸内存（长度已知，免 strlen）
    LogStream& AppendRaw(const char* p, size_t n) { Append(p, n); return *this; }

    LogStream& operator<<(bool v)               { return *this << (v ? "true" : "false"); }
    LogStream& operator<<(char v)               { Append(&v, 1); return *this; }
    LogStream& operator<<(short v)              { return FormatInt(v); }
    LogStream& operator<<(unsigned short v)     { return FormatUInt(v); }
    LogStream& operator<<(int v)                { return FormatInt(v); }
    LogStream& operator<<(unsigned int v)       { return FormatUInt(v); }
    LogStream& operator<<(long v)               { return FormatInt(v); }
    LogStream& operator<<(unsigned long v)      { return FormatUInt(v); }
    LogStream& operator<<(long long v)          { return FormatInt(v); }
    LogStream& operator<<(unsigned long long v) { return FormatUInt(v); }
    LogStream& operator<<(double v) {
        char t[32];
        int n = std::snprintf(t, sizeof t, "%.6f", v);
        Append(t, n > 0 ? static_cast<size_t>(n) : 0);
        return *this;
    }
    LogStream& operator<<(const char* s) {
        if (s) Append(s, std::strlen(s));
        return *this;
    }
    LogStream& operator<<(const std::string& s) { Append(s.data(), s.size()); return *this; }
    LogStream& operator<<(const void* p) {
        char t[32];
        int n = std::snprintf(t, sizeof t, "%p", p);
        Append(t, n > 0 ? static_cast<size_t>(n) : 0);
        return *this;
    }

private:
    void Append(const char* p, size_t n) {
        size_t room = static_cast<size_t>(end_ - cur_);
        if (n > room) n = room;
        std::memcpy(cur_, p, n);
        cur_ += n;
    }

    template <typename T>
    LogStream& FormatInt(T v) {
        char   t[24];
        size_t n = detail::ConvertInt(t, v);
        Append(t, n);
        return *this;
    }

    template <typename T>
    LogStream& FormatUInt(T v) {
        char   t[24];
        size_t n = detail::ConvertUInt(t, v);
        Append(t, n);
        return *this;
    }

    char  buf_[kMaxLine];
    char* cur_;
    char* end_;
};

// ---------------------------------------------------------------------------
// 运行统计
// ---------------------------------------------------------------------------
struct AsyncLoggerStats {
    uint64_t lines   = 0;  // 已接收行数
    uint64_t bytes   = 0;  // 已落盘字节
    uint64_t buffers = 0;  // 累计分配缓冲块数
    uint64_t wakeups = 0;  // eventfd 唤醒次数
    uint64_t dropped = 0;  // 丢弃行数（单行超 4MB 才会发生，正常恒为 0）
};

// ---------------------------------------------------------------------------
// 异步日志器（进程内单例，Start/Stop 显式控制生命周期）
// ---------------------------------------------------------------------------
class AsyncLogger {
public:
    static const size_t kMaxFreeBuffers = 16;  // 空闲池上限：16 × 4MB

    static AsyncLogger& Instance();

    void Start(const std::string& filename,
               LogLevel level         = LogLevel::INFO,
               int      flush_interval_ms = 3000,
               size_t   roll_size     = 256ull * 1024 * 1024);
    void Stop();

    void SetLevel(LogLevel lv) { level_.store(lv, std::memory_order_relaxed); }
    bool ShouldLog(LogLevel lv) const {
        return lv >= level_.load(std::memory_order_relaxed);
    }

    void Append(const char* line, size_t len);
    void Flush();

    AsyncLoggerStats Stats() const;
    const std::string& Filename() const { return filename_; }

private:
    AsyncLogger() = default;
    ~AsyncLogger();
    AsyncLogger(const AsyncLogger&)            = delete;
    AsyncLogger& operator=(const AsyncLogger&) = delete;

    void BackendLoop();                                            // 后端线程主循环
    void WriteRound(std::vector<std::unique_ptr<FixedBuffer>>& s);  // 整块落盘 + 回收缓冲
    void RollFile();                                               // 按大小滚动
    void Wakeup();
    void OpenFile();
    std::unique_ptr<FixedBuffer> NewBuffer();                      // 分配 + 预触页

    std::string filename_;
    FILE*       fp_ = nullptr;

    std::atomic<LogLevel> level_{LogLevel::INFO};
    int                   flush_interval_ms_{3000};
    size_t                roll_size_{256ull * 1024 * 1024};

    mutable std::mutex mutex_;
    std::unique_ptr<FixedBuffer>              cur_;   // 前端正在写
    std::unique_ptr<FixedBuffer>              next_;  // 双缓冲的第二块
    std::vector<std::unique_ptr<FixedBuffer>> full_;  // 写满待落盘
    std::vector<std::unique_ptr<FixedBuffer>> free_;  // 空闲池

    int               wakeup_fd_ = -1;   // eventfd
    std::atomic<bool> running_{false};
    std::atomic<bool> stop_{false};
    std::thread       backend_;

    size_t rolled_bytes_ = 0;            // 仅后端线程访问
    uint64_t roll_seq_   = 0;            // 滚动序号，避免同一秒内多次滚动互相覆盖

    std::atomic<uint64_t> lines_{0}, bytes_{0}, buffers_{0}, wakeups_{0}, dropped_{0};
};

// ---------------------------------------------------------------------------
// 一行日志的 RAII 对象：析构时把整行一次性投递到 AsyncLogger
// ---------------------------------------------------------------------------
class LogLine {
public:
    LogLine(const char* file, int line, LogLevel lv);
    ~LogLine();

    LogStream& Stream() { return stream_; }

private:
    LogStream stream_;
    LogLevel  level_;
};

}  // namespace bite

#define BITE_LOG_LEVEL(lv)                                                  \
    if (!::bite::AsyncLogger::Instance().ShouldLog(lv)) {                   \
    } else                                                                  \
        ::bite::LogLine(__FILE__, __LINE__, lv).Stream()

#define LOG_TRACE BITE_LOG_LEVEL(::bite::LogLevel::TRACE)
#define LOG_DEBUG BITE_LOG_LEVEL(::bite::LogLevel::DEBUG)
#define LOG_INFO  BITE_LOG_LEVEL(::bite::LogLevel::INFO)
#define LOG_WARN  BITE_LOG_LEVEL(::bite::LogLevel::WARN)
#define LOG_ERROR BITE_LOG_LEVEL(::bite::LogLevel::ERROR)
#define LOG_FATAL BITE_LOG_LEVEL(::bite::LogLevel::FATAL)
