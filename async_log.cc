// async_log.cc —— 异步日志模块实现
#include "async_log.hpp"

#include <cerrno>
#include <ctime>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

namespace bite {

namespace {

const char* const kLevelNames[] = {"TRACE", "DEBUG", "INFO ", "WARN ", "ERROR", "FATAL"};

// 线程内缓存 TID：gettid 是真正的系统调用，每行都调一次成本可观
inline pid_t CurrentTid() {
    static thread_local pid_t tid = [] {
#ifdef SYS_gettid
        return static_cast<pid_t>(::syscall(SYS_gettid));
#else
        return ::getpid();
#endif
    }();
    return tid;
}

// 时间戳热路径优化：
//   localtime_r + snprintf 是每行最贵的两步（时区查询要加锁）。
//   这里把 "YYYY-MM-DD HH:MM:SS" 前缀按"秒"缓存，秒没变就直接 memcpy，
//   只有微秒部分每次现算（定宽 6 位手写转换）。统计上命中率 ≈ 1 - 1/每行耗时占比。
struct TimePrefixCache {
    time_t sec = 0;
    int    len = 0;
    char   text[32] = {0};
};

inline void FormatTimestampFast(char* buf, size_t n, int* out_len) {
    static thread_local TimePrefixCache cache;
    struct timespec ts;
    ::clock_gettime(CLOCK_REALTIME, &ts);
    if (ts.tv_sec != cache.sec) {
        struct tm tmv;
        ::localtime_r(&ts.tv_sec, &tmv);
        cache.len = std::snprintf(cache.text, sizeof cache.text,
                                  "%04d-%02d-%02d %02d:%02d:%02d",
                                  tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday,
                                  tmv.tm_hour, tmv.tm_min, tmv.tm_sec);
        if (cache.len < 0) cache.len = 0;
        cache.sec = ts.tv_sec;
    }
    char* p = buf;
    size_t cp = static_cast<size_t>(cache.len);
    if (cp + 8 > n) cp = (n > 8) ? n - 8 : 0;
    std::memcpy(p, cache.text, cp);
    p += cp;
    *p++ = '.';
    int us = static_cast<int>(ts.tv_nsec / 1000);   // 0..999999
    for (int div = 100000; div > 0; div /= 10) {
        *p++ = static_cast<char>('0' + (us / div) % 10);
    }
    *out_len = static_cast<int>(p - buf);
}

// 递归创建日志目录（只处理一层，够用）
void EnsureDirectory(const std::string& path) {
    std::string::size_type pos = path.find_last_of('/');
    if (pos == std::string::npos) return;
    std::string dir = path.substr(0, pos);
    if (dir.empty()) return;
    if (::mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST) {
        std::fprintf(stderr, "[async_log] mkdir %s failed: %s\n", dir.c_str(), std::strerror(errno));
    }
}

inline uint64_t CountLines(const char* p, size_t n) {
    uint64_t c = 0;
    for (size_t i = 0; i < n; ++i)
        if (p[i] == '\n') ++c;
    return c;
}

}  // namespace

const char* LevelName(LogLevel lv) {
    int i = static_cast<int>(lv);
    if (i < 0 || i >= static_cast<int>(sizeof(kLevelNames) / sizeof(kLevelNames[0])))
        return "UNKNW";
    return kLevelNames[i];
}

// ---------------------------------------------------------------------------
// LogLine
// ---------------------------------------------------------------------------
LogLine::LogLine(const char* file, int line, LogLevel lv) : level_(lv) {
    char ts[48];
    int  tslen = 0;
    FormatTimestampFast(ts, sizeof ts, &tslen);
    const char* base = std::strrchr(file, '/');
    base = base ? base + 1 : file;
    stream_.AppendRaw(ts, static_cast<size_t>(tslen));
    stream_ << ' ' << LevelName(lv) << " ["
            << static_cast<int>(::getpid()) << ':' << static_cast<int>(CurrentTid())
            << "] " << base << ':' << line << " - ";
}

LogLine::~LogLine() {
    stream_ << '\n';
    AsyncLogger::Instance().Append(stream_.Data(), stream_.Length());
}

// ---------------------------------------------------------------------------
// AsyncLogger
// ---------------------------------------------------------------------------
AsyncLogger& AsyncLogger::Instance() {
    static AsyncLogger inst;
    return inst;
}

AsyncLogger::~AsyncLogger() { Stop(); }

std::unique_ptr<FixedBuffer> AsyncLogger::NewBuffer() {
    auto b = std::make_unique<FixedBuffer>();
    b->Touch();   // 预触页：避免业务线程在热路径上吃缺页中断（实测单次可达 1.8ms）
    buffers_.fetch_add(1, std::memory_order_relaxed);
    return b;
}

void AsyncLogger::OpenFile() {
    EnsureDirectory(filename_);
    fp_ = std::fopen(filename_.c_str(), "a");   // 追加：崩溃重启不覆盖历史日志
    if (!fp_) {
        std::fprintf(stderr, "[async_log] open %s failed: %s\n",
                     filename_.c_str(), std::strerror(errno));
    }
}

void AsyncLogger::Start(const std::string& filename, LogLevel level,
                        int flush_interval_ms, size_t roll_size) {
    std::lock_guard<std::mutex> lk(mutex_);
    if (running_.load(std::memory_order_relaxed)) return;

    filename_          = filename;
    level_.store(level, std::memory_order_relaxed);
    flush_interval_ms_ = flush_interval_ms > 0 ? flush_interval_ms : 3000;
    roll_size_         = roll_size > 0 ? roll_size : (256ull * 1024 * 1024);

    OpenFile();
    wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wakeup_fd_ < 0) {
        std::fprintf(stderr, "[async_log] eventfd failed: %s\n", std::strerror(errno));
    }

    cur_  = NewBuffer();
    next_ = NewBuffer();
    // 预留 2 块空闲缓冲：让前端在"写满换缓冲"的热路径上永远不需要分配内存
    for (int i = 0; i < 2; ++i) free_.push_back(NewBuffer());

    stop_.store(false, std::memory_order_relaxed);
    running_.store(true, std::memory_order_relaxed);
    backend_ = std::thread([this] { BackendLoop(); });
}

void AsyncLogger::Stop() {
    {
        std::lock_guard<std::mutex> lk(mutex_);
        if (!running_.load(std::memory_order_relaxed)) return;
        stop_.store(true, std::memory_order_release);
    }
    Wakeup();                       // 叫醒后端，做最后一轮落盘
    if (backend_.joinable()) backend_.join();

    std::lock_guard<std::mutex> lk(mutex_);
    running_.store(false, std::memory_order_relaxed);
    if (fp_) {
        ::fflush(fp_);
        ::fclose(fp_);
        fp_ = nullptr;
    }
    if (wakeup_fd_ >= 0) {
        ::close(wakeup_fd_);
        wakeup_fd_ = -1;
    }
    cur_.reset();
    next_.reset();
    full_.clear();
    free_.clear();
}

void AsyncLogger::Wakeup() {
    if (wakeup_fd_ < 0) return;
    uint64_t one = 1;
    ssize_t n = ::write(wakeup_fd_, &one, sizeof one);   // eventfd 计数，不会阻塞
    (void)n;
}

void AsyncLogger::Append(const char* line, size_t len) {
    // 未启动：退化成直接落盘（不丢日志），保证模块可被独立使用
    if (!running_.load(std::memory_order_relaxed)) {
        std::lock_guard<std::mutex> lk(mutex_);
        FILE* out = fp_ ? fp_ : stdout;
        ::fwrite(line, 1, len, out);
        return;
    }

    std::lock_guard<std::mutex> lk(mutex_);
    if (!cur_) cur_ = NewBuffer();

    if (cur_->Avail() < len) {
        // 当前缓冲写满 —— 交给后端，换用备用缓冲（双缓冲交换）
        full_.push_back(std::move(cur_));
        if (next_) {
            cur_ = std::move(next_);
        } else if (!free_.empty()) {
            cur_ = std::move(free_.back());
            free_.pop_back();
        } else {
            cur_ = NewBuffer();   // 极端情况（后端落后太多）才会走到这里
        }
        Wakeup();
    }

    if (cur_ && cur_->Avail() >= len) {
        cur_->Append(line, len);
        lines_.fetch_add(1, std::memory_order_relaxed);
    } else {
        dropped_.fetch_add(1, std::memory_order_relaxed);   // 单行超过 4MB，理论不可达
    }
}

void AsyncLogger::Flush() {
    Wakeup();
}

AsyncLoggerStats AsyncLogger::Stats() const {
    AsyncLoggerStats s;
    s.lines   = lines_.load(std::memory_order_relaxed);
    s.bytes   = bytes_.load(std::memory_order_relaxed);
    s.buffers = buffers_.load(std::memory_order_relaxed);
    s.wakeups = wakeups_.load(std::memory_order_relaxed);
    s.dropped = dropped_.load(std::memory_order_relaxed);
    return s;
}

void AsyncLogger::BackendLoop() {
    std::vector<std::unique_ptr<FixedBuffer>> round;
    struct pollfd pfd;
    pfd.fd      = wakeup_fd_;
    pfd.events  = POLLIN;
    pfd.revents = 0;

    while (true) {
        int n = (wakeup_fd_ >= 0) ? ::poll(&pfd, 1, flush_interval_ms_) : flush_interval_ms_;
        bool stopping = stop_.load(std::memory_order_acquire);

        {
            std::lock_guard<std::mutex> lk(mutex_);
            if (n > 0 && (pfd.revents & POLLIN)) {
                uint64_t v = 0;
                while (::read(wakeup_fd_, &v, sizeof v) > 0) { }   // 合并多次唤醒
                wakeups_.fetch_add(1, std::memory_order_relaxed);
            }
            // 1) 把前端"正在写但没写满"的缓冲也摘下来（否则它会一直等不到落盘）
            if (cur_ && !cur_->Empty()) full_.push_back(std::move(cur_));
            // 2) 立刻给前端补一块备用缓冲，保证它下一次写满时无需分配
            if (!next_) {
                if (!free_.empty()) {
                    next_ = std::move(free_.back());
                    free_.pop_back();
                } else {
                    next_ = NewBuffer();
                }
            }
            if (!cur_) cur_ = std::move(next_);
            // 3) 拿走本轮待写缓冲，锁外做磁盘 IO
            round.swap(full_);
        }

        if (!round.empty()) WriteRound(round);

        if (stopping) {
            // 收尾：把可能残留的前端缓冲再摘一次并落盘
            std::lock_guard<std::mutex> lk(mutex_);
            if (cur_ && !cur_->Empty()) { full_.push_back(std::move(cur_)); }
            round.swap(full_);
            if (!round.empty()) WriteRound(round);
            break;
        }
    }
}

void AsyncLogger::WriteRound(std::vector<std::unique_ptr<FixedBuffer>>& bufs) {
    uint64_t bytes = 0;
    for (auto& b : bufs) {
        size_t n = b->Length();
        if (n == 0) continue;
        if (fp_) {
            size_t w = ::fwrite(b->Data(), 1, n, fp_);
            bytes += w;
        }
    }
    if (fp_) ::fflush(fp_);          // 每轮结束刷一次，兼顾吞吐与可见性
    bytes_.fetch_add(bytes, std::memory_order_relaxed);
    rolled_bytes_ += static_cast<size_t>(bytes);

    if (fp_ && rolled_bytes_ >= roll_size_) {
        RollFile();
    }

    // 回收缓冲到空闲池（上限 kMaxFreeBuffers，避免长期占用内存）
    std::lock_guard<std::mutex> lk(mutex_);
    for (auto& b : bufs) {
        b->Reset();
        if (free_.size() < kMaxFreeBuffers) {
            free_.push_back(std::move(b));
        } else {
            b.reset();
        }
    }
    bufs.clear();
}

void AsyncLogger::RollFile() {
    if (!fp_) return;
    ::fflush(fp_);
    ::fclose(fp_);
    fp_ = nullptr;

    char suffix[64];
    time_t now = ::time(nullptr);
    struct tm tmv;
    ::localtime_r(&now, &tmv);
    std::snprintf(suffix, sizeof suffix, ".%04d%02d%02d-%02d%02d%02d",
                  tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday,
                  tmv.tm_hour, tmv.tm_min, tmv.tm_sec);

    // ⚠️ 踩坑记录：只带"秒级"时间戳是不够的 —— 同一秒内连续滚动多次时，
    //    rename() 会把上一个滚动文件**直接覆盖**（实测 8MB 阈值下 5 次滚动只活下来 1 个）。
    //    这里追加进程内自增序号，并在极端情况下继续 +1 直到目标名不存在为止。
    std::string rolled;
    for (;;) {
        rolled = filename_ + suffix + "." + std::to_string(++roll_seq_);
        if (::access(rolled.c_str(), F_OK) != 0) break;   // 不覆盖已存在的文件
    }
    if (::rename(filename_.c_str(), rolled.c_str()) != 0) {
        std::fprintf(stderr, "[async_log] rename %s -> %s failed: %s\n",
                     filename_.c_str(), rolled.c_str(), std::strerror(errno));
    }

    rolled_bytes_ = 0;
    OpenFile();
}

}  // namespace bite
