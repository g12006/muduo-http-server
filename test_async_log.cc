// test_async_log.cc —— 异步日志模块：功能验证 + 三栏同口径性能对比
//   ./test_async_log [常规行数] [线程数] [大流量行数]
//   默认           200000        4        8000000
//
// 三栏口径（关键，避免"拿编译期字面量和真实格式化比"的假加速）：
//   A 同步-朴素      ：每行 clock_gettime + localtime_r + snprintf + fwrite（大多数项目的写法）
//   B 同步-同款优化  ：与异步模块**完全相同的格式化热路径**（秒级时间戳缓存 + 手写整数转换），
//                      唯一区别是直接 fwrite 到文件 —— 用来隔离"格式化"与"IO 策略"两个变量
//   C 异步-模块      ：同样的格式化 + 只写内存缓冲 + 后端线程批量落盘
//   → A vs B 差在"格式化"；B vs C 差在"IO 策略/是否阻塞业务线程"
//   所有对比都统计**业务线程耗时**与**单行最大阻塞时间**（长尾才是异步日志的价值所在）
#include "async_log.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <unistd.h>
#include <sys/syscall.h>

using namespace bite;
using Clock = std::chrono::steady_clock;

static double Ms(Clock::duration d) { return std::chrono::duration<double, std::milli>(d).count(); }
static double Us(Clock::duration d) { return std::chrono::duration<double, std::micro>(d).count(); }

// 线程 CPU 时间（用户态+内核态），排除被抢占的等待时间 —— 2 核机上做同口径对比更可靠
static double ThreadCpuUs() {
    struct timespec ts;
    ::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return ts.tv_sec * 1e6 + ts.tv_nsec / 1000.0;
}

static inline pid_t RawTid() {
#ifdef SYS_gettid
    return static_cast<pid_t>(::syscall(SYS_gettid));
#else
    return ::getpid();
#endif
}

// ---------- A：朴素同步实现 ----------
static int MakeNaiveLine(char* buf, size_t n, const char* file, int line, int i) {
    struct timespec ts;
    ::clock_gettime(CLOCK_REALTIME, &ts);
    struct tm tmv;
    ::localtime_r(&ts.tv_sec, &tmv);
    return std::snprintf(
        buf, n,
        "%04d-%02d-%02d %02d:%02d:%02d.%06d INFO  [%d:%d] %s:%d - "
        "hello log line %d payload=%d\n",
        tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday,
        tmv.tm_hour, tmv.tm_min, tmv.tm_sec,
        static_cast<int>(ts.tv_nsec / 1000), static_cast<int>(::getpid()),
        static_cast<int>(RawTid()), file, line, i, i * 7 % 97);
}

// ---------- B：同款优化（秒级时间戳缓存 + 手写整数转换），直接 fwrite ----------
struct TestTimeCache {
    time_t sec = 0;
    int    len = 0;
    char   text[32] = {0};
};
static thread_local TestTimeCache g_cache;

static size_t BuildFastLine(char* out, size_t n, const char* file, int line, int i) {
    (void)n;   // 与 MakeNaiveLine 保持同一签名，便于同一模板复用
    struct timespec ts;
    ::clock_gettime(CLOCK_REALTIME, &ts);
    if (ts.tv_sec != g_cache.sec) {
        struct tm tmv;
        ::localtime_r(&ts.tv_sec, &tmv);
        g_cache.len = std::snprintf(g_cache.text, sizeof g_cache.text,
                                    "%04d-%02d-%02d %02d:%02d:%02d",
                                    tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday,
                                    tmv.tm_hour, tmv.tm_min, tmv.tm_sec);
        g_cache.sec = ts.tv_sec;
    }
    char* p = out;
    std::memcpy(p, g_cache.text, static_cast<size_t>(g_cache.len));
    p += g_cache.len;
    *p++ = '.';
    int us = static_cast<int>(ts.tv_nsec / 1000);
    for (int d = 100000; d > 0; d /= 10) *p++ = static_cast<char>('0' + (us / d) % 10);
    *p++ = ' ';
    std::memcpy(p, "INFO  [", 7); p += 7;
    p += detail::ConvertInt(p, static_cast<int>(::getpid()));
    *p++ = ':';
    static thread_local pid_t tid = RawTid();
    p += detail::ConvertInt(p, static_cast<int>(tid));
    std::memcpy(p, "] ", 2); p += 2;
    size_t fl = std::strlen(file);
    std::memcpy(p, file, fl); p += fl;
    *p++ = ':';
    p += detail::ConvertInt(p, line);
    std::memcpy(p, " - hello log line ", 18); p += 18;
    p += detail::ConvertInt(p, i);
    std::memcpy(p, " payload=", 9); p += 9;
    p += detail::ConvertInt(p, i * 7 % 97);
    *p++ = '\n';
    return static_cast<size_t>(p - out);
}

struct RunResult {
    double ms = 0;        // 业务线程总耗时（墙上时间）
    double avg_us = 0;    // 单行平均（墙上）
    double max_us = 0;    // 单行最大（长尾）
    double cpu_us = -1;   // 单行消耗的线程 CPU 时间（排除调度等待）
    double mb = 0;
};

struct FileStat {
    long long lines = 0;
    long long bytes = 0;
};

static FileStat CountFile(const std::string& path) {
    FileStat s;
    if (FILE* fp = std::fopen(path.c_str(), "rb")) {
        int ch;
        while ((ch = std::fgetc(fp)) != EOF) {
            ++s.bytes;
            if (ch == '\n') ++s.lines;
        }
        std::fclose(fp);
    }
    return s;
}

// 扫描当前目录下所有以 prefix 开头的文件（含滚动副本）
static std::vector<std::pair<std::string, FileStat>> ScanPrefix(const std::string& prefix) {
    std::vector<std::pair<std::string, FileStat>> out;
    DIR* d = ::opendir(".");
    if (!d) return out;
    struct dirent* e;
    while ((e = ::readdir(d)) != nullptr) {
        std::string name = e->d_name;
        if (name.rfind(prefix, 0) != 0) continue;
        out.emplace_back(name, CountFile(name));
    }
    ::closedir(d);
    std::sort(out.begin(), out.end(),
              [](const std::pair<std::string, FileStat>& a,
                 const std::pair<std::string, FileStat>& b) { return a.first < b.first; });
    return out;
}

// 通用同步循环：builder 决定用哪种格式化；统计业务线程与单行长尾
template <typename F>
static RunResult RunSyncLoop(int n, const char* filename, F builder,
                             const char* file, int line) {
    RunResult r;
    char buf[512];
    FILE* fp = std::fopen(filename, "w");
    double sum = 0, mx = 0;
    double cpu0 = ThreadCpuUs();
    auto t0 = Clock::now();
    for (int i = 0; i < n; ++i) {
        auto a = Clock::now();
        size_t len = builder(buf, sizeof buf, file, line, i);
        std::fwrite(buf, 1, len, fp);
        auto b = Clock::now();
        double us = Us(b - a);
        sum += us;
        if (us > mx) mx = us;
    }
    std::fclose(fp);
    double cpu1 = ThreadCpuUs();
    r.ms = Ms(Clock::now() - t0);
    r.avg_us = sum / n;
    r.max_us = mx;
    r.cpu_us = (cpu1 - cpu0) / n;
    r.mb = n * 86.0 / 1024 / 1024;
    return r;
}

static void Row(const char* tag, long long lines, const RunResult& r) {
    char per[32] = "-";
    char mx[32]  = "-";
    char cp[32]  = "-";
    if (r.avg_us >= 0) std::snprintf(per, sizeof per, "%.2f", r.avg_us);
    if (r.max_us >= 0) std::snprintf(mx, sizeof mx, "%.1f", r.max_us);
    if (r.cpu_us >= 0) std::snprintf(cp, sizeof cp, "%.2f", r.cpu_us);
    std::printf("%-16s %10lld 行 %8.1f ms %9.0f 行/秒  单行 %6s µs  CPU %6s µs  最大阻塞 %9s µs  %.0f MB\n",
                tag, lines, r.ms, lines / (r.ms / 1000.0), per, cp, mx, r.mb);
}

int main(int argc, char** argv) {
    const int kLines   = argc > 1 ? std::atoi(argv[1]) : 200000;
    const int kThreads = argc > 2 ? std::atoi(argv[2]) : 4;
    const int kBig     = argc > 3 ? std::atoi(argv[3]) : 8000000;
    const int kWarmup  = 1000;
    const char* kFile  = "async_log_test.log";
    const char* kCode  = "test_async_log.cc";

    long long expected = kWarmup + kLines + static_cast<long long>(kBig) + 1LL * kThreads * kLines;

    std::printf("==================== 异步日志模块验证 ====================\n");
    std::printf("常规行数=%d  线程数=%d  大流量行数=%d  缓冲块=4MB  刷盘间隔=3000ms\n\n",
                kLines, kThreads, kBig);

    // ---------- A/B：常规量级，三栏对比 ----------
    RunResult a = RunSyncLoop(kLines, "sync_naive.log", MakeNaiveLine, kCode, 88);
    Row("[A 同步-朴素]", kLines, a);

    RunResult b = RunSyncLoop(kLines, "sync_fast.log", BuildFastLine, kCode, 88);
    Row("[B 同步-同款]", kLines, b);

    // 主测试用 1GB 滚动阈值：总量 < 1GB，落盘只有一个文件，行数校验无歧义
    AsyncLogger::Instance().Start(kFile, LogLevel::INFO, 3000, 1ull * 1024 * 1024 * 1024);
    for (int i = 0; i < kWarmup; ++i) LOG_INFO << "warmup " << i;

    RunResult c;
    {
        double sum = 0, mx = 0;
        double cpu0 = ThreadCpuUs();
        auto t0 = Clock::now();
        for (int i = 0; i < kLines; ++i) {
            auto s = Clock::now();
            LOG_INFO << "hello log line " << i << " payload=" << (i * 7 % 97);
            auto e = Clock::now();
            double us = Us(e - s);
            sum += us;
            if (us > mx) mx = us;
        }
        double cpu1 = ThreadCpuUs();
        c.ms = Ms(Clock::now() - t0);
        c.avg_us = sum / kLines;
        c.max_us = mx;
        c.cpu_us = (cpu1 - cpu0) / kLines;
        c.mb = kLines * 86.0 / 1024 / 1024;
    }
    Row("[C 异步-模块]", kLines, c);

    std::printf("\n  格式化优化收益 (A/B) : 单行墙上 %.2f µs → %.2f µs   CPU %.2f µs → %.2f µs\n",
                a.avg_us, b.avg_us, a.cpu_us, b.cpu_us);
    std::printf("  IO 策略收益    (B/C) : 单行墙上 %.2f µs → %.2f µs   CPU %.2f µs → %.2f µs（业务线程不再做 write 系统调用）\n",
                b.avg_us, c.avg_us, b.cpu_us, c.cpu_us);
    std::printf("  长尾           (B/C) : 单行最大 %.1f µs → %.1f µs\n\n", b.max_us, c.max_us);

    // ---------- 大流量：压出内核真实回写，看长尾 ----------
    std::printf("--- 大流量 %d 行（约 %.0f MB，足以压出 page cache 回写） ---\n",
                kBig, kBig * 86.0 / 1024 / 1024);

    RunResult bs = RunSyncLoop(kBig, "sync_big.log", BuildFastLine, kCode, 88);
    Row("[B 同步-同款]", kBig, bs);

    RunResult ca;
    {
        double sum = 0, mx = 0;
        double cpu0 = ThreadCpuUs();
        auto t0 = Clock::now();
        for (int i = 0; i < kBig; ++i) {
            auto s = Clock::now();
            LOG_INFO << "big volume line " << i << " payload=" << (i * 7 % 97);
            auto e = Clock::now();
            double us = Us(e - s);
            sum += us;
            if (us > mx) mx = us;
        }
        double cpu1 = ThreadCpuUs();
        ca.ms = Ms(Clock::now() - t0);
        ca.avg_us = sum / kBig;
        ca.max_us = mx;
        ca.cpu_us = (cpu1 - cpu0) / kBig;
        ca.mb = kBig * 86.0 / 1024 / 1024;
    }
    Row("[C 异步-模块]", kBig, ca);
    std::printf("  → 业务线程：墙上 %.1f ms vs %.1f ms（%.2f 倍）| CPU %.1f ms vs %.1f ms（%.2f 倍）\n",
                bs.ms, ca.ms, bs.ms / ca.ms,
                bs.cpu_us * kBig / 1000.0, ca.cpu_us * kBig / 1000.0,
                (bs.cpu_us * kBig) / (ca.cpu_us * kBig));
    std::printf("  → 单行 CPU：同步 %.2f µs vs 异步 %.2f µs；单行最大阻塞：%.1f µs vs %.1f µs\n\n",
                bs.cpu_us, ca.cpu_us, bs.max_us, ca.max_us);

    // ---------- 多线程并发 ----------
    RunResult mt;
    {
        double cpu0 = ThreadCpuUs();
        auto t0 = Clock::now();
        std::vector<std::thread> ts;
        ts.reserve(kThreads);
        for (int t = 0; t < kThreads; ++t) {
            ts.emplace_back([t, kLines] {
                for (int i = 0; i < kLines; ++i)
                    LOG_INFO << "thread " << t << " line " << i;
            });
        }
        for (auto& th : ts) th.join();
        double cpu1 = ThreadCpuUs();
        mt.ms = Ms(Clock::now() - t0);
        mt.avg_us = -1;   // 多线程不做单行插桩，避免插桩本身影响并发测量
        mt.max_us = -1;
        mt.cpu_us = -1;
        (void)cpu0; (void)cpu1;
        mt.mb = 1LL * kThreads * kLines * 86.0 / 1024 / 1024;
    }
    {
        char tag[64];
        std::snprintf(tag, sizeof tag, "[C 异步-%d线程]", kThreads);
        Row(tag, 1LL * kThreads * kLines, mt);
    }

    // ---------- Stop：残留缓冲全部落盘 ----------
    auto t2 = Clock::now();
    AsyncLogger::Instance().Stop();
    double stop_ms = Ms(Clock::now() - t2);

    AsyncLoggerStats st = AsyncLogger::Instance().Stats();
    std::printf("\n[停止收尾] %.1f ms 内把残留缓冲全部落盘\n", stop_ms);
    std::printf("[模块统计] 接收 %llu 行 | 落盘 %.1f MB | 分配缓冲 %llu 块 | eventfd 唤醒 %llu 次 | 丢弃 %llu 行\n",
                (unsigned long long)st.lines, st.bytes / 1024.0 / 1024.0,
                (unsigned long long)st.buffers, (unsigned long long)st.wakeups,
                (unsigned long long)st.dropped);

    // ---------- 落盘完整性校验（含滚动文件） ----------
    {
        long long file_lines = 0;
        double    file_mb = 0;
        auto files = ScanPrefix(kFile);
        for (auto& pr : files) {
            std::printf("[落盘文件] %-42s %8lld 行 / %7.1f MB\n",
                        pr.first.c_str(), pr.second.lines, pr.second.bytes / 1024.0 / 1024.0);
            file_lines += pr.second.lines;
            file_mb += pr.second.bytes / 1024.0 / 1024.0;
        }
        std::printf("[落盘校验] 合计 %lld 行 / %.1f MB   期望 %lld 行   %s\n",
                    file_lines, file_mb, expected,
                    file_lines == expected ? "✅ 无丢失" : "❌ 行数不符");
    }

    // ---------- 滚动功能验证：小 roll_size 强制切分 ----------
    {
        std::printf("\n--- 滚动功能验证（roll_size=8MB，写 60 万行）---\n");
        AsyncLoggerStats before = AsyncLogger::Instance().Stats();
        AsyncLogger::Instance().Start("roll_test.log", LogLevel::INFO, 200, 8ull * 1024 * 1024);
        const int kRoll = 600000;
        for (int i = 0; i < kRoll; ++i) LOG_INFO << "roll line " << i;
        AsyncLogger::Instance().Stop();
        AsyncLoggerStats after = AsyncLogger::Instance().Stats();

        long long lines = 0;
        int files = 0;
        for (auto& pr : ScanPrefix("roll_test.log")) {
            ++files;
            lines += pr.second.lines;
        }
        std::printf("  模块接收 %llu 行 | 生成 %d 个文件 | 合计 %lld 行  %s\n",
                    (unsigned long long)(after.lines - before.lines), files, lines,
                    (lines == kRoll && files >= 4) ? "✅ 滚动切分正确且无丢失" : "❌ 异常");
    }

    // ---------- 尾部抽样 ----------
    {
        std::printf("[尾部抽样]\n");
        if (FILE* f2 = std::fopen(kFile, "rb")) {
            std::fseek(f2, 0, SEEK_END);
            long sz = std::ftell(f2);
            long from = sz > 300 ? sz - 300 : 0;
            std::fseek(f2, from, SEEK_SET);
            std::vector<char> buf(static_cast<size_t>(sz - from) + 1, 0);
            size_t rd = std::fread(buf.data(), 1, buf.size() - 1, f2);
            buf[rd] = 0;
            std::fclose(f2);
            std::printf("%s", buf.data());
        }
    }

    std::printf("========================================================\n");
    return 0;
}
