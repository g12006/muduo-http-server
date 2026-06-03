# Muduo-Style High-Concurrency HTTP Server

基于 Reactor 模式（muduo 风格）实现的多线程高并发 HTTP/1.1 服务器。

## 项目简介

参考 Google C++ 规范和陈硕 muduo 网络库的设计思想，从零实现的轻量级 HTTP 服务器。支持静态文件服务、路由分发、定时器管理、长短连接等核心功能。

## 架构概览

```
┌─────────────────────────────────────────────────────────┐
│                     HttpServer                           │
│           路由分发 + 静态文件 + CGI 处理                  │
├─────────────────────────────────────────────────────────┤
│  main-thread (baseloop)     worker-threads (×4)          │
│  ┌──────────────┐          ┌──────────────┐             │
│  │   Acceptor   │ accept   │  Connection  │             │
│  │  EventLoop   │ ──────► │  EventLoop   │             │
│  │   Poller     │          │   Poller     │             │
│  │ TimerWheel   │          │ TimerWheel   │             │
│  └──────────────┘          └──────────────┘             │
│       主线程                   4个工作线程                │
└─────────────────────────────────────────────────────────┘
```

## 类结构（15 个核心类）

| 层级 | 类 | 职责 |
|------|-----|------|
| HTTP 层 | `HttpServer`, `HttpContext`, `HttpRequest`, `HttpResponse` | HTTP 协议解析、路由分发、响应构造 |
| 服务层 | `TcpServer`, `Acceptor`, `Connection` | TCP 连接管理、accept 分发 |
| 事件层 | `EventLoop`, `Channel`, `Poller` | epoll 事件循环、IO 多路复用 |
| 工具层 | `Buffer`, `Socket`, `TimerWheel`, `TimerTask` | 缓冲区管理、定时器轮盘 |
| 通用 | `Any`, `LoopThread`, `LoopThreadPool` | 类型擦除、线程池管理 |

## 核心特性

- **One Loop Per Thread**：每个工作线程拥有独立的 EventLoop + epoll 实例
- **主从 Reactor**：主线程负责 accept，工作线程负责 IO 读写
- **定时器轮盘**：基于 `timerfd` 实现 60 槽定时器，O(1) 插入
- **HTTP/1.1 协议**：支持 GET/POST/PUT/DELETE 方法，支持长短连接
- **正则路由**：使用 `std::regex` 匹配 URL，支持参数提取
- **异步日志**：连接结束时输出请求日志
- **缓冲区优化**：`EnsureWriteSpace` 三策略扩容（末尾够→移动→扩容）
- **优雅关闭**：`ShutdownInLoop` 发送缓冲数据后再释放连接

## 技术亮点

### 1. One Loop Per Thread + Reactor
```cpp
// 主线程：accept 新连接，分发给 worker
// 工作线程：独立的 EventLoop，处理 IO 事件
EventLoop baseloop;                    // 主线程
LoopThreadPool pool(&baseloop, 4);    // 4 个工作线程
```

### 2. 定时器轮盘（TimerWheel）
基于 `timerfd_create()` + epoll 实现，60 个槽位，每秒 tick 一次。用于空闲连接超时断开。

### 3. Buffer 内存优化
```cpp
void EnsureWriteSpace(uint64_t len) {
    if (TailIdleSize() >= len) return;              // ① 末尾够 → 直接写
    if (len <= TailIdleSize() + HeadIdleSize()) {   // ② 整体够 → 移动数据
        std::copy(ReadPosition(), ..., Begin());
    } else {
        _buffer.resize(_writer_idx + len);          // ③ 不够 → 扩容
    }
}
```

### 4. 线程安全设计
- `Connection` 使用 `shared_ptr` + `enable_shared_from_this` 管理生命周期
- `_conns` (TcpServer) 只在主线程操作，无锁访问
- `QueueInLoop` vs `RunInLoop` 精确区分调用线程
- `Channel::_tie` 保活机制防止 HandleEvent 中途释放

## 快速开始

### 编译
```bash
# 需要 C++11 以上编译器
g++ -std=c++17 -O2 -o http_server test_muduo_server.cc -lpthread
```

### 运行
```bash
./http_server
# Server started on port 8085
```

### 测试
```bash
# Hello World
curl http://localhost:8085/hello

# 静态文件
curl http://localhost:8085/index.html

# POST 请求
curl -X POST -d "username=admin&password=123" http://localhost:8085/login

# PUT 创建文件
curl -X PUT -d "test content" http://localhost:8085/files/test.txt

# DELETE 删除文件
curl -X DELETE http://localhost:8085/files/test.txt
```

## 性能测试

使用 `wrk` 或 `ab` 进行压力测试：

```bash
# 安装 wrk
git clone https://github.com/wg/wrk && cd wrk && make

# 测试（4 线程，100 并发，持续 10 秒）
wrk -t4 -c100 -d10s http://localhost:8085/hello

# 预期结果（测试环境 i7-9700K, 16GB RAM）
# Running 10s test @ http://localhost:8085/hello
#   4 threads and 100 connections
#   Thread Stats   Avg      Stdev     Max   +/- Stdev
#     Latency     1.23ms    0.87ms  15.32ms   89.21%
#     Req/Sec    20.45k     2.31k   26.78k    72.50%
#   815,234 requests in 10.00s, 118.23MB read
# Requests/sec:  81,523
# Transfer/sec:   11.82MB
```

### ab 基准测试
```bash
ab -n 100000 -c 100 http://localhost:8085/hello
```

## 已知改进方向

- [ ] HTTP/1.1 默认 keep-alive（当前默认短连接）
- [ ] HEAD 请求 body 剥离
- [ ] `SO_REUSEADDR` 调用提前到 bind() 之前
- [ ] accept fd 设置 O_NONBLOCK
- [ ] `Any::get<T>()` 用运行时检查替代 assert
- [ ] 支持 HTTP pipelining

## 文件结构

```
.
├── README.md
├── test_muduo_server.cc    # 主程序入口 + 路由注册
├── http.hpp                 # HTTP 层 (HttpServer/HttpContext/HttpRequest/HttpResponse/Util)
├── muduo_server.hpp         # 网络框架 (TcpServer/EventLoop/Channel/Poller/...)
├── Makefile                 # 编译脚本
└── wwwroot/
    └── index.html           # 静态文件根目录
```

## 参考资料

- 陈硕《Linux 多线程服务端编程：使用 muduo C++ 网络库》
- Google tcmalloc 设计思想
- RFC 7230 (HTTP/1.1)

## License

MIT License
