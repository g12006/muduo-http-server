# Muduo-Style High-Concurrency HTTP Server

基于 Reactor 模式（muduo 风格）从零实现的多线程高并发 HTTP/1.1 服务器。支持静态文件服务、路由分发、定时器管理、长短连接等核心功能。

## 项目简介

参考陈硕 muduo 网络库的 One Loop Per Thread 设计思想，从零构建的轻量级 HTTP 服务器。旨在学习 Reactor 模型、epoll 事件驱动、非阻塞 IO、定时器轮盘、HTTP 协议解析等高并发服务端核心技术。

## 架构总览

```
                     ┌──────────────────────────────────────┐
                     │            HttpServer                 │
                     │   路由分发 + 静态文件 + CGI 处理      │
                     ├──────────────────────────────────────┤
    main-thread      │           work-threads (×4)           │
    (baseloop)       │                                       │
  ┌──────────────┐   │  ┌──────────────┐  ┌──────────────┐  │
  │   Acceptor   │   │  │  Connection  │  │  Connection  │  │
  │  EventLoop   │───▶  │  EventLoop   │  │  EventLoop   │  │
  │   Poller     │   │  │   Poller     │  │   Poller     │  │
  │ TimerWheel   │   │  │ TimerWheel   │  │ TimerWheel   │  │
  └──────────────┘   │  └──────────────┘  └──────────────┘  │
    主线程 accept     │     工作线程处理 IO 读写事件          │
                     └──────────────────────────────────────┘
```

**主从 Reactor 模型**：主 Reactor 负责 accept 新连接，通过 LoopThreadPool 轮转分发给从 Reactor。每个从 Reactor 运行独立的 EventLoop + epoll 实例。

---

## Reactor 模型对比

| 模型 | 优点 | 缺点 | 本项目 |
|------|------|------|--------|
| 单 Reactor 单线程 | 简单、无锁 | 无法利用多核 | — |
| 单 Reactor 多线程 | 利用多核 | Reactor 成瓶颈 | — |
| **多 Reactor 多线程** | **多核满载、主从解耦** | **实现复杂** | ✅ |

---

## 模块详解

### 1. Buffer — 字节缓冲区

不关心上层协议，只管理原始字节流。读写指针分离，优先复用已读空间。

```cpp
class Buffer {
    std::vector<char> _buffer;   // 底层存储
    uint64_t _reader_idx = 0;    // 读指针 (已消费区域)
    uint64_t _writer_idx = 0;    // 写指针 (已填充区域)
};
```

**三策略扩容**：末尾够 → 直接写 / 整体够 → 移动数据 / 都不够 → 倍增扩容：

```cpp
void EnsureWriteSpace(uint64_t len) {
    if (TailIdleSize() >= len) return;
    if (len <= TailIdleSize() + HeadIdleSize()) {
        std::copy(ReadPosition(), WritePosition(), Begin());  // compaction
    } else {
        _buffer.resize(_writer_idx + len);                    // expand
    }
}
```

### 2. Socket — TCP 系统调用封装

屏蔽平台差异，统一错误处理语义：

| 方法 | 返回值语义 |
|------|-----------|
| `Recv()` | `> 0` 成功 / `= 0` 对端关闭 / `< 0` 错误 |
| `Send()` | `> 0` 成功（可能部分发送）/ `< 0` 错误 |
| `Accept()` | 返回新 fd + 对端地址 / `-1` 错误 |
| `CreateTcpServer()` | 创建非阻塞监听 socket |
| `CreateTcpClient()` | 创建非阻塞连接 socket |
| `SetNonBlocking()` | 设置 O_NONBLOCK |
| `SetReuseAddr()` | SO_REUSEADDR |
| `ShutdownWrite()` | 半关闭写端 |

### 3. Channel — 事件分发单元

`Channel = fd + 事件类型 + 事件回调`。每个 fd 绑定一个 Channel，Channel 携带回调函数注册到 EventLoop。

```cpp
class Channel {
    int _fd;
    uint32_t _events;    // 关心的事件 (EPOLLIN | EPOLLOUT | ...)
    uint32_t _revents;   // 就绪的事件
    EventCallback _readCallback;
    EventCallback _writeCallback;
    EventCallback _closeCallback;
    EventCallback _errorCallback;
};
```

**`_tie` 保活机制**：当 Channel 的回调中可能销毁关联对象（Connection）时，通过 `weak_ptr` 延长生命周期，防止 HandleEvent 中途突然释放。

### 4. Poller — epoll 封装

对 Linux epoll 的 OOP 封装，提供统一的 `Poll()` 和 `UpdateChannel()` 接口：

```cpp
class Poller {
    int _epfd;                              // epoll fd
    std::vector<epoll_event> _events;       // 就绪事件数组
    std::unordered_map<int, Channel*> _channels;  // fd → Channel

    void Poll(std::vector<Channel*>* activeChannels);
    void UpdateChannel(Channel* channel);
    void RemoveChannel(Channel* channel);
};
```

### 5. EventLoop — 事件循环

每个线程一个 EventLoop。核心循环 `Start()` 内三件事：

```
while (running) {
    Poller::Poll(activeChannels);           // ① 收集就绪事件
    for (each active Channel)               // ② 逐个分发
        Channel::HandleEvent();
    DoPendingFunctors();                    // ③ 执行跨线程投递的任务
}
```

`RunInLoop` vs `QueueInLoop`：如果调用者处于当前 EventLoop 线程，直接执行回调；否则将回调放入任务队列，由 EventLoop 在下一轮 `DoPendingFunctors()` 中执行。

### 6. TimerWheel — 定时器轮盘

基于 `timerfd_create()` + epoll 实现的定时器。60 个槽位组成环形轮盘，每秒 tick 一次，秒针旋转推进。

```
  ┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐
  │ 0 │ 1 │ 2 │...│...│...│...│58 │59 │ ← 秒针
  └───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘
       ↑                                    ↑
   定时任务挂在对应槽上             每秒推进一格
```

**O(1) 插入**：新定时器直接挂在当前秒针对应的槽上，无需排序。到期时遍历该槽所有定时任务，检查是否真正到期。

**用途**：空闲连接超时断开（默认 60s）。

### 7. Connection — TCP 连接

封装单个 TCP 连接的全生命周期管理：

```cpp
class Connection : public std::enable_shared_from_this<Connection> {
    EventLoop* _loop;       // 所属 EventLoop
    int _fd;                // socket fd
    Channel _channel;       // fd 的事件分发
    Buffer _inBuffer;       // 接收缓冲区
    Buffer _outBuffer;      // 发送缓冲区
    ConnectionState _state; // CONNECTING → CONNECTED → DISCONNECTING → DISCONNECTED
};
```

**生命周期**：`shared_ptr` + `enable_shared_from_this` 管理，确保在异步回调中对象不被提前释放。

**读写流程**：
- 读：epoll 触发 EPOLLIN → `Channel::_readCallback` → `Connection::HandleRead()` → 读入 `_inBuffer` → 回调上层
- 写：上层写入 `_outBuffer` → 注册 EPOLLOUT → epoll 触发 → `Connection::HandleWrite()` → 发送 → 取消 EPOLLOUT

**优雅关闭 (ShutdownInLoop)**：先确保发送缓冲区数据全部发出，再半关闭写端，等待对端关闭后最终释放。

### 8. Acceptor — 连接接收器

在主 Reactor 线程中运行，监听端口并接受新连接：

```cpp
class Acceptor {
    EventLoop* _loop;
    Socket _listenSock;
    Channel _acceptChannel;        // 监听 EPOLLIN
    NewConnectionCallback _newConnCallback;
};
```

accept 流程：epoll 触发 EPOLLIN → `Acceptor::HandleRead()` → `accept()` → 回调 `TcpServer::NewConnection()` → 分发给 LoopThreadPool。

### 9. LoopThread / LoopThreadPool — 线程池

**LoopThread**：封装一个线程 + 一个 EventLoop。线程函数内启动 EventLoop：
```cpp
void LoopThread::ThreadFunc() {
    EventLoop loop;
    _loop = &loop;
    _cond.notify_one();      // 通知主线程 EventLoop 已就绪
    loop.Start();            // 开始事件循环
}
```

**LoopThreadPool**：管理多个 LoopThread，轮转分发新连接：
```cpp
EventLoop* LoopThreadPool::GetNextLoop() {
    EventLoop* loop = _loops[_next];  // 轮转
    _next = (_next + 1) % _loops.size();
    return loop;
}
```

### 10. TcpServer — TCP 服务端

将 Acceptor + LoopThreadPool + Connection 组装成完整的 TCP 服务器：

```
TcpServer
  ├── Acceptor (运行在主 EventLoop)
  ├── LoopThreadPool (管理 N 个 LoopThread)
  ├── Connection 集合 (_conns, 仅主线程操作, 无锁)
  ├── 回调接口 (OnConnection / OnMessage / OnClose)
  └── 生命周期管理 (Start / Stop / RemoveConnection)
```

---

## HTTP 层详解

### HTTP 解析状态机

```
RECV_HTTP_LINE → RECV_HTTP_HEADER → RECV_HTTP_BODY → RECV_HTTP_DONE
                                                    ↘ RECV_HTTP_ERROR
```

| 阶段 | 解析内容 | 产物 |
|------|---------|------|
| LINE | 请求行 (GET /path HTTP/1.1) | Method, URL, Version |
| HEADER | 请求头 (Host, Content-Length, ...) | 键值对集合 |
| BODY | 请求正文 | POST/PUT 数据 |

### HttpRequest / HttpResponse

```cpp
class HttpRequest {
    std::string _method;     // GET / POST / PUT / DELETE
    std::string _path;       // /hello
    std::string _version;    // HTTP/1.1
    Headers _headers;        // key-value 头部
    std::string _body;       // POST/PUT 正文
};

class HttpResponse {
    int _statusCode;         // 200 / 404 / 500 ...
    Headers _headers;        // 响应头
    std::string _body;       // 响应正文
    void AddHeader(key, val);
    void SetContent(text, type);
    void SetRedirect(url);
};
```

### HttpServer — 路由分发

支持 4 种路由匹配模式：

| 模式 | 示例 | 用途 |
|------|------|------|
| 精确匹配 | `/hello` → handler | 固定路径 |
| 正则匹配 | `/user/(\\w+)` → handler | REST 风格参数提取 |
| 静态文件 | `/index.html` → 文件 | 静态资源服务 |
| 默认路由 | 未匹配 → 404 | 兜底处理 |

```cpp
HttpServer server;
server.Get("/hello", [](HttpRequest& req, HttpResponse& resp) {
    resp.SetContent("Hello World", "text/plain");
});
server.Get("/user/(\\w+)", [](HttpRequest& req, HttpResponse& resp) {
    std::string name = req.GetParam(0);  // 正则捕获组
    resp.SetContent("User: " + name, "text/plain");
});
```

---

## 线程安全设计

| 组件 | 安全策略 |
|------|---------|
| `Connection` | `shared_ptr` + `enable_shared_from_this` 生命周期管理 |
| `Channel::_tie` | `weak_ptr` 保活，防止 HandleEvent 中途释放 |
| `TcpServer::_conns` | 仅主线程操作，无需加锁 |
| `EventLoop` 任务队列 | `RunInLoop` vs `QueueInLoop` 精确区分子线程/跨线程 |

---

## 编译与运行

### 编译
```bash
make
# 或手动：
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

---

## 性能测试

### wrk 压测 (4 线程, 100 并发, 10 秒)

```bash
wrk -t4 -c100 -d10s http://localhost:8085/hello
```

```
Running 10s test @ http://localhost:8085/hello
  4 threads and 100 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.23ms    0.87ms  15.32ms   89.21%
    Req/Sec    20.45k     2.31k   26.78k    72.50%
  815,234 requests in 10.00s, 118.23MB read
Requests/sec:  81,523
Transfer/sec:   11.82MB
```

### ab 基准测试

```bash
ab -n 100000 -c 100 http://localhost:8085/hello
```

> 测试环境：i7-9700K / 16GB RAM / Ubuntu 22.04

---

## 项目结构

```
.
├── README.md
├── Makefile
├── test_muduo_server.cc     # 入口 + 路由注册
├── http.hpp                 # HTTP 层 (HttpServer/HttpContext/HttpRequest/HttpResponse/Util)
├── muduo_server.hpp         # 网络框架 (TcpServer/EventLoop/Channel/Poller/Buffer/...)
└── wwwroot/
    └── index.html           # 静态文件根目录
```

## 核心类速查 (15 个)

| 层级 | 类 | 职责 |
|------|-----|------|
| HTTP 层 | `HttpServer` | 路由注册、请求分发、静态文件 |
| | `HttpContext` | HTTP 协议状态机解析 |
| | `HttpRequest` | 请求数据容器 |
| | `HttpResponse` | 响应构造与序列化 |
| 服务层 | `TcpServer` | 服务端入口，组装 Acceptor + 线程池 |
| | `Acceptor` | 监听端口，接收新连接 |
| | `Connection` | 单 TCP 连接全生命周期 |
| 事件层 | `EventLoop` | 事件循环，任务调度 |
| | `Channel` | fd + events + callback |
| | `Poller` | epoll 封装 |
| 工具层 | `Buffer` | 字节缓冲区 (三策略扩容) |
| | `Socket` | TCP 系统调用封装 |
| | `TimerWheel` | 60 槽定时器轮盘 |
| | `TimerTask` | 定时任务封装 |
| 线程层 | `LoopThread` | 线程 + EventLoop |
| | `LoopThreadPool` | 线程池，轮转分发 |

---

## 已知改进方向

- [ ] HTTP/1.1 默认 keep-alive（当前默认短连接）
- [ ] HEAD 请求 body 剥离
- [ ] `SO_REUSEADDR` 调用提前到 `bind()` 之前
- [ ] accept fd 设置 `O_NONBLOCK`
- [ ] `Any::get<T>()` 用运行时检查替代 `assert`
- [ ] 支持 HTTP pipelining
- [ ] 支持 HTTPS (OpenSSL)
- [ ] 接入 MySQL / Redis 业务层

---

## 面试拷打要点

1. **Reactor 模型**：主从 Reactor 与单 Reactor 的本质区别是什么？
2. **One Loop Per Thread**：为什么每个线程一个 EventLoop？多 EventLoop 共享一个线程有什么问题？
3. **Buffer 三策略扩容**：为什么不是每次都直接扩容？移动数据的代价是什么？
4. **优雅关闭**：`ShutdownInLoop` 如何保证发送缓冲全部发出再关闭？
5. **线程安全**：`Channel::_tie` 保活机制解决了什么问题？没有它会怎样？
6. **定时器轮盘**：60 槽时间轮的 O(1) 插入原理？和最小堆定时器比优劣？
7. **epoll 边缘触发**：ET 模式下为什么必须循环读取直到 EAGAIN？
8. **TCP 粘包**：HTTP 协议如何解决粘包问题？（Content-Length / chunked）

## 参考资料

- 陈硕《Linux 多线程服务端编程：使用 muduo C++ 网络库》
- RFC 7230 (HTTP/1.1)

## License

MIT
