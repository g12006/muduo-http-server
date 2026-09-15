# Muduo-Style High-Concurrency HTTP Server

参考陈硕 muduo 网络库的 One Loop Per Thread 设计思想，从零实现的 C++17 多线程高并发 HTTP/1.1 服务器。支持静态文件服务、正则路由分发、定时踢除空闲连接，并带一个**双缓冲 + eventfd 唤醒的异步日志模块**（`async_log.hpp/.cc`）。

**实测性能**（腾讯云 2 vCPU / 1.9GB，loopback，`wrk -t2 -c100 -d60s` 长连接）：**13,107 QPS / 78.7 万请求 / 0 错误**，单连接（c=1）平均延迟 **138 µs**；`ab -k -n100000 -c100` 为 13,667 QPS。压测同时暴露并修复了原始实现在高并发下的 `std::bad_weak_ptr` 崩溃（见「性能测试」一节）。

**核心链路**：主 Reactor `accept` 新连接 → `LoopThreadPool` 轮转分发 → 从 Reactor 的 epoll 事件触发 `Channel` 回调 → `Connection` 读入 `Buffer` → HTTP 状态机解析（LINE → HEADER → BODY → DONE）→ 路由匹配（精确 / 正则 / 静态文件）→ 响应写入 `Buffer`、按需注册 `EPOLLOUT` 发出。

**架构亮点**：

- **主从 Reactor 多线程模型**：主 Reactor 只负责 accept，IO 读写分散到 N 个 One Loop Per Thread 从 Reactor——多核满载、主从解耦、连接内操作无锁
- **异步回调下的生命周期管理**：`Connection` 以 `shared_ptr` + `enable_shared_from_this` 保活，`Channel::_tie` 用 `weak_ptr` 防止 HandleEvent 执行中途对象被释放；跨线程操作统一经 `RunInLoop`/`QueueInLoop` 投递回所属线程执行
- **O(1) 定时器轮盘**：`timerfd` + 60 槽时间轮，每秒 tick 一次，空闲连接超时断开的插入/删除均为 O(1)，无需最小堆排序

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

## 异步日志模块（async_log）

`async_log.hpp` / `async_log.cc` —— **双缓冲交换 + `eventfd` 唤醒 + 后端线程批量落盘**。业务线程全程不做磁盘 IO。

```
业务线程                                      后端线程
────────                                     ────────
LOG_INFO << ...                              poll(eventfd, timeout=3s)
 ① 拼行到栈上 4KB 缓冲（零系统调用）            ① eventfd 可读 → 摘走整块缓冲
 ② 锁 + memcpy 进当前 4MB 缓冲                 ② 锁外 fwrite 整块落盘（4MB/次）
 ③ 写满 → 交给待写队列，立刻换备用缓冲           ③ 缓冲归还空闲池（上限 16 块）
 ④ write(eventfd) 唤醒后端
```

**关键设计**

| 机制 | 说明 |
|---|---|
| 双缓冲交换 | `cur_` / `next_` 两块 4MB 缓冲轮换，业务线程任何时刻都有可写缓冲，永不等待落盘 |
| `eventfd` 唤醒 | 写满才通知一次；实测落盘 896MB 只唤醒 **222 次**（≈ 每次落盘 4MB） |
| 预触页 | 4MB 缓冲首次写入触发约 1024 次缺页中断（实测单次卡 **1.8ms**），分配时一次性 `memset` 掉 |
| 热路径优化 | 时间戳前缀按秒缓存（绕开 `localtime_r` 内部加锁）、手写整数转换替代 `snprintf`、TID 线程内缓存 |
| 按大小滚动 | 滚动文件名带**自增序号** —— 只带秒级时间戳时，同一秒内多次滚动会 `rename()` 互相覆盖（实测踩到的坑） |
| 优雅退出 | `Stop()` 把残留缓冲全部落盘再关闭（实测 1.8ms 写完剩余数据） |
| 水位保护 | 单行超过 4MB 才可能丢弃，计数在 `Stats().dropped`（正常恒为 0） |

**实测数据**（同机 `g++ -O2`，对照组使用**完全相同的格式化路径**，只差 IO 策略）

| 指标 | 同步写 | 异步模块 |
|---|---|---|
| 单行耗时（墙上） | 0.28 µs | 0.28 µs |
| 单行 CPU 时间 | 0.32 µs | 0.32 µs |
| 单行最大阻塞 | 213.9 µs | **59.1 µs** |
| 大流量（656MB）业务线程总耗时 | 3,092 ms | **2,777 ms**（1.11×） |
| 4 线程并发吞吐 | — | **3.93M 行/秒** |
| 落盘完整性 | — | 900 万行 / 896MB **零丢失**（含滚动切分验证：60 万行→7 文件） |

对照"朴素同步实现"（每行 `clock_gettime + localtime_r + snprintf + fwrite`）：单行 **0.96 µs → 0.28 µs**，其中**格式化优化贡献 3.4 倍**，这部分收益对同步日志同样成立。

**用法**

```cpp
#include "async_log.hpp"

int main() {
    bite::AsyncLogger::Instance().Start("logs/server.log");   // 目录自动创建，追加模式
    LOG_INFO  << "server started, port=" << 8085;
    LOG_ERROR << "accept failed, errno=" << errno;
    bite::AsyncLogger::Instance().Stop();                     // 退出前把残留缓冲落盘
}
```

```bash
# 功能 + 三栏性能对比
g++ -std=c++17 -O2 -pthread -o test_async_log async_log.cc test_async_log.cc
./test_async_log 200000 4 8000000
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

### 异步日志模块单独编译
```bash
make test-async-log
# 或手动：
g++ -std=c++17 -O2 -pthread -o test_async_log async_log.cc test_async_log.cc
./test_async_log 200000 4 8000000   # [常规行数] [线程数] [大流量行数]
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

### 测试环境

| 项 | 值 |
|---|---|
| 机器 | 腾讯云 CVM **2 vCPU / 1.9GB**（AMD EPYC 7K83 @2.0GHz） |
| 系统 | Ubuntu 24.04.4 LTS，内核 6.8.0，`g++ -std=c++17 -O2` |
| 压测方式 | **loopback**（wrk / ab 与服务器同机），排除网络因素 |
| 说明 | 该机器常驻云主机安全 agent（约占 18% CPU），故数据偏保守 |

### 压测结果（修复后）

```bash
# 长连接，10 秒
wrk -t2 -c100 -d10s -s ka.lua http://127.0.0.1:8085/hello

# 长连接，60 秒长稳
wrk -t2 -c100 -d60s -s ka.lua http://127.0.0.1:8085/hello
```

| 场景 | 命令 | 结果 |
|---|---|---|
| 长连接 10s ×3 | `wrk -t2 -c100 -d10s` | 12,755 / 13,284 / 13,180 QPS |
| **长连接 60s 长稳** | `wrk -t2 -c100 -d60s` | **13,107 QPS**，787,739 请求，0 错误，进程存活 |
| keep-alive（ab） | `ab -k -n100000 -c100` | **13,667 QPS**，`Keep-Alive requests: 100000` |
| 单连接延迟 | `c=1` | 平均 **138 µs**（真实服务耗时） |
| 高并发 | `wrk -t2 -c500 -d10s` | 12,010 QPS |

**怎么读这组数字**：2 核 2G 单机 + loopback 同机压测，QPS 上限由 CPU 与 epoll 事件分发决定。100 并发下 wrk 报出的毫秒级延迟里绝大部分是**排队时延**（Little's Law：100 ÷ 13000 ≈ 7.7ms），不是服务处理耗时 —— 单连接实测 138 µs 才是真实处理成本。面试时这两者必须区分清楚。

### ⚠️ 压测发现的崩溃（已修复）

原始实现在第一次 wrk 压测（约 13.6 万请求）后就 `Aborted (core dumped)`：

```
terminate called after throwing an instance of 'std::bad_weak_ptr'
```

根因三条，全部已修：

| # | 问题 | 修复 |
|---|---|---|
| 1 | `Connection::Release()` 经 `QueueInLoop` **永远延迟入队**（`RunInLoop` 才会在 loop 线程内同步执行），而同一连接存在多条释放路径（EPOLLIN 读到 0 / EPOLLRDHUP / EPOLLHUP / DISCONNECTING / 空闲超时），于是**重复投递** → 第二次执行时对象已析构，`shared_from_this()` 抛 `bad_weak_ptr`，IO 线程内无人 catch → `terminate` | `Release()` 增加 `_released` **幂等标志**；队列任务执行期间持有 `shared_ptr` 保活 |
| 2 | 空闲超时定时任务 `TimerAdd(..., std::bind(&Connection::Release, this))` 捕获**裸 this**，而 `~TimerTask()` 会执行未取消的回调 → 回调落在已析构对象上 | 改捕获 `weak_ptr<Connection>`，回调前 `lock()` 判空 |
| 3 | `Socket::CreateServer` 把 `ReuseAddress()` 放在 `bind()` **之后** → SO_REUSEADDR 失效 → 重启被 TIME_WAIT 挡住 → `Acceptor::CreateServer` 的 `assert(ret == true)` 直接打挂进程 | `ReuseAddress()` 提前到 `bind()` 之前 |

另修复两个正确性问题：

- `GetHeader("Connection") == "keep-alive"` **大小写敏感** → ab / 浏览器的 `Keep-Alive` 判定失败，keep-alive 形同虚设（修复前 `ab -k` 的 `Keep-Alive requests` 为 **0**，修复后 10 万）
- 全程未设置 `TCP_NODELAY` → 小响应包被 Nagle 延迟，补上

---

## 项目结构

```
.
├── README.md
├── Makefile
├── test_muduo_server.cc     # 入口 + 路由注册
├── http.hpp                 # HTTP 层 (HttpServer/HttpContext/HttpRequest/HttpResponse/Util)
├── muduo_server.hpp         # 网络框架 (TcpServer/EventLoop/Channel/Poller/Buffer/...)
├── async_log.hpp            # 异步日志模块 (AsyncLogger/FixedBuffer/LogStream)
├── async_log.cc             # 异步日志实现 (eventfd + 后端线程 + 滚动)
├── test_async_log.cc        # 异步日志功能与性能测试
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

- [x] HTTP/1.1 keep-alive（`Connection` 头比较已改为大小写不敏感，ab `-k` 10 万请求全命中）
- [x] `SO_REUSEADDR` 调用提前到 `bind()` 之前
- [x] 异步日志模块（双缓冲交换 + eventfd 唤醒 + 按大小滚动）
- [ ] HEAD 请求 body 剥离
- [ ] accept fd 设置 `O_NONBLOCK`
- [ ] `Any::get<T>()` 用运行时检查替代 `assert`
- [ ] 支持 HTTP pipelining
- [ ] 支持 HTTPS (OpenSSL)
- [ ] 接入 MySQL / Redis 业务层

---

## 设计权衡速答

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
