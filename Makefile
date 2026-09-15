CXX = g++
CXXFLAGS = -std=c++17 -O2 -Wall -Wextra
LDFLAGS = -lpthread

TARGET = http_server
SRCS = test_muduo_server.cc
HEADERS = http.hpp muduo_server.hpp

LOG_TARGET = test_async_log
LOG_SRCS = async_log.cc test_async_log.cc
LOG_HEADERS = async_log.hpp

.PHONY: all clean run test test-async-log

all: $(TARGET)

$(TARGET): $(SRCS) $(HEADERS)
	$(CXX) $(CXXFLAGS) -o $@ $(SRCS) $(LDFLAGS)

# 异步日志模块（双缓冲交换 + eventfd 唤醒）
$(LOG_TARGET): $(LOG_SRCS) $(LOG_HEADERS)
	$(CXX) $(CXXFLAGS) -pthread -o $@ $(LOG_SRCS)

test-async-log: $(LOG_TARGET)
	./$(LOG_TARGET) 200000 4 8000000

run: $(TARGET)
	./$(TARGET)

clean:
	rm -f $(TARGET) $(LOG_TARGET)

# 压力测试
test-hello:
	@echo "Testing /hello endpoint..."
	curl -s http://localhost:8085/hello | head -5

test-post:
	@echo "Testing POST /login..."
	curl -s -X POST -d "username=test&password=123" http://localhost:8085/login

test-static:
	@echo "Testing static file..."
	curl -s http://localhost:8085/index.html

# 基准测试 (需要安装 wrk)
bench:
	wrk -t4 -c100 -d10s http://localhost:8085/hello

bench-post:
	wrk -t4 -c100 -d10s -s bench_post.lua http://localhost:8085

bench-static:
	wrk -t4 -c100 -d10s http://localhost:8085/index.html
