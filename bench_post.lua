-- wrk POST benchmark script
-- Usage: wrk -t4 -c100 -d10s -s bench_post.lua http://localhost:8085

wrk.method = "POST"
wrk.body = "username=admin&password=123456"
wrk.headers["Content-Type"] = "application/x-www-form-urlencoded"
