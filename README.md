# QueueForge · 租约与栅栏持久任务队列

**v0.2.0 可运行工程阶段**。为本机后台计算提供可恢复任务状态：幂等提交、并发领取、租约过期重领、token栅栏、失败退避、死信重试与事件记录。内置真实文本统计/摘要任务，可在控制台和独立worker执行；worker自动续租，不开放用户代码执行。

## 运行与验证

需要Python3.12+，无第三方运行时依赖；浏览器页面不需要Node构建。

```sh
python -m queueforge serve
# http://127.0.0.1:8781，提交任务 → 执行一个待处理任务
python -m queueforge worker --name worker-1
python -m queueforge worker --name worker-1 --lease-seconds 30 --heartbeat-interval 5
python -m queueforge submit --key demo-1 --task sha256 --text hello
python -m unittest discover -s tests -v
python -m pip install .
```

## 工程证据与边界

worker默认租约30秒、每10秒尝试心跳。可配置租约0.1–300秒；心跳间隔必须有限、为正且小于租约。无任务时不启动续租线程；短任务可能在首次心跳前完成。心跳失败或token失效会拒绝结果提交，保留过期重领；它不能停止正在执行的计算，也不能保证外部副作用恰好一次。线程启动失败会保留已领取租约等待过期，不伪造任务已执行。

这是本机可信任务模型。CPU密集计算、进程停顿及SQLite写锁都可能延误心跳；10秒数据库等待和11秒线程收尾预算不构成实时保证。墙钟回退会延迟到期，跨主机时钟、优雅取消长计算和资源隔离仍未完成。24项合成回归包含独立worker续租后真实kill、最后租约到期重领和旧token拒绝；不是生产压力或可用性指标。

[设计与语义](docs/DESIGN.md) · [已实现和后续验收](docs/ROADMAP.md) · [验证记录](docs/PROGRESS.md) · [面试演示](docs/INTERVIEW.md)

所有示例明确为原创合成数据，测试不读取私人资料。本机首版不等于公网生产系统。
