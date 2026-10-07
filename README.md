# QueueForge · 租约与栅栏持久任务队列

**v0.1 可运行纵向功能版**。为本机后台计算提供可恢复任务状态：幂等提交、并发领取、租约过期重领、token栅栏、失败退避、死信重试与事件记录。内置真实文本统计/摘要任务，可在控制台和独立worker执行。

## 运行与验证

需要Python3.12+，无第三方运行时依赖；浏览器页面不需要Node构建。

```sh
python -m queueforge serve
# http://127.0.0.1:8781，提交任务 → 执行一个待处理任务
python -m queueforge worker --name worker-1
python -m queueforge submit --key demo-1 --task sha256 --text hello
python -m unittest discover -s tests -v
python -m pip install .
```

## 工程证据与边界

[设计与语义](docs/DESIGN.md) · [已实现和后续验收](docs/ROADMAP.md) · [验证记录](docs/PROGRESS.md) · [面试演示](docs/INTERVIEW.md)

所有示例明确为原创合成数据，测试不读取私人资料。本机首版不等于公网生产系统。
