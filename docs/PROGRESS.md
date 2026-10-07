# 2026-10-07 v0.1

15项回归通过，含20任务30线程竞争/20并发幂等、过期栅栏/心跳/退避/死信/取消、真实独立进程claim后os._exit恢复、HTTP Host/Origin/幂等/结果/不返回token。首次WindowsSQLite句柄未close导致临时目录清理失败，改explicit closing后通过。Python compile与独立venv安装-I核心/资源通过；浏览器实际文本任务submitted→claimed→succeeded及事件记录验证。没有分布式部署/长任务自动心跳/外部副作用恰好一次。发布同SHA CI另见维护报告。
