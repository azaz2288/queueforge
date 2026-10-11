# 2026-10-11 v0.2.0 自动心跳worker

- work_once独立续租线程、CLI租约/心跳间隔参数、配置claim前拒绝、bool租约拒绝。续租故障/取消不写成功或失败；handler正常/异常/KeyboardInterrupt停止线程，线程启动失败不执行handler、原租约过期恢复。
- 9项新增回归包含长handler竞争不抢占、真实独立worker续租后kill→最终租约过期→attempts2重领→旧token拒绝、存储故障/取消/短任务/空队列/线程启动失败。原首6方法2fail/11errors含参数subcase来自未实现API，语法fixture错误已修，不称11个产品漏洞。总24方法以及安装包源码外-I真实进程回归、CLI参数/资源验收见根手动报告。
- 最终本机24方法5.406s通过，compile/前端JS语法/pip check/diff通过。新wheel13681B，SHA256 6f746391b52416557960b3183c5bfa89215c7671b6684dc901dc784f8722b8ed；全新隔离环境安装后-I从site-packages运行9项心跳/真实kill回归2.608s通过，并验CLI新参数/worker资源/pip check。不是旧二进制或可编辑源码安装。
- 时钟回退/锁饥饿/资源预算/优雅中断仍未验收，整个首未完里程碑保留unchecked。API/UI任务仍仅可信内置sha256/text_stats，不开放shell/user code，不读取真实数据库/既有8781服务；无UI变更，本阶段不新增浏览器验收。最终fullSHA与同SHA Windows/Linux CI保存在根维护报告。

# 2026-10-07 v0.1

15项回归通过，含20任务30线程竞争/20并发幂等、过期栅栏/心跳/退避/死信/取消、真实独立进程claim后os._exit恢复、HTTP Host/Origin/幂等/结果/不返回token。首次WindowsSQLite句柄未close导致临时目录清理失败，改explicit closing后通过。Python compile与独立venv安装-I核心/资源通过；浏览器实际文本任务submitted→claimed→succeeded及事件记录验证。没有分布式部署/长任务自动心跳/外部副作用恰好一次。发布同SHA CI另见维护报告。
