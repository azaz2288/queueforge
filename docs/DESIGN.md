# 设计与正确性边界

SQLite WAL保存jobs和events；BEGIN IMMEDIATE把到期回收、选择最早待处理任务、生成随机token和增加attempts绑定同一事务。token仅库/worker可见，浏览器列表不返回token。状态pending→running→succeeded；失败回pending并指数退避，上限dead；到期使用原attempt预算回收；manual retry开启新预算并保留历史；pending/running可cancel。

完成、心跳、失败更新必须同时匹配id/token/running/未到期；过期或取消worker不能写队列结果。只保证队列结果提交的栅栏，不保护邮件/外部数据库等副作用，不承诺exactly-once；实际执行是at-least-once，外部系统仍需幂等。v0.2.0的work_once领取后启动独立续租线程，默认间隔lease/3；通过Event定时等待/停止，handler结束后先停止并join，再提交。续租的token冲突或存储异常使本次结果/失败都不提交；已有租约按原机制到期恢复。线程启动失败不执行handler，已领取租约仍需到期恢复。未知任务拒绝，不执行shell/user code。进程崩溃后无需worker本地状态，下一claim回收过期租约。

每次连接显式close；写事务serial，10秒busy_timeout。系统墙钟回退会延迟租约，跨主机时钟不支持。取消不能中止正在计算的worker，只拒绝提交。适合单机/共享本机文件系统；不用于NFS、多机分布式broker。HTTP默认回环Host/Origin拒绝跨站，非公网服务。64KiB任务与结果，attempt1–10，lease .1–300秒，状态页最近100，事件/任务尚无保留策略。

自动心跳不是硬实时保障：CPU/GIL饥饿、OS暂停、写锁都可能造成到期失租。handler完成时续租线程join预算11秒，超时拒绝提交但daemon线程可能仍等存储，不能强杀线程；不承诺任务计算期限或优雅中止。间隔验证发生在claim前，bool/NaN/Inf/零/大于等于租约均拒绝；任务执行或KeyboardInterrupt结束会设置stop。时钟故障演练仍待独立验收，整里程碑不勾完。
