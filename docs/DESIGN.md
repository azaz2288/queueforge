# 设计与正确性边界

SQLite WAL保存jobs和events；BEGIN IMMEDIATE把到期回收、选择最早待处理任务、生成随机token和增加attempts绑定同一事务。token仅库/worker可见，浏览器列表不返回token。状态pending→running→succeeded；失败回pending并指数退避，上限dead；到期使用原attempt预算回收；manual retry开启新预算并保留历史；pending/running可cancel。

完成、心跳、失败更新必须同时匹配id/token/running/未到期；过期或取消worker不能写队列结果。只保证队列结果提交的栅栏，不保护邮件/外部数据库等副作用，不承诺exactly-once；实际执行是at-least-once，外部系统仍需幂等。心跳是API，内置任务短小且worker没有长期任务自动heartbeat线程。未知任务拒绝，不执行shell/user code。进程崩溃后无需worker本地状态，下一claim回收过期租约。

每次连接显式close；写事务serial，10秒busy_timeout。系统墙钟回退会延迟租约，跨主机时钟不支持。取消不能中止正在计算的worker，只拒绝提交。适合单机/共享本机文件系统；不用于NFS、多机分布式broker。HTTP默认回环Host/Origin拒绝跨站，非公网服务。64KiB任务与结果，attempt1–10，lease .1–300秒，状态页最近100，事件/任务尚无保留策略。
