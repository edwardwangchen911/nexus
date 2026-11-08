# 本地工作台预构建服务

正式本机页面使用已构建的本地 Worker，避免开发服务器在首次访问时编译模块。只监听 127.0.0.1；不发布云端，不开启模型调用或调度。编辑源码时可在隔离端口使用 npm run dev。

构建入口：`.venv/bin/python scripts/macos/local-web-release.py build --repo-root "$REPO" --release-dir "$PRIVATE_RELEASE"`。父目录必须为本人所有的 0700 目录；从当前提交归档构建，构建进程不继承业务凭据。保留当前和可用回滚版本，至少留出 3 GiB 空间。

启动前只读校验：`.venv/bin/python scripts/macos/local-web-release.py serve --repo-root "$REPO" --release-dir "$PRIVATE_RELEASE" --env-file infra/.env.local --port 5173 --check`。Git revision、tree 与全部 dist 文件摘要必须匹配。正式启动去掉 `--check`，由既有本机 Web 服务管理；先确认端口和占用，不干预其他任务。

运行时仅从权限 0600 的既有配置中读取本机 BFF 地址及 HMAC，写入私有运行目录，不放入客户端或进程参数。远程资源绑定拒绝启动。页面与 API 的预算、未知状态、执行锁和实际数据读取合同保持原样。

验收前记录服务日志偏移、版本与健康状态；服务就绪后使用全新浏览器直接进入 /workspace，不预读工作台 API。分别保存首次请求发出时间、接口耗时、页面可用时间与刷新结果，并核对页面、BFF、持久化事实。

回滚：使用已保存的原 Web plist 和匹配当前 API/Git 的构建产物，沿既有服务维护、停止与恢复流程切换。若版本有变化，API、宿主及 Web 一起按现有交付流程恢复，不能只改 Git 或浏览器标签。此变更无数据库迁移，不重放研究或费用未知请求。
