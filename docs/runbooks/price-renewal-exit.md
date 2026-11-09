# 已退出宿主的同费率价格续核

仅适用于原已激活宿主的同 boot、同 schema 非零非 75 退出，且原选定价格依据已到期。
先按正常 `register-sol-profile` 流程核验官方来源并创建新的不可变模型记录。
旧价档、原退出、激活回执、费用未知和预算预留均不得修改。

新旧档案必须同模型、同费率、同完整能力、同输出格式及同最坏预留；只允许来源原件 hash、
核验时间、有效期及派生身份变化。该入口不允许迁移旧任务或放宽冻结预算上下文。

在旧配置仍原样保留、服务已进入 maintenance、原实例确已退出且无 pending 启动时执行：

```bash
.venv/bin/python scripts/macos/agent-paper-loop-service.py reconcile-exit \
  --check-only --target-model-config-id "$NEW_MODEL_ID" \
  --repo-root "$PWD" --env-file "$PWD/infra/.env.local" --project-name nexus-fosun-live
```

检查通过后去掉 `--check-only`，通过同一正常入口正式发布证明。此操作不启动服务。
命令在原控制锁和生命周期锁内捕获旧 producer，并对新目标运行完整当前业务准入。
v4 证明分别绑定原退出、旧配置 hash 和精确新目标配置/model/price hash。
旧配置只保存于权限 600 的独立私有文件；证明不包含配置正文。

发布确认后，保留旧配置备份，仅把原配置中唯一的
`NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID` 行改为该新 ID，核对整个文件的 hash
等于证明的 `recovery_target.config_hash`。不得同时改变其他字节、凭据、预算或权限。
随后按原正常 `stop` 退役旧注册，并显式 `enable-recovery`。启用和子进程激活仍重新校验
当前配置、业务占用、价档时效及原证明；旧配置不能消费新目标启动权。

观察超时或确认丢失时，只按原退出/证明/attempt 读回；不得改目标、删除指针、清除
`consumed_by` 或重发研究。启动故障沿原新 attempt 的恢复链处理。
新价档后来到期不改变历史证明，但会阻断当前启用资格。

v1–3 证明的含义不变；不理解 v4 的旧代码不能用于恢复启动。
回滚时保留全部私有证明与原件，使用支持 v4 的维护版本，宿主保持维护停用，
不降级数据库、不重写旧身份，也不以直接启动绕过保护。

验收分别核对实际版本、唯一新 attempt、子进程激活回执与新鲜心跳，并确认原 unknown、
费用、预留和业务记录保持。价格续核解决恢复阻塞，不证明原 inspect timeout 已修复。
