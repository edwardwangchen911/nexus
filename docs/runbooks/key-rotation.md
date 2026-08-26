# 密钥与证书轮换 runbook

## 原则

每个密钥域独立轮换，不复用材料，不在仓库、命令、日志、ticket 或对话中传递值。本 runbook 只描述受控环境的顺序；真实 Provider 密钥、账户权限或生产证书的配置必须单独授权。

| 密钥域 | 持有者 | 轮换支持 | 不得复用于 |
| --- | --- | --- | --- |
| Engine HMAC | Web BFF 签名，Engine 验证 | current + previous | gateway HMAC、AEAD、Provider |
| gateway HMAC | broker-worker 签名，gateway 验证 | gateway 验证 current + previous；broker-worker 只用 current | Engine HMAC、mTLS |
| gateway mTLS | broker-worker client / gateway server / CA | 通过临时信任 bundle 重叠 | HMAC、Provider 密钥 |
| external-ID AEAD | broker-worker | current + previous 解密，新写入只用 current | blind-index、HMAC |
| external-ID blind index | broker-worker | 当前为单一 key，需受控重建才能更换 | AEAD、HMAC |
| Fosun Provider | fosun-gateway | 由 Provider/secret manager 决定 | 任何 NEXUS 内部域 |

## 通用前置

1. 确认轮换对象、负责人、时间窗、回滚点和脱敏验证项。
2. 在 secret manager/PKI 中生成新材料；不在 shell history 中生成或展开。
3. 证明新旧 key ID/version 不同，密钥材料不同，所有配对变量完整。
4. 先开启验证端重叠，再切换签名/加密端；监视旧 key ID 的脱敏使用计数。
5. 验证新材料后等待至少一个最大请求/任务/缓存生存窗，才移除 previous/旧 trust anchor。

## Engine 和 gateway HMAC

1. 在验证端将旧 current 降为 previous，加入新 current；重启后先用旧请求验证重叠。
2. 将签名端切换到新 current key ID/secret。
3. 验证正常请求、错误 key ID、过期 timestamp、重放 nonce 与脱敏审计。
4. 旧 key 使用归零并越过最大重试窗后，从验证端移除 previous 并撤销旧 secret。

Engine 和 gateway 必须分别执行，不能在一次轮换中使用同一新值。

## mTLS

1. 新建 CA/server/client 材料或按 PKI 策略签发，验证 SAN、EKU、期限和文件权限。
2. 先在对端 trust bundle 同时信任新旧 CA，验证无客户端证书的连接仍被拒绝。
3. 切换 server，再切换 client，每步都做 mTLS + gateway HMAC 双重验证。
4. 确认旧证书无连接后，移除旧 trust anchor 并撤销旧证书。

## external-ID AEAD 与 blind index

AEAD 轮换先配置新 current + 旧 previous，新写入仅使用 current，读取按已认证 envelope 的 key version 选择。只有所有存量密文都在受控任务中重加密、计数/摘要对账成功后，才能移除 previous。

blind-index 当前没有 current/previous 双写合同，不得直接替换环境值。需要新增双 blind-index 版本、受控 backfill、唯一性/引用对账和回滚路径；这属于独立数据迁移变更。

## Fosun Provider key

已经在对话或其他非密密渠道暴露过的历史 key 必须先由 Provider 撤销，不能仅“换个存放位置”。新 key 必须是最小只读权限，只在 secret manager 直接配置，并重跑 SDK admission、固定出站 IP allowlist 和受控只读 probe 门禁。这一步涉及真实 Provider 与权限，必须另行确认。

## 回滚

重叠窗内将签名/加密端切回旧 current，保留新材料作为 previous/受信任项并调查。一旦旧材料已撤销，不得通过恢复旧 secret 回滚；应生成另一组新材料重新执行。
