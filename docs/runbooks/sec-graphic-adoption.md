# 单份 SEC 图片判断的本机采纳

该入口仅记录用户已明确批准的 NVDA `0001045810-26-000088` 中
`nvdalogoa19.jpg` 的页眉品牌装饰判断。它不形成 logo 白名单，也不沿用 EPS 采纳用途。
生产代码固定精确材料、独立报告、用户指令和图片的 SHA256；无运行配置扩展范围。

可信本机 operator 必须先核对实际用户指令及独立报告出处，准备私有 designation JSON，
再调用独立管理入口。投资 Agent、普通 collector、research-monitor、BFF/HTTP 均没有此操作。
`--operator-confirmed` 记录完成步骤，不是人类身份的密码学认证；本机管理环境与持管理
凭据的服务代码仍在既有信任边界内，不宣称独立数据库角色隔离。

```sh
.venv/bin/python -m nexus.ops.sec_graphic_adoption_local --repo-root /absolute/runtime/repo \
  register --designation /absolute/private/designation.json --operator-confirmed

.venv/bin/python -m nexus.ops.sec_graphic_adoption_local --repo-root /absolute/runtime/repo \
  inspect --snapshot-id UUID

# 提交响应不明且未收到 UUID 时，省略 snapshot-id，沿唯一原对象只读查验。
.venv/bin/python -m nexus.ops.sec_graphic_adoption_local --repo-root /absolute/runtime/repo inspect
```

输入包含 `material`、`report_text/source`、`owner_message_text/source`、
`purpose=nvda_000088_header_graphic_only`、`graphic_filename/sha256` 及
`research_qualified=false`。material 为正常 `prepare-content-review` 的原始导出对象，
不得重生成其时间身份。owner_message_text 保存真实问题及回答的规范 JSON；不得从
助手草稿、评审建议或 CLI 标记推导授权。全部输入留在受保护的本地业务存储。

入口使用正常本机配置，在 schema 0134/0135 下重新强读全部原件、回执、UU 关系与
HTML 依赖观察，重建同一冻结材料。完成验证后按真实 DB 时点追加独立
`nexus_sec_graphic_adoption` DataSnapshot 和同事务全局审计。幂等读回原记录，
冲突拒绝；提交结果不明时沿原对象读回，不能自动再登记或触发研究。

读回核验记录 hash、唯一审计锚、时点、范围及完整材料。既有审计禁止修改和删除；
本实现没有新增迁移或 SQL 签发权限，不能抵抗持管理凭据者修改可信代码。
旧原件、旧关系证明和旧 Run 保持原状态；采纳事实本身固定不具研究资格。
当前入口仅完成图片判断记录，不发布合格 FilingArchive，也不自动接入研究消费者。
其他必要材料、研究准入、预算、once 和发送条件继续分别核验。

没有新依赖、配置或数据库结构。回退代码后旧消费者继续拒绝该独立来源；保留已登记
事实和审计，不通过删除或改写历史实现回退。未来撤回或扩大适用范围需独立设计和授权。
