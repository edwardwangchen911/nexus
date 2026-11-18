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
采纳入口仅完成图片判断记录。下述独立发布入口组合全部原件与正文，不改变旧记录。
其他必要材料、研究准入、预算、once 和发送条件继续分别核验。

没有新依赖、配置或数据库结构。回退代码后旧消费者继续拒绝该独立来源；保留已登记
事实和审计，不通过删除或改写历史实现回退。未来撤回或扩大适用范围需独立设计和授权。


## 完整材料组合与消费（schema 0136）

```sh
.venv/bin/python -m nexus.ops.research_monitor --repo-root /absolute/runtime/repo \
  inspect-reviewed-content --adoption-snapshot-id UUID

.venv/bin/python -m nexus.ops.research_monitor --repo-root /absolute/runtime/repo \
  publish-reviewed-content --adoption-snapshot-id UUID
```

只读检查兼容 0134/0135/0136；发布使用当前完整 schema 0136。发布重新核验完整原件、
所有声明正文、UU 关系、限定图片采纳及本包静态依赖，追加 v3 FilingArchive、evidence ref
和审计。v1/v2 的 canonical 与嵌套序列化不增加字段。普通 collector 仍不签发采纳权限。
必要材料有缺口时拒绝；XSD 仅证明格式元数据身份，不声明所有 DataFiles 已语义审查。

发布不创建研究、Job、通知、预算预留或交易权限。研究消费者继续按原入口核验价格、
SEC 新鲜度、时段、预算、逐证券限额及 once；历史索引矛盾与未知状态保持原事实。

0136 固定本次材料、报告、授权问答、图片、四份正文及依赖证明的精确 hash，并在发布、
混合材料及两种日研冻结路径核验实际记录、原索引、原件绑定、审计及各自 cutoff。
本包的精确原归档和索引修订固定不可更改，发布并发不能修改其依赖。晚创建的审计
不能支持更早的 cutoff；普通旧 snapshot 不能 UPDATE 重标为 v3。

迁移不重写旧业务证据。没有 v3 事实时允许恢复精确前驱 SQL；存在任何 v3 时，
数据库降级固定拒绝，须使用已验证的 0136 兼容应用/维护镜像回退。旧 2d 应用不属于
有 v3 数据后的兼容回滚版本。部署前须完成该镜像、备份恢复及正常退出/启用核验，
不得为了交付而删除 v3 或修改旧证据。独立依赖风险决定仍需按原发布门处理。

## 0137 正文规范化性能修复

0137 仅替换规范化函数的字符串实现：可打印 ASCII 直接保留 JSON 转义结果，其余字符
一次拆分、按原顺序聚合并生成相同的小写 UTF-16 转义。对象键排序、数组顺序、数值、
STRICT 和全部校验条件不变，不缓存资格、不改旧哈希或原件、不提高正常入口的5秒保护。
同一真实四份正文的64,938字节及 `d29624…83cf` 哈希保持一致。

迁移只增加一行原函数定义备份，不更新业务记录。正常降级精确恢复原函数；恢复旧实现
也会恢复其性能限制。部署使用当前 schema 的兼容维护镜像保留读回，不能把不认识新 head
的旧应用当作兼容回滚。三个原件/采纳只读入口保留134—136兼容并显式支持137。
