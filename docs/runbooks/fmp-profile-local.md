# FMP 公司背景只读采集与导入

仅调用现有 `/stable/profile` 单证券端点。结果是当前公司背景，不是财报、价格、历史可用性或经审核行业风险分类；缺值保持未知，执行锁始终保留。不会运行模型、订单或自动调度。

## 显式采集

在仓库根目录使用项目虚拟环境，凭据通过正常 `infra/.env.local` / 环境配置入口的 `FMP_API_KEY` 提供，不放命令行或输出：

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.fmp_profile_local \
  --repo-root "$PWD" collect --symbol AAPL \
  --output-dir /absolute/private/path/new-profile-collection
```

目录须尚不存在，父目录预先准备好。入口创建私有目录、原件 `profile.json` 与 `receipt.json`；收到成功响应后先保存原始 bytes 和本机实际接收时间，再解析，JSON 数字以 Decimal 保精度。只发一次请求，不重试，不跟随重定向；原件上限 1 MiB、HTTP 总时限 30 秒、采集操作总时限 45 秒。对外请求沿用既有环境路由。凭据只放 `apikey` 请求头，receipt/source URL 不含凭据。若响应直接反射凭据则拒绝归档。

错误或中断可能留下不完整私有目录，不能当作未发请求而自动重放。原件与 receipt 缺失、被修改、时间在未来、符号链接、非普通文件或权限过宽均不导入。

## 导入已有归档

本地 PostgreSQL/对象存储须已就绪；对应应用需包含 `0042_fmp_profile` 冻结 guard，迁移按既有流程由运行环境负责人执行，不在此入口自动迁移。

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.fmp_profile_local \
  --repo-root "$PWD" import --symbol AAPL \
  --instrument-id <existing-local-instrument-uuid> \
  --collection-dir /absolute/private/path/new-profile-collection \
  --output /absolute/private/path/new-profile-import.json
```

导入不访问 FMP。只绑定已存在且接收时点可见的证券/别名，不自创或重标证券。原件、receipt 与 canonical 文档写入现有内容寻址对象存储并读回校验；数据库绑定在一个事务内，相同归档精确重放复用版本。终端仅显示安全状态；精确证据 ID 写入私有结果文件，不覆盖已存在结果。

`stored_context` 仅表示背景证据已保存，`research_created=false`。单独 profile 不创建研究快照、不替代 SEC/发行人/价格证据。共享正常研究准备入口只能在原研究基底已成立时附加它；历史快照按原引用读取，不补入后来版本。只投影公司名、描述、CIK/ISIN/CUSIP、原始 industry/sector，不投影价格、市值或 beta。接收时间不是来源发布时间，内容 hash 不是来源修订号，所有金融/风险分类/价格/历史资格保持 false。

对象写入早于数据库提交，失败可能留下不可变孤立对象；结果文件写入又在提交之后，写文件失败不能声称导入事务未发生。固定失败码不承诺零写入；先核对本地存储，不自动重采集。停用入口可回滚代码，不删除归档或历史证据；0042 是否可降级由既有冻结数据 guard 决定。

## SDK 对照

[FMP 官方资源页](https://site.financialmodelingprep.com/developer/docs/resources)推荐第三方 `daxm/fmpsdk`，并非 FMP 自营 Python 包。[20260824.0](https://pypi.org/project/fmpsdk/20260824.0/) 已覆盖 stable/profile，支持 header key、超时、关闭重试及公开 Session 注入。公开 Session 可以截获原件后交给本项目 canonical parser，技术上可行，不要求覆盖 SDK 私有解析。

该版本 profile 默认返回解析后的 JSON，仍需新增有界 requests Session 实现原件先保存、流式体积上限、整体时限、禁重定向及安全错误。对于这一个固定端点，相较复用现有 `FixedOriginHttpClient` 并无减码收益，因此本片不新增 SDK 或通用兼容层。认证采用[官方支持的 `apikey` 请求头](https://site.financialmodelingprep.com/developer/docs)。SDK 和成功 profile 调用均不证明其他端点或历史套餐权限。
