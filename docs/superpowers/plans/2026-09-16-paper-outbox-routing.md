# Paper outbox 路由收敛实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 保留批次级审计事实，同时停止为 `strategy_decision.persisted` 和 `paper.order_batch.evaluated` 创建或领取没有消费者的 outbox event。

**Architecture:** 将 Paper 服务现有的“审计 + outbox”辅助函数拆出一个审计专用入口，两个批次级事实只走审计入口，其余订单级事件维持既有成对写入。同步收窄 Paper 兼容 dispatcher 的注册表；Bark 白名单、数据库 schema 和历史记录均不变。

**Tech Stack:** Python 3.12、SQLAlchemy 2 async、PostgreSQL 17、pytest、Ruff、Mypy

**Spec:** `docs/superpowers/specs/2026-09-16-paper-outbox-routing-design.md`

## Global Constraints

- 不修改 `engine/src/nexus/notifications/bark.py` 或 Bark Worker 白名单。
- 不执行迁移，不删除或改写任何历史 outbox、delivery attempt、审计或业务事实。
- `strategy_decision.persisted` 与 `paper.order_batch.evaluated` 仍须在业务事务内写入 `audit_events`。
- 风控拒绝、订单提交、资金预留和账本事件继续保持现有审计/outbox 行为。
- 集成测试只使用 `migrated_database_url` 创建的临时数据库，不把真实业务数据库作为测试目标。
- 保留用户已有的 `README.md` 与未跟踪文档、图片改动，不纳入提交。

---

### Task 1: 收窄 Paper 批次级 outbox 生产

**Files:**
- Modify: `engine/src/nexus/paper/service.py:636-661,961-979`
- Test: `engine/tests/integration/test_paper_order_batch_pg.py:1-210`

**Interfaces:**
- Consumes: `append_audit_event(session, actor_id, action, schema_version, payload, occurred_at)` 和现有 `OutboxEvent` 模型。
- Produces: `_add_audit_event(session, *, action: str, payload: dict[str, object], occurred_at: datetime) -> None`；现有 `_add_event_pair(...) -> None` 继续作为审计与 outbox 的组合入口。

- [ ] **Step 1: 写出生产合同的失败回归**

在 `test_paper_order_batch_pg.py` 中从 `collections` 同时导入 `Counter` 与 `defaultdict`，把原来的 `audit_actions == outbox_types` 改成精确多重集断言：

```python
assert Counter(audit_actions) == Counter(outbox_types) + Counter(
    {
        "strategy_decision.persisted": 1,
        "paper.order_batch.evaluated": 1,
    }
)
assert "strategy_decision.persisted" not in outbox_types
assert "paper.order_batch.evaluated" not in outbox_types
assert "paper.order.risk_rejected" in outbox_types
```

保留已有审计、未发布数量和风控拒绝通知快照断言，以同时证明审计没有丢失、其他 outbox 没有被误删。

- [ ] **Step 2: 运行单个回归并确认按预期失败**

```bash
set -a
source infra/.env.local
set +a
.venv/bin/pytest -q \
  engine/tests/integration/test_paper_order_batch_pg.py::test_complete_fixture_creates_one_atomic_deterministic_paper_batch
```

Expected: FAIL；差异只包含两条仍存在于 `outbox_types` 的批次级事件。

- [ ] **Step 3: 实现最小生产端拆分**

在 `service.py` 新增审计专用辅助函数，并让原组合函数复用它：

```python
async def _add_audit_event(
    session: AsyncSession,
    *,
    action: str,
    payload: dict[str, object],
    occurred_at: datetime,
) -> None:
    await append_audit_event(
        session,
        actor_id=_ACTOR,
        action=action,
        schema_version="paper_event_v1",
        payload=payload,
        occurred_at=occurred_at,
    )
```

`_add_event_pair` 先调用 `_add_audit_event(...)`，再保持现有 `OutboxEvent(...)` 写入不变。将两个批次级调用改为：

```python
await _add_audit_event(
    session,
    action="strategy_decision.persisted",
    payload={**common, "decision_hash": decision_hash},
    occurred_at=request_time,
)
await _add_audit_event(
    session,
    action="paper.order_batch.evaluated",
    payload={
        **common,
        "request_hash": request_hash,
        "result_hash": result_hash,
        "idempotency_key_hash": idempotency_key_hash,
    },
    occurred_at=request_time,
)
```

不要给 `_add_audit_event` 保留未使用的 `aggregate_id` 参数。

- [ ] **Step 4: 运行生产合同与原子性回归**

```bash
.venv/bin/pytest -q \
  engine/tests/integration/test_paper_order_batch_pg.py \
  engine/tests/integration/test_paper_atomicity_pg.py::test_injected_failure_rolls_back_the_entire_task18_fact_set
```

Expected: PASS；审计多重集恰好多出两条批次级事实，风控拒绝 outbox 和事务回滚仍成立。

- [ ] **Step 5: 提交生产端切片**

```bash
git add engine/src/nexus/paper/service.py \
  engine/tests/integration/test_paper_order_batch_pg.py
git commit -m "fix(paper): stop emitting unconsumed batch outbox events"
```

---

### Task 2: 防止兼容 dispatcher 领取历史批次事件

**Files:**
- Modify: `engine/src/nexus/paper/outbox.py:18-30`
- Test: `engine/tests/integration/test_paper_atomicity_pg`

**Interfaces:**
- Consumes: `dispatch_paper_outbox(session, *, publisher, published_at, limit=100) -> int`。
- Produces: `_PAPER_EVENT_SCHEMAS` 不再注册 `strategy_decision.persisted@paper_event_v1` 或 `paper.order_batch.evaluated@paper_event_v1`；其余注册项不变。

- [ ] **Step 1: 写出历史 queued event 不被领取的失败回归**

在 `test_paper_atomicity_pg.py` 增加一个 PostgreSQL 集成测试。测试在临时数据库中手工插入两条历史形状的 queued event，调用 `dispatch_paper_outbox`，然后核对零发布、零 attempt：

```python
@pytest.mark.asyncio
async def test_compat_dispatcher_ignores_unconsumed_batch_events(
    migrated_database_url: str,
) -> None:
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    event_ids = (new_uuid7(), new_uuid7())
    try:
        async with sessions.begin() as session:
            for event_id, event_type in zip(
                event_ids,
                ("strategy_decision.persisted", "paper.order_batch.evaluated"),
                strict=True,
            ):
                session.add(
                    OutboxEvent(
                        id=event_id,
                        event_type=event_type,
                        aggregate_id=new_uuid7(),
                        schema_version="paper_event_v1",
                        payload={"historical": True},
                        published_at=None,
                    )
                )
        async with sessions() as session:
            available_at = max(
                row.available_at
                for row in await session.scalars(
                    select(OutboxEvent).where(OutboxEvent.id.in_(event_ids))
                )
            )
        delivered: list[UUID] = []

        async def record_publish(message: PaperOutboxMessage) -> None:
            delivered.append(message.event_id)

        async with sessions.begin() as session:
            count = await dispatch_paper_outbox(
                session,
                publisher=record_publish,
                published_at=available_at + timedelta(seconds=1),
            )
        async with sessions() as session:
            rows = tuple(
                await session.scalars(
                    select(OutboxEvent)
                    .where(OutboxEvent.id.in_(event_ids))
                    .order_by(OutboxEvent.id)
                )
            )

        assert count == 0
        assert delivered == []
        assert [(row.status, row.attempt_count) for row in rows] == [
            ("queued", 0),
            ("queued", 0),
        ]
    finally:
        await engine.dispose()
```

同时从 `nexus.domain.ids` 导入 `new_uuid7`。不要连接或修改真实业务数据库。

- [ ] **Step 2: 运行新回归并确认按预期失败**

```bash
.venv/bin/pytest -q \
  engine/tests/integration/test_paper_atomicity_pg.py::test_compat_dispatcher_ignores_unconsumed_batch_events
```

Expected: FAIL；当前兼容注册表会领取并发布两条事件，因此 `count == 2`。

- [ ] **Step 3: 最小收窄兼容注册表**

从 `_PAPER_EVENT_SCHEMAS` 精确删除：

```python
("strategy_decision.persisted", "paper_event_v1"),
("paper.order_batch.evaluated", "paper_event_v1"),
```

不要修改 Bark 的 `BARK_EVENT_SCHEMAS`，也不要改变 dispatcher 的领取、租约、重试或死信逻辑。

- [ ] **Step 4: 运行兼容 dispatcher 回归**

```bash
.venv/bin/pytest -q engine/tests/integration/test_paper_atomicity_pg.py
```

Expected: PASS；历史两类 event 仍为 queued/0 attempts，其余 Paper outbox 失败与重放测试仍通过。

- [ ] **Step 5: 提交兼容边界切片**

```bash
git add engine/src/nexus/paper/outbox.py \
  engine/tests/integration/test_paper_atomicity_pg.py
git commit -m "fix(paper): narrow compatibility outbox routing"
```

---

### Task 3: 完成定向验收

**Files:**
- Verify only: `engine/src/nexus/paper/service.py`
- Verify only: `engine/src/nexus/paper/outbox.py`
- Verify only: `engine/tests/integration/test_paper_order_batch_pg.py`
- Verify only: `engine/tests/integration/test_paper_atomicity_pg.py`

**Interfaces:**
- Consumes: Task 1 和 Task 2 的已提交生产与兼容路由合同。
- Produces: 可复核的测试、静态检查、Git 与真实数据库未变证据。

- [ ] **Step 1: 记录真实数据库基线计数，只读且不显示凭据**

```bash
docker exec nexus-fosun-live-postgres-1 sh -lc \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -X -A -F "|" -c \
  "SELECT status,count(*) FROM outbox_events GROUP BY status ORDER BY status;"'
```

- [ ] **Step 2: 运行完整定向测试**

```bash
.venv/bin/pytest -q \
  engine/tests/integration/test_paper_order_batch_pg.py \
  engine/tests/integration/test_paper_atomicity_pg.py \
  engine/tests/unit/notifications/test_bark.py \
  engine/tests/unit/notifications/test_bark_worker.py
```

Expected: 全部通过且没有 skipped；若因未加载 `DATABASE_URL` 而 skipped，先重新从 `infra/.env.local` 加载环境，再重跑，不把 skipped 当成验收。

- [ ] **Step 3: 运行格式、静态检查与差异检查**

```bash
.venv/bin/ruff check \
  engine/src/nexus/paper/service.py \
  engine/src/nexus/paper/outbox.py \
  engine/tests/integration/test_paper_order_batch_pg.py \
  engine/tests/integration/test_paper_atomicity_pg.py
.venv/bin/mypy engine/src
git diff --check
```

Expected: 三个命令均以 0 退出。

- [ ] **Step 4: 再次读取真实数据库计数并与基线比较**

重复 Step 1 的只读 SQL。Expected: 各状态计数与基线完全一致，证明测试只使用临时数据库。

- [ ] **Step 5: 核对提交范围与运行边界**

```bash
git status --short
git log -n 3 --oneline
git diff HEAD~2..HEAD --stat
```

Expected: 只包含计划中的两个代码文件、两个测试文件以及先前批准的设计/计划文档；用户原有 `README.md`、`docs/images/`、`docs/user-guide.md` 和 9 月 13 日文档保持未提交。无需重启 Bark、API 或 Worker；本切片不运行新的 Agent cycle，也不触发通知或订单。
