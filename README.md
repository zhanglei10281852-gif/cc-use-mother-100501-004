# 应急通信保障编排服务

山区暴雨切断地面通信后，医院、救援队、安置点可能向不同运营商重复申请卫星保障，
有限带宽被先到的低优先级请求占满，链路恢复后也无人释放。本服务在统一的事件溯源
内核上完成**能力发布 → 保障请求 → 规则裁决 → 重评估 → 到期回收 → 全程可解释**。

## 它保证什么

- **不双占**：任何时段每条链路的已授权带宽不超过运营商发布的容量；高优先级（1 最高）
  可抢占低优先级，同级抢占保护更早获批的请求；被抢占者按替代区域重新安置，否则候补。
- **重复送达幂等**：同一事件经多个渠道送达（同标识同内容）只生效一次；同标识不同内容
  按冲突拒绝；所有写操作支持显式 `idem` 键，命令在全局锁内串行结算。
- **变化即重评估**：灾情升级（优先级变化）、能力发布/撤回、链路中断/恢复、人工特批、
  到期扫描都会重新裁决所有未结束任务。
- **授权与用量不可改写**：一切状态变化都是追加事件；授权只新增新版本或按原因关闭，
  用量只追加，取消、切换、终止都不动历史。重放事件日志可完整重建状态。
- **过期自动回收**：授权、特批、请求窗口到期由 `sweep` 关闭并尝试为候补补位；
  人工特批必须携带**明确期限、责任人和原因**，到期自动回到规则编排。
- **可解释**：每次状态变化都落一条带原因的决策（分配/候补/拒绝/保留/降级/切换/
  恢复/终止），可通过 API 或 CLI 查看某请求在每次变化后的完整事实链。

## 裁决规则摘要

1. 按请求/能力/特批的时间边界切分时段，逐时段独立裁决；
2. 人工特批最先预留并可让任何人让位，普通请求按 `优先级 → 提交时间 → 编号` 排序；
3. 选路顺序为「主区域 → 按意愿排序的替代区域」，区域内优先沿用原链路（链路粘性）；
4. 容量不足时抢占最低优先级占用者；被抢占者尝试替代区域，仍不行则候补；
5. 可接受区域在**整个剩余窗口**都不存在足够大的物理能力时才结构性拒绝
   （链路中断是暂时的，只候补不拒绝）；
6. 授权只物化「当前时段」，未来时段每次重算，边界到达时由扫描激活。

## 运行环境

Python 3.11+，仅标准库，无外部服务依赖。

## HTTP API

启动（`--store` 省略则仅用内存；提供路径则事件持久化到 JSONL）：

```bash
PYTHONPATH=src python3 -m emergency_connectivity serve --store events.jsonl --port 8080
```

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /offers` | 运营方发布能力（区域、链路、容量、时段） |
| `POST /offers/{code}/withdraw` | 撤回能力 |
| `POST /requests` | 提交请求（地点、等级、最低能力、时段、替代区域） |
| `POST /requests/{code}/cancel` | 取消（并发/重复调用幂等） |
| `POST /requests/{code}/renew` | 续期到新窗口，历史授权与用量不动 |
| `POST /requests/{code}/priority` | 灾情升级/降级后立即重评 |
| `POST /links/{code}/interrupt` `/restore` | 链路中断/恢复 |
| `POST /overrides` | 人工特批（必须含 start/end/owner/reason） |
| `POST /overrides/{code}/revoke` | 撤销特批 |
| `POST /usage` | 登记已传输用量（须对号到生效授权） |
| `POST /sweep` | 到期回收与候补补位 |
| `GET /requests/{code}/explain` | 解释每次变化的原因 |
| `GET /events` | 追加事件审计流 |

时间统一为带时区的 ISO-8601；写操作可带 `idem` 与 `at`（便于确定性重放历史）。

```bash
curl -s localhost:8080/requests/REQ-HOSP/explain
```

## 命令行

```bash
# 端到端剧情演示（确定性时间线，覆盖抢占/降级/恢复/特批/回收/用量保全）
python3 run_cli.py

# 查看持久化日志中的请求变化原因
PYTHONPATH=src python3 -m emergency_connectivity --store events.jsonl explain REQ-HOSP
PYTHONPATH=src python3 -m emergency_connectivity --store events.jsonl events
```

## 测试

```bash
python3 -m compileall -q src tests run_cli.py
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

34 个测试覆盖：容量不双占、优先级抢占、同级保护、候补恢复、结构性拒绝、
链路中断降级与恢复切回、能力撤回、多渠道幂等、同标识冲突、并发 10/12 线程竞争、
特批期限/责任人与到期回收、续期、用量只追加与对号校验、事件日志重建、HTTP API 全链路。

## 代码结构

```
src/emergency_connectivity/
  model.py     值对象：窗口、能力发布、请求规格、特批、不可变授权、用量、决策
  events.py    领域/派生事件类型与确定性幂等键
  store.py     线程安全追加日志（JSONL 持久化）与事件重放折叠
  engine.py    分时段裁决、抢占、对账、重评估、决策分类、解释
  api.py       标准库 HTTP API
  cli.py       命令行入口
  demo.py      端到端剧情（可执行规格）
```
