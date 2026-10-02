# fleet_fusion — 车队遥测增量跨源融合库

以 SQLite 为持久化的增量融合库：速度（mph）锚点关联最近温度（°F），
维护 stress 与 300 秒滚动均值，并保留完整血缘。调用串行执行即可。

## 接口

```python
from fleet_fusion import Fusion

fx = Fusion("telemetry.db")          # 打开/创建 SQLite 文件
fx.ingest(records)                   # 任意可迭代记录（dict 或同名属性对象）
rows = fx.query(vehicle, start, end) # 生成器，[start, end) 按时间排序
info = fx.explain(vehicle, speed_id) # 匹配来源 + rolling 血缘 + 版本/方法
fx.close()                           # 关闭后重开同一文件可继续查询/写入
```

记录字段：`id`（全局唯一）、`vehicle`、`source`（`"speed"`/`"temp"`）、
`ts`（毫秒）、`value`（整数）。同 ID 再次出现表示修订数值，其余字段不变。

- 匹配：±30 秒内最近温度，等距取较早者；`exact`/`near`/`missing`。
- `stress = speed * max(0, temp - 180)`，未匹配为 `None`。
- `rolling = (分子, 分母)`：锚点前 300 秒（左开右闭）内非空 stress 的
  和与计数，未约分；无贡献为 `None`。
- `explain` 返回 `version="v1"`、`method`（最近邻方法标识）、匹配来源，
  以及 `rolling_sources`（生成器：rolling 采用的全部速度 ID 与各自温度 ID）。
- `query` 与 `explain["rolling_sources"]` 均为流式生成器，可逐项消费。

## 增量维护策略（每次 ingest 返回即等价于全量重算）

1. 原始记录先 upsert 进 `records` 表，之后所有派生变更都基于
   upsert 后的原始状态计算。
2. 新速度锚点：索引点查最近温度；并用一次流式扫描批量取得每个新锚点
   在既有数据中的 300 秒窗口和（pull），避免逐锚点重扫窗口。
3. 新温度：对其 ±30 秒并集范围做单次扫描，范围内锚点与新温度二分比较，
   仅在“更近或等距更早”时改匹配（与全量重算的最近邻规则等价）。
4. 修订：温度修订只重算当前匹配到它的锚点（`temp_id` 索引）；
   速度修订只重算自身 stress。
5. 所有 stress 变化记为增量事件，按扫描线一次性推进受影响
   `[t, t+300s)` 区间内锚点的 `roll_sum/roll_count`，从不逐锚点重扫
   完整 300 秒窗口，也不触碰无关历史。

`Fusion.stats` 记录每次 ingest 读/写的行数，作为局部性的可复现证据。

## 运行测试

```bash
python3 -m pytest tests/            # 全部（含约 35 秒的百万条规模测试）
python3 -m pytest tests/ -k "not slow"   # 跳过规模测试
python3 -m pytest tests/ -s         # 打印内存与局部性证据
```

覆盖内容：

- `tests/test_boundaries.py` — 等距竞争、30 秒匹配边界、300 秒窗口开闭、
  晚到温度改匹配、修订旧值、重复投递幂等、完整血缘。
- `tests/test_differential.py` — 随机乱序分批、重投、修订，对照独立全量
  参考实现（`tests/reference.py`）差分验证；关闭重开数据库后继续验证。
- `tests/test_scale.py` — 100 万条原始记录（10 万锚点同处一个 300 秒窗口），
  额外工作内存 < 64 MiB（RSS 与 tracemalloc 双重证据）；固定局部改动在
  无关历史增长时处理行数恒定。
