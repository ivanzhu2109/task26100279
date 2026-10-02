# fleet-fusion

车队遥测增量跨源融合库（Python + SQLite，零外部依赖）。

## API

```python
from fleet_fusion import Fusion

f = Fusion("fusion.db")          # 打开或创建 SQLite 文件
f.ingest([                       # 任意乱序、可重投、可修订（同 ID 改 value）
    {"id": "s1", "vehicle": "v", "source": "speed", "ts": 1000, "value": 60},
    {"id": "t1", "vehicle": "v", "source": "temp",  "ts": 1000, "value": 200},
])
for row in f.query("v", 0, 10**15):   # 流式返回 [start, end) 内锚点
    ...   # speed_id, ts, speed, temp_id, label, stress, rolling{num,den}
info = f.explain("v", "s1")      # 匹配来源 + rolling 全部血缘 (version "v1")
list(info["contributions"])      # 逐项消费的血缘迭代器
f.close()                        # 重新打开同一文件仍可查询
```

- 每条速度记录是一个锚点；匹配同车 ±30s 内最近温度（等距取较早），
  标签 `exact` / `near` / `missing`。
- `stress = speed * max(0, temp - 180)`（未匹配为 `None`）。
- `rolling` 为左开右闭 300s 窗口内非空 stress 的均值，以精确
  `{num, den}` 返回（未约分），无贡献为 `None`。

## 增量设计

`ingest` 分块 upsert 原始记录并分类变更（新增/修订/重复），随后只做局部传播：

1. 变更速度点 → 重建对应锚点（最近邻 = 温度索引上的前驱/后继两次点查）。
2. 变更温度点 → 合并为 ±30s 区间，只重匹配区间内锚点。
3. stress 变化的锚点产生 `[t, t+300s)` 区间，流式合并后用滑动窗口
   重算 rolling（每锚点均摊 O(1)，只读窗口前置数据）。

`query` 只读已存派生值；`explain` 按需展开血缘。两者均为生成器，可逐项消费。

## 运行测试

```bash
python3 -m pytest              # 全部测试（含 1M 记录内存测试，约 20s）
python3 -m pytest -m "not slow"   # 跳过大规模测试
```

覆盖：等距竞争、30s/300s 边界、晚到温度重选与修订的数值+血缘核对、
随机乱序/重投/修订对独立全量参考实现的差分验证（含重开数据库）、
1M 记录下 ingest 峰值内存 < 64 MiB、局部改动工作量与历史规模无关
（计数器证据）。
