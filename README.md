# wallet-xray

> 一键给 Polymarket 钱包拍 X 光 — 把输出的 JSON 丢给 ChatGPT / Claude 即可得到完整策略解读。
>
> 只覆盖 Crypto UpDown 市场（BTC / ETH / SOL / XRP × 5m / 15m / 1h）。Python 3.9+，**零运行时依赖**。

## 为什么做这个？

研究 Polymarket 上某个赚钱钱包的打法时，绕不开的流程是：

1. 打开区块浏览器 / data-api 翻 activity
2. 按 slug 聚合成一个个 5 分钟 / 15 分钟窗口
3. 算胜率、算仓位、算入场时机
4. 交叉对照结算结果、REDEEM、Gamma API
5. 才能得到一句话结论

**wallet-xray 一次命令做完 1-4 步**，把结构化 JSON 交给 AI，让 AI 做第 5 步。

## 快速上手

```bash
git clone https://github.com/eason4kim-rocket/wallet-xray.git
cd wallet-xray
pip install -e .

# 交互式：粘贴地址按回车
wallet-xray

# 或直接传地址
wallet-xray 0xYourWalletAddressHere

# 拉全量历史（whale 钱包可能跑 5-10 分钟）
wallet-xray 0x... --days all

# 把 stdout Markdown 存成文件
wallet-xray 0x... > wallet.md
```

## 输出

运行后有两份产物：

- **stdout**：完整 Markdown 报告，12 个 section 一次性打印（翻滚 / 复制 / 重定向都随意）
- **`reports/<地址简写>_<时间戳>.json`**：结构化数据，~500KB（不论钱包大小基本固定），粘给 AI 用

## 报告覆盖的 12 个维度

1. **meta** — 钱包、生成时间、lb-api 全期 PnL 交叉校验、过滤条件
2. **overview** — 总投入 / 赎回 / PnL / ROI / 双份胜率 / 活跃天数
3. **by_symbol_tf** — BTC-5m / ETH-15m / SOL-1h 等分类成绩
4. **by_date** — 每天的成绩时间序列（看连胜连败）
5. **by_hour_utc** — 24 小时内每小时的胜率 / PnL（交易时段偏好）
6. **position_sizing** — 仓位桶（<$5 / $5-20 / $20-100 / $100-500 / $500+）
7. **first_price_distribution** — 首笔入场价桶（便宜单 / 中价 / 贵单）
8. **entry_timing** — 首笔相对窗口开始的 offset 桶（T+0-30s 早盘到 T+300s+ 后窗口）
9. **multi_trade_behavior** — 单窗口笔数、首末笔时间跨度分布
10. **two_sided_behavior** — 双边买入比例、对冲 vs 方向玩法的对比
11. **direction_bias** — 用户 Up/Down 偏好 vs 市场实际结果
12. **per_window_sample** — 100 个分层采样的窗口完整明细，带每笔交易

## 三种"赢"分开记录（关键设计）

报告对"赢"做了三个独立字段，避免把**方向对**和**赚钱**混为一谈：

| 字段 | 含义 |
|---|---|
| `winner_side` | 市场客观结果：Up 赢还是 Down 赢 |
| `directional_win` | 用户的主方向是不是对的（用户主方向 == 赢方） |
| `user_won` | 用户实际是不是赚了钱（`pnl > 0`） |

**为什么要分**：双边对冲场景下两者可能分离。例如：

- 用户下 $5 Up @ $0.5 + $3 Down @ $0.15，Down 赢 → `directional_win=False`（主方向 Up 错了），`user_won=True`（对冲拿回 $10，净赚 $2）
- 用户 all-in $95 Up @ $0.95，Up 赢 → `directional_win=True`，`user_won=True`，但仅赚 $5（薄利）
- 用户下 $95 Up @ $0.95 + $10 Down @ $0.20 对冲，Up 赢 → 赎回 $100，净亏 $5：**方向对但亏钱**

双边对冲钱包典型差异：**`directional_win` 可以比 `user_won` 高出 15-20 个百分点**，差值就是 hedge 成本吃掉的利润。只看"胜率"会严重高估这种打法的盈利能力。

## 命令行参数

```
wallet-xray [wallet] [options]

  wallet                钱包地址（不传则交互式提示）
  --days N              只分析最近 N 天（默认 21，~3 周，一般能得到 200-500 个解析窗口；传 all 拉全量）
  --symbols btc,eth     过滤币种（默认全部）
  --tfs 5m,15m,1h       过滤周期（默认全部）
  --out-dir DIR         JSON 输出目录（默认 ./reports/）
  --no-gamma            跳过 Gamma 兜底（只靠 REDEEM 推断赢家，最快）
  --sample-size N       分层采样窗口数（默认 100）
  --source SRC          数据源：data-api（默认，快）或 subgraph（绕开 3500 cap 的 whale 模式）
  --no-save             不保存 JSON，只打印 Markdown
  --quiet               关闭 stderr 进度提示
```

### `--source subgraph` 什么时候用

当对 **whale 钱包**（每天成交 >1000 笔）跑 `--days > 1` 时，data-api 会被它 3500 条的硬上限截断。表现是：

```
total activity rows: 3500
💡 data-api returned 3500 rows (hit the hard cap). This wallet is likely a whale.
```

这时切 `--source subgraph` 走 Polygon orderbook subgraph，无事件数上限，按 timestamp 游标翻页。翻译阶段一次性从 gamma 拿到 `outcomePrices`，winner 缓存命中率 100%，不再串行调用 gamma。

## AI 解读提示词模板

跑完后，把 `reports/*.json` 内容复制，和下面这段一起发给 ChatGPT / Claude：

```
以下是一个 Polymarket 钱包在 BTC/ETH/SOL/XRP UpDown 市场的完整交易画像 JSON。请分析：

1. 主策略类型（趋势跟随 / 逆势马丁 / 双边对冲 / 后窗口扫残 / GTC 挂价 等）
2. 入场时机偏好（窗口早期 vs 末期 vs 后窗口）与价位偏好（便宜单 vs 中价 vs 贵单）
3. 仓位管理特征（固定 vs 动态、单笔 vs 加仓 vs 双边）
4. 优势场景与劣势场景（看 by_hour_utc / by_date 的分布）
5. 可复制性评估（需要什么信号源 / 基础设施 / 资金规模）
6. 关键的三个"赢"字段对比：directional_win vs user_won 的差距反映了什么问题？

[粘贴 JSON 内容]
```

英文版：

```
Below is a full trading profile JSON for a Polymarket wallet on BTC/ETH/SOL/XRP UpDown markets. Please analyze:

1. Primary strategy type (trend-following / martingale / two-sided hedge / post-window sweep / GTC ladder)
2. Entry timing preference (early vs late vs post-close) and price preference (cheap / mid / expensive)
3. Position sizing patterns (fixed vs dynamic, single-shot vs ladder vs hedge)
4. Favorable vs unfavorable market conditions
5. Replicability assessment (signals, infra, capital)
6. Compare `directional_win` vs `user_won` — what does the gap tell you?

[paste JSON]
```

## 数据来源

| Endpoint | 作用 |
|---|---|
| `data-api.polymarket.com/activity` | 拉所有 TRADE / REDEEM 事件 |
| `gamma-api.polymarket.com/markets/slug/<slug>` | 未结算窗口的兜底查询 |
| `lb-api.polymarket.com/profit` | 全期 PnL 交叉验证 |

### REDEEM 反推赢家优化

对每个窗口，**优先从用户自己的 REDEEM 记录推断赢家**：

- REDEEM size > 0 → 用户持有赢方 → 赢方 = 用户买入方向
- REDEEM size == 0 → 用户持有输方 → 赢方 = 用户买入方向的反面
- 只有完全没有 REDEEM 的窗口（比如最近 5 分钟未结算的）才调 Gamma 兜底

效果：即使是上万笔交易的 whale 钱包，Gamma 调用接近 0，不会被限流。

## 已知限制

**1. data-api activity 存在 3500 条硬上限。** Polymarket 的 `data-api` 在 offset=3500 时会返回 HTTP 400。工具会优雅降级停止翻页。

**2. 早停优化可以极大缓解这个问题。** 由于 data-api 返回按 timestamp 倒序排列，工具在看到"某一页最旧的行已经早于 `--days` 的截止时间"时就会 break。所以 whale 钱包 + `--days 7` 只会翻到覆盖 7 天的页数，通常不会碰到 3500 cap。

**3. whale 钱包用 `--source subgraph` 绕开 cap。** 极度活跃的 whale（每天 ≥ 1000 成交）即便用 `--days 1` 也会撞 3500 cap。这时切 Polygon orderbook subgraph：事件无上限 + 按 timestamp 游标翻页。翻译阶段顺手把每个市场的 `outcomePrices` 缓存下来，winner 推断直接命中缓存，不再调 gamma。代价：慢一些（每个 market 都要 gamma 反查 slug/outcome 映射），但只需一次性。

## 开发

```bash
pip install -e ".[dev]"
pytest -v            # 28 个单测
ruff check src tests  # lint
```

## License

MIT
