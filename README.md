# A 股量化选股系统

这是一个面向 A 股研究场景的量化选股项目，包含数据更新、因子计算、候选股筛选、策略打分、回测验证和 Web 可视化界面。

当前仓库已经切到一套可落地运行的 `多因子 + XGBoost CUDA` 选股流程，支持：

- 更新到最新交易日的本地行情数据
- 生成最新 `Top10` 候选
- 跑历史回测并输出具体收益
- 每天记录预测的未来 10 日收益
- 在 10 个交易日后自动回填实际收益并做误差评估

如果你是第一次接触这个项目，可以先把它理解成一条完整流程：

1. 拉取或更新行情数据
2. 清洗并过滤不适合参与选股的股票
3. 计算趋势、动量、流动性、波动等基础指标
4. 给股票打分并生成候选名单
5. 输出买入 / 持有 / 卖出建议
6. 在网页界面中查看结果

## 项目特点

- 面向 A 股场景，默认考虑 ST、停牌、流动性等基础过滤条件
- 选股逻辑清晰，重点关注趋势、动量、流动性、估值和风险
- 支持增量更新数据
- 支持输出 Top10 / Top50 候选和操作建议
- 支持 GPU 训练与打分
- 支持预测留档、10 日后自动对账和校准
- 提供 Streamlit Web 界面，便于直接查看结果

## 目录说明

```text
app/                 Web 界面入口
configs/             项目配置
data/                本地数据目录，不建议推送到 GitHub
reports/             运行结果、策略输出、持仓和交易记录
scripts/             启动脚本
src/ashare_quant/    核心代码
tests/               测试
```

## 环境要求

- macOS / Linux
- Python 3.11
- `uv`

如果本机还没有 `uv`，可先安装：

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## 快速开始

### 1. 安装依赖

在项目根目录执行：

```bash
export PATH="$HOME/.local/bin:$PATH"
uv sync
```

### 2. 配置环境变量

如果你要使用 Tushare 更新数据，先准备 `.env`：

```bash
cp .env.example .env
```

然后在 `.env` 中填写：

```env
TUSHARE_TOKEN=你的_tushare_token
```

如果你有自定义网关，也可以额外配置：

```env
TUSHARE_HTTP_URL=http://your-custom-gateway
```

如果你在 WSL 中运行，并且本机配置了代理，有些数据源可能需要临时绕过代理才能稳定抓取。

### 3. 启动量化系统

当前可用的 Web 入口是：

```bash
./scripts/run_decision_app.sh
```

启动后在浏览器打开：

```text
http://127.0.0.1:8512
```

## 如何更新数据

### 方式一：通过 Web 界面更新

启动系统后，页面中有数据更新和策略运行入口，适合日常使用。

### 方式二：命令行增量更新

项目当前保留的增量更新入口是：

```bash
PYTHONPATH=src uv run python -m ashare_quant.pipeline.stage13_incremental_refresh
```

这个脚本会在已有数据基础上，从最后一个交易日之后继续拉取新数据，并更新：

- `data/stage4_all_buyable/market_daily_all_buyable_20210101_20260314.parquet`
- `data/stage4_all_buyable/refresh_meta.json`

如果你想限制本次更新的截止日期，可以临时指定：

```bash
STAGE13_END_DATE=2026-03-13 PYTHONPATH=src uv run python -m ashare_quant.pipeline.stage13_incremental_refresh
```

### 方式三：按最新收盘批量补今日行情

如果 `Tushare` 不可用，项目也可以用新浪批量行情接口补最新交易日的收盘数据。当前这条路径已经实测用于把数据更新到 `2026-03-18`。

推荐做法：

1. 先确认本地基础 parquet 已存在
2. 拉取当日批量行情快照
3. 合并到 `data/stage4_all_buyable/market_daily_all_buyable_20210101_20260314.parquet`
4. 再重跑策略

### 更新数据前要注意什么

增量更新依赖本地已经存在基础数据文件和股票列表文件。如果缺少下面两个文件，更新会失败：

- `data/stage4_all_buyable/market_daily_all_buyable_20210101_20260314.parquet`
- `data/stage4_all_buyable/buyable_symbols.csv`

也就是说，这个项目当前不是“零数据直接一键拉全量”的模式，而是基于已有数据做增量刷新。

## 如何运行策略

数据准备好之后，可以直接运行当前正式策略：

```bash
PYTHONPATH=src uv run python -m ashare_quant.pipeline.stage12_dual_signal_strategy
```

这一步当前会做 4 件事：

1. 读取最新行情和估值缓存
2. 训练 / 打分多因子模型
3. 输出最新 `Top10`
4. 记录预测日志并更新回测

运行后会在 `reports/` 下生成常用结果文件，例如：

- `reports/stage12_dual_top50.csv`
- `reports/stage12_top10.csv`
- `reports/stage12_actions.json`
- `reports/stage12_summary.json`
- `reports/stage12_backtest_summary.json`
- `reports/stage12_backtest_curve.parquet`
- `reports/stage12_prediction_journal.parquet`
- `reports/stage12_prediction_eval_summary.json`

## 当前策略

当前正式使用的是 `多因子 + XGBoost CUDA` 的 `Top10` 策略。

### 1. 基础过滤

- 剔除 `ST`
- 剔除停牌
- 要求最小成交额
- 价格过低的股票不参与

### 2. 使用的因子

策略会综合以下几类信息：

- 动量：`ret_20`、`ret_60`、`ret_120`
- 趋势：`ma20`、`ma60`、`ma120`、均线距离、突破强度
- 流动性：`amount`、`amount_20`、`amount_60`、放量程度
- 风险 / 质量：波动率、20 日 / 60 日回撤
- 估值：`pe_ttm`、`pb`、`mv_total`

### 3. 分数结构

会先按交易日做横截面分位排名，再形成几个子分数：

- `momentum_score`
- `quality_score`
- `liquidity_score`
- `value_score`

然后合成：

- `rule_score = 0.42 * momentum + 0.23 * quality + 0.20 * liquidity + 0.15 * value`
- `risk_score`

### 4. 机器学习部分

- 使用 `XGBoost`，优先走 `CUDA`
- 训练目标是未来 `10` 个交易日收益 `fwd_ret_10`
- 输出 `ml_score`
- 最终排序分数：

```text
final_score = 0.65 * expected_ret_10 + 0.35 * rule_score
```

其中 `expected_ret_10` 是模型预测值经过历史误差校准后的结果。

### 5. 交易规则

- 每天按 `final_score` 排序
- 选出可交易且最强的 `Top10`
- 默认持有 `10` 个交易日
- 回测也按这个规则执行

## 预测记录与模型校准

当前策略已经支持预测闭环。

每天运行策略时会：

1. 记录当天 `Top10` 的 `predicted_ret_10`
2. 记录校准后的 `expected_ret_10`
3. 在 10 个交易日后自动回填 `actual_ret_10`
4. 计算误差、绝对误差和方向是否判断正确
5. 基于成熟样本做线性校准，修正下一轮预测输出

核心文件：

- `reports/stage12_prediction_journal.parquet`
- `reports/stage12_prediction_eval_summary.json`

第一天运行时成熟样本为 `0` 是正常的；等满 10 个交易日后才会开始自动对账。

## 回测怎么看

当前回测输出同时提供摘要和完整曲线。

- `reports/stage12_backtest_summary.json`
- `reports/stage12_backtest_curve.parquet`

常见指标说明：

- `ann_return`：年化收益，小数形式，`3.00` 表示 `300%`
- `ann_vol`：年化波动，小数形式
- `max_drawdown`：最大回撤，小数形式，`-0.18` 表示 `-18%`
- `win_rate`：胜率，小数形式

如果要看更直观的结果，建议结合累计收益、资金净值和滚动持有 10 日收益一起看，而不要只看年化收益。

## 当前已验证状态

截至当前 README 更新时，这套流程已经完成：

- 本地数据更新到 `2026-03-18`
- 用 GPU 版多因子策略生成了最新 `Top10`
- 重跑并落盘了最新回测
- 启动了每日预测日志

如果后续发现 `README` 与实际输出不一致，应优先以 `reports/` 下最新文件和 `src/ashare_quant/pipeline/stage12_dual_signal_strategy.py` 的实现为准。

## 测试

```bash
uv run pytest -q
```

## `data` 文件夹能不能推送到 GitHub？

默认不建议，也默认不会。

原因很简单：

- `data/` 往往包含大量行情文件，体积很大
- 这些文件属于运行数据，不属于源码
- 推送后会让仓库膨胀很快，后续同步和克隆都会变慢
- 数据经常更新，不适合直接当作代码版本管理

当前项目的 `.gitignore` 已经默认忽略了：

```gitignore
data/**
reports/**
logs/**
```

只保留轻量样例目录：

```gitignore
!data/samples/
!data/samples/**
```

所以结论是：

- 真实数据文件：不建议推送
- 小型样例数据：可以按需保留在 `data/samples/`
- 代码、配置模板、脚本：建议推送

如果你确实要共享数据，建议优先用下面几种方式：

1. 只提供小样本数据
2. 把大文件放到对象存储或网盘
3. 在 README 中写清楚数据获取方式，而不是把整份数据直接放进仓库

## 常见问题

### 1. 为什么启动脚本不是 `run_dashboard.sh`？

当前仓库中实际可用的是：

```bash
./scripts/run_decision_app.sh
```

它会启动 `app/decision_app.py`，端口为 `8512`。

### 2. 为什么更新数据失败？

常见原因包括：

- 没有配置 `TUSHARE_TOKEN`
- 本地缺少基础数据文件
- 网络或接口访问失败
- WSL 代理配置导致外部行情接口返回失败

### 3. 为什么有些股票进了 Top10，但预测的 10 日收益不是正数？

因为当前排序不是只看 `expected_ret_10`，还同时混合了 `rule_score`。这是一种折中设计，优点是更稳，缺点是偶尔会把预测收益偏弱但规则形态较好的股票也推上来。

### 4. 为什么 GitHub 上看不到 `data/`？

这是正常现象，因为 `data/` 默认被 `.gitignore` 忽略，不会进入仓库。

## 推荐使用顺序

新用户建议按下面顺序上手：

1. `uv sync` 安装依赖
2. 配置 `.env`
3. 启动 `./scripts/run_decision_app.sh`
4. 先查看页面和已有结果
5. 再执行数据更新
6. 最后运行策略并查看输出

## 许可证

如需开源发布，建议后续补充明确的 License 文件。
