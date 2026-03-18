# A 股量化选股系统

这是一个面向 A 股研究场景的量化选股项目，包含数据更新、因子计算、候选股筛选、策略打分、回测验证和 Web 可视化界面。

如果你是第一次接触这个项目，可以先把它理解成一条完整流程：

1. 拉取或更新行情数据
2. 清洗并过滤不适合参与选股的股票
3. 计算趋势、动量、流动性、波动等基础指标
4. 给股票打分并生成候选名单
5. 输出买入 / 持有 / 卖出建议
6. 在网页界面中查看结果

## 项目特点

- 面向 A 股场景，默认考虑 ST、停牌、流动性等基础过滤条件
- 选股逻辑清晰，重点关注趋势、动量、成交活跃度和风险
- 支持增量更新数据
- 支持输出 Top50 候选和操作建议
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

项目当前的增量更新脚本是：

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

### 更新数据前要注意什么

增量更新依赖本地已经存在基础数据文件和股票列表文件。如果缺少下面两个文件，更新会失败：

- `data/stage4_all_buyable/market_daily_all_buyable_20210101_20260314.parquet`
- `data/stage4_all_buyable/buyable_symbols.csv`

也就是说，这个项目当前不是“零数据直接一键拉全量”的模式，而是基于已有数据做增量刷新。

## 如何运行策略

数据准备好之后，可以直接运行双信号策略：

```bash
PYTHONPATH=src uv run python -m ashare_quant.pipeline.stage12_dual_signal_strategy
```

运行后会在 `reports/` 下生成常用结果文件，例如：

- `reports/stage12_dual_top50.csv`
- `reports/stage12_actions.json`
- `reports/stage12_summary.json`

## 选股逻辑说明

这个系统当前的核心思路不是追求最复杂的黑盒模型，而是尽量让选股逻辑清楚、容易排查。

策略大致会看三类信息：

- 趋势和动量：例如 5 日涨幅、20 日涨幅、20 日均线与 60 日均线
- 流动性：例如成交额、成交量变化
- 风险：例如波动率、短期振幅

最终会形成两个核心分数：

- `up_score`：上涨潜力分数
- `risk_score`：风险分数

系统更偏向选择这类股票：

- 最近走势不弱
- 有一定成交活跃度
- 风险不要太高

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

### 3. 为什么 GitHub 上看不到 `data/`？

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
