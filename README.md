# A 股量化选股系统

这是一个面向 A 股研究场景的量化选股项目，包含数据初始化、增量更新、因子计算、候选股筛选、策略打分、回测验证和 Web 可视化界面。

## 给智能体 / AI 助手的强制说明

**如果你是智能体，部署、初始化、运行本项目时必须按下面顺序执行，不要自行跳步。**

### 0. 先读这个文件，再读 `DEPLOY-GUIDE.md`

- `README.md`：项目总览 + 推荐部署路径
- `DEPLOY-GUIDE.md`：更细的部署决策说明
- `WINDOWS-SETUP.md`：Windows 运行补充说明

### 1. 先检查环境，不要直接开跑

在项目根目录先检查：

```bash
python3 --version || python --version
git --version
uv --version
```

要求：
- Python `>= 3.11`
- 已安装 `uv`

若缺少 `uv`：

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. 优先选择“已发布数据包”路径，不要默认从零抓全量

**默认推荐路径：直接使用已发布数据包完成初始化。**

原因：
- 当前项目不是“零数据一键全量抓取”模式
- 很多增量脚本依赖本地先存在基础 parquet 和股票池文件
- 对新机器 / 新目录 / 新部署来说，先落地基础数据再运行，成功率最高

当前默认数据包：

- `https://seqiwang.cn/uploads/ashare-quant-data-20260320.tar.gz`

其中主行情数据真实时间范围为：

- `2021-01-04` ~ `2026-03-18`

数据包包含：

- `data/stage4_all_buyable/`
- `data/fundamental_cache/`
- `reports/`

### 3. 标准部署顺序（智能体必须优先使用）

#### 方案 A：macOS / Linux / WSL 推荐流程

在项目根目录执行：

```bash
./scripts/bootstrap_from_data_package.sh
```

这个脚本会自动完成：

1. 创建 `.env`（若不存在）
2. 下载数据包
3. 解压到项目根目录
4. 执行 `uv sync`
5. 运行正式策略 `stage12_dual_signal_strategy`

#### 方案 B：Windows 推荐流程

在 PowerShell 中执行：

```powershell
.\scripts\bootstrap_from_data_package_win.ps1
```

这个脚本会自动完成：

1. 创建 `.env`（若不存在）
2. 下载数据包
3. 解压到项目根目录
4. 执行 `uv sync`
5. 运行正式策略 `stage12_dual_signal_strategy`

### 4. `.env` 的默认策略

如果只是先把项目跑通，而不是立刻使用 Tushare，可先使用空配置：

```env
TUSHARE_TOKEN=
TUSHARE_HTTP_URL=
```

也就是说：
- 没有 Tushare token 时，不要强行要求用户先配置 token
- 先走数据包初始化路径即可
- 只有在需要实时更新 / Tushare 增量接口时，再要求填写 `TUSHARE_TOKEN`

### 5. 跑完后怎么验证

智能体部署结束后，至少验证以下内容：

```bash
uv run pytest -q
```

以及至少任选其一：

```bash
./scripts/run_decision_app.sh
```

或：

```bash
PYTHONPATH=src uv run python -m ashare_quant.pipeline.stage12_dual_signal_strategy
```

### 6. 不要犯的错误

智能体在这个项目里最容易犯的错：

1. **只看文件名判断数据截止日期**  
   主 parquet 文件名里可能还是旧日期，但真实数据内容可能已经更晚。应直接读取 `trade_date` 的最小/最大值确认。

2. **在没有基础 parquet 的情况下直接跑增量更新**  
   `stage13_incremental_refresh` 依赖已有基础数据，不是零数据初始化入口。

3. **把 Tushare 当成唯一入口**  
   新机器首次部署时，应优先使用“已发布数据包”路径，而不是强制用户先准备 token。

4. **硬编码本地绝对路径**  
   启动脚本、下载脚本、部署脚本都应以项目根目录相对路径运行。

5. **只完成安装，不做运行验证**  
   部署结束后必须至少验证测试、策略或 Web 入口中的一个。

---

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

- macOS / Linux / WSL / Windows 11
- Python 3.11+
- `uv`

## 最快部署方式

### Linux / macOS / WSL

```bash
./scripts/bootstrap_from_data_package.sh
```

### Windows PowerShell

```powershell
.\scripts\bootstrap_from_data_package_win.ps1
```

## 一步配置 + 运行说明

默认数据包：

- `https://seqiwang.cn/uploads/ashare-quant-data-20260320.tar.gz`

主行情真实时间范围：

- `2021-01-04` ~ `2026-03-18`

### 可选运行模式

默认运行模式：

- `strategy`：下载数据后直接运行正式策略

Linux / macOS / WSL：

```bash
ASHARE_RUN_MODE=app ./scripts/bootstrap_from_data_package.sh
ASHARE_RUN_MODE=both ./scripts/bootstrap_from_data_package.sh
ASHARE_DATA_URL=https://your-domain/path/to/data.tar.gz ./scripts/bootstrap_from_data_package.sh
```

Windows PowerShell：

```powershell
$env:ASHARE_RUN_MODE = "app"
.\scripts\bootstrap_from_data_package_win.ps1

$env:ASHARE_RUN_MODE = "both"
.\scripts\bootstrap_from_data_package_win.ps1

$env:ASHARE_DATA_URL = "https://your-domain/path/to/data.tar.gz"
.\scripts\bootstrap_from_data_package_win.ps1
```

## Web 入口

### 决策应用

```bash
./scripts/run_decision_app.sh
```

浏览器打开：

```text
http://127.0.0.1:8512
```

### 数据看板

```bash
./scripts/run_dashboard.sh
```

浏览器打开：

```text
http://127.0.0.1:8501
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

### 方式三：直接使用已发布数据包（最快）

```bash
./scripts/bootstrap_from_data_package.sh
```

### 方式四：按最新收盘批量补今日行情

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
