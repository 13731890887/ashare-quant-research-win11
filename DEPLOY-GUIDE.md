# A-Share Quant Research — AI 自动化部署指导

> 适用项目：[ashare-quant-research-win11](https://github.com/13731890887/ashare-quant-research-win11)
> 平台：Windows 11（主要）/ macOS / Linux
> Python：3.11+

---

## 第一步：环境前置检查

在开始之前，AI 助手会自动检查以下依赖是否已安装：

```powershell
# 检查 Python 版本（需要 >= 3.11）
python --version

# 检查 Git
git --version

# 检查 uv（推荐包管理器）
uv --version
```

**若 `uv` 未安装，请先执行：**

```powershell
# Windows（PowerShell 管理员模式）
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
```

---

## 第二步：克隆项目

```powershell
git clone https://github.com/13731890887/ashare-quant-research-win11.git
cd ashare-quant-research-win11
```

---

## 第三步：数据源选择（必须由用户决定）

> **AI 提示用户：** 本系统支持多种 A 股数据获取方式，请根据您的实际条件选择以下方案之一。不同方案在数据质量、获取成本和配置复杂度上有所差异。

---

### 方案 A：Tushare Pro（推荐 · 数据最完整）

**适用场景：** 您已有或愿意注册 Tushare Pro 账号，追求最高数据质量。

**数据覆盖：** 日线 OHLCV、财务指标、基本面数据（PE/PB）、成分股、除权数据等。

**前置条件：**
- 注册 [Tushare Pro](https://tushare.pro/register) 账号
- 积分要求：至少 **120 积分**（可通过实名认证 + 手机绑定获得，免费）
- 获取 API Token（注册后在个人中心查看）

**配置步骤：**

```powershell
# 1. 复制环境变量模板
copy .env.example .env

# 2. 编辑 .env 文件，填入你的 Token
notepad .env
```

`.env` 文件内容：
```env
TUSHARE_TOKEN=你的tushare_token（必填）
TUSHARE_HTTP_URL=                  # 可选，留空即可
```

**验证 Token 是否有效：**

```powershell
.\.venv\Scripts\Activate.ps1
python -c "import tushare as ts; ts.set_token('你的TOKEN'); pro=ts.pro_api(); print(pro.trade_cal(exchange='SSE', start_date='20240101', end_date='20240110'))"
```

---

### 方案 B：AKShare（免费 · 无需注册）

**适用场景：** 无 Tushare 账号，或希望完全免费获取数据。

**数据覆盖：** 日线 OHLCV、基础财务数据，部分历史深度有限。

**前置条件：** 无需注册，直接安装使用。

**配置步骤：**

```powershell
# 1. 复制环境变量模板（AKShare 无需 Token，但仍需创建 .env 文件）
copy .env.example .env

# 2. .env 文件保持默认即可，TUSHARE_TOKEN 留空
```

**验证 AKShare 可用：**

```powershell
.\.venv\Scripts\Activate.ps1
python -c "import akshare as ak; df=ak.stock_zh_a_hist(symbol='000001', period='daily', start_date='20240101', end_date='20240110'); print(df.head())"
```

**注意事项：**
- AKShare 抓取速度较慢，全量历史数据拉取耗时较长（约数小时）
- 部分接口有访问频率限制，请勿并发过高
- 建议在非交易时间运行全量数据拉取

---

### 方案 C：混合模式（最稳定 · 推荐生产环境）

**适用场景：** 同时配置 Tushare 和 AKShare，互为备份，提升数据可靠性。

**配置步骤：**

```powershell
# 同方案 A，配置好 Tushare Token
copy .env.example .env
notepad .env
```

系统会优先使用 Tushare，当 Tushare 接口失败时自动回退至 AKShare。

**对应管道脚本：**
- `stage1_data_ingest.py` → 使用 Tushare
- `stage1_round2_akshare.py` → 使用 AKShare 补全
- `stage1_round3_tushare_baseline.py` → Tushare 基准校验

---

### 方案 D：本地历史数据（离线模式）

**适用场景：** 您已有本地 Parquet 格式历史数据，不需要实时拉取。

**数据要求：**
- 格式：`.parquet`，列名需符合项目规范（ts_code, trade_date, open, high, low, close, vol 等）
- 存放路径：`data/stage4_all_buyable/market_daily_all_buyable_<起始日>_<结束日>.parquet`

**配置步骤：**

```powershell
# 1. 将本地数据文件放入对应目录
mkdir data\stage4_all_buyable
# 复制你的数据文件到该目录

# 2. 修改 configs/research.yaml 中的数据路径
notepad configs\research.yaml
```

`configs/research.yaml` 关键字段：
```yaml
data_source:
  path: "data/stage4_all_buyable/your_data_file.parquet"
  start_date: "20210101"
  end_date: "20260314"
```

---

### 方案 E：已发布数据包（最快上手，推荐新机器）

**适用场景：** 您希望在新机器上最快完成部署，不想先手动准备 parquet / csv / 基本面缓存。

**数据包地址：**
- `https://seqiwang.cn/uploads/ashare-quant-data-20260320.tar.gz`

**当前主行情真实时间范围：**
- `2021-01-04` ~ `2026-03-18`

**数据包内容：**
- `data/stage4_all_buyable/`
- `data/fundamental_cache/`
- `reports/`

**配置方式：**

```powershell
# 复制环境变量模板（如不存在）
copy .env.example .env

# 不使用 Tushare 时，可保持空值
notepad .env
```

`.env` 可写为：
```env
TUSHARE_TOKEN=
TUSHARE_HTTP_URL=
```

**一步配置 + 运行：**

```powershell
.\scripts\bootstrap_from_data_package_win.ps1
```

这个脚本会自动完成：
1. 创建 `.env`（如果不存在）
2. 下载数据包
3. 解压到项目根目录
4. 执行 `uv sync`
5. 运行正式策略 `stage12_dual_signal_strategy`

**如果要下载后直接启动 Web：**

```powershell
$env:ASHARE_RUN_MODE = "app"
.\scripts\bootstrap_from_data_package_win.ps1
```

**如果要先跑策略再启动 Web：**

```powershell
$env:ASHARE_RUN_MODE = "both"
.\scripts\bootstrap_from_data_package_win.ps1
```

**如果你有自己的数据包地址：**

```powershell
$env:ASHARE_DATA_URL = "https://your-domain/path/to/data.tar.gz"
.\scripts\bootstrap_from_data_package_win.ps1
```

---

## 第四步：安装依赖

```powershell
# 使用 uv（推荐，速度更快）
uv sync

# 或使用传统 pip
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .
```

---

## 第五步：配置认证与通知（可选）

```powershell
# 认证配置（如需 Web 界面登录保护）
copy config\auth.example.yaml config\auth.yaml
notepad config\auth.yaml

# 通知配置（如需消息推送）
copy config\notify.example.yaml config\notify.yaml
notepad config\notify.yaml
```

---

## 第六步：运行数据管道

> AI 会根据您在第三步选择的数据源，自动引导您执行对应的数据拉取命令。若您选择的是“方案 E：已发布数据包”，可直接跳过全量拉取，改用一键脚本初始化。

### 6.1 拉取全量历史数据（首次运行）

**方案 A / C（Tushare）：**

```powershell
.\.venv\Scripts\Activate.ps1
.\scripts\run_stock_fetch_win.ps1
```

**方案 B（AKShare）：**

```powershell
.\.venv\Scripts\Activate.ps1
python src/ashare_quant/pipeline/stage1_round2_akshare.py
```

**预计耗时：** 首次全量拉取约 2-6 小时（取决于网络和数据源）

---

### 6.2 运行策略管道（核心阶段）

```powershell
# Stage 11：策略筛选与修复
.\scripts\run_stage11_win.ps1

# Stage 16：多因子 ML 选股
.\scripts\run_stage16_win.ps1
```

**输出文件：**
- `reports/stage11_summary.json` — 阶段 11 统计摘要
- `reports/stage16_summary.json` — 阶段 16 统计摘要
- `reports/stage16_picks.csv` — **最终选股结果**

---

## 第七步：启动 Web 界面

```powershell
# 决策应用（主界面）
.\scripts\run_decision_app.sh
# 访问地址：http://127.0.0.1:8512

# 数据看板（可视化）
.\scripts\run_dashboard.sh
```

---

## 第八步：每日自动更新（生产部署）

配置 Windows 任务计划程序，每个交易日收盘后（建议 16:30）自动运行：

```powershell
# 创建计划任务（PowerShell 管理员模式）
$action = New-ScheduledTaskAction -Execute "PowerShell.exe" `
  -Argument "-File C:\path\to\ashare-quant-research-win11\scripts\run_stage11_win.ps1"

$trigger = New-ScheduledTaskTrigger -Daily -At "16:30"

Register-ScheduledTask -TaskName "AShareQuantDaily" `
  -Action $action -Trigger $trigger -RunLevel Highest
```

---

## 常见问题排查

| 问题 | 原因 | 解决方案 |
|------|------|----------|
| `TUSHARE_TOKEN` 无效 | Token 填写错误或积分不足 | 检查 `.env` 文件；登录 tushare.pro 确认积分 |
| AKShare 请求超时 | 网络问题或接口频率限制 | 设置代理或降低并发；稍后重试 |
| Parquet 文件未找到 | 数据拉取未完成 | 确认 `data/stage4_all_buyable/` 目录下有数据文件 |
| XGBoost GPU 错误 | 无 CUDA 显卡 | 忽略 GPU 相关警告；系统会自动降级为 CPU 运行 |
| PowerShell 执行策略错误 | 系统限制脚本执行 | 管理员运行：`Set-ExecutionPolicy RemoteSigned` |
| `uv sync` 失败 | uv 版本过旧 | 重新安装 uv：`uv self update` |

---

## 数据源对比速查

| 特性 | Tushare Pro | AKShare | 混合模式 | 本地数据 |
|------|:-----------:|:-------:|:--------:|:--------:|
| 是否免费 | 部分免费（积分制） | 完全免费 | 部分免费 | 免费 |
| 数据完整性 | ★★★★★ | ★★★☆☆ | ★★★★★ | 取决于来源 |
| 更新及时性 | T+0（当日） | T+0（当日） | T+0 | 手动更新 |
| 配置复杂度 | 中（需 Token） | 低 | 中 | 低 |
| 推荐场景 | 生产/研究 | 快速验证 | 稳定生产 | 离线研究 |
