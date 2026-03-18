# quant-next 迁移方案（兼容 ashare-quant-research）

目标：在不重拉历史数据的前提下，将旧项目数据直接迁移到新框架使用。

## 一、架构分层

```text
quant-next/
  adapters/      # 旧数据适配层（读取旧 parquet -> 标准契约）
  data/lake/     # 标准化数据湖（daily_bars / valuation_daily）
  features/      # 因子工程（后续）
  models/        # 模型层（后续）
  backtest/      # 回测层（后续）
  configs/       # 配置（schema/strategy）
  scripts/       # 迁移与校验脚本
```

## 二、数据契约（核心）

### 1) daily_bars
- trade_date: date
- ts_code: string(6)
- open/high/low/close: float
- volume/amount: float
- is_st/is_suspended: bool
- up_limit/down_limit: float
- data_vendor: string

### 2) valuation_daily
- trade_date: date
- ts_code: string(6)
- pe_ttm: float
- pb: float
- mv_total: float

## 三、迁移步骤

1. 使用 `adapters/legacy_ashare_adapter.py` 读取旧项目：
   - `data/stage4_all_buyable/market_daily_all_buyable_20210101_20260314.parquet`
   - `data/fundamental_cache/pe_ttm.parquet`
   - `data/fundamental_cache/pb.parquet`
   - `data/fundamental_cache/mv_total.parquet`
2. 输出到 `quant-next/data/lake/`：
   - `daily_bars.parquet`
   - `valuation_daily.parquet`
3. 运行 `scripts/check_migration.py` 做一致性校验（股票数、日期覆盖、空值率）

## 四、运行命令

```powershell
cd $HOME\Desktop\ashare-quant-research-win11\quant-next
python .\adapters\legacy_ashare_adapter.py
python .\scripts\check_migration.py
```

## 五、后续
- 先迁 stage11（CPU稳定基线）
- 再迁 stage16（多因子+ML）
- 最后按需替换 GPU 模型（XGBoost/LightGBM）
