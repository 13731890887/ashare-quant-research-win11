# Windows 11 运行说明（GPU 可选）

## 1) 安装环境
- 安装 Python 3.11（勾选 Add Python to PATH）
- 安装 Git
- 可选：安装 uv（https://astral.sh/uv）

## 2) 打开 PowerShell 并进入目录
```powershell
cd $HOME\Desktop\ashare-quant-research-win11
```

## 3) 创建虚拟环境并安装依赖
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -U pip
pip install -e .
```

## 4) 运行（先拉取股票数据，再跑策略）
```powershell
.\scripts\run_stock_fetch_win.ps1
.\scripts\run_stage11_win.ps1
.\scripts\run_stage16_win.ps1
```

## 5) 输出文件
- `reports/stage11_summary.json`
- `reports/stage16_summary.json`
- `reports/stage16_picks.csv`

## 6) 说明
- 当前代码主要吃 CPU，5060Ti 不会自动加速。
- 若要 GPU 加速训练，需要改用 XGBoost/LightGBM GPU 版本。
