from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pandas as pd
import pyarrow.dataset as pds
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
DATA = ROOT / "data" / "stage4_all_buyable"
DATASET_PATH = DATA / "market_daily_all_buyable_20210101_20260314.parquet"

st.set_page_config(page_title="A股量化统一控制台", layout="wide")
st.title("A股量化研究统一控制台")


def run_cmd(cmd: str) -> tuple[int, str]:
    p = subprocess.run(cmd, shell=True, cwd=ROOT, capture_output=True, text=True)
    return p.returncode, (p.stdout + "\n" + p.stderr).strip()


def read_json(path: Path):
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


@st.cache_data(ttl=120)
def load_top50() -> pd.DataFrame:
    p = REPORTS / "stage12_dual_top50.csv"
    return pd.read_csv(p) if p.exists() else pd.DataFrame()


@st.cache_data(ttl=120)
def load_stock_history(symbol: str, max_rows: int = 400) -> pd.DataFrame:
    if not DATASET_PATH.exists():
        return pd.DataFrame()
    ds = pds.dataset(str(DATASET_PATH), format="parquet")
    tbl = ds.to_table(
        columns=["trade_date", "ts_code", "open", "high", "low", "close", "volume", "amount"],
        filter=(pds.field("ts_code") == symbol),
    )
    if tbl.num_rows == 0:
        return pd.DataFrame()
    df = tbl.to_pandas().sort_values("trade_date").tail(max_rows).reset_index(drop=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["ret_20"] = df["close"].pct_change(20)
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma60"] = df["close"].rolling(60).mean()
    return df


def section_ops():
    st.subheader("操作区")
    c1, c2, c3 = st.columns(3)

    if c1.button("1) 增量更新数据 (stage13)", use_container_width=True):
        code, out = run_cmd("uv run python -m ashare_quant.pipeline.stage13_incremental_refresh")
        st.code(out[:8000])
        st.success("完成" if code == 0 else "失败")

    if c2.button("2) 跑双信号策略 (stage12)", use_container_width=True):
        code, out = run_cmd("uv run python -m ashare_quant.pipeline.stage12_dual_signal_strategy")
        st.code(out[:8000])
        st.success("完成" if code == 0 else "失败")

    if c3.button("一键全流程（更新+策略）", use_container_width=True):
        code, out = run_cmd("uv run python -m ashare_quant.pipeline.stage13_incremental_refresh && uv run python -m ashare_quant.pipeline.stage12_dual_signal_strategy")
        st.code(out[:10000])
        st.success("完成" if code == 0 else "失败")


def section_summary():
    st.subheader("核心结果")
    sumj = read_json(REPORTS / "stage12_summary.json")
    actj = read_json(REPORTS / "stage12_actions.json")
    rmeta = read_json(DATA / "refresh_meta.json")

    c1, c2, c3, c4 = st.columns(4)
    if sumj:
        c1.metric("交易日", sumj.get("trade_date", "-"))
        c2.metric("买入数", sumj.get("buy_count", 0))
        c3.metric("持有数", sumj.get("hold_count", 0))
        c4.metric("卖出数", sumj.get("sell_count", 0))
    else:
        st.info("尚无 stage12_summary.json，请先运行策略")

    if rmeta:
        st.caption(f"数据最新日期: {rmeta.get(date_max_after, N/A)} | 覆盖股票: {rmeta.get(symbols_after, N/A)} | 抓取错误: {rmeta.get(fetch_errors, N/A)}")

    if actj:
        st.markdown("**动作清单 (Buy / Hold / Sell)**")
        tabs = st.tabs(["Buy", "Hold", "Sell"])
        with tabs[0]:
            st.json(actj.get("buy", []))
        with tabs[1]:
            st.json(actj.get("hold", []))
        with tabs[2]:
            st.json(actj.get("sell", []))


def section_single_stock():
    st.subheader("单个股票分析")
    c1, c2 = st.columns([2, 1])
    symbol = c1.text_input("输入股票代码（6位）", value="600519").strip().zfill(6)
    max_rows = c2.number_input("查看最近N条", min_value=60, max_value=800, value=240, step=20)

    if st.button("分析该股票", use_container_width=True):
        df = load_stock_history(symbol, int(max_rows))
        if df.empty:
            st.warning(f"未找到 {symbol} 的历史数据")
            return

        latest = df.iloc[-1]
        top50 = load_top50()
        row = top50[top50["ts_code"].astype(str).str.zfill(6) == symbol]

        st.markdown(f"**{symbol} 最新日期：{latest[trade_date].date()}**")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("最新收盘", f"{latest[close]:.2f}")
        m2.metric("20日涨幅", f"{(latest[ret_20] or 0)*100:.2f}%")
        m3.metric("MA20", f"{latest[ma20]:.2f}" if pd.notna(latest[ma20]) else "N/A")
        m4.metric("MA60", f"{latest[ma60]:.2f}" if pd.notna(latest[ma60]) else "N/A")

        if not row.empty:
            r = row.iloc[0]
            up_score = float(r.get("up_score", 0))
            risk_score = float(r.get("risk_score", 1))
            st.info(f"模型分数：up_score={up_score:.3f}, risk_score={risk_score:.3f}")
            if up_score >= 0.70 and risk_score <= 0.35:
                st.success("信号判定：可买入候选")
            elif risk_score >= 0.65 or up_score < 0.45:
                st.error("信号判定：风险偏高/信号弱，偏卖出")
            else:
                st.warning("信号判定：中性，观察或持有")
        else:
            st.caption("该股票不在当前Top50候选中")

        chart = df[["trade_date", "close", "ma20", "ma60"]].set_index("trade_date")
        st.line_chart(chart)

        show_cols = ["trade_date", "open", "high", "low", "close", "volume", "amount", "ret_20", "ma20", "ma60"]
        st.dataframe(df[show_cols].tail(30), use_container_width=True, height=300)


def section_tables():
    st.subheader("分析表")
    top50 = REPORTS / "stage12_dual_top50.csv"
    if top50.exists():
        df = pd.read_csv(top50)
        st.dataframe(df.head(50), use_container_width=True, height=420)
    else:
        st.info("尚无 stage12_dual_top50.csv")

    st.subheader("配置")
    cfg = ROOT / "configs" / "research.yaml"
    if cfg.exists():
        st.code(cfg.read_text(encoding="utf-8"), language="yaml")


def section_health():
    st.subheader("系统状态")
    if st.button("检查关键文件", use_container_width=True):
        checks = {
            "dataset": DATASET_PATH,
            "summary": REPORTS / "stage12_summary.json",
            "actions": REPORTS / "stage12_actions.json",
            "top50": REPORTS / "stage12_dual_top50.csv",
        }
        st.json({k: str(v.exists()) for k, v in checks.items()})


section_ops()
st.divider()
section_summary()
st.divider()
section_single_stock()
st.divider()
section_tables()
st.divider()
section_health()
