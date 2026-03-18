import akshare as ak
import pandas as pd
from pathlib import Path
import json, time

START_DATE = pd.Timestamp("2021-01-04")
END_DATE = pd.Timestamp("2026-03-17")
CHECK_EVERY = 50
SLEEP_SEC = 0.05

base = Path("/Users/seqi/Desktop/ashare-quant-research/data/stage4_all_buyable")
base.mkdir(parents=True, exist_ok=True)
out = base / "market_daily_all_buyable_20210101_20260314.parquet"
progress_file = base / "ak_progress.json"
err_file = base / "refresh_errors.log"

codes = ak.stock_info_a_code_name()
codes["code"] = codes["code"].astype(str).str.zfill(6)
codes["name"] = codes["name"].astype(str)
codes = codes[codes["code"].str.match(r"^(000|001|002|003|300|301|600|601|603|605|688)")].copy()

name_map = dict(zip(codes["code"], codes["name"]))
symbols = codes["code"].drop_duplicates().tolist()

pd.DataFrame({"ts_code": symbols, "name": [name_map[s] for s in symbols]}).to_csv(base / "buyable_symbols.csv", index=False)

frames = []
done = set()
errs = []

if out.exists():
    old = pd.read_parquet(out)
    if not old.empty:
        old["trade_date"] = pd.to_datetime(old["trade_date"]).dt.strftime("%Y-%m-%d")
        frames.append(old)
        done = set(old["ts_code"].astype(str).str.zfill(6).unique().tolist())

pending = [s for s in symbols if s not in done]
start_ts = time.time()
new_ok = 0

print({"all_symbols": len(symbols), "already_done": len(done), "pending": len(pending)})

def to_sina_symbol(code: str) -> str:
    return f"sh{code}" if code.startswith("6") else f"sz{code}"

def checkpoint(tag: str):
    if not frames:
        return
    full = pd.concat(frames, ignore_index=True)
    full = full.sort_values(["trade_date", "ts_code"]).drop_duplicates(["trade_date", "ts_code"], keep="last").reset_index(drop=True)
    full.to_parquet(out, index=False)
    prog = {
        "tag": tag,
        "all_symbols": len(symbols),
        "already_done": len(done),
        "pending": len(pending),
        "new_ok": new_ok,
        "errors": len(errs),
        "rows": int(len(full)),
        "symbols_after": int(full["ts_code"].astype(str).nunique()),
        "date_min": str(full["trade_date"].min()),
        "date_max": str(full["trade_date"].max()),
        "elapsed_min": round((time.time()-start_ts)/60, 2),
        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        "source": "sina_daily"
    }
    progress_file.write_text(json.dumps(prog, ensure_ascii=False, indent=2), encoding="utf-8")
    if errs:
        err_file.write_text("\n".join(errs[-2000:]), encoding="utf-8")
    print(prog, flush=True)

for i, code in enumerate(pending, 1):
    s = to_sina_symbol(code)
    ok = False
    for k in range(3):
        try:
            df = ak.stock_zh_a_daily(symbol=s, adjust="qfq")
            if df is not None and not df.empty:
                x = df.copy()
                x["trade_date"] = pd.to_datetime(x["date"])
                x = x[(x["trade_date"] >= START_DATE) & (x["trade_date"] <= END_DATE)].copy()
                if not x.empty:
                    out_df = pd.DataFrame({
                        "trade_date": x["trade_date"].dt.strftime("%Y-%m-%d"),
                        "open": x["open"].astype(float),
                        "high": x["high"].astype(float),
                        "low": x["low"].astype(float),
                        "close": x["close"].astype(float),
                        "volume": x["volume"].astype(float),
                        "amount": x["amount"].astype(float),
                    })
                    out_df["ts_code"] = code
                    nm = name_map.get(code, "")
                    out_df["is_st"] = ("ST" in nm) or ("退" in nm)
                    out_df["is_suspended"] = False
                    out_df["up_limit"] = out_df["close"] * 1.1
                    out_df["down_limit"] = out_df["close"] * 0.9
                    out_df["data_vendor"] = "sina_daily"
                    frames.append(out_df)
            done.add(code)
            new_ok += 1
            ok = True
            break
        except Exception as e:
            if k == 2:
                errs.append(f"{code}: {type(e).__name__}: {e}")
            time.sleep(0.6 + 0.6*k)
    if i % CHECK_EVERY == 0 or i == len(pending):
        checkpoint(f"progress_{i}")
    time.sleep(SLEEP_SEC)

checkpoint("final")
meta = json.loads(progress_file.read_text(encoding="utf-8"))
meta["mode"] = "sina_rebuild_full_checkpoint"
(base / "refresh_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
print({"done": True, "errors": len(errs)})
