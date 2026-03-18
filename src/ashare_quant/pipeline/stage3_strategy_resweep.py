from __future__ import annotations

from pathlib import Path
import json
import numpy as np
import pandas as pd

from ashare_quant.universe.filtering import apply_universe_filters
from ashare_quant.factors.price_volume import add_basic_factors
from ashare_quant.reporting.metrics import evaluate_curve
from ashare_quant.utils.ranking import cs_rank


def score(df: pd.DataFrame, family: str) -> pd.DataFrame:
    x = df.copy()
    if family == "trend":
        x["rule_score"] = (x["ma20"] > x["ma60"]).astype(int) * 0.45 + cs_rank(x, "ret_20") * 0.35 + cs_rank(x, "amount") * 0.20
    elif family == "momentum":
        x["rule_score"] = cs_rank(x, "ret_20") * 0.7 + cs_rank(x, "ret_5") * 0.3
    elif family == "breakout":
        g = x.groupby("ts_code", group_keys=False)
        x["hh55"] = g["high"].transform(lambda s: s.shift(1).rolling(55).max())
        x["rule_score"] = (x["close"] >= x["hh55"]).fillna(False).astype(int) * 0.7 + cs_rank(x, "vol_ratio_20") * 0.3
    else:
        raise ValueError(family)
    return x


def bt(df: pd.DataFrame, start: str, end: str, top_n=12, hold_days=5):
    x = df[(df["trade_date"] >= start) & (df["trade_date"] <= end)].copy()
    x = x.sort_values(["ts_code", "trade_date"])
    x["fwd_ret_1"] = x.groupby("ts_code")["close"].shift(-1) / x["close"] - 1
    dates = sorted(x["trade_date"].unique())
    h, eq, rows = {}, 1.0, []
    for d in dates:
        day = x[x["trade_date"] == d].sort_values("rule_score", ascending=False)
        rr = []
        if h:
            k = day.set_index("ts_code")
            rr = [float(k.loc[c, "fwd_ret_1"]) for c in h if c in k.index and pd.notna(k.loc[c, "fwd_ret_1"])]
        dr = float(np.mean(rr)) if rr else 0.0
        sell=0
        for c in list(h.keys()):
            h[c]+=1
            if h[c]>=hold_days:
                del h[c]; sell+=1
        buy=0
        slots=max(0,top_n-len(h))
        if slots>0:
            for _,r in day.iterrows():
                c=r["ts_code"]
                if c in h: continue
                # simple tradability filter
                if bool(r.get("is_limit_up_close", False)): 
                    continue
                h[c]=0; buy+=1; slots-=1
                if slots<=0: break
        trade_frac=(buy+sell)/max(top_n,1)
        net=dr - trade_frac*(2.5+5.0)/10000 - (sell/max(top_n,1))*10/10000
        eq*=(1+net)
        rows.append({"trade_date":d,"daily_ret":net,"equity":eq})
    return pd.DataFrame(rows)


def run():
    p=Path("data/market_daily_stage3_akshare120.parquet")
    if not p.exists():
        raise FileNotFoundError(p)
    df=pd.read_parquet(p)
    df=apply_universe_filters(df)
    df=add_basic_factors(df)

    families=["trend","momentum","breakout"]
    results=[]
    for f in families:
        s=score(df,f)
        tr=bt(s,"2019-01-02","2023-12-31")
        te=bt(s,"2024-01-01","2026-03-13")
        mtr,mte=evaluate_curve(tr),evaluate_curve(te)
        results.append({"family":f,"train":mtr,"test":mte})

    ranked=sorted(results,key=lambda r:(r["test"]["sharpe"],r["test"]["ann_return"]),reverse=True)
    best=ranked[0]
    s=score(df,best["family"])
    latest=s[s["trade_date"]==s["trade_date"].max()].sort_values("rule_score",ascending=False)
    picks=latest[["trade_date","ts_code","close","rule_score","ret_20","amount"]].head(5).copy()
    picks["target_weight"]=1/len(picks)

    out=Path("reports"); out.mkdir(exist_ok=True,parents=True)
    (out/"stage3_family_resweep.json").write_text(json.dumps(results,ensure_ascii=False,indent=2),encoding="utf-8")
    picks.to_csv(out/"daily_picks_stage3.csv",index=False)
    summary={"best_family":best["family"],"best_test":best["test"],"n_picks":int(len(picks)),"trade_date":str(picks["trade_date"].iloc[0])}
    (out/"stage3_execution_summary.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps(summary,ensure_ascii=False,indent=2))
    print(picks.to_string(index=False))

if __name__=="__main__":
    run()
