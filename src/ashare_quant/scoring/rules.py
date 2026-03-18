from __future__ import annotations
import pandas as pd
from ashare_quant.utils.ranking import cs_rank

def score_transparent_rules(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["score_trend"] = ((out["ma20"] > out["ma60"]).fillna(False)).astype(int)
    out["score_mom"] = (cs_rank(out, "ret_20") * 100).fillna(0)
    out["score_liq"] = (cs_rank(out, "amount") * 100).fillna(0)
    out["rule_score"] = 0.4 * out["score_trend"] * 100 + 0.4 * out["score_mom"] + 0.2 * out["score_liq"]
    return out
