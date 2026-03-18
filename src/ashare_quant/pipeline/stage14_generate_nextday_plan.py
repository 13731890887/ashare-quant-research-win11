from __future__ import annotations

from pathlib import Path
import json
import pandas as pd


def run() -> None:
    rep = Path('reports')
    act_p = rep / 'stage12_actions.json'
    sum_p = rep / 'stage12_summary.json'
    top_p = rep / 'stage12_dual_top50.csv'

    if not act_p.exists() or not sum_p.exists():
        raise FileNotFoundError('missing stage12 outputs, run stage12 first')

    act = json.loads(act_p.read_text(encoding='utf-8'))
    s = json.loads(sum_p.read_text(encoding='utf-8'))
    top = pd.read_csv(top_p) if top_p.exists() else pd.DataFrame()

    trade_date = s.get('trade_date')
    buy = [str(x).zfill(6) for x in act.get('buy', [])]
    sell = [str(d.get('ts_code','')).zfill(6) for d in act.get('sell', [])]

    rows = []
    for c in buy:
        note = '分批买入，避免开盘前15分钟追价；若涨停封单则放弃'
        if not top.empty:
            r = top[top['ts_code'].astype(str).str.zfill(6)==c]
            if not r.empty:
                rr=r.iloc[0]
                note = f"up={rr.get('up_score', 'NA'):.3f}, risk={rr.get('risk_score', 'NA'):.3f}; " + note
        rows.append({'trade_date': trade_date, 'action': 'BUY', 'ts_code': c, 'risk_note': note})
    for c in sell:
        rows.append({'trade_date': trade_date, 'action': 'SELL', 'ts_code': c, 'risk_note': '若跌停无法卖出，次日优先排队卖出'})

    plan = pd.DataFrame(rows)
    out_csv = rep / 'nextday_trade_plan.csv'
    plan.to_csv(out_csv, index=False)

    summary = {
        'trade_date': trade_date,
        'buy_count': int(len(buy)),
        'sell_count': int(len(sell)),
        'status': 'empty_plan' if plan.empty else 'ready',
        'output': str(out_csv),
    }
    (rep / 'nextday_trade_plan_summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(summary)


if __name__ == '__main__':
    run()
