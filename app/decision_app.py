from __future__ import annotations

import pandas as pd
import streamlit as st
from pathlib import Path
from dataclasses import dataclass
import pyarrow.dataset as pds
from plotly.subplots import make_subplots
import plotly.graph_objects as go

REPORTS = Path(__file__).resolve().parents[1] / 'reports'
DATASET = Path(__file__).resolve().parents[1] / 'data' / 'stage4_all_buyable' / 'market_daily_all_buyable_20210101_20260314.parquet'
TRADE_LOG = REPORTS / 'trade_records.csv'
HOLDINGS_BOOK = REPORTS / 'holdings_book.csv'
st.set_page_config(page_title='股票决策辅助', layout='wide')


def load_candidates() -> pd.DataFrame:
    p = REPORTS / 'stage12_dual_top50.csv'
    if not p.exists():
        raise FileNotFoundError('缺少真实候选文件 reports/stage12_dual_top50.csv，请先运行策略生成。')
    df = pd.read_csv(p).copy()
    if df.empty:
        raise RuntimeError('真实候选文件为空，请先更新数据并重跑策略。')
    df['股票代码'] = df.get('ts_code', '').astype(str).str.zfill(6)
    df['股票名称'] = 'N/A'
    df['当前价'] = pd.to_numeric(df.get('close', 0), errors='coerce').fillna(0.0)
    df['当日涨跌幅(%)'] = (pd.to_numeric(df.get('ret_20', 0), errors='coerce').fillna(0.0) * 100).round(2)
    up = pd.to_numeric(df.get('up_score', 0.0), errors='coerce').fillna(0.0)
    risk = pd.to_numeric(df.get('risk_score', 1.0), errors='coerce').fillna(1.0)
    df['策略评分'] = (up * 100).clip(0, 100).round(1)
    df['入选原因'] = [f'up={u:.3f}, risk={r:.3f}' for u, r in zip(up, risk)]
    df['风险等级'] = risk.apply(lambda x: '低' if x < 0.35 else ('中' if x < 0.65 else '高'))
    out = df[['股票代码', '股票名称', '当前价', '当日涨跌幅(%)', '策略评分', '入选原因', '风险等级']]
    return out.sort_values('策略评分', ascending=False).reset_index(drop=True)


@st.cache_data(ttl=120)
def load_price_series_real(symbol: str, months: int) -> pd.DataFrame:
    if not DATASET.exists():
        return pd.DataFrame()
    ds = pds.dataset(str(DATASET), format='parquet')
    tbl = ds.to_table(
        columns=['trade_date', 'ts_code', 'open', 'high', 'low', 'close', 'volume', 'amount'],
        filter=(pds.field('ts_code') == symbol),
    )
    if tbl.num_rows == 0:
        # fallback for possible unpadded code storage
        tbl = ds.to_table(
            columns=['trade_date', 'ts_code', 'open', 'high', 'low', 'close', 'volume', 'amount'],
            filter=(pds.field('ts_code') == str(int(symbol))),
        )
        if tbl.num_rows == 0:
            return pd.DataFrame()
    d = tbl.to_pandas()
    d['date'] = pd.to_datetime(d['trade_date'])
    d = d.sort_values('date')
    n = {1: 22, 3: 66, 6: 132}.get(months, 132)
    d = d.tail(n).reset_index(drop=True)
    d['ma5'] = d['close'].rolling(5).mean()
    d['ma10'] = d['close'].rolling(10).mean()
    d['ma20'] = d['close'].rolling(20).mean()
    prev_close = d['close'].shift(1)
    prev_ma10 = d['ma10'].shift(1)
    d['buy_signal'] = (prev_close <= prev_ma10) & (d['close'] > d['ma10'])
    return d




def diagnose_missing_days(symbol: str, df_symbol: pd.DataFrame) -> tuple[int, int, list[str]]:
    if not DATASET.exists() or df_symbol.empty:
        return 0, 0, []
    # compare against actual market trading dates in same window from full dataset
    full = pd.read_parquet(DATASET, columns=['trade_date'])
    full_dates = pd.to_datetime(full['trade_date']).drop_duplicates().sort_values()
    smin, smax = df_symbol['date'].min(), df_symbol['date'].max()
    mkt_dates = full_dates[(full_dates >= smin) & (full_dates <= smax)]
    got_dates = pd.to_datetime(df_symbol['date']).drop_duplicates().sort_values()
    missing = sorted(set(mkt_dates.dt.strftime('%Y-%m-%d')) - set(got_dates.dt.strftime('%Y-%m-%d')))
    return int(len(mkt_dates)), int(len(got_dates)), missing[:20]
def resample_ohlcv(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    if df.empty or freq == '日线':
        return df
    x = df.copy().set_index('date').sort_index()
    rule = 'W-FRI' if freq == '周线' else 'M'
    agg = pd.DataFrame()
    agg['open'] = x['open'].resample(rule).first()
    agg['high'] = x['high'].resample(rule).max()
    agg['low'] = x['low'].resample(rule).min()
    agg['close'] = x['close'].resample(rule).last()
    agg['volume'] = x['volume'].resample(rule).sum()
    agg = agg.dropna().reset_index()
    agg['ma5'] = agg['close'].rolling(5).mean()
    agg['ma10'] = agg['close'].rolling(10).mean()
    agg['ma20'] = agg['close'].rolling(20).mean()
    prev_close = agg['close'].shift(1)
    prev_ma10 = agg['ma10'].shift(1)
    agg['buy_signal'] = (prev_close <= prev_ma10) & (agg['close'] > agg['ma10'])
    return agg


def make_professional_chart(dfp: pd.DataFrame, symbol: str):
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.04,
        row_heights=[0.75, 0.25], subplot_titles=[f'{symbol} K线', '成交量']
    )

    fig.add_trace(
        go.Candlestick(
            x=dfp['date'], open=dfp['open'], high=dfp['high'], low=dfp['low'], close=dfp['close'],
            increasing_line_color='#E74C3C', decreasing_line_color='#2ECC71', name='K线'
        ), row=1, col=1
    )

    fig.add_trace(go.Scatter(x=dfp['date'], y=dfp['ma5'], mode='lines', line=dict(color='#F39C12', width=1.3), name='MA5'), row=1, col=1)
    fig.add_trace(go.Scatter(x=dfp['date'], y=dfp['ma10'], mode='lines', line=dict(color='#3498DB', width=1.3), name='MA10'), row=1, col=1)
    fig.add_trace(go.Scatter(x=dfp['date'], y=dfp['ma20'], mode='lines', line=dict(color='#8E44AD', width=1.3), name='MA20'), row=1, col=1)

    sig = dfp[dfp['buy_signal'] == True]
    if not sig.empty:
        fig.add_trace(go.Scatter(x=sig['date'], y=sig['close'], mode='markers', marker=dict(color='gold', size=8, symbol='star'), name='买点信号'), row=1, col=1)

    up = dfp['close'] >= dfp['open']
    vol_colors = ['#E74C3C' if u else '#2ECC71' for u in up]
    fig.add_trace(go.Bar(x=dfp['date'], y=dfp['volume'], marker_color=vol_colors, name='Volume'), row=2, col=1)

    fig.update_layout(
        height=680,
        xaxis_rangeslider_visible=False,
        template='plotly_dark',
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(orientation='h', yanchor='bottom', y=1.01, xanchor='left', x=0),
    )
    fig.update_xaxes(showspikes=True, spikemode='across', spikesnap='cursor', showline=True)
    fig.update_yaxes(showspikes=True, spikemode='across', spikesnap='cursor')
    return fig


@dataclass
class BuyCalc:
    shares: int
    turnover: float
    fee_cost: float
    slip_cost: float
    total_cost: float
    cash_left: float
    stock_weight_after: float
    total_position_after: float


def calc_buy(total_asset, cash_available, planned_buy, buy_price, fee_pct, slippage_pct, current_position_mv):
    fee_rate = fee_pct / 100.0
    slip_rate = slippage_pct / 100.0
    gross_per_share = buy_price * (1 + fee_rate + slip_rate)
    shares = int(planned_buy // gross_per_share)
    shares = max(0, shares // 100 * 100)
    turnover = shares * buy_price
    fee_cost = turnover * fee_rate
    slip_cost = turnover * slip_rate
    total_cost = turnover + fee_cost + slip_cost
    cash_left = cash_available - total_cost
    new_position_mv = current_position_mv + turnover
    stock_weight_after = turnover / total_asset if total_asset > 0 else 0
    total_position_after = new_position_mv / total_asset if total_asset > 0 else 0
    return BuyCalc(shares, turnover, fee_cost, slip_cost, total_cost, cash_left, stock_weight_after, total_position_after)


def scenario_pnl(shares, buy_price, fee_pct, slippage_pct, move_pct):
    fee_rate = fee_pct / 100.0
    slip_rate = slippage_pct / 100.0
    buy_cost = shares * buy_price * (1 + fee_rate + slip_rate)
    sell_price = buy_price * (1 + move_pct)
    sell_net = shares * sell_price * (1 - fee_rate - slip_rate - 0.0005)
    return sell_net - buy_cost




def _load_trade_log() -> pd.DataFrame:
    if TRADE_LOG.exists():
        return pd.read_csv(TRADE_LOG)
    return pd.DataFrame(columns=['ts','action','ts_code','price','shares','turnover','fee','slippage','tax','net_cash_change','note'])


def _save_trade_log(df: pd.DataFrame) -> None:
    REPORTS.mkdir(parents=True, exist_ok=True)
    df.to_csv(TRADE_LOG, index=False)


def _load_holdings() -> pd.DataFrame:
    if HOLDINGS_BOOK.exists():
        return pd.read_csv(HOLDINGS_BOOK)
    return pd.DataFrame(columns=['ts_code','shares','avg_cost','updated_at'])


def _save_holdings(df: pd.DataFrame) -> None:
    REPORTS.mkdir(parents=True, exist_ok=True)
    df.to_csv(HOLDINGS_BOOK, index=False)


def execute_buy(ts_code: str, price: float, shares: int, fee_pct: float, slip_pct: float, note: str = '') -> None:
    if shares <= 0:
        return
    fee_rate = fee_pct / 100.0
    slip_rate = slip_pct / 100.0
    turnover = shares * price
    fee = turnover * fee_rate
    slippage = turnover * slip_rate
    cash_change = -(turnover + fee + slippage)
    h = _load_holdings()
    if not h.empty and (h['ts_code'] == ts_code).any():
        i = h.index[h['ts_code'] == ts_code][0]
        old_shares = int(h.loc[i, 'shares'])
        old_cost = float(h.loc[i, 'avg_cost'])
        new_shares = old_shares + shares
        new_cost = ((old_shares * old_cost) + turnover + fee + slippage) / max(new_shares, 1)
        h.loc[i, 'shares'] = new_shares
        h.loc[i, 'avg_cost'] = new_cost
        h.loc[i, 'updated_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    else:
        h = pd.concat([h, pd.DataFrame([{'ts_code': ts_code,'shares': shares,'avg_cost': (turnover + fee + slippage) / max(shares, 1),'updated_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}])], ignore_index=True)
    _save_holdings(h)
    log = _load_trade_log()
    log = pd.concat([log, pd.DataFrame([{'ts': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),'action': 'BUY','ts_code': ts_code,'price': price,'shares': shares,'turnover': turnover,'fee': fee,'slippage': slippage,'tax': 0.0,'net_cash_change': cash_change,'note': note}])], ignore_index=True)
    _save_trade_log(log)


def execute_sell(ts_code: str, price: float, shares: int, fee_pct: float, slip_pct: float, note: str = '') -> tuple[bool, str]:
    if shares <= 0:
        return False, '卖出股数必须大于0'
    h = _load_holdings()
    if h.empty or not (h['ts_code'] == ts_code).any():
        return False, f'{ts_code} 当前无持仓'
    i = h.index[h['ts_code'] == ts_code][0]
    hold_shares = int(h.loc[i, 'shares'])
    if shares > hold_shares:
        return False, f'卖出股数超过持仓（持仓 {hold_shares}）'
    fee_rate = fee_pct / 100.0
    slip_rate = slip_pct / 100.0
    tax_rate = 0.0005
    turnover = shares * price
    fee = turnover * fee_rate
    slippage = turnover * slip_rate
    tax = turnover * tax_rate
    cash_change = turnover - fee - slippage - tax
    h.loc[i, 'shares'] = hold_shares - shares
    h.loc[i, 'updated_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    h = h[h['shares'] > 0].reset_index(drop=True)
    _save_holdings(h)
    log = _load_trade_log()
    log = pd.concat([log, pd.DataFrame([{'ts': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),'action': 'SELL','ts_code': ts_code,'price': price,'shares': shares,'turnover': turnover,'fee': fee,'slippage': slippage,'tax': tax,'net_cash_change': cash_change,'note': note}])], ignore_index=True)
    _save_trade_log(log)
    return True, '卖出记录已保存'


def advice(up_score, risk_level, stock_weight, total_pos):
    risk_map = {'低': 0.2, '中': 0.5, '中高': 0.7, '高': 0.9}
    rv = risk_map.get(risk_level, 0.6)
    score = up_score / 100
    if score >= 0.85 and rv <= 0.5 and stock_weight <= 0.2:
        decision, pos = '建议介入', '标准仓'
    elif score >= 0.75 and rv <= 0.7 and stock_weight <= 0.15:
        decision, pos = '可谨慎介入', '轻仓'
    else:
        decision, pos = '不建议介入', '不建议'
    sl = 0.05 if rv <= 0.5 else 0.04
    tp = 0.10 if score >= 0.85 else 0.08
    warn = []
    if rv >= 0.8: warn.append('风险等级偏高，波动可能较大')
    if total_pos >= 0.85: warn.append('总仓位偏高，新增仓位需谨慎')
    if stock_weight >= 0.2: warn.append('单票仓位偏高，注意集中度风险')
    if not warn: warn.append('风险可控，仍需严格执行止损')
    return {'是否建议介入': decision, '建议仓位': pos, '风险提示': '；'.join(warn), '建议止损位(%)': -sl*100, '建议止盈位(%)': tp*100, '决策摘要': f'信号评分{up_score:.0f}分、风险等级{risk_level}，建议{pos}。建议止损{sl*100:.1f}%，止盈{tp*100:.1f}%。'}


st.title('股票决策辅助页面')
st.caption('仅使用真实数据：候选=stage12_dual_top50.csv，行情=全池真实数据')

cand = load_candidates()

# 第一行：候选与选择
st.subheader('一、候选股票列表')
selected_code = st.selectbox('选择股票（按评分排序）', cand['股票代码'].tolist(), index=0, key='sel_code')
display = cand.copy()
display['选中'] = display['股票代码'].apply(lambda x: '✅' if x == selected_code else '')
st.dataframe(display[['选中', '股票代码', '股票名称', '当前价', '当日涨跌幅(%)', '策略评分', '入选原因', '风险等级']], width='stretch', height=280)
if (cand['股票代码'] == selected_code).any():
    row = cand[cand['股票代码'] == selected_code].iloc[0]
    st.success(f"当前选中：{row['股票代码']} | 评分 {row['策略评分']} | 风险 {row['风险等级']}")
else:
    row = pd.Series({'股票代码': selected_code, '股票名称': 'N/A', '当前价': 0.0, '策略评分': 0.0, '风险等级': '中'})
    st.warning(f"{selected_code} 不在候选池中，已按手动代码查询走势。")

st.divider()

# 第二行：走势图独占整行（你要求）
st.subheader('二、专业走势图（单独一行）')
cspan, ck, ccode = st.columns([2, 1, 1.2])
span = cspan.radio('区间', ['近1月', '近3月', '近6月'], horizontal=True, key='span')
ktype = ck.selectbox('K线周期', ['日线', '周线', '月线'], index=0, key='ktype')
manual_code = ccode.text_input('股票代码(6位)', value=selected_code, max_chars=6, key='manual_code').strip()
if manual_code:
    selected_code = manual_code.zfill(6)

m_map = {'近1月': 1, '近3月': 3, '近6月': 6}
dfp = load_price_series_real(selected_code, m_map[span])
dfp = resample_ohlcv(dfp, ktype)

if dfp.empty:
    st.error('该股票无可用真实历史数据，请切换股票或先刷新数据。')
else:
    st.plotly_chart(make_professional_chart(dfp, selected_code), width='stretch')
    st.caption(f"已加载 {selected_code} | {ktype} | 样本点数: {len(dfp)}")

    latest = dfp.iloc[-1]
    prev_close = float(dfp['close'].iloc[-2]) if len(dfp) >= 2 else float(latest['close'])
    chg = (float(latest['close']) / prev_close - 1.0) * 100 if prev_close else 0.0

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric('最新收盘', f"{float(latest['close']):.2f}", f"{chg:+.2f}%")
    m2.metric('开盘', f"{float(latest['open']):.2f}")
    m3.metric('最高/最低', f"{float(latest['high']):.2f} / {float(latest['low']):.2f}")
    m4.metric('成交量', f"{int(latest['volume']):,}")
    m5.metric('MA5/10/20', f"{float(latest['ma5']) if pd.notna(latest['ma5']) else 0:.2f} / {float(latest['ma10']) if pd.notna(latest['ma10']) else 0:.2f} / {float(latest['ma20']) if pd.notna(latest['ma20']) else 0:.2f}")

    st.line_chart(dfp.set_index('date')[['close','ma5','ma10','ma20']], width='stretch', height=220)
    st.dataframe(dfp[['date','open','high','low','close','volume','ma5','ma10','ma20']].tail(30), width='stretch', height=260)

    trend_state = '多头' if (pd.notna(latest['ma5']) and pd.notna(latest['ma10']) and pd.notna(latest['ma20']) and latest['ma5'] > latest['ma10'] > latest['ma20']) else '震荡/空头'
    pos_ma20 = (float(latest['close']) / float(latest['ma20']) - 1) * 100 if pd.notna(latest['ma20']) and latest['ma20'] else 0.0
    rng = (float(latest['high']) / float(latest['low']) - 1) * 100 if latest['low'] else 0.0

    dx1, dx2, dx3 = st.columns(3)
    dx1.metric('趋势状态', trend_state)
    dx2.metric('相对MA20偏离', f"{pos_ma20:+.2f}%")
    dx3.metric('当日振幅', f"{rng:.2f}%")
    st.caption('诊断规则：MA5>MA10>MA20 记为多头；其余记为震荡/空头。')

st.divider()
st.subheader('三、买入试算')
c1, c2, c3 = st.columns(3)
with c1:
    total_asset = st.number_input('总资金', min_value=0.0, value=500000.0, step=10000.0)
    cash_available = st.number_input('当前可用资金', min_value=0.0, value=200000.0, step=10000.0)
with c2:
    planned_buy = st.number_input('计划买入金额', min_value=0.0, value=50000.0, step=1000.0)
    plan_price = st.number_input('计划买入价格', min_value=0.01, value=float(row['当前价']), step=0.01)
with c3:
    fee_pct = st.number_input('手续费(%)', min_value=0.0, value=0.01, step=0.005, format='%.3f')
    slip_pct = st.number_input('滑点(%)', min_value=0.0, value=0.05, step=0.01, format='%.3f')
current_pos_mv = st.number_input('当前持仓市值', min_value=0.0, value=250000.0, step=10000.0)

bc = calc_buy(total_asset, cash_available, planned_buy, plan_price, fee_pct, slip_pct, current_pos_mv)
r1, r2, r3 = st.columns(3)
r1.metric('预计可买股数', f'{bc.shares:,}')
r1.metric('预计成交金额', f'¥{bc.turnover:,.2f}')
r2.metric('手续费成本', f'¥{bc.fee_cost:,.2f}')
r2.metric('滑点成本', f'¥{bc.slip_cost:,.2f}')
r3.metric('买入后剩余现金', f'¥{bc.cash_left:,.2f}')
r3.metric('该股票仓位占比', f'{bc.stock_weight_after*100:.2f}%')
st.caption(f'买入后总仓位占比：{bc.total_position_after*100:.2f}%')

st.subheader('三-2、交易执行（买入/卖出）与记录')
holdings_df = _load_holdings()
held_shares = int(holdings_df.loc[holdings_df['ts_code'] == selected_code, 'shares'].sum()) if not holdings_df.empty else 0

cx1, cx2, cx3 = st.columns(3)
with cx1:
    st.metric('当前代码持仓股数', f'{held_shares:,}')
with cx2:
    buy_shares = st.number_input('确认买入股数(100整数倍)', min_value=0, value=int(max(bc.shares, 0)), step=100)
with cx3:
    sell_shares = st.number_input('确认卖出股数(100整数倍)', min_value=0, value=0, step=100)

cnote = st.text_input('交易备注', value='')
b1, b2 = st.columns(2)
if b1.button('✅ 买入并写入记录', use_container_width=True):
    if buy_shares <= 0 or buy_shares % 100 != 0:
        st.error('买入股数必须为大于0的100整数倍')
    else:
        execute_buy(selected_code, float(plan_price), int(buy_shares), float(fee_pct), float(slip_pct), cnote)
        st.success(f'已记录买入：{selected_code} {buy_shares}股 @ {plan_price}')

if b2.button('🟠 卖出并写入记录', use_container_width=True):
    if sell_shares <= 0 or sell_shares % 100 != 0:
        st.error('卖出股数必须为大于0的100整数倍')
    else:
        ok, msg = execute_sell(selected_code, float(plan_price), int(sell_shares), float(fee_pct), float(slip_pct), cnote)
        if ok:
            st.success(f'已记录卖出：{selected_code} {sell_shares}股 @ {plan_price}')
        else:
            st.error(msg)

holdings_show = _load_holdings()
st.caption('持仓簿（本地持久化）')
st.dataframe(holdings_show.sort_values(['ts_code']) if not holdings_show.empty else holdings_show, width='stretch', height=180)

log_show = _load_trade_log()
st.caption('交易记录（最近50条）')
st.dataframe(log_show.tail(50).iloc[::-1] if not log_show.empty else log_show, width='stretch', height=220)

st.divider()
st.subheader('四、情景测算')
if bc.shares <= 0:
    st.warning('按当前输入无法买入至少1手（100股）。')
else:
    scenarios = {'上涨3%': 0.03, '上涨5%': 0.05, '下跌3%': -0.03, '下跌5%': -0.05}
    rows = []
    for name, mv in scenarios.items():
        rows.append((name, scenario_pnl(bc.shares, plan_price, fee_pct, slip_pct, mv)))
    sl, tp = -0.05, 0.10
    rows.append((f'跌到止损位({sl*100:.1f}%)', scenario_pnl(bc.shares, plan_price, fee_pct, slip_pct, sl)))
    rows.append((f'涨到目标位({tp*100:.1f}%)', scenario_pnl(bc.shares, plan_price, fee_pct, slip_pct, tp)))
    st.dataframe(pd.DataFrame(rows, columns=['情景', '预计盈亏(元)']), width='stretch', height=260)

    st.divider()
    st.subheader('五、建议区')
    adv = advice(float(row['策略评分']), str(row['风险等级']), bc.stock_weight_after, bc.total_position_after)
    a1, a2 = st.columns(2)
    with a1:
        st.metric('是否建议介入', adv['是否建议介入'])
        st.metric('建议仓位', adv['建议仓位'])
        st.metric('建议止损位', f"{adv['建议止损位(%)']:.2f}%")
        st.metric('建议止盈位', f"{adv['建议止盈位(%)']:.2f}%")
    with a2:
        st.warning(adv['风险提示'])
        st.info(adv['决策摘要'])

st.caption('说明：图表与候选均为真实数据来源；若切换无变化，请确认候选列表本身是否更新。')
