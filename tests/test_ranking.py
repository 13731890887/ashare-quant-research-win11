import pandas as pd
from ashare_quant.utils.ranking import cs_rank

def test_cs_rank_by_date():
    df = pd.DataFrame({
        'trade_date': ['2024-01-01','2024-01-01','2024-01-02','2024-01-02'],
        'x': [1, 2, 100, 200],
    })
    r = cs_rank(df, 'x')
    # each date should independently have 0.5 and 1.0 for two rows
    assert sorted(r[df.trade_date=='2024-01-01'].tolist()) == [0.5, 1.0]
    assert sorted(r[df.trade_date=='2024-01-02'].tolist()) == [0.5, 1.0]
