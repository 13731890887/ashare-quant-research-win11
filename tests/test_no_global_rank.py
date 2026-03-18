from pathlib import Path

FILES = [
    'src/ashare_quant/scoring/rules.py',
    'src/ashare_quant/pipeline/stage2_strategy_family_sweep.py',
    'src/ashare_quant/pipeline/stage2_harmonize_and_walkforward.py',
    'src/ashare_quant/pipeline/stage3_strategy_resweep.py',
    'src/ashare_quant/pipeline/stage5_parallel_strategy_pack.py',
]

def test_no_global_rank_pct_true():
    for fp in FILES:
        s = Path(fp).read_text(encoding='utf-8')
        assert '.rank(pct=True)' not in s
