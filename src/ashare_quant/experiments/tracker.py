from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path

def log_experiment(name: str, params: dict, metrics: dict, out_dir: str = "experiments") -> Path:
    p = Path(out_dir)
    p.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fp = p / f"{ts}_{name}.json"
    fp.write_text(
        json.dumps({"name": name, "params": params, "metrics": metrics}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return fp
