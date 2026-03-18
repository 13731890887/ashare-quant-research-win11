from __future__ import annotations

from pathlib import Path
from datetime import datetime
import json

ROOT = Path(__file__).resolve().parents[3]
REPORTS = ROOT / 'reports'
AUDIT = REPORTS / 'audit_log.jsonl'


def append_audit(user: str, action: str, detail: dict | None = None) -> None:
    REPORTS.mkdir(parents=True, exist_ok=True)
    rec = {
        'ts': datetime.now().isoformat(timespec='seconds'),
        'user': user,
        'action': action,
        'detail': detail or {},
    }
    with AUDIT.open('a', encoding='utf-8') as f:
        f.write(json.dumps(rec, ensure_ascii=False) + '\n')


def read_audit(limit: int = 200):
    if not AUDIT.exists():
        return []
    lines = AUDIT.read_text(encoding='utf-8', errors='ignore').splitlines()[-limit:]
    out = []
    for ln in lines:
        try:
            out.append(json.loads(ln))
        except Exception:
            pass
    return out
