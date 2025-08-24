# rt/agg.py
from statistics import median
from datetime import datetime
from typing import List, Dict, Tuple

def _parse_price(v: str):
    if not v: return None
    digits = "".join(ch for ch in v if ch.isdigit())
    return int(digits) if digits else None

def _parse_date(v: str):
    for fmt in ("%d/%m/%Y","%Y-%m-%d","%d-%b-%Y"):
        try: return datetime.strptime(v.strip(), fmt)
        except: pass
    return None

def compute_medians(rows: List[Dict], asof: datetime) -> Dict[Tuple[str,str], Dict[str, float]]:
    idx: Dict[Tuple[str,str], Dict[str, List[int]]] = {}
    for r in rows:
        price = _parse_price(r.get("_sold_price"))
        d = _parse_date(r.get("_sold_date"))
        if price is None or d is None: continue
        key = (r["_suburb"], r["_category"])
        bucket = idx.setdefault(key, {"week": [], "month": [], "quarter": []})
        if (asof - d).days <= 7: bucket["week"].append(price)
        if (asof - d).days <= 30: bucket["month"].append(price)
        if (asof - d).days <= 90: bucket["quarter"].append(price)
    out = {}
    for key, buckets in idx.items():
        out[key] = {k: (float(median(v)) if v else None) for k, v in buckets.items()}
    return out
