# rt/match.py
from typing import Dict, List

def label_category(prop_type: str, beds, baths, cars) -> str:
    def ok(x, t): return (x is not None) and (int(x) >= t)
    if prop_type == "house":
        if ok(beds,4) and ok(baths,2) and ok(cars,1): return "hx_b"  # 4+,2+,1+
        if ok(beds,3) and ok(baths,2) and ok(cars,1): return "hx_a"  # 3,2,1+
    if prop_type == "townhouse":
        if ok(beds,4) and ok(baths,2) and ok(cars,1): return "tx_b"
        if ok(beds,3) and ok(baths,2) and ok(cars,1): return "tx_a"
    if prop_type == "duplex":
        if ok(beds,4) and ok(baths,2) and ok(cars,1): return "dx_b"
        if ok(beds,3) and ok(baths,2) and ok(cars,1): return "dx_a"
    if prop_type == "villa":
        if ok(beds,4) and ok(baths,2) and ok(cars,1): return "vx_b"
        if ok(beds,3) and ok(baths,2) and ok(cars,1): return "vx_a"
    return "other"

def dedupe_by_sale(rows: List[Dict]) -> List[Dict]:
    seen = {}
    def key(r): return (r["_address_norm"], r.get("_sold_date"))
    for r in rows:
        k = key(r)
        cur = seen.get(k)
        score = int(bool(r.get("_enriched"))) - len(r.get("_flags",[]))
        if not cur or score > cur.get("_score", -999):
            r["_score"] = score
            seen[k] = r
    return list(seen.values())
