# rt/emit.py
import csv, json, os
from collections import defaultdict
from typing import List, Dict

FILE_MAP = {
  "hx_a": "hx_a.csv",
  "hx_b": "hx_b.csv",
  "tx_a": "tx_a.csv",
  "tx_b": "tx_b.csv",
  "dx_a": "dx_a.csv",
  "dx_b": "dx_b.csv",
  "vx_a": "vx_a.csv",
  "vx_b": "vx_b.csv",
}

HEADERS = ["Suburb","Address","Sold Price","Median Sold Price - Week","Median Sold Price - Month","Median Sold Price - Quarter","SQM on title","1y Median ($, % growth)","5y Median ($, % growth)","10y Median ($, % growth)","Source","Flags"]

def write_outputs(rows: List[Dict], med: Dict, trends: Dict, outdir="artifacts"):
    os.makedirs(outdir, exist_ok=True)
    buckets = defaultdict(list)
    for r in rows:
        cat = r["_category"]
        if cat in FILE_MAP:
            buckets[cat].append(r)

    index = {"files": [], "summary": {}}
    for cat, items in buckets.items():
        filename = FILE_MAP[cat]
        path = os.path.join(outdir, filename)
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(HEADERS)
            for r in items:
                key = (r["_suburb"], r["_category"])
                m = med.get(key, {})
                t = trends.get(r["_suburb"], {})
                w.writerow([
                    r["_suburb"],
                    r.get("_address_display") or r.get("_address_norm"),
                    f"{r.get('_sold_price'):,.0f}" if r.get("_sold_price") else "",
                    f"{m.get('week',0):,.0f}" if m.get("week") else "",
                    f"{m.get('month',0):,.0f}" if m.get("month") else "",
                    f"{m.get('quarter',0):,.0f}" if m.get("quarter") else "",
                    r.get("_land_sqm") or "",
                    f"{(t.get('1y') or ('',''))[0] or ''}",
                    f"{(t.get('5y') or ('',''))[0] or ''}",
                    f"{(t.get('10y') or ('',''))[0] or ''}",
                    r.get("_source") or "VG",
                    ";".join(r.get("_flags",[])) if r.get("_flags") else ""
                ])
        index["files"].append({"category": cat, "path": f"artifacts/{filename}", "rows": len(items)})

    index["summary"] = {cat: len(v) for cat, v in buckets.items()}
    with open(os.path.join(outdir, "idx.json"), "w", encoding="utf-8") as jf:
        json.dump(index, jf, indent=2)
