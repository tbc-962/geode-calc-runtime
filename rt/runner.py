# rt/runner.py
import os
from datetime import datetime
from typing import List, Dict
from ingest import fetch_weekly_lgas
from gate import normalize_type, within_price, land_area_sqm
from match import label_category, dedupe_by_sale
from agg import compute_medians
from emit import write_outputs

TARGET_LGAS = [
    "Ryde", "Canada Bay", "Strathfield", "Inner West", "Hunters Hill",
    "Lane Cove", "Ku-ring-gai", "Willoughby", "Parramatta"
]

def parse_vg_rows(vg_rows: List[Dict]) -> List[Dict]:
    out = []
    for r in vg_rows:
        addr = " ".join([r.get("ADDRESS",""), r.get("LOCALITY","")]).strip() or r.get("PROPERTY_ADDRESS") or ""
        suburb = r.get("LOCALITY") or r.get("SUBURB") or ""
        price_str = r.get("PURCHASE_PRICE") or r.get("CONTRACT_PRICE")
        date_str = r.get("SALE_DATE") or r.get("CONTRACT_DATE")
        prop_type = normalize_type(r)
        land = land_area_sqm(r)
        sold_price = None
        if price_str:
            try: sold_price = int("".join(ch for ch in price_str if ch.isdigit()))
            except: pass
        out.append({
            "_address_norm": addr.upper(),
            "_address_display": addr.title(),
            "_suburb": suburb.title(),
            "_type": prop_type,
            "_land_sqm": land,
            "_sold_price": sold_price,
            "_sold_date": date_str,   # parsed later in medians
            "_source": "VG",
            "_flags": []
        })
    return out

def apply_hard_filters(rows: List[Dict]) -> List[Dict]:
    return [r for r in rows if within_price({"PURCHASE_PRICE": str(r["_sold_price"] or "")}) and r["_type"] in {"house","townhouse","duplex","villa"}]

def label_categories(rows: List[Dict]) -> None:
    for r in rows:
        # Note: without enrichment for beds/baths/cars, many rows will not meet category bands and will be dropped.
        cat = label_category(r["_type"], r.get("beds"), r.get("baths"), r.get("cars"))
        r["_category"] = cat

def main():
    asof = datetime.now()
    # 1) Fetch weekly PSI for target LGAs
    vg_by_lga = fetch_weekly_lgas(TARGET_LGAS)
    vg_rows = []
    for _, rows in vg_by_lga.items():
        vg_rows.extend(parse_vg_rows(rows))

    # 2) Price/type filters
    filtered = apply_hard_filters(vg_rows)

    # 3) (Optional) Enrichment could fill beds/baths/cars here

    # 4) Categorise into 8 buckets
    label_categories(filtered)
    filtered = [r for r in filtered if r["_category"] != "other"]

    # 5) Dedupe
    deduped = dedupe_by_sale(filtered)

    # 6) Medians
    med = compute_medians(deduped, asof)

    # 7) Trends (placeholder; add later)
    trends = { r["_suburb"]: {"1y": (None,None), "5y": (None,None), "10y": (None,None)} for r in deduped }

    # 8) Emit artifacts
    write_outputs(deduped, med, trends, outdir="artifacts")

if __name__ == "__main__":
    main()
