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
            "_sold_date": date_str,
            "_source": "VG",
            "_flags": []
        })
    return out

def apply_hard_filters(rows: List[Dict]) -> List[Dict]:
    return [r for r in rows if within_price({"PURCHASE_PRICE": str(r["_sold_price"] or "")}) and r["_type"] in {"house","townhouse","duplex","villa"}]

def label_categories(rows: List[Dict]) -> None:
    for r in rows:
        r["_category"] = label_category(r["_type"], r.get("beds"), r.get("baths"), r.get("cars"))

def main():
    asof = datetime.now()
    print("[1] Fetching weekly PSI for LGAs:", TARGET_LGAS)
    vg_by_lga = fetch_weekly_lgas(TARGET_LGAS)
    total_raw = sum(len(v) for v in vg_by_lga.values())
    print(f"[1] Raw rows from VG (all LGAs): {total_raw}")

    vg_rows = []
    for lga, rows in vg_by_lga.items():
        parsed = parse_vg_rows(rows)
        vg_rows.extend(parsed)
        print(f"    - {lga}: {len(rows)} rows, parsed {len(parsed)}")

    print(f"[2] Parsed total rows: {len(vg_rows)}")
    filtered = apply_hard_filters(vg_rows)
    print(f"[3] After price/type filters (A$1–2m, house/townhouse/duplex/villa): {len(filtered)}")

    # No enrichment yet; most rows won't meet 3x2x1 or 4+2+1+, but we still proceed
    label_categories(filtered)
    categories = {}
    for r in filtered:
        categories[r["_category"]] = categories.get(r["_category"], 0) + 1
    print("[4] Category counts:", categories)
    filtered = [r for r in filtered if r["_category"] != "other"]
    print(f"[4] Rows that fit your 8 categories (without enrichment): {len(filtered)}")

    deduped = dedupe_by_sale(filtered)
    print(f"[5] After de-duplication: {len(deduped)}")

    med = compute_medians(deduped, asof)
    print(f"[6] Median buckets computed: {len(med)}")

    # Trends placeholders (none yet)
    trends = { r["_suburb"]: {"1y": (None,None), "5y": (None,None), "10y": (None,None)} for r in deduped }

    os.makedirs("artifacts", exist_ok=True)
    write_outputs(deduped, med, trends, outdir="artifacts")
    print("[7] Wrote outputs to artifacts/. Expect idx.json + up to 8 CSVs if any rows matched.")

if __name__ == "__main__":
    main()
