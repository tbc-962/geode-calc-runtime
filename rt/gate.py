# rt/gate.py
from typing import Dict
import re

PRICE_MIN, PRICE_MAX = 1_000_000, 2_000_000

TYPE_MAP = {
    "HOUSE": "house", "RESIDENTIAL": "house", "DWELLING": "house",
    "TOWNHOUSE": "townhouse",
    "DUPLEX": "duplex", "SEMI": "duplex", "SEMI-DETACHED": "duplex",
    "VILLA": "villa",
}

def normalize_type(vg_row: Dict[str, str]) -> str:
    raw = " ".join([vg_row.get("PROPERTY_DESCRIPTION",""), vg_row.get("ZONE",""), vg_row.get("PURPOSE","")]).upper()
    for k, v in TYPE_MAP.items():
        if k in raw:
            return v
    return "unknown"

def within_price(vg_row: Dict[str, str]) -> bool:
    try:
        p = int(re.sub(r"[^\d]", "", vg_row.get("PURCHASE_PRICE","")))
        return PRICE_MIN <= p <= PRICE_MAX
    except Exception:
        return False

def land_area_sqm(vg_row: Dict[str, str]):
    v = vg_row.get("AREA") or vg_row.get("AREA_M2") or vg_row.get("LOT_AREA")
    if not v: return None
    try: return float(str(v).replace(",",""))
    except: return None
