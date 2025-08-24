# rt/ingest.py
import csv, io, re, zipfile
from typing import List, Dict, Iterable, Tuple
import requests
from bs4 import BeautifulSoup

VG_EMBED = "https://valuation.property.nsw.gov.au/embed/propertySalesInformation"

def _latest_week_url(session: requests.Session) -> str:
    r = session.get(VG_EMBED, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    weekly = soup.find("a", string=re.compile(r"\d{2}\s\w{3}\s\d{4}"))
    if not weekly or not weekly.get("href"):
        raise RuntimeError("Could not find weekly VG link")
    return weekly["href"]

def _list_lga_zip_urls(session: requests.Session, weekly_url: str) -> List[Tuple[str, str]]:
    r = session.get(weekly_url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        txt = (a.get_text() or "").strip()
        if href.lower().endswith(".zip") or href.lower().endswith(".dat"):
            urls.append((txt, href if href.startswith("http") else requests.compat.urljoin(weekly_url, href)))
    if not urls:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".zip" in href and "PSI" in href.upper():
                urls.append(("UNKNOWN", href if href.startswith("http") else requests.compat.urljoin(weekly_url, href)))
    return urls

def _read_dat_from_zip(content: bytes) -> Iterable[Dict[str, str]]:
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        dat_names = [n for n in zf.namelist() if n.lower().endswith(".dat")]
        if not dat_names:
            return []
        with zf.open(dat_names[0]) as fh:
            raw = io.TextIOWrapper(fh, encoding="utf-8", errors="ignore")
            reader = csv.reader(raw, delimiter="|")
            rows = []
            header = None
            for i, row in enumerate(reader):
                if i == 0 and all(len(c) > 0 for c in row):
                    header = [c.strip() for c in row]
                    continue
                if header and len(row) == len(header):
                    rows.append({header[j]: row[j].strip() for j in range(len(header))})
            return rows

def fetch_weekly_lgas(target_lgas: List[str]) -> Dict[str, List[Dict[str, str]]]:
    session = requests.Session()
    weekly_url = _latest_week_url(session)
    lga_urls = _list_lga_zip_urls(session, weekly_url)
    out: Dict[str, List[Dict[str, str]]] = {lga: [] for lga in target_lgas}
    tgt_lower = {l.lower(): l for l in target_lgas}
    for link_text, href in lga_urls:
        key = (link_text or "").lower()
        match = next((tgt_lower[t] for t in tgt_lower if t in key), None)
        if not match:
            continue
        resp = session.get(href, timeout=60)
        resp.raise_for_status()
        if href.lower().endswith(".zip"):
            rows = list(_read_dat_from_zip(resp.content))
        else:
            rows = list(csv.DictReader(io.StringIO(resp.text), delimiter="|"))
        out[match].extend(rows)
    return out
