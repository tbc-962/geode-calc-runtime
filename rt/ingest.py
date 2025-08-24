# rt/ingest.py
import csv, io, re, zipfile
from typing import List, Dict, Iterable, Tuple, Set
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

VG_EMBED = "https://valuation.property.nsw.gov.au/embed/propertySalesInformation"

# --- Helpers -----------------------------------------------------------------

def _ua_session() -> requests.Session:
    s = requests.Session()
    # Friendly UA – some sites return empty pages to unknown clients
    s.headers.update({"User-Agent": "Mozilla/5.0 (compatible; DataRoutine/1.0; +https://example.org/)"})
    return s

def _get_links(session: requests.Session, url: str) -> List[Tuple[str, str]]:
    """Return all (text, absolute_href) links on a page."""
    r = session.get(url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    out = []
    for a in soup.find_all("a", href=True):
        txt = (a.get_text() or "").strip()
        href = a["href"]
        if not href.startswith("http"):
            href = urljoin(url, href)
        out.append((txt, href))
    return out

def _read_dat_from_zip(content: bytes) -> Iterable[Dict[str, str]]:
    """Extract the first .DAT from ZIP (PSI is pipe-delimited)."""
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

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

# --- LGA matching -------------------------------------------------------------

# Synonyms to catch common variations in link text / filenames
LGA_SYNONYMS = {
    "Ryde": ["ryde", "cityofryde"],
    "Canada Bay": ["canadabay", "cityofcanadabay"],
    "Strathfield": ["strathfield", "municipalityofstrathfield"],
    "Inner West": ["innerwest", "innerwestcouncil"],
    "Hunters Hill": ["huntershill", "municipalityofhuntershill", "hunters_hill"],
    "Lane Cove": ["lanecove", "municipalityoflanecove"],
    "Ku-ring-gai": ["kuringgai", "ku-ring-gai", "ku ring gai", "kuring-gai"],
    "Willoughby": ["willoughby", "willoughbycity"],
    "Parramatta": ["parramatta", "cityofparramatta"],
}

def _match_lga(link_text: str, href: str, target_lgas: List[str]) -> str|None:
    t = _norm(link_text) + " " + _norm(href)
    for lga in target_lgas:
        for syn in LGA_SYNONYMS.get(lga, []):
            if syn in t:
                return lga
    return None

# --- Main fetch ---------------------------------------------------------------

def fetch_weekly_lgas(target_lgas: List[str]) -> Dict[str, List[Dict[str, str]]]:
    """
    Try 2 pages:
      1) The EMBED page (sometimes already lists weekly files)
      2) If we find a likely "weekly" page link, open it and collect again.
    Match links by href/text using LGA synonyms; download matched ZIP/DAT.
    """
    s = _ua_session()

    # 1) Collect links on the embed page
    links1 = _get_links(s, VG_EMBED)

    # Try to find a "weekly" link (often shows a date)
    weekly_candidates = [(txt, href) for (txt, href) in links1
                         if re.search(r"\b(20\d{2}|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b", txt, re.I)]

    # 2) If we found a likely weekly page, open it and gather links
    links2 = []
    if weekly_candidates:
        # Take the first candidate – usually the most recent
        weekly_url = weekly_candidates[0][1]
        try:
            links2 = _get_links(s, weekly_url)
        except Exception:
            links2 = []

    # Combine links from both pages
    all_links = links1 + links2

    # Keep only potential PSI downloads
    dl_links = []
    for (txt, href) in all_links:
        if href.lower().endswith(".zip") or href.lower().endswith(".dat"):
            dl_links.append((txt, href))

    # DEBUG: print counts (visible in your Actions logs)
    print(f"[ingest] Found {len(all_links)} total links; {len(dl_links)} look like downloads")

    out: Dict[str, List[Dict[str, str]]] = {lga: [] for lga in target_lgas}

    # Match each download link against LGA synonyms, and download those that match
    matched_any: Set[str] = set()
    for (txt, href) in dl_links:
        lga = _match_lga(txt, href, target_lgas)
        if not lga:
            continue
        try:
            resp = s.get(href, timeout=60)
            resp.raise_for_status()
        except Exception as e:
            print(f"[ingest] Skip {href} ({e})")
            continue

        if href.lower().endswith(".zip"):
            rows = list(_read_dat_from_zip(resp.content))
        else:
            # Rare case: raw .dat
            rows = list(csv.DictReader(io.StringIO(resp.text), delimiter="|"))

        print(f"[ingest] {lga}: +{len(rows)} rows from {href.split('/')[-1]}")
        out[lga].extend(rows)
        matched_any.add(lga)

    if not matched_any:
        print("[ingest] WARNING: No LGA files matched by name. The page may have changed or uses different labels this week.")
        print("[ingest] TIP: we can broaden matching or download all and filter by suburb, but that increases bandwidth/time.")

    return out
