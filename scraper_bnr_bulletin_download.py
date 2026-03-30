"""
BNR Monthly Bulletin — PDF Discovery and Download
==================================================
Purpose: Collect all BNR Monthly Bulletin PDF URLs from 2019 onwards,
         then download them to the archive directory.

How it works:
  - Page 1 uses POST /blocks → returns raw HTML
  - Page 2+ uses POST /getpaginare → returns JSON with HTML in data.content
  - Each page contains 10 bulletins, newest first
  - We page through until we pass 2019, then stop

Three modes:
  --discover    List all PDF URLs from 2019+ (no downloads)
  --download    Discover all + download (for initial backfill)
  --latest      Check page 1 only + download new (for daily automated runs)

Usage:
  python scraper_bnr_bulletin_download.py --discover
  python scraper_bnr_bulletin_download.py --download
  python scraper_bnr_bulletin_download.py --latest
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import re
import os
import sys

# --- Configuration ---
BLOCKS_URL = "https://www.bnr.ro//blocks"
PAGINARE_URL = "https://www.bnr.ro/getpaginare"
USER_AGENT = "RECONOMANIA data aggregator, contact@reconomania.com"
EARLIEST_YEAR = 2019
DELAY_BETWEEN_PAGES = 2       # seconds between listing page requests
DELAY_BETWEEN_DOWNLOADS = 5   # seconds between PDF downloads
ARCHIVE_DIR = "archive/bnr_monthly_bulletin"

HEADERS = {
    "User-Agent": USER_AGENT,
    "Content-Type": "application/x-www-form-urlencoded",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://www.bnr.ro/en/12072-monthly-bulletins",
}


def extract_bulletins_from_html(html_text):
    """Parse HTML (from either endpoint) and extract bulletin titles and PDF URLs."""
    soup = BeautifulSoup(html_text, "html.parser")
    bulletins = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if href.lower().endswith(".pdf"):
            # Find the associated title
            row = link.find_parent("div", class_="row")
            title_span = row.find("span", class_="bolded") if row else None
            title = title_span.get_text(strip=True) if title_span else "(unknown)"
            # Make URL absolute
            if href.startswith("/"):
                href = "https://www.bnr.ro" + href
            bulletins.append({"title": title, "url": href})
    return bulletins


def extract_year_month(title):
    """
    Extract year and month from a bulletin title.
    Examples:
      'Monthly Bulletin no. 3/2025' → (2025, 3)
      'Monthly Bulletin no. 12/2024' → (2024, 12)
    Returns (year, month) or (None, None) if not parseable.
    """
    match = re.search(r"(\d{1,2})/(\d{4})", title)
    if match:
        return int(match.group(2)), int(match.group(1))
    return None, None


def make_filename(title):
    """
    Convert a bulletin title to a clean filename.
    'Monthly Bulletin no. 3/2025' → 'bnr_monthly_bulletin_2025_03.pdf'
    """
    year, month = extract_year_month(title)
    if year and month:
        return f"bnr_monthly_bulletin_{year}_{month:02d}.pdf"
    # Fallback: slugify the title
    slug = re.sub(r"[^a-z0-9]+", "_", title.lower()).strip("_")
    return f"{slug}.pdf"


def fetch_page_1():
    """Fetch the first page of bulletins from the /blocks endpoint."""
    payload = {
        "bid": "13158",
        "currentSlug": "12072-monthly-bulletins",
        "cat_id": "",
    }
    response = requests.post(BLOCKS_URL, data=payload, headers=HEADERS, timeout=30)
    response.raise_for_status()
    
    # /blocks returns raw HTML
    bulletins = extract_bulletins_from_html(response.text)
    
    # Extract total pages from pagination controls
    soup = BeautifulSoup(response.text, "html.parser")
    pagination = soup.find("div", class_="pagination-controls")
    total_pages = int(pagination.get("data-total-pages", 0)) if pagination else None
    
    return bulletins, total_pages


def fetch_page_n(page_num):
    """Fetch page N (2+) of bulletins from the /getpaginare endpoint."""
    payload = {
        "bid": "13158",
        "pagina": str(page_num),
    }
    response = requests.post(PAGINARE_URL, data=payload, headers=HEADERS, timeout=30)
    response.raise_for_status()
    
    # /getpaginare returns JSON with HTML inside data.content
    data = json.loads(response.text)
    
    if data.get("error") != 0:
        print(f"  API error: {data.get('message', 'unknown')}")
        return []
    
    html_content = data.get("data", {}).get("content", "")
    bulletins = extract_bulletins_from_html(html_content)
    return bulletins


def discover_all_bulletins():
    """
    Page through the BNR monthly bulletin listing and collect all PDF URLs
    from EARLIEST_YEAR onwards.
    """
    all_bulletins = []
    
    # Page 1
    print("Fetching page 1...")
    bulletins, total_pages = fetch_page_1()
    print(f"  Found {len(bulletins)} bulletins. Total pages: {total_pages}")
    for b in bulletins:
        print(f"    {b['title']}")
    all_bulletins.extend(bulletins)
    
    if not total_pages:
        print("  WARNING: Could not determine total pages. Will try up to 35.")
        total_pages = 35
    
    # Pages 2+
    for page_num in range(2, total_pages + 1):
        time.sleep(DELAY_BETWEEN_PAGES)
        
        print(f"Fetching page {page_num}/{total_pages}...")
        bulletins = fetch_page_n(page_num)
        
        if not bulletins:
            print(f"  Empty page — stopping.")
            break
        
        # Check the oldest bulletin on this page
        oldest_year = None
        for b in bulletins:
            year, _ = extract_year_month(b["title"])
            if year:
                oldest_year = year
        
        print(f"  Found {len(bulletins)} bulletins (oldest year on page: {oldest_year})")
        for b in bulletins:
            print(f"    {b['title']}")
        
        all_bulletins.extend(bulletins)
        
        # Stop if we've gone past our cutoff
        if oldest_year and oldest_year < EARLIEST_YEAR:
            print(f"  Reached {oldest_year} — past our {EARLIEST_YEAR} cutoff. Stopping.")
            break
    
    # Filter to only bulletins from EARLIEST_YEAR onwards
    filtered = []
    for b in all_bulletins:
        year, month = extract_year_month(b["title"])
        if year and year >= EARLIEST_YEAR:
            b["year"] = year
            b["month"] = month
            b["filename"] = make_filename(b["title"])
            filtered.append(b)
    
    return filtered


def download_bulletins(bulletins):
    """Download all bulletin PDFs to the archive directory."""
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    
    downloaded = 0
    skipped = 0
    failed = 0
    
    for b in bulletins:
        filepath = os.path.join(ARCHIVE_DIR, b["filename"])
        
        # Skip if already downloaded
        if os.path.exists(filepath):
            print(f"  SKIP (exists): {b['filename']}")
            skipped += 1
            continue
        
        print(f"  Downloading: {b['filename']}...", end="", flush=True)
        
        try:
            # Use a clean headers dict for PDF download (no form content-type)
            dl_headers = {"User-Agent": USER_AGENT}
            response = requests.get(b["url"], headers=dl_headers, timeout=60)
            response.raise_for_status()
            
            with open(filepath, "wb") as f:
                f.write(response.content)
            
            size_mb = len(response.content) / (1024 * 1024)
            print(f" {size_mb:.1f} MB ✓")
            downloaded += 1
            
        except requests.RequestException as e:
            print(f" FAILED: {e}")
            failed += 1
        
        time.sleep(DELAY_BETWEEN_DOWNLOADS)
    
    return downloaded, skipped, failed


def discover_latest():
    """
    Check only page 1 of the bulletin listing for new bulletins.
    Used for daily automated checks — no need to page through the
    entire archive every time.
    """
    print("Fetching page 1 (latest check)...")
    bulletins, _ = fetch_page_1()
    print(f"  Found {len(bulletins)} bulletins on page 1")

    filtered = []
    for b in bulletins:
        year, month = extract_year_month(b["title"])
        if year and year >= EARLIEST_YEAR:
            b["year"] = year
            b["month"] = month
            b["filename"] = make_filename(b["title"])
            filtered.append(b)

    return filtered


# === Main ===
if __name__ == "__main__":
    # Parse command line
    mode = "discover"  # default
    if len(sys.argv) > 1:
        if sys.argv[1] == "--download":
            mode = "download"
        elif sys.argv[1] == "--discover":
            mode = "discover"
        elif sys.argv[1] == "--latest":
            mode = "latest"
        else:
            print(f"Unknown argument: {sys.argv[1]}")
            print("Usage: python scraper_bnr_bulletin_download.py [--discover|--download|--latest]")
            exit(1)
    
    print(f"Mode: {mode.upper()}")
    if mode != "latest":
        print(f"Scope: {EARLIEST_YEAR} onwards")
        print(f"Delay between pages: {DELAY_BETWEEN_PAGES}s")
    print(f"Delay between downloads: {DELAY_BETWEEN_DOWNLOADS}s")
    print()
    
    # Step 1: Discover bulletin URLs
    if mode == "latest":
        bulletins = discover_latest()
    else:
        bulletins = discover_all_bulletins()
    
    # Report
    print()
    print("=" * 70)
    print(f"DISCOVERY COMPLETE: {len(bulletins)} bulletins")
    print("=" * 70)
    for b in bulletins:
        print(f"  {b['title']:40s} → {b['filename']}")
    print()
    
    # Step 2: Download (if requested)
    if mode in ("download", "latest"):
        print("=" * 70)
        print(f"DOWNLOADING to {ARCHIVE_DIR}/")
        print("=" * 70)
        downloaded, skipped, failed = download_bulletins(bulletins)
        print()
        print(f"Done. Downloaded: {downloaded}, Skipped: {skipped}, Failed: {failed}")
    else:
        print(f"Run with --download to download all {len(bulletins)} PDFs.")
        print(f"  python scraper_bnr_bulletin_download.py --download")
