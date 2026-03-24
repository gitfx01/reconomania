"""
============================================================================
RECONOMANIA — BNR Monthly Bulletin: Monetary Policy Operations Scraper
============================================================================

FILE PURPOSE:
    Extracts time series from Tables 3.1 and 3.2 of BNR Monthly Bulletin
    PDFs (monetary policy instruments) and loads them into the RECONOMANIA
    PostgreSQL database.

    Table 3.1: Open-market operations (repo, reverse repo, deposits taken)
    Table 3.2: Standing facilities (credit facility, deposit facility)

    From each instrument, we extract the "Stock → daily average (lei mn.)"
    column. Together these show the central bank's net position vs. the
    banking system:
      - Injection (BNR providing liquidity): repo stock, credit facility stock
      - Sterilisation (BNR absorbing liquidity): reverse repo stock,
        deposits taken stock, deposit facility stock

DATA SOURCE:
    BNR Monthly Bulletins (PDF), Tables 3.1 and 3.2
    Publication page: https://www.bnr.ro/en/12072-monthly-bulletins
    Frequency: Monthly

SERIES EXTRACTED:
    - bnr_repo_stock:           Repo operations, stock daily avg (lei mn.)
    - bnr_reverse_repo_stock:   Reverse repo, stock daily avg (lei mn.)
    - bnr_deposits_taken_stock: Deposits taken by BNR, stock daily avg (lei mn.)
    - bnr_credit_facility_stock: Credit (lending) facility, stock daily avg (lei mn.)
    - bnr_deposit_facility_stock: Deposit facility, stock daily avg (lei mn.)

TABLE LAYOUT:
    Both tables have 3 header rows + 1 data blob row:
      Row 0: instrument groups (Repo, Reverse repo, Deposits taken / Credit, Deposit)
      Row 1: Flow vs Stock
      Row 2: sub-columns (daily average, interest rate)
      Row 3: data blob (all months concatenated, one line per month)

    The layout is consistent between 2019 and 2026.

    Table 3.1 data positions (0-based, after stripping year/month tokens):
      0: Policy rate, 1-2: Repo Flow, 3-4: Repo Stock, 5-6: Rev.repo Flow,
      7-8: Rev.repo Stock, 9-10: Dep.taken Flow, 11-12: Dep.taken Stock

    Table 3.2 data positions:
      0-1: Credit Flow, 2-3: Credit Stock, 4-5: Deposit Flow, 6-7: Deposit Stock

SPECIAL VALUES:
    "–" (en-dash), "x", and "-" mean zero for these instruments — if BNR
    isn't running an operation, the stock is zero, not unknown.

MODES:
    --backfill    Process ALL PDFs in the archive (initial database load)
    --update      Process only the most recent PDF (monthly updates)

HOW TO RUN:
    python scraper_bnr_bulletin_monetary.py --backfill
    python scraper_bnr_bulletin_monetary.py --update

DATE:        March 2026
PHASE:       Phase Two
============================================================================
"""

# ============================================================================
# IMPORTS
# ============================================================================

import pdfplumber
import psycopg2
import os
import sys
import re
import glob
from datetime import datetime, timezone, date, timedelta
from decimal import Decimal


# ============================================================================
# CONFIGURATION
# ============================================================================

DB_CONFIG = {
    "dbname": "reconomania",
}

ARCHIVE_DIR = "archive/bnr_monthly_bulletin"

MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "may": 5, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

# Values that mean zero (not "no data") for monetary policy instruments.
# If BNR isn't running an operation, the stock is 0, not unknown.
ZERO_MARKERS = {"–", "-", "x", "…", "..."}


# ============================================================================
# SERIES CONFIGURATION
# ============================================================================
# Each entry defines one series. "table" identifies which PDF table it comes
# from, and "data_position" is the 0-based index in the data blob after
# stripping year and month tokens.
#
# Table 3.1 (14 cols): identified by "Repo" in header row[0]
#   Positions: 0=policy rate, 1-2=repo flow, 3-4=repo stock,
#              5-6=rev.repo flow, 7-8=rev.repo stock,
#              9-10=dep.taken flow, 11-12=dep.taken stock
#
# Table 3.2 (9 cols): identified by "Credit" in header row[0]
#   Positions: 0-1=credit flow, 2-3=credit stock,
#              4-5=deposit flow, 6-7=deposit stock

SERIES_CONFIG = [
    # --- Table 3.1: Open-market operations ---
    {
        "series_id": "bnr_repo_stock",
        "table_id": "3.1",
        "table_marker": "Repo",
        "data_position": 3,
        "name": "Repo operations — stock daily average",
        "description": (
            "Daily average stock of repo operations (liquidity injection) "
            "performed by the National Bank of Romania. Represents the average "
            "outstanding balance of repos during the month. "
            "Source: BNR Monthly Bulletin, Table 3.1."
        ),
        "units": "lei millions",
        "chart_colour": "#0F3B5C",
        "topic_path": "Monetary Policy/Open-Market Operations",
        "direction": "injection",
    },
    {
        "series_id": "bnr_reverse_repo_stock",
        "table_id": "3.1",
        "table_marker": "Repo",
        "data_position": 7,
        "name": "Reverse repo — stock daily average",
        "description": (
            "Daily average stock of reverse repo operations (liquidity "
            "sterilisation) performed by the National Bank of Romania. "
            "Source: BNR Monthly Bulletin, Table 3.1."
        ),
        "units": "lei millions",
        "chart_colour": "#2D8C5A",
        "topic_path": "Monetary Policy/Open-Market Operations",
        "direction": "sterilisation",
    },
    {
        "series_id": "bnr_deposits_taken_stock",
        "table_id": "3.1",
        "table_marker": "Repo",
        "data_position": 11,
        "name": "Deposits taken by BNR — stock daily average",
        "description": (
            "Daily average stock of deposits taken by the National Bank of "
            "Romania from credit institutions (liquidity sterilisation). "
            "Source: BNR Monthly Bulletin, Table 3.1."
        ),
        "units": "lei millions",
        "chart_colour": "#8FA03E",
        "topic_path": "Monetary Policy/Open-Market Operations",
        "direction": "sterilisation",
    },
    # --- Table 3.2: Standing facilities ---
    {
        "series_id": "bnr_credit_facility_stock",
        "table_id": "3.2",
        "table_marker": "Deposit",
        "data_position": 2,
        "name": "Credit (lending) facility — stock daily average",
        "description": (
            "Daily average stock of the credit (lending) facility used by "
            "credit institutions at the National Bank of Romania (liquidity "
            "injection). Source: BNR Monthly Bulletin, Table 3.2."
        ),
        "units": "lei millions",
        "chart_colour": "#5B9BD5",
        "topic_path": "Monetary Policy/Standing Facilities",
        "direction": "injection",
    },
    {
        "series_id": "bnr_deposit_facility_stock",
        "table_id": "3.2",
        "table_marker": "Deposit",
        "data_position": 6,
        "name": "Deposit facility — stock daily average",
        "description": (
            "Daily average stock of the deposit facility at the National "
            "Bank of Romania, used by credit institutions to park excess "
            "liquidity (sterilisation). Source: BNR Monthly Bulletin, Table 3.2."
        ),
        "units": "lei millions",
        "chart_colour": "#5C4D8A",
        "topic_path": "Monetary Policy/Standing Facilities",
        "direction": "sterilisation",
    },
]


# ============================================================================
# DATABASE HELPERS
# ============================================================================

def get_db_connection():
    """Opens a connection to the PostgreSQL database."""
    return psycopg2.connect(**DB_CONFIG)


def ensure_series_registered(connection, series_cfg):
    """Register a time series in the metadata table. Idempotent."""
    cursor = connection.cursor()
    cursor.execute(
        """
        INSERT INTO time_series (
            series_id, name, description, source_institution, source_url,
            frequency, temporal_type, units, expected_update_schedule,
            historical_start_date, chart_colour, topic_path
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s
        )
        ON CONFLICT (series_id) DO NOTHING
        """,
        (
            series_cfg["series_id"],
            series_cfg["name"],
            series_cfg["description"],
            "BNR",
            "https://www.bnr.ro/en/12072-monthly-bulletins",
            "monthly",
            "period_average",  # daily average over the month
            series_cfg["units"],
            "Monthly, with ~6-8 week publication lag",
            date(2018, 1, 1),
            series_cfg["chart_colour"],
            series_cfg["topic_path"],
        ),
    )
    connection.commit()
    cursor.close()


def store_data_points(connection, series_id, data_points, source_file):
    """Insert data points into the database. Returns (new_count, skipped_count)."""
    cursor = connection.cursor()
    new_count = 0
    skipped_count = 0

    for dp in data_points:
        year = dp["year"]
        month = dp["month"]

        # Last day of the month (end of the averaging period)
        if month == 12:
            obs_date = date(year, 12, 31)
        else:
            obs_date = date(year, month + 1, 1) - timedelta(days=1)

        try:
            cursor.execute(
                """
                INSERT INTO data_points (series_id, observation_date, value, source_file)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (series_id, observation_date, recorded_at) DO NOTHING
                """,
                (series_id, obs_date, Decimal(str(dp["value"])), source_file),
            )
            if cursor.rowcount == 1:
                new_count += 1
            else:
                skipped_count += 1
        except Exception as e:
            print(f"    [ERROR] Failed to insert {series_id} {obs_date}: {e}")
            connection.rollback()

    connection.commit()
    cursor.close()
    return new_count, skipped_count


def log_scrape(connection, series_id, status, records_fetched, records_new,
               records_updated, error_message, source_file_archived,
               started_at):
    """Log a scraper run to the scrape_log table."""
    cursor = connection.cursor()
    cursor.execute(
        """
        INSERT INTO scrape_log
            (series_id, run_started_at, run_finished_at, status,
             records_fetched, records_new, records_updated,
             error_message, source_file_archived)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            series_id,
            started_at,
            datetime.now(timezone.utc),
            status,
            records_fetched,
            records_new,
            records_updated,
            error_message,
            source_file_archived,
        ),
    )
    connection.commit()
    cursor.close()


# ============================================================================
# PDF EXTRACTION
# ============================================================================

def find_monetary_tables(pdf):
    """
    Find Tables 3.1 and 3.2 in the PDF.

    Both tables appear on the same page (the statistical section page that
    contains "Open-market operations" and "Standing facilities").

    Table identification:
      - Table 3.1: header row[0] contains "Repo" (14 columns)
      - Table 3.2: header row[0] contains "Credit" (9 columns)

    Each table has 3 header rows + 1 data blob row. The data blob is in
    the last row, cell [0].

    Returns a dict: {"3.1": data_blob_string, "3.2": data_blob_string}
    or partial results if only one table is found.
    """
    found = {}

    for page_idx, page in enumerate(pdf.pages):
        text = page.extract_text() or ""
        if "open-market operations" not in text.lower():
            continue

        tables = page.extract_tables()
        for table in tables:
            if len(table) < 2:
                continue

            header_row = table[0]
            header_labels = [c.strip() if c else "" for c in header_row]

            # Find the data blob: the first row where cell[0] starts with
            # a year (4 digits). This handles both 2-row and 4-row tables.
            data_blob = None
            for row_idx in range(1, len(table)):
                cell = table[row_idx][0]
                if cell and re.match(r"\d{4}", cell.strip()):
                    data_blob = cell
                    break

            if not data_blob:
                continue

            # Identify which table this is
            if any("Repo" in label for label in header_labels if label):
                found["3.1"] = data_blob
            elif any("Deposit" in label for label in header_labels if label):
                found["3.2"] = data_blob

        # If we found at least one table, don't keep searching other pages
        if found:
            break

    return found


def parse_value(token):
    """
    Parse a single value token from the data blob.

    Handles:
      - Normal numbers: "6.50" → 6.5
      - Thousands separators: "29,678.7" → 29678.7
      - Zero markers: "–", "x", "-" → 0.0
      - Unparseable: returns None
    """
    if token in ZERO_MARKERS:
        return 0.0

    try:
        # Strip thousands separators (commas) before converting
        cleaned = token.replace(",", "")
        return float(cleaned)
    except ValueError:
        return None


def parse_data_blob(data_blob, data_position):
    """
    Parse the data blob line by line to extract monthly values at a
    specific column position.

    Each line format:
      Year line:    "2022 6.50 0.0 6.50 0.0 ..."  (annual — skip)
      Year+month:   "2025 Feb. 6.50 0.0 ..."
      Month only:   "Mar. 6.50 0.0 ..."

    Args:
        data_blob: text string from the PDF table
        data_position: 0-based index of the target value after removing
                       year and month tokens

    Returns list of dicts: [{'year': 2025, 'month': 2, 'value': 0.0}, ...]
    """
    results = []
    current_year = None

    for line in data_blob.strip().split("\n"):
        line = line.strip()
        if not line:
            continue

        tokens = line.split()
        if not tokens:
            continue

        # Check for year at the start
        if re.match(r"^\d{4}\*?$", tokens[0]):
            current_year = int(tokens[0].rstrip("*"))
            tokens = tokens[1:]

        # Check for month abbreviation
        has_month = False
        month_value = None
        if tokens:
            month_clean = tokens[0].lower().rstrip(".*")
            if month_clean in MONTH_MAP:
                has_month = True
                month_value = MONTH_MAP[month_clean]
                tokens = tokens[1:]

        # Extract value for month rows only (skip annual summary rows)
        if has_month and current_year:
            if len(tokens) > data_position:
                value = parse_value(tokens[data_position])
                if value is not None:
                    results.append({
                        "year": current_year,
                        "month": month_value,
                        "value": value,
                    })

    return results


def extract_from_pdf(filepath):
    """
    Open a PDF and extract all monetary policy series.

    Returns:
      results: dict of {series_id: [data_points_list]}
      error: error message, or None
    """
    try:
        with pdfplumber.open(filepath) as pdf:
            table_blobs = find_monetary_tables(pdf)

            if not table_blobs:
                return {}, "Could not find Tables 3.1/3.2"

            results = {}
            for cfg in SERIES_CONFIG:
                table_id = cfg["table_id"]
                blob = table_blobs.get(table_id)
                if blob:
                    data = parse_data_blob(blob, cfg["data_position"])
                    results[cfg["series_id"]] = data
                else:
                    results[cfg["series_id"]] = []

            return results, None

    except Exception as e:
        return {}, str(e)


# ============================================================================
# MAIN OPERATIONS
# ============================================================================

def get_pdf_files():
    """Get sorted list of bulletin PDFs (oldest first)."""
    pattern = os.path.join(ARCHIVE_DIR, "bnr_monthly_bulletin_*.pdf")
    files = glob.glob(pattern)
    files.sort()
    return files


def run_backfill():
    """Process ALL PDFs in the archive."""
    print("=" * 70)
    print("MONETARY POLICY OPERATIONS (Tables 3.1 & 3.2) — BACKFILL MODE")
    print("=" * 70)

    pdf_files = get_pdf_files()
    if not pdf_files:
        print(f"ERROR: No PDF files found in {ARCHIVE_DIR}/")
        print("Run scraper_bnr_bulletin_download.py --download first.")
        return

    print(f"Found {len(pdf_files)} PDFs in {ARCHIVE_DIR}/")
    print(f"Extracting {len(SERIES_CONFIG)} series:")
    for cfg in SERIES_CONFIG:
        print(f"  - {cfg['series_id']} ({cfg['direction']})")
    print()

    conn = get_db_connection()

    try:
        for cfg in SERIES_CONFIG:
            ensure_series_registered(conn, cfg)
            print(f"  Series '{cfg['series_id']}' registered.")
        print()

        totals = {cfg["series_id"]: {"new": 0, "skipped": 0}
                  for cfg in SERIES_CONFIG}
        total_failed = 0

        for i, filepath in enumerate(pdf_files, 1):
            filename = os.path.basename(filepath)
            started_at = datetime.now(timezone.utc)

            print(f"[{i}/{len(pdf_files)}] {filename}...", end=" ", flush=True)

            results, error = extract_from_pdf(filepath)

            if error:
                print(f"FAILED: {error}")
                for cfg in SERIES_CONFIG:
                    log_scrape(conn, cfg["series_id"], "failure", 0, 0, 0,
                               error, filepath, started_at)
                total_failed += 1
                continue

            # Store each series and build compact output
            parts = []
            for cfg in SERIES_CONFIG:
                sid = cfg["series_id"]
                data = results.get(sid, [])
                short = sid.replace("bnr_", "").replace("_stock", "")

                if data:
                    new_count, skipped = store_data_points(
                        conn, sid, data, filename)
                    totals[sid]["new"] += new_count
                    totals[sid]["skipped"] += skipped
                    parts.append(f"{short}:{len(data)}")
                else:
                    parts.append(f"{short}:0")

                log_scrape(conn, sid, "success", len(data),
                           new_count if data else 0, 0, None,
                           filepath, started_at)

            print(", ".join(parts))

        # Summary
        print()
        print("=" * 70)
        print("BACKFILL COMPLETE")
        print(f"  Files processed: {len(pdf_files)}")
        print(f"  Files failed:    {total_failed}")
        for cfg in SERIES_CONFIG:
            sid = cfg["series_id"]
            t = totals[sid]
            print(f"  {sid}: {t['new']} new, {t['skipped']} skipped")
        print("=" * 70)

    finally:
        conn.close()


def run_update():
    """Process only the most recent PDF."""
    print("=" * 70)
    print("MONETARY POLICY OPERATIONS (Tables 3.1 & 3.2) — UPDATE MODE")
    print("=" * 70)

    pdf_files = get_pdf_files()
    if not pdf_files:
        print(f"ERROR: No PDF files found in {ARCHIVE_DIR}/")
        return

    latest_file = pdf_files[-1]
    filename = os.path.basename(latest_file)
    print(f"Processing: {filename}")
    print()

    conn = get_db_connection()

    try:
        for cfg in SERIES_CONFIG:
            ensure_series_registered(conn, cfg)

        started_at = datetime.now(timezone.utc)
        results, error = extract_from_pdf(latest_file)

        if error:
            print(f"FAILED: {error}")
            for cfg in SERIES_CONFIG:
                log_scrape(conn, cfg["series_id"], "failure", 0, 0, 0,
                           error, latest_file, started_at)
            return

        for cfg in SERIES_CONFIG:
            sid = cfg["series_id"]
            data = results.get(sid, [])
            new_count, skipped = store_data_points(
                conn, sid, data, filename) if data else (0, 0)
            print(f"  {sid}: {len(data)} extracted, {new_count} new, "
                  f"{skipped} existing")
            log_scrape(conn, sid, "success", len(data), new_count, 0,
                       None, latest_file, started_at)

    finally:
        conn.close()


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in ("--backfill", "--update"):
        print("BNR Monthly Bulletin — Monetary Policy Operations Scraper")
        print()
        print("Extracts from Tables 3.1 & 3.2:")
        for cfg in SERIES_CONFIG:
            print(f"  - {cfg['series_id']}: {cfg['name']} [{cfg['direction']}]")
        print()
        print("Usage:")
        print("  python scraper_bnr_bulletin_monetary.py --backfill")
        print("  python scraper_bnr_bulletin_monetary.py --update")
        exit(1)

    if sys.argv[1] == "--backfill":
        run_backfill()
    elif sys.argv[1] == "--update":
        run_update()
