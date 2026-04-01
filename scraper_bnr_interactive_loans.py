"""
============================================================================
RECONOMANIA — BNR Interactive Database: Loan Balances Scraper
============================================================================

FILE PURPOSE:
    Fetches loan balance data from BNR's interactive database XML endpoint
    and loads all series needed for the interactive loan explorer chart.

DATA SOURCE:
    BNR Interactive Database — XML export
    URL: https://www.bnr.ro/idbsfiles?cid=571&dfrom=&dto=&period=all&format=XML
    
    Monthly data from January 2007, 39 series in total. We extract 27 that
    are needed for the interactive chart (different currency/segment combos).

VALUE FORMAT:
    Source values are in "mii lei" (thousands RON) with Romanian formatting:
    "206 082 197,5" → 206082197.5. Stored as-is in RON thousands.

MODES:
    --backfill    Fetch all available data (2007 onwards)
    --update      Same — the endpoint always returns the full dataset.
                  ON CONFLICT handles duplicates.

HOW TO RUN:
    python scraper_bnr_interactive_loans.py --backfill
    python scraper_bnr_interactive_loans.py --update

DATE:        March 2026
PHASE:       Phase Two
============================================================================
"""

import requests
import xml.etree.ElementTree as ET
import psycopg2
import sys
from datetime import datetime, timezone, date, timedelta
from decimal import Decimal

# ============================================================================
# CONFIGURATION
# ============================================================================

DB_CONFIG = {"dbname": "reconomania"}

XML_URL = "https://www.bnr.ro/idbsfiles?cid=571&dfrom=&dto=&period=all&format=XML"
USER_AGENT = "RECONOMANIA data aggregator (reconomania.com) - contact@reconomania.com"
BNR_NS = {"ns": "https://www.bnr.ro/xsd"}

# All XML codes needed for the interactive loan chart.
LOAN_CODES = [
    # --- Totals by sector ---
    "IFMCL_G",       # Households total
    "IFMCL_S",       # Corporates (non-financial) total
    "IFMCL_I",       # Non-bank financial institutions total
    "IFMCL_AP",      # Public sector total
    # --- Households by currency ---
    "IFMCL_GR",      # Households — RON
    "IFMCL_GE",      # Households — EUR
    "IFMCL_GO",      # Households — other FX
    # --- Households: housing loans ---
    "IFMCL_GL",      # Housing total
    "IFMCL_GLL",     # Housing — RON
    "IFMCL_GLE",     # Housing — EUR
    "IFMCL_GLX",     # Housing — other FX
    # --- Households: consumer loans ---
    "IFMCL_GC",      # Consumer total
    "IFMCL_GCL",     # Consumer — RON
    "IFMCL_GCE",     # Consumer — EUR
    "IFMCL_GCX",     # Consumer — other FX
    # --- Corporates by currency ---
    "IFMCL_SL",      # Corporates — RON
    "IFMCL_SE",      # Corporates — EUR
    "IFMCL_SX",      # Corporates — other FX
    # --- Corporates by maturity: RON ---
    "IFMCL_SL1A",    # RON < 1 year
    "IFMCL_SL1A5",   # RON 1-5 years
    "IFMCL_SLX5A",   # RON > 5 years
    # --- Corporates by maturity: EUR ---
    "IFMCL_SE1A",    # EUR < 1 year
    "IFMCL_SE1A5",   # EUR 1-5 years
    "IFMCL_SEX5A",   # EUR > 5 years
    # --- Corporates by maturity: other FX ---
    "IFMCL_SX1A",    # Other FX < 1 year
    "IFMCL_SX1A5",   # Other FX 1-5 years
    "IFMCL_SXX5A",   # Other FX > 5 years
]


# ============================================================================
# DATABASE HELPERS
# ============================================================================

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def ensure_series_registered(connection, series_id, xml_code, full_name):
    """Register a series with auto-generated metadata from the XML attributes."""
    cursor = connection.cursor()
    cursor.execute(
        """
        INSERT INTO time_series (
            series_id, name, description, source_institution, source_url,
            frequency, temporal_type, units, expected_update_schedule,
            historical_start_date, topic_path
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (series_id) DO NOTHING
        """,
        (
            series_id,
            full_name or f"Loan balance: {xml_code}",
            f"Loan balance series {xml_code} from NBR Interactive Database. "
            f"Original name: {full_name}",
            "BNR",
            "https://www.bnr.ro/1074-baza-de-date-interactiva",
            "monthly",
            "end_of_period",
            "RON thousands",
            "Monthly, typically available within 6 weeks of reference month",
            date(2007, 1, 1),
            "Banking/Loans",
        ),
    )
    connection.commit()
    cursor.close()


def store_data_points(connection, series_id, data_points, source_file):
    cursor = connection.cursor()
    new_count = 0
    skipped_count = 0

    for dp in data_points:
        try:
            cursor.execute(
                """
                INSERT INTO data_points (series_id, observation_date, value, source_file)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (series_id, observation_date, recorded_at) DO NOTHING
                """,
                (series_id, dp["date"], Decimal(str(dp["value"])), source_file),
            )
            if cursor.rowcount == 1:
                new_count += 1
            else:
                skipped_count += 1
        except Exception as e:
            print(f"    [ERROR] {series_id} {dp['date']}: {e}")
            connection.rollback()

    connection.commit()
    cursor.close()
    return new_count, skipped_count


def log_scrape(connection, series_id, status, records_fetched, records_new,
               records_updated, error_message, source_file_archived, started_at):
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
            series_id, started_at, datetime.now(timezone.utc), status,
            records_fetched, records_new, records_updated,
            error_message, source_file_archived,
        ),
    )
    connection.commit()
    cursor.close()


# ============================================================================
# XML PARSING
# ============================================================================

def parse_romanian_number(text):
    """Parse "206 082 197,5" → 206082197.5"""
    if not text or text.strip() == "":
        return None
    cleaned = text.strip().replace(" ", "").replace("\xa0", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_date(text):
    """Parse "01.02.2026" → date(2026, 2, 1)"""
    parts = text.strip().split(".")
    if len(parts) != 3:
        return None
    try:
        day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
        return date(year, month, day)
    except ValueError:
        return None


def fetch_and_parse_xml():
    """
    Fetch the XML and parse all rows for the target codes.
    Also extracts FullName attributes for auto-registration.
    
    Returns:
        data: dict {xml_code: [{"date": date, "value": float}, ...]}
        names: dict {xml_code: full_name_string}
    """
    print(f"Fetching: {XML_URL}")
    headers = {"User-Agent": USER_AGENT}
    response = requests.get(XML_URL, headers=headers, timeout=60)
    response.raise_for_status()
    print(f"  Response: {len(response.text):,} bytes")

    target_codes = set(LOAN_CODES)
    root = ET.fromstring(response.content)
    rows = root.findall(".//ns:Row", BNR_NS)
    print(f"  Rows found: {len(rows)}")

    results = {code: [] for code in target_codes}
    names = {}

    for row in rows:
        date_elem = row.find("ns:Data", BNR_NS)
        if date_elem is None or date_elem.text is None:
            continue
        obs_date = parse_date(date_elem.text)
        if obs_date is None:
            continue

        # "01.02.2026" means balance at end of February 2026.
        # Store as last day of that month.
        if obs_date.month == 12:
            eom = date(obs_date.year, 12, 31)
        else:
            eom = date(obs_date.year, obs_date.month + 1, 1) - timedelta(days=1)

        for code in target_codes:
            elem = row.find(f"ns:{code}", BNR_NS)
            if elem is not None and elem.text is not None:
                value = parse_romanian_number(elem.text)
                if value is not None:
                    results[code].append({"date": eom, "value": value})
                if code not in names and elem.get("FullName"):
                    names[code] = elem.get("FullName")

    return results, names


# ============================================================================
# MAIN
# ============================================================================

def run():
    print("=" * 70)
    print("NBR INTERACTIVE DATABASE — LOAN BALANCES (FULL)")
    print(f"Extracting {len(LOAN_CODES)} series")
    print("=" * 70)

    started_at = datetime.now(timezone.utc)

    try:
        raw_data, names = fetch_and_parse_xml()
    except Exception as e:
        print(f"FAILED to fetch XML: {e}")
        return

    conn = get_db_connection()

    try:
        total_new = 0
        total_skipped = 0

        for code in LOAN_CODES:
            series_id = f"bnr_{code.lower()}"
            full_name = names.get(code, f"Loan balance: {code}")
            data = raw_data.get(code, [])

            ensure_series_registered(conn, series_id, code, full_name)
            new_count, skipped = store_data_points(
                conn, series_id, data, f"idbsfiles_cid571_{code}"
            )
            total_new += new_count
            total_skipped += skipped

            log_scrape(conn, series_id, "success", len(data), new_count, 0,
                       None, XML_URL, started_at)

            print(f"  {series_id:30s} {len(data)} pts, {new_count} new")

        print()
        print(f"Total: {total_new} new, {total_skipped} existing")
        print("Done.")

    finally:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ("--backfill", "--update"):
        run()
    else:
        print("NBR Interactive Database — Loan Balances Scraper (Full)")
        print()
        print(f"Extracts {len(LOAN_CODES)} series:")
        for code in LOAN_CODES:
            print(f"  - {code}")
        print()
        print("Usage:")
        print("  python scraper_bnr_interactive_loans.py --backfill")
        print("  python scraper_bnr_interactive_loans.py --update")
