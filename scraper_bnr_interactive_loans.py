"""
============================================================================
RECONOMANIA — BNR Interactive Database: Loan Balances Scraper
============================================================================

FILE PURPOSE:
    Fetches loan balance data from BNR's interactive database XML endpoint
    and loads selected series into the RECONOMANIA PostgreSQL database.

DATA SOURCE:
    BNR Interactive Database — XML export
    URL: https://www.bnr.ro/idbsfiles?cid=571&dfrom=&dto=&period=all&format=XML
    
    The XML contains 39 loan balance series covering households, corporates,
    non-bank financials, government, and non-residents. Each series has a
    code (e.g., IFMCL_G), unit (mii lei = thousands RON), and full name.
    
    Data available from January 2007, updated monthly.

SERIES EXTRACTED:
    - bnr_loans_households:   Household loans total (IFMCL_G)
    - bnr_loans_corporates:   Corporate loans total (IFMCL_S)
    - bnr_loans_nbfi:         Non-bank financial institution loans (IFMCL_I)

    These three are the building blocks for "total private sector loans"
    which is computed by derive_private_loans.py.

VALUE FORMAT:
    Source values are in "mii lei" (thousands RON) with Romanian formatting:
    "206 082 197,5" = 206,082,197.5 thousands RON = ~206 billion RON.
    We store as-is (thousands RON) and record the unit in time_series.

MODES:
    --backfill    Fetch all available data (2007 onwards)
    --update      Same as backfill — the XML endpoint always returns the
                  full dataset, so there's no incremental mode. The
                  ON CONFLICT clause handles duplicates.

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
import re
from datetime import datetime, timezone, date, timedelta
from decimal import Decimal

# ============================================================================
# CONFIGURATION
# ============================================================================

DB_CONFIG = {"dbname": "reconomania"}

# The XML endpoint. cid=571 is the loan balances dataset.
XML_URL = "https://www.bnr.ro/idbsfiles?cid=571&dfrom=&dto=&period=all&format=XML"

USER_AGENT = "RECONOMANIA data aggregator (reconomania.com) - contact@reconomania.com"

# BNR XML namespace (same as the EUR/RON feed)
BNR_NS = {"ns": "https://www.bnr.ro/xsd"}

# Series to extract: XML element name → series config
SERIES_CONFIG = [
    {
        "series_id": "bnr_loans_households",
        "xml_code": "IFMCL_G",
        "name": "Household loans — total balance",
        "description": (
            "Total outstanding loan balance to households (gospodării ale populației), "
            "all currencies, all purposes. "
            "Source: NBR Interactive Database, loan balances by institutional sector."
        ),
        "units": "RON thousands",
        "chart_colour": "#0F3B5C",
        "topic_path": "Banking/Loans/Households",
    },
    {
        "series_id": "bnr_loans_corporates",
        "xml_code": "IFMCL_S",
        "name": "Corporate loans — total balance",
        "description": (
            "Total outstanding loan balance to non-financial corporations "
            "(societăți nefinanciare), all currencies, all maturities. "
            "Source: NBR Interactive Database, loan balances by institutional sector."
        ),
        "units": "RON thousands",
        "chart_colour": "#2D8C5A",
        "topic_path": "Banking/Loans/Corporates",
    },
    {
        "series_id": "bnr_loans_nbfi",
        "xml_code": "IFMCL_I",
        "name": "Non-bank financial institution loans — total balance",
        "description": (
            "Total outstanding loan balance to non-monetary financial institutions "
            "(instituții financiare nemonetare), including insurance companies "
            "and other financial intermediaries. "
            "Source: NBR Interactive Database, loan balances by institutional sector."
        ),
        "units": "RON thousands",
        "chart_colour": "#5B9BD5",
        "topic_path": "Banking/Loans/NBFI",
    },
]


# ============================================================================
# DATABASE HELPERS
# ============================================================================

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def ensure_series_registered(connection, cfg):
    cursor = connection.cursor()
    cursor.execute(
        """
        INSERT INTO time_series (
            series_id, name, description, source_institution, source_url,
            frequency, temporal_type, units, expected_update_schedule,
            historical_start_date, chart_colour, topic_path
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (series_id) DO NOTHING
        """,
        (
            cfg["series_id"], cfg["name"], cfg["description"],
            "BNR",
            "https://www.bnr.ro/1074-baza-de-date-interactiva",
            "monthly", "end_of_period", cfg["units"],
            "Monthly, typically available within 6 weeks of reference month",
            date(2007, 1, 1),
            cfg["chart_colour"], cfg["topic_path"],
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
            print(f"    [ERROR] Failed to insert {series_id} {dp['date']}: {e}")
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
    """
    Parse a Romanian-formatted number: "206 082 197,5" → 206082197.5
    Spaces are thousands separators, comma is decimal separator.
    """
    if not text or text.strip() == "":
        return None
    cleaned = text.strip().replace(" ", "").replace("\xa0", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_date(text):
    """Parse BNR date format: "01.02.2026" → date(2026, 2, 1)"""
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
    Fetch the XML from BNR and parse all rows.
    
    Returns a dict: {xml_code: [{"date": date, "value": float}, ...]}
    """
    print(f"Fetching: {XML_URL}")
    headers = {"User-Agent": USER_AGENT}
    response = requests.get(XML_URL, headers=headers, timeout=60)
    response.raise_for_status()
    print(f"  Response: {len(response.text):,} bytes")

    # Build a set of XML codes we care about
    target_codes = {cfg["xml_code"] for cfg in SERIES_CONFIG}

    # Parse the XML
    root = ET.fromstring(response.content)

    # Find all Row elements
    rows = root.findall(".//ns:Row", BNR_NS)
    print(f"  Rows found: {len(rows)}")

    # Extract data for each target series
    results = {code: [] for code in target_codes}

    for row in rows:
        # Get the date
        date_elem = row.find("ns:Data", BNR_NS)
        if date_elem is None or date_elem.text is None:
            continue
        obs_date = parse_date(date_elem.text)
        if obs_date is None:
            continue

        # BNR uses "01.02.2026" to mean the balance at end of February 2026.
        # The "01" is a placeholder — the real meaning is the month.
        # We store as the last day of that month (our end-of-period convention).
        if obs_date.month == 12:
            eom = date(obs_date.year, 12, 31)
        else:
            eom = date(obs_date.year, obs_date.month + 1, 1) - timedelta(days=1)

        # Extract values for each target series
        for code in target_codes:
            elem = row.find(f"ns:{code}", BNR_NS)
            if elem is not None and elem.text is not None:
                value = parse_romanian_number(elem.text)
                if value is not None:
                    results[code].append({"date": eom, "value": value})

    return results


# ============================================================================
# MAIN
# ============================================================================

def run():
    print("=" * 70)
    print("NBR INTERACTIVE DATABASE — LOAN BALANCES")
    print("=" * 70)

    started_at = datetime.now(timezone.utc)

    # Fetch and parse
    try:
        raw_data = fetch_and_parse_xml()
    except Exception as e:
        print(f"FAILED to fetch XML: {e}")
        return

    conn = get_db_connection()

    try:
        for cfg in SERIES_CONFIG:
            ensure_series_registered(conn, cfg)
            print(f"  Series '{cfg['series_id']}' registered.")
        print()

        for cfg in SERIES_CONFIG:
            sid = cfg["series_id"]
            code = cfg["xml_code"]
            data = raw_data.get(code, [])

            new_count, skipped = store_data_points(conn, sid, data, f"idbsfiles_cid571_{code}")

            log_scrape(conn, sid, "success", len(data), new_count, 0,
                       None, XML_URL, started_at)

            print(f"  {sid}: {len(data)} extracted, {new_count} new, {skipped} existing")

        print()
        print("Done.")

    finally:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ("--backfill", "--update"):
        run()
    else:
        print("NBR Interactive Database — Loan Balances Scraper")
        print()
        print("Extracts:")
        for cfg in SERIES_CONFIG:
            print(f"  - {cfg['series_id']}: {cfg['name']} ({cfg['xml_code']})")
        print()
        print("Usage:")
        print("  python scraper_bnr_interactive_loans.py --backfill")
        print("  python scraper_bnr_interactive_loans.py --update")
