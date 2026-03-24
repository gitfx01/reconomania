"""
============================================================================
RECONOMANIA — BNR Monthly Bulletin: Key Prudential Indicators Scraper
============================================================================

FILE PURPOSE:
    Extracts time series from Table 11.1 ("Key prudential indicators") of
    BNR Monthly Bulletin PDFs and loads them into the RECONOMANIA PostgreSQL
    database.

    This scraper handles MULTIPLE series from the same table. Each column
    of interest is defined in SERIES_CONFIG below. Adding a new column from
    Table 11.1 means adding one entry to that list — no structural changes.

DATA SOURCE:
    BNR Monthly Bulletins (PDF), Table 11.1 "Key prudential indicators"
    Publication page: https://www.bnr.ro/en/12072-monthly-bulletins
    Frequency: Monthly, published ~6-8 weeks after reference month

SERIES CURRENTLY EXTRACTED:
    - bnr_npl_ratio_eba: Non-performing loan ratio (EBA definition), percent
    - bnr_lcr: Liquidity coverage ratio, percent

HOW IT WORKS:
    1. Opens each PDF with pdfplumber
    2. Finds Table 11.1 by searching for "Key prudential indicators"
    3. Identifies target columns by matching header text (not by position —
       the layout changed between 2019 and 2025)
    4. Parses the data blob line by line (pdfplumber merges all data rows
       into a single text cell because the table has no grid lines)
    5. Inserts monthly values into data_points, logs to scrape_log
    6. Skips annual summary rows (year-only lines)

DATA OVERLAP:
    Each bulletin contains ~13 months of data. The same month appears in
    multiple bulletins. This is handled by the data_points table's versioning:
    each PDF gets its own recorded_at timestamp. Queries use the latest
    recorded_at for each observation_date — so the most recent bulletin's
    value automatically prevails.

TABLE LAYOUT:
    The table layout changed between 2019 and 2025:
      - Pre-2025: 7 data columns (includes separate NPL ratio + NPL EBA ratio)
      - 2025+: 5 data columns (some columns dropped, "loans" → "assets")
    The extractor handles both by finding columns via header text matching.

MODES:
    --backfill    Process ALL PDFs in the archive (initial database load)
    --update      Process only the most recent PDF (monthly updates)

HOW TO RUN:
    From the project directory (~/reconomania), with venv activated:

        python scraper_bnr_bulletin_prudential.py --backfill
        python scraper_bnr_bulletin_prudential.py --update

    PDFs must already be downloaded using scraper_bnr_bulletin_download.py.

WHAT TO DO IF IT BREAKS:
    - Check scrape_log:
        psql -d reconomania -c "SELECT * FROM scrape_log
          WHERE series_id LIKE 'bnr_%' AND source_file_archived LIKE '%bulletin%'
          ORDER BY id DESC LIMIT 10;"
    - If a specific PDF fails, inspect it with pdfplumber directly:
        python -c "import pdfplumber; pdf = pdfplumber.open('path/to/file.pdf');
          [print(t) for t in pdf.pages[45].extract_tables()]"

DATE:        March 2026
PHASE:       Phase Two
============================================================================
"""

# ============================================================================
# IMPORTS
# ============================================================================

# pdfplumber — opens PDFs and extracts tables. Unlike simpler PDF readers,
# pdfplumber understands table structure (rows, columns, cells). It's not
# perfect (tables without grid lines get merged), but it reliably extracts
# headers, which is what we need for column identification.
import pdfplumber

# psycopg2 — connects Python to PostgreSQL (same as scraper_bnr_eurron.py).
import psycopg2

# Standard library imports
import os
import sys
import re
import glob
from datetime import datetime, timezone, date, timedelta
from decimal import Decimal


# ============================================================================
# CONFIGURATION
# ============================================================================

# --- Database connection settings ---
# Same as scraper_bnr_eurron.py: Unix socket, no password needed locally.
DB_CONFIG = {
    "dbname": "reconomania",
}

# --- File paths ---
ARCHIVE_DIR = "archive/bnr_monthly_bulletin"

# --- Month abbreviation to number mapping ---
# Used to parse "Jan.", "Feb.", etc. from the PDF table rows.
MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "may": 5, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


# ============================================================================
# SERIES CONFIGURATION
# ============================================================================
# Each entry defines one time series to extract from Table 11.1.
#
# To add a new column from this table:
#   1. Add an entry to this list with the header_patterns and metadata
#   2. Run --backfill to extract it from all 85 PDFs
#   That's it. No structural changes needed anywhere else.
#
# Fields:
#   series_id:        Human-readable ID for the time_series table and data_points
#   header_patterns:  List of strings that must ALL appear (case-insensitive) in
#                     the column header to identify it. Use the most distinctive
#                     words — enough to uniquely match this column.
#   name:             Display name for the time_series metadata
#   description:      Longer description for the time_series metadata
#   temporal_type:    How the data relates to time (end_of_period for both)
#   units:            Unit of measurement
#   chart_colour:     Hex colour for charts (from the RECONOMANIA palette)
#   topic_path:       Navigation hierarchy path

SERIES_CONFIG = [
    {
        "series_id": "bnr_npl_ratio_eba",
        "header_patterns": ["non-performing", "eba"],
        "name": "Non-performing loan ratio (EBA definition)",
        "description": (
            "Non-performing loan ratio for the Romanian banking system, "
            "calculated based on the European Banking Authority's definition. "
            "Extracted from Table 11.1 (Key prudential indicators) of BNR's "
            "Monthly Bulletin."
        ),
        "temporal_type": "end_of_period",
        "units": "percent",
        "chart_colour": "#0F3B5C",
        "topic_path": "Banking/Asset Quality",
    },
    {
        "series_id": "bnr_lcr",
        "header_patterns": ["liquidity", "coverage"],
        "name": "Liquidity Coverage Ratio (LCR)",
        "description": (
            "Liquidity Coverage Ratio (LCR) for the Romanian banking system. "
            "Measures the stock of high-quality liquid assets relative to "
            "net cash outflows over a 30-day stress period. Minimum regulatory "
            "requirement is 100%. "
            "Extracted from Table 11.1 (Key prudential indicators) of BNR's "
            "Monthly Bulletin."
        ),
        "temporal_type": "end_of_period",
        "units": "percent",
        "chart_colour": "#2D8C5A",
        "topic_path": "Banking/Liquidity",
    },
]


# ============================================================================
# DATABASE HELPERS
# ============================================================================

def get_db_connection():
    """
    Opens a connection to the PostgreSQL database and returns it.
    Same pattern as scraper_bnr_eurron.py.
    """
    return psycopg2.connect(**DB_CONFIG)


def ensure_series_registered(connection, series_cfg):
    """
    Make sure a time series exists in the time_series metadata registry.
    Called once per series at the start of each run. Idempotent — safe to
    call repeatedly thanks to ON CONFLICT DO NOTHING.
    """
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
            series_cfg["temporal_type"],
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
    """
    Insert extracted data points into the data_points table.

    Same pattern as scraper_bnr_eurron.py:
      - Uses ON CONFLICT DO NOTHING to handle re-runs
      - Returns (new_count, skipped_count)
      - recorded_at is set automatically by DEFAULT NOW()

    The observation_date is the last day of the month, because these are
    end-of-period indicators (the ratio as of month-end).
    """
    cursor = connection.cursor()
    new_count = 0
    skipped_count = 0

    for dp in data_points:
        year = dp["year"]
        month = dp["month"]

        # Last day of the month
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
    """
    Records the outcome of processing one PDF for one series in scrape_log.
    Same pattern as scraper_bnr_eurron.py.
    """
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

def find_prudential_table(pdf):
    """
    Search all pages of a PDF for Table 11.1 ("Key prudential indicators").

    The table has a quirk in pdfplumber: the header row is extracted as
    separate cells (good), but all data rows are merged into a single text
    blob in one cell (because the table has no grid lines). So:
      table[0] = list of header strings (one per column)
      table[1][0] = giant text blob with all data rows

    This function finds the page containing both the header AND data for
    our target columns. It may appear on multiple pages (the table can span
    two pages, and the narrative section also mentions the title).

    Returns a dict with:
      - header_row: list of column header strings
      - data_blob: the text blob containing all data rows
      - column_map: {series_id: data_position_index} for each found column
    Or None if the table wasn't found.
    """
    for page_idx, page in enumerate(pdf.pages):
        text = page.extract_text() or ""
        if "key prudential indicators" not in text.lower():
            continue

        tables = page.extract_tables()
        for table in tables:
            if len(table) < 2:
                continue

            header_row = table[0]
            data_blob = table[1][0] if table[1][0] else ""

            if not data_blob.strip():
                continue

            # Try to find each target series in this table's headers
            column_map = {}
            for cfg in SERIES_CONFIG:
                col_idx = _match_column(header_row, cfg["header_patterns"])
                if col_idx is not None:
                    # Position among data values = column index minus 1
                    # (first column is "Period", not a data value)
                    column_map[cfg["series_id"]] = col_idx - 1

            # Only return if we found at least one target column
            if column_map:
                return {
                    "page_idx": page_idx,
                    "header_row": header_row,
                    "data_blob": data_blob,
                    "column_map": column_map,
                }

    return None


def _match_column(header_row, patterns):
    """
    Find the column index whose header contains ALL the given patterns.

    Args:
        header_row: list of strings (column headers from pdfplumber)
        patterns: list of strings that must all appear (case-insensitive)

    Returns the column index, or None if no match.
    """
    for col_idx, cell in enumerate(header_row):
        if cell is None:
            continue
        cell_lower = cell.lower()
        if all(p.lower() in cell_lower for p in patterns):
            return col_idx
    return None


def parse_data_blob(data_blob, data_position):
    """
    Parse the text blob line by line to extract monthly values at a
    specific column position.

    Each line in the blob has one of these formats:
      Year line:    "2022 0.93 0.55 0.51 2.65 209.16"
      Year+month:   "2025 Jan. 0.99 0.56 0.51 2.52 254.07"
      Month only:   "Feb. 1.03 0.59 0.53 2.50 260.88"

    Args:
        data_blob: the text string from the PDF
        data_position: 0-based index of the value to extract from each line
                       (after removing the year and month tokens)

    Returns a list of dicts: [{'year': 2025, 'month': 1, 'value': 2.52}, ...]
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

        # Check for year at the start (e.g., "2022", "2025*")
        if re.match(r"^\d{4}\*?$", tokens[0]):
            current_year = int(tokens[0].rstrip("*"))
            tokens = tokens[1:]

        # Check for month abbreviation (e.g., "Jan.", "Dec.*")
        has_month = False
        month_value = None
        if tokens:
            month_clean = tokens[0].lower().rstrip(".*")
            if month_clean in MONTH_MAP:
                has_month = True
                month_value = MONTH_MAP[month_clean]
                tokens = tokens[1:]

        # If this is a month line and we know the year, extract the value
        if has_month and current_year:
            if len(tokens) > data_position:
                value_str = tokens[data_position]
                # Skip non-numeric values ("x", "-", "…")
                if value_str.lower() in ("x", "-", "…", "..."):
                    continue
                try:
                    value = float(value_str.replace(",", "."))
                    results.append({
                        "year": current_year,
                        "month": month_value,
                        "value": value,
                    })
                except ValueError:
                    pass  # Silently skip unparseable values

    return results


def extract_from_pdf(filepath):
    """
    Open a single PDF and extract monthly values for all configured series.

    Returns:
      results: dict of {series_id: [data_points_list]}
      error: error message string, or None if successful
    """
    try:
        with pdfplumber.open(filepath) as pdf:
            table_info = find_prudential_table(pdf)

            if table_info is None:
                return {}, "Could not find Table 11.1 with target columns"

            results = {}
            for series_id, data_position in table_info["column_map"].items():
                data = parse_data_blob(
                    table_info["data_blob"],
                    data_position,
                )
                results[series_id] = data

            return results, None

    except Exception as e:
        return {}, str(e)


# ============================================================================
# MAIN OPERATIONS
# ============================================================================

def get_pdf_files():
    """
    Get a sorted list of all bulletin PDF files in the archive directory.
    Sorted alphabetically = chronologically (oldest first), so the most
    recent bulletin's recorded_at timestamp ends up latest — making it the
    "current" value for any overlapping months.
    """
    pattern = os.path.join(ARCHIVE_DIR, "bnr_monthly_bulletin_*.pdf")
    files = glob.glob(pattern)
    files.sort()
    return files


def run_backfill():
    """
    Process ALL PDFs in the archive directory.

    Opens each PDF once, extracts all configured series from Table 11.1,
    and loads them into the database. Files are processed oldest-first so
    that the newest bulletin's data has the latest recorded_at timestamp.
    """
    print("=" * 70)
    print("PRUDENTIAL INDICATORS (Table 11.1) — BACKFILL MODE")
    print("=" * 70)

    pdf_files = get_pdf_files()
    if not pdf_files:
        print(f"ERROR: No PDF files found in {ARCHIVE_DIR}/")
        print("Run scraper_bnr_bulletin_download.py --download first.")
        return

    print(f"Found {len(pdf_files)} PDFs in {ARCHIVE_DIR}/")
    series_names = [cfg["series_id"] for cfg in SERIES_CONFIG]
    print(f"Extracting: {', '.join(series_names)}")
    print()

    conn = get_db_connection()

    try:
        # Register all series (idempotent)
        for cfg in SERIES_CONFIG:
            ensure_series_registered(conn, cfg)
            print(f"  Series '{cfg['series_id']}' registered.")
        print()

        # Track totals per series
        totals = {cfg["series_id"]: {"new": 0, "skipped": 0} for cfg in SERIES_CONFIG}
        total_failed_files = 0

        for i, filepath in enumerate(pdf_files, 1):
            filename = os.path.basename(filepath)
            started_at = datetime.now(timezone.utc)

            print(f"[{i}/{len(pdf_files)}] {filename}...", end=" ", flush=True)

            # Extract all series from this PDF
            results, error = extract_from_pdf(filepath)

            if error:
                print(f"FAILED: {error}")
                # Log failure for each series
                for cfg in SERIES_CONFIG:
                    log_scrape(
                        connection=conn,
                        series_id=cfg["series_id"],
                        status="failure",
                        records_fetched=0,
                        records_new=0,
                        records_updated=0,
                        error_message=error,
                        source_file_archived=filepath,
                        started_at=started_at,
                    )
                total_failed_files += 1
                continue

            # Store and log each series
            parts = []
            for cfg in SERIES_CONFIG:
                sid = cfg["series_id"]
                data = results.get(sid, [])

                if data:
                    new_count, skipped_count = store_data_points(
                        conn, sid, data, filename
                    )
                    totals[sid]["new"] += new_count
                    totals[sid]["skipped"] += skipped_count
                    # Short label for output (e.g., "npl:13" or "lcr:13")
                    short = sid.split("_")[-1] if "_" in sid else sid
                    parts.append(f"{short}:{len(data)}")
                else:
                    new_count = 0
                    parts.append(f"{sid.split('_')[-1]}:0")

                log_scrape(
                    connection=conn,
                    series_id=sid,
                    status="success",
                    records_fetched=len(data),
                    records_new=new_count if data else 0,
                    records_updated=0,
                    error_message=None,
                    source_file_archived=filepath,
                    started_at=started_at,
                )

            print(", ".join(parts))

        # Summary
        print()
        print("=" * 70)
        print("BACKFILL COMPLETE")
        print(f"  Files processed: {len(pdf_files)}")
        print(f"  Files failed:    {total_failed_files}")
        for cfg in SERIES_CONFIG:
            sid = cfg["series_id"]
            print(f"  {sid}: {totals[sid]['new']} new, "
                  f"{totals[sid]['skipped']} skipped")
        print("=" * 70)

    finally:
        conn.close()


def run_update():
    """
    Process only the most recent PDF in the archive directory.
    Typically called after scraper_bnr_bulletin_download.py has fetched
    the latest bulletin.
    """
    print("=" * 70)
    print("PRUDENTIAL INDICATORS (Table 11.1) — UPDATE MODE")
    print("=" * 70)

    pdf_files = get_pdf_files()
    if not pdf_files:
        print(f"ERROR: No PDF files found in {ARCHIVE_DIR}/")
        return

    latest_file = pdf_files[-1]
    filename = os.path.basename(latest_file)
    print(f"Processing most recent bulletin: {filename}")
    print()

    conn = get_db_connection()

    try:
        # Register all series (idempotent)
        for cfg in SERIES_CONFIG:
            ensure_series_registered(conn, cfg)

        started_at = datetime.now(timezone.utc)
        results, error = extract_from_pdf(latest_file)

        if error:
            print(f"FAILED: {error}")
            for cfg in SERIES_CONFIG:
                log_scrape(
                    connection=conn,
                    series_id=cfg["series_id"],
                    status="failure",
                    records_fetched=0,
                    records_new=0,
                    records_updated=0,
                    error_message=error,
                    source_file_archived=latest_file,
                    started_at=started_at,
                )
            return

        for cfg in SERIES_CONFIG:
            sid = cfg["series_id"]
            data = results.get(sid, [])
            new_count, skipped_count = store_data_points(
                conn, sid, data, filename
            ) if data else (0, 0)

            print(f"  {sid}: {len(data)} extracted, {new_count} new, "
                  f"{skipped_count} existing")

            log_scrape(
                connection=conn,
                series_id=sid,
                status="success",
                records_fetched=len(data),
                records_new=new_count,
                records_updated=0,
                error_message=None,
                source_file_archived=latest_file,
                started_at=started_at,
            )

    finally:
        conn.close()


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in ("--backfill", "--update"):
        print("BNR Monthly Bulletin — Key Prudential Indicators Scraper")
        print()
        print("Extracts from Table 11.1:")
        for cfg in SERIES_CONFIG:
            print(f"  - {cfg['series_id']}: {cfg['name']}")
        print()
        print("Usage:")
        print("  python scraper_bnr_bulletin_prudential.py --backfill")
        print("  python scraper_bnr_bulletin_prudential.py --update")
        exit(1)

    if sys.argv[1] == "--backfill":
        run_backfill()
    elif sys.argv[1] == "--update":
        run_update()
