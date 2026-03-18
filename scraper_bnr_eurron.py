"""
============================================================================
RECONOMANIA — BNR EUR/RON Exchange Rate Scraper
============================================================================

FILE PURPOSE:
    This is the first scraper in the RECONOMANIA platform. It fetches the
    EUR/RON daily reference exchange rate from the National Bank of Romania
    (BNR) and stores it in the PostgreSQL database.

DATA SOURCE:
    BNR publishes exchange rates in clean XML format at predictable URLs:
    - Today's rates:     https://www.bnr.ro/nbrfxrates.xml
    - Last 10 days:      https://www.bnr.ro/nbrfxrates10days.xml
    - Full year archive: https://www.bnr.ro/files/xml/years/nbrfxrates{YEAR}.xml
      (available from 2005 to the current year)

    The XML structure is consistent across all files:
    <DataSet>
      <Body>
        <Cube date="2025-03-14">
          <Rate currency="EUR">4.9737</Rate>
          <Rate currency="USD">4.5678</Rate>
          ... (other currencies)
        </Cube>
        ... (more Cubes, one per business day)
      </Body>
    </DataSet>

    We extract only the EUR rate from each Cube.

HOW TO RUN:
    From the project directory (~/reconomania), with the virtual environment
    activated:

    Initial backfill (run once, fetches all history from 2005):
        python scraper_bnr_eurron.py --backfill

    Daily update (run regularly, fetches recent rates):
        python scraper_bnr_eurron.py --update

    Both modes are safe to run multiple times — they skip data points that
    already exist in the database.

WHAT TO DO IF IT BREAKS:
    - "Connection refused" or "could not connect to server":
        PostgreSQL is not running. Start it with: sudo service postgresql start
    - "relation does not exist":
        The database tables haven't been created. Run 001_create_schema.sql first.
    - "HTTP Error 403" or "Connection timed out":
        BNR's server may be temporarily blocking requests or be down.
        Wait a few minutes and try again. If persistent, BNR may have changed
        their URL structure — check their website manually.
    - Any other error: the scrape_log table will contain the error message.
        Check it with: psql -d reconomania -c "SELECT * FROM scrape_log ORDER BY id DESC LIMIT 5;"

DATE:        March 2026
PHASE:       Phase One
============================================================================
"""

# ============================================================================
# IMPORTS
# ============================================================================
# Each import is a library (a collection of pre-written code) that gives us
# specific capabilities. Python's strength is that thousands of these libraries
# exist, so we don't have to write everything from scratch.

# 'requests' — Makes HTTP requests (fetching web pages). When you type a URL
# in your browser, the browser sends an HTTP GET request to the server.
# The 'requests' library lets Python do the same thing programmatically.
# We installed this with 'pip install requests'.
import requests

# 'xml.etree.ElementTree' — Parses XML files. XML is a structured text format
# (similar to HTML). BNR publishes its exchange rates in XML. This library
# reads the XML and lets us navigate its structure to extract the data we need.
# This is part of Python's standard library (comes pre-installed).
import xml.etree.ElementTree as ET

# 'psycopg2' — Connects Python to PostgreSQL. It lets us send SQL commands
# to the database and read results back. Think of it as a translator between
# Python and PostgreSQL. We installed this with 'pip install psycopg2-binary'.
import psycopg2

# 'datetime' — Works with dates and times. We use it to generate timestamps,
# calculate date ranges (which years to fetch), and format dates.
# Standard library.
from datetime import datetime, timezone

# 'time' — Provides the sleep() function, which pauses execution for a
# specified number of seconds. We use this to add polite delays between
# requests to BNR's server (see Concept Paper Section 7.4).
# Standard library.
import time

# 'os' — Interacts with the operating system: creating directories, checking
# if files exist, building file paths. Standard library.
import os

# 'argparse' — Parses command-line arguments. This is what lets you write
# 'python scraper.py --backfill' or '--update' and have the script behave
# differently based on which flag you passed. Standard library.
import argparse

# 'decimal.Decimal' — Exact decimal arithmetic. We use this to convert the
# exchange rate strings from XML into precise decimal numbers before storing
# them in the database's NUMERIC column. Using Python's built-in float would
# introduce tiny rounding errors (e.g., 4.9737 becoming 4.973699999999997).
# Standard library.
from decimal import Decimal


# ============================================================================
# CONFIGURATION
# ============================================================================
# All settings are collected here in one place, not scattered through the code.
# If anything changes (a URL, a database name, a delay), you change it here
# and nowhere else. This is the "separation of configuration from logic"
# principle from Section 14.2 of the Concept Paper.

# --- Database connection settings ---
# These match the PostgreSQL setup from Phase Zero.
# 'dbname' is the database we created with 'createdb reconomania'.
# 'host' is 'localhost' because PostgreSQL runs on the same machine.
# In a production deployment, these would come from environment variables
# (never hardcoded), but for local development this is fine.
DB_CONFIG = {
    "dbname": "reconomania",
    # If your PostgreSQL requires a password, add it here:
    # "password": "your_password",
    # If your PostgreSQL user is different from your Linux username, add:
    # "user": "your_username",
}

# --- BNR URL patterns ---
# The yearly archive URL follows a predictable pattern: replace {year} with
# the actual year (2005, 2006, ..., 2026).
BNR_YEARLY_URL = "https://www.bnr.ro/files/xml/years/nbrfxrates{year}.xml"

# The "last 10 business days" feed. Used for daily updates because it provides
# a safety buffer: if we miss a day, we still pick up the gap.
BNR_RECENT_URL = "https://www.bnr.ro/nbrfxrates10days.xml"

# --- Scraper behaviour settings ---
# The series_id must match exactly what we inserted into the time_series table
# in 001_create_schema.sql. This string is the link between the scraper and
# the metadata registry.
SERIES_ID = "bnr_eurron_daily"

# Backfill range: the first year of available data, and the current year.
BACKFILL_START_YEAR = 2005
BACKFILL_END_YEAR = datetime.now().year  # Automatically uses the current year

# Polite delay between HTTP requests, in seconds.
# The Concept Paper (Section 7.4) requires us to mimic human browsing.
# 2 seconds between requests is respectful — a human wouldn't click faster.
REQUEST_DELAY_SECONDS = 2

# User-Agent string: identifies our scraper to BNR's server.
# The Concept Paper (Section 7.4) requires transparent identification.
# This tells BNR exactly who is making the request and how to contact us.
USER_AGENT = "RECONOMANIA data aggregator (reconomania.com) - contact@reconomania.com"

# Directory where raw XML files are archived (the "unalterable receipt").
# Relative to the project directory.
ARCHIVE_DIR = "archive/bnr"

# BNR's XML uses a namespace — a technical XML detail. All elements in the
# file are prefixed with this namespace URI. We need to include it in every
# XML query, otherwise the parser won't find anything. Think of it like a
# surname: the element's full name is '{http://www.bnr.ro/xsd}Rate', not
# just 'Rate'.
BNR_XML_NAMESPACE = {"ns": "http://www.bnr.ro/xsd"}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
# Small, reusable pieces of logic. Each function does one thing well.
# Breaking code into functions makes it easier to understand, test, and debug.


def ensure_archive_dir():
    """
    Creates the archive directory if it doesn't already exist.

    os.makedirs() creates a directory and any parent directories needed.
    'exist_ok=True' means "don't throw an error if the directory already exists."

    After this runs, the path 'archive/bnr/' is guaranteed to exist.
    """
    os.makedirs(ARCHIVE_DIR, exist_ok=True)


def get_db_connection():
    """
    Opens a connection to the PostgreSQL database and returns it.

    A database connection is like opening a phone line to the database server.
    Once open, you can send SQL commands through it. You should close it when
    you're done (we handle this in the main functions).

    psycopg2.connect() uses the settings from DB_CONFIG to find and
    authenticate with the database.

    Returns:
        A psycopg2 connection object.

    Raises:
        psycopg2.OperationalError if the database is unreachable.
    """
    return psycopg2.connect(**DB_CONFIG)
    # The **DB_CONFIG syntax "unpacks" the dictionary, so this is equivalent to:
    # psycopg2.connect(dbname="reconomania", host="localhost")
    # Using a dictionary makes it easy to add or change settings in one place.


def fetch_xml(url):
    """
    Downloads an XML file from the given URL and returns its content as text.

    This is the function that actually talks to BNR's server. It sends an
    HTTP GET request (the same thing your browser does when you visit a URL)
    and receives the XML file content in response.

    Args:
        url: The full URL to fetch (e.g., 'https://www.bnr.ro/nbrfxrates.xml').

    Returns:
        The XML content as a string, or None if the request failed.

    The function handles errors gracefully: if BNR's server is down, returns
    an HTTP error, or anything else goes wrong, it prints a warning and
    returns None instead of crashing the entire scraper.
    """
    # 'headers' is metadata sent along with the request. The User-Agent header
    # tells the server who is making the request. Without it, Python's requests
    # library sends a generic identifier. Our custom User-Agent follows the
    # Concept Paper's requirement for transparent identification.
    headers = {"User-Agent": USER_AGENT}

    try:
        # requests.get() sends an HTTP GET request to the URL.
        # 'timeout=30' means: if the server doesn't respond within 30 seconds,
        # give up. Without a timeout, a request could hang forever.
        response = requests.get(url, headers=headers, timeout=30)

        # raise_for_status() checks the HTTP status code. Servers respond with
        # codes: 200 = success, 404 = not found, 500 = server error, etc.
        # If the code indicates an error, this method raises an exception
        # (jumps to the 'except' block below).
        response.raise_for_status()

        # If we reach here, the request succeeded. Return the XML text.
        return response.text

    except requests.exceptions.RequestException as e:
        # 'RequestException' is the umbrella error type for anything that can
        # go wrong with an HTTP request: network errors, timeouts, bad status
        # codes, etc. We catch it broadly because the response is the same
        # regardless of the specific failure: log it and move on.
        print(f"  [WARNING] Failed to fetch {url}: {e}")
        return None


def archive_xml(xml_text, source_filename):
    """
    Saves the raw XML content to the archive directory.

    This is the "unalterable receipt" from Section 7.2 of the Concept Paper.
    We save the exact XML that BNR served to us, byte for byte, before we
    parse or transform it in any way. If our parser has a bug, or if BNR
    changes their format, we can always go back to the original file.

    The filename includes a timestamp so that multiple downloads of the same
    source (e.g., nbrfxrates.xml fetched on different days) don't overwrite
    each other.

    Args:
        xml_text: The raw XML content as a string.
        source_filename: A human-readable name for the file (e.g., 'nbrfxrates2024.xml').

    Returns:
        The full path where the file was saved.
    """
    ensure_archive_dir()

    # Generate a timestamp string like '20260316T143022' (date T time).
    # This ensures unique filenames even if we fetch the same source twice.
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")

    # Build the filename: 'nbrfxrates2024_20260316T143022.xml'
    # os.path.splitext() splits 'nbrfxrates2024.xml' into ('nbrfxrates2024', '.xml')
    # so we can insert the timestamp before the extension.
    name, ext = os.path.splitext(source_filename)
    archive_filename = f"{name}_{timestamp}{ext}"
    archive_path = os.path.join(ARCHIVE_DIR, archive_filename)

    # Write the XML text to a file.
    # 'with open(...) as f' is Python's safe way to work with files.
    # It guarantees the file is properly closed even if an error occurs.
    # 'w' means write mode; 'encoding="utf-8"' ensures characters are
    # stored correctly (BNR's XML is UTF-8 encoded).
    with open(archive_path, "w", encoding="utf-8") as f:
        f.write(xml_text)

    return archive_path


def parse_eurron_from_xml(xml_text):
    """
    Extracts EUR/RON exchange rates from a BNR XML string.

    This is the core parsing logic — the function that understands BNR's
    XML structure and pulls out the specific data we need.

    BNR's XML structure (simplified):
        <DataSet xmlns="http://www.bnr.ro/xsd">
          <Body>
            <Cube date="2025-03-14">
              <Rate currency="EUR">4.9737</Rate>
              ... (other currencies)
            </Cube>
            <Cube date="2025-03-13">
              <Rate currency="EUR">4.9741</Rate>
              ...
            </Cube>
          </Body>
        </DataSet>

    Each <Cube> represents one business day. Inside each Cube, each <Rate>
    element has a 'currency' attribute and the exchange rate as text content.
    We want only the Rate where currency="EUR".

    Args:
        xml_text: The raw XML content as a string.

    Returns:
        A list of tuples, each containing (date_string, Decimal_value).
        Example: [('2025-03-14', Decimal('4.9737')), ('2025-03-13', Decimal('4.9741'))]
        Returns an empty list if parsing fails.
    """
    results = []

    try:
        # ET.fromstring() parses the XML text into a tree structure that we
        # can navigate programmatically. Think of it as converting raw text
        # into an organised, searchable structure.
        root = ET.fromstring(xml_text)

        # Find all <Cube> elements. The './/ns:Cube' syntax means:
        # '..'  = search anywhere in the tree (not just top-level)
        # 'ns:' = use the namespace prefix we defined in BNR_XML_NAMESPACE
        # 'Cube' = the element name
        #
        # Without the namespace prefix, this would find nothing — BNR's XML
        # elements all belong to the 'http://www.bnr.ro/xsd' namespace.
        cubes = root.findall(".//ns:Cube", BNR_XML_NAMESPACE)

        for cube in cubes:
            # Each Cube has a 'date' attribute: the business day this rate applies to.
            date_str = cube.get("date")  # e.g., '2025-03-14'

            if date_str is None:
                # Safety check: skip any Cube without a date (shouldn't happen,
                # but defensive coding prevents crashes on unexpected input).
                continue

            # Inside this Cube, find all <Rate> elements.
            rates = cube.findall("ns:Rate", BNR_XML_NAMESPACE)

            for rate in rates:
                # Check if this Rate element is for EUR.
                if rate.get("currency") == "EUR":
                    # rate.text is the text content of the element: '4.9737'
                    # We convert it to a Decimal for exact precision.
                    # Decimal('4.9737') is exactly 4.9737, no floating-point tricks.
                    value = Decimal(rate.text)
                    results.append((date_str, value))
                    # Each Cube has at most one EUR rate, so we can stop
                    # searching this Cube's rates after finding it.
                    break

    except ET.ParseError as e:
        # If the XML is malformed (corrupted download, server error returning
        # HTML instead of XML), this catches the error gracefully.
        print(f"  [ERROR] XML parsing failed: {e}")

    return results


def store_data_points(connection, data_points, source_file):
    """
    Inserts exchange rate data points into the database.

    This function takes the parsed data (a list of date-value pairs) and
    writes each one to the data_points table. It skips any data points that
    already exist in the database, making it safe to run multiple times
    on the same data.

    Args:
        connection: An open psycopg2 database connection.
        data_points: A list of (date_string, Decimal_value) tuples from
                     parse_eurron_from_xml().
        source_file: The name of the XML file this data came from,
                     for the audit trail.

    Returns:
        A tuple of (new_count, skipped_count) — how many records were
        inserted vs. how many already existed.
    """
    # A cursor is the object you use to execute SQL commands on a connection.
    # Think of the connection as the phone line, and the cursor as the
    # conversation happening on that line.
    cursor = connection.cursor()

    new_count = 0
    skipped_count = 0

    for date_str, value in data_points:
        try:
            # This SQL statement inserts a new row into data_points.
            #
            # The %s placeholders are filled in by psycopg2 with the values
            # from the tuple at the end. NEVER build SQL by concatenating
            # strings (like f"... VALUES ('{date_str}', ...)") — that creates
            # a security vulnerability called SQL injection. Always use
            # parameterised queries (%s placeholders).
            #
            # ON CONFLICT DO NOTHING: if a row with the same (series_id,
            # observation_date, recorded_at) already exists, skip this insert
            # instead of throwing an error. This makes the function idempotent
            # (safe to run multiple times with the same data).
            #
            # Note: recorded_at uses DEFAULT NOW(), so all rows from a single
            # scraper run share the same timestamp (approximately). This is
            # correct — they were all recorded during the same scraping event.
            cursor.execute(
                """
                INSERT INTO data_points (series_id, observation_date, value, source_file)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (series_id, observation_date, recorded_at) DO NOTHING
                """,
                (SERIES_ID, date_str, value, source_file),
            )

            # cursor.rowcount tells us how many rows were affected by the last
            # SQL command. For an INSERT, it's 1 if a row was inserted, 0 if
            # ON CONFLICT triggered (the row already existed).
            if cursor.rowcount == 1:
                new_count += 1
            else:
                skipped_count += 1

        except Exception as e:
            # If a single data point fails (e.g., invalid date format),
            # log it and continue with the rest. Don't let one bad row
            # abort the entire batch.
            print(f"  [ERROR] Failed to insert {date_str}: {e}")
            # Rollback the failed transaction so we can continue.
            connection.rollback()

    # commit() saves all the inserts to the database permanently.
    # Without this, the data would be discarded when the connection closes.
    # Think of it as pressing "Save" after editing a document.
    connection.commit()
    cursor.close()

    return new_count, skipped_count


def log_scrape(connection, status, records_fetched, records_new,
               records_updated, error_message, source_file_archived,
               started_at):
    """
    Records the outcome of a scraper run in the scrape_log table.

    Every run — success or failure — gets logged. This is the operational
    audit trail from Section 11 of the Concept Paper.

    Args:
        connection: An open psycopg2 database connection.
        status: 'success', 'failure', or 'partial'.
        records_fetched: Total number of data points parsed from the source.
        records_new: How many were genuinely new (not already in the database).
        records_updated: How many existing records were updated (for exchange
                        rates, this should always be 0).
        error_message: The error message if status is 'failure', else None.
        source_file_archived: Path to the archived raw XML file.
        started_at: When the scraper run started (datetime object).
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
            SERIES_ID,
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
# MAIN OPERATIONS
# ============================================================================
# These are the two high-level operations: backfill and daily update.
# Each orchestrates the helper functions above into a complete workflow.


def run_backfill():
    """
    Downloads all historical EUR/RON data from 2005 to the current year.

    This is the initial data load — run it once when setting up the system.
    It fetches one XML file per year from BNR's archive, parses each one,
    and loads all the data points into the database.

    The function is polite to BNR's server: it waits between requests and
    identifies itself clearly (see REQUEST_DELAY_SECONDS and USER_AGENT).

    Safe to run multiple times: existing data points are skipped, not
    duplicated.
    """
    print("=" * 70)
    print("RECONOMANIA — BNR EUR/RON Backfill")
    print(f"Fetching yearly archives from {BACKFILL_START_YEAR} to {BACKFILL_END_YEAR}")
    print("=" * 70)

    started_at = datetime.now(timezone.utc)
    total_fetched = 0
    total_new = 0
    total_skipped = 0
    errors = []

    # Open one database connection for the entire backfill.
    # Opening/closing connections is expensive; reusing one is efficient.
    conn = get_db_connection()

    try:
        # Loop through each year from 2005 to the current year (inclusive).
        # range() in Python excludes the end value, so we add 1.
        for year in range(BACKFILL_START_YEAR, BACKFILL_END_YEAR + 1):

            # Build the URL for this year's archive file.
            # .format() replaces {year} with the actual year number.
            url = BNR_YEARLY_URL.format(year=year)
            source_filename = f"nbrfxrates{year}.xml"

            print(f"\n[{year}] Fetching {source_filename}...")

            # --- Step 1: Download the XML ---
            xml_text = fetch_xml(url)
            if xml_text is None:
                # fetch_xml already printed a warning. Record the error
                # and move to the next year.
                errors.append(f"{year}: failed to download")
                continue

            # --- Step 2: Archive the raw XML ---
            archive_path = archive_xml(xml_text, source_filename)
            print(f"  Archived to: {archive_path}")

            # --- Step 3: Parse the XML to extract EUR/RON rates ---
            data_points = parse_eurron_from_xml(xml_text)
            print(f"  Parsed {len(data_points)} EUR/RON rates")
            total_fetched += len(data_points)

            # --- Step 4: Store in the database ---
            new_count, skipped_count = store_data_points(
                conn, data_points, source_filename
            )
            total_new += new_count
            total_skipped += skipped_count
            print(f"  Inserted: {new_count} new, {skipped_count} already existed")

            # --- Step 5: Polite pause before the next request ---
            # We don't pause after the last year (no next request to delay).
            if year < BACKFILL_END_YEAR:
                print(f"  Waiting {REQUEST_DELAY_SECONDS}s before next request...")
                time.sleep(REQUEST_DELAY_SECONDS)

        # --- Backfill complete: log the result ---
        status = "success" if not errors else "partial"
        error_msg = "; ".join(errors) if errors else None

        log_scrape(
            connection=conn,
            status=status,
            records_fetched=total_fetched,
            records_new=total_new,
            records_updated=0,
            error_message=error_msg,
            source_file_archived=ARCHIVE_DIR,
            started_at=started_at,
        )

        # Update the last_updated timestamp in the time_series metadata.
        if total_new > 0:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE time_series SET last_updated = NOW() WHERE series_id = %s",
                (SERIES_ID,),
            )
            conn.commit()
            cursor.close()

        # --- Print summary ---
        print("\n" + "=" * 70)
        print("BACKFILL COMPLETE")
        print(f"  Total data points parsed:  {total_fetched}")
        print(f"  New records inserted:      {total_new}")
        print(f"  Already existed (skipped): {total_skipped}")
        if errors:
            print(f"  Errors: {len(errors)}")
            for err in errors:
                print(f"    - {err}")
        print("=" * 70)

    except Exception as e:
        # If something unexpected goes wrong (database crash, etc.),
        # log the failure and re-raise the error so we see the full traceback.
        print(f"\n[FATAL ERROR] {e}")
        log_scrape(
            connection=conn,
            status="failure",
            records_fetched=total_fetched,
            records_new=total_new,
            records_updated=0,
            error_message=str(e),
            source_file_archived=ARCHIVE_DIR,
            started_at=started_at,
        )
        raise

    finally:
        # 'finally' runs no matter what — even if an error occurred.
        # Always close the database connection to free resources.
        conn.close()


def run_update():
    """
    Fetches the most recent EUR/RON rates and adds any new ones to the database.

    This is the daily operation. It downloads BNR's "last 10 business days"
    feed rather than just today's single rate. Why? Because if we miss a day
    (server down, forgot to run it, weekend), the 10-day feed fills the gap
    automatically. The ON CONFLICT DO NOTHING clause in store_data_points()
    means rates we already have are silently skipped — no duplicates.
    """
    print("=" * 70)
    print("RECONOMANIA — BNR EUR/RON Daily Update")
    print("=" * 70)

    started_at = datetime.now(timezone.utc)

    conn = get_db_connection()

    try:
        # --- Step 1: Download the 10-day XML feed ---
        print("\nFetching recent rates from BNR...")
        xml_text = fetch_xml(BNR_RECENT_URL)

        if xml_text is None:
            log_scrape(
                connection=conn,
                status="failure",
                records_fetched=0,
                records_new=0,
                records_updated=0,
                error_message="Failed to download BNR 10-day feed",
                source_file_archived=None,
                started_at=started_at,
            )
            print("[FAILED] Could not download data from BNR.")
            return

        # --- Step 2: Archive ---
        archive_path = archive_xml(xml_text, "nbrfxrates10days.xml")
        print(f"  Archived to: {archive_path}")

        # --- Step 3: Parse ---
        data_points = parse_eurron_from_xml(xml_text)
        print(f"  Parsed {len(data_points)} EUR/RON rates")

        # --- Step 4: Store ---
        new_count, skipped_count = store_data_points(
            conn, data_points, "nbrfxrates10days.xml"
        )

        # --- Step 5: Log ---
        log_scrape(
            connection=conn,
            status="success",
            records_fetched=len(data_points),
            records_new=new_count,
            records_updated=0,
            error_message=None,
            source_file_archived=archive_path,
            started_at=started_at,
        )

        # Update last_updated timestamp if we got new data.
        if new_count > 0:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE time_series SET last_updated = NOW() WHERE series_id = %s",
                (SERIES_ID,),
            )
            conn.commit()
            cursor.close()

        # --- Print summary ---
        print(f"\n  New records inserted:      {new_count}")
        print(f"  Already existed (skipped): {skipped_count}")
        print("  Done.")

    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        log_scrape(
            connection=conn,
            status="failure",
            records_fetched=0,
            records_new=0,
            records_updated=0,
            error_message=str(e),
            source_file_archived=None,
            started_at=started_at,
        )
        raise

    finally:
        conn.close()


# ============================================================================
# ENTRY POINT
# ============================================================================
# This block runs only when the script is executed directly (not when it's
# imported as a module by another script). It reads the command-line arguments
# to determine whether to run a backfill or a daily update.
#
# Usage:
#   python scraper_bnr_eurron.py --backfill    (initial historical load)
#   python scraper_bnr_eurron.py --update      (daily incremental update)

if __name__ == "__main__":
    # argparse builds a command-line interface for the script.
    # After this setup, running 'python scraper_bnr_eurron.py --help' will
    # display usage instructions automatically.
    parser = argparse.ArgumentParser(
        description="RECONOMANIA — BNR EUR/RON Exchange Rate Scraper"
    )

    # 'add_mutually_exclusive_group' means the user must pick one of these
    # options, not both. You either backfill or update, not both at once.
    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument(
        "--backfill",
        action="store_true",  # This means: if the flag is present, set it to True
        help="Download all historical data from 2005 to present",
    )

    group.add_argument(
        "--update",
        action="store_true",
        help="Fetch the latest rates (last 10 business days)",
    )

    # Parse the command-line arguments.
    args = parser.parse_args()

    # Run the appropriate function based on which flag was provided.
    if args.backfill:
        run_backfill()
    elif args.update:
        run_update()
