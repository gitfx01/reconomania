"""
============================================================================
RECONOMANIA — Derived Series: Private Sector Loans
============================================================================

FILE PURPOSE:
    Computes derived series from raw loan balance data and stores them
    in the database as regular time series.

    This script runs AFTER scraper_bnr_interactive_loans.py has loaded
    the raw series.

DERIVED SERIES:
    - bnr_loans_private_total:  Household + Corporate + NBFI loans
      Formula: bnr_loans_households + bnr_loans_corporates + bnr_loans_nbfi

    - bnr_loans_private_yoy:    Year-on-year growth rate of total private loans
      Formula: (total_t / total_t-12 - 1) * 100
      Starts 12 months after the earliest data point.

DESIGN NOTE:
    Derived series are stored in data_points just like raw series. The
    frontend and API don't know or care whether a series is raw or derived.
    This is intentional — it keeps the query layer simple.

    The derivation_source field in time_series (planned) will eventually
    record the formula for traceability. For now, this script IS the
    documentation.

HOW TO RUN:
    python derive_private_loans.py

    Safe to run multiple times — ON CONFLICT DO NOTHING prevents duplicates.
    Run after every update of the raw loan series.

DATE:        March 2026
PHASE:       Phase Two
============================================================================
"""

import psycopg2
import psycopg2.extras
import sys
from datetime import datetime, timezone, date, timedelta
from decimal import Decimal

# ============================================================================
# CONFIGURATION
# ============================================================================

DB_CONFIG = {"dbname": "reconomania"}

# Input series (must exist in data_points)
INPUT_SERIES = [
    "bnr_loans_households",
    "bnr_loans_corporates",
    "bnr_loans_nbfi",
]

# Output series
TOTAL_SERIES = {
    "series_id": "bnr_loans_private_total",
    "name": "Total private sector loans — balance",
    "description": (
        "Total outstanding loan balance to the private sector (residents): "
        "households + non-financial corporations + non-monetary financial "
        "institutions. Derived series: sum of bnr_loans_households, "
        "bnr_loans_corporates, and bnr_loans_nbfi."
    ),
    "units": "RON thousands",
    "chart_colour": "#0F3B5C",
    "topic_path": "Banking/Loans/Total",
}

YOY_SERIES = {
    "series_id": "bnr_loans_private_yoy",
    "name": "Total private sector loans — Y-o-Y growth",
    "description": (
        "Year-on-year growth rate of total private sector loans. "
        "Calculated as (total_t / total_t-12 - 1) × 100. "
        "Derived from bnr_loans_private_total."
    ),
    "units": "percent",
    "chart_colour": "#2D8C5A",
    "topic_path": "Banking/Loans/Growth",
}


# ============================================================================
# DATABASE
# ============================================================================

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)


def ensure_series_registered(conn, cfg):
    cursor = conn.cursor()
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
            "RECONOMANIA (derived)", None,
            "monthly", "end_of_period", cfg["units"],
            "Computed after raw data update",
            date(2007, 1, 1),
            cfg["chart_colour"], cfg["topic_path"],
        ),
    )
    conn.commit()
    cursor.close()


def get_latest_values(conn, series_id):
    """
    Get the most recent version of each data point for a series.
    Returns a dict: {observation_date: value}
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT DISTINCT ON (observation_date)
            observation_date, value
        FROM data_points
        WHERE series_id = %s
        ORDER BY observation_date, recorded_at DESC
        """,
        (series_id,),
    )
    rows = cursor.fetchall()
    cursor.close()
    return {row["observation_date"]: float(row["value"]) for row in rows}


def store_derived(conn, series_id, data_points):
    """Store derived data points. Returns (new_count, skipped_count)."""
    cursor = conn.cursor()
    new_count = 0
    skipped = 0

    for dp in data_points:
        try:
            cursor.execute(
                """
                INSERT INTO data_points (series_id, observation_date, value, source_file)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (series_id, observation_date, recorded_at) DO NOTHING
                """,
                (series_id, dp["date"], Decimal(str(round(dp["value"], 2))),
                 "derived"),
            )
            if cursor.rowcount == 1:
                new_count += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"    [ERROR] {series_id} {dp['date']}: {e}")
            conn.rollback()

    conn.commit()
    cursor.close()
    return new_count, skipped


# ============================================================================
# DERIVATION LOGIC
# ============================================================================

def compute_total(conn):
    """
    Sum the three input series for each month.
    Only includes months where ALL three inputs have data.
    """
    print("Loading input series...")
    inputs = {}
    for sid in INPUT_SERIES:
        data = get_latest_values(conn, sid)
        inputs[sid] = data
        print(f"  {sid}: {len(data)} data points")

    # Find months where all three have data
    all_dates = set(inputs[INPUT_SERIES[0]].keys())
    for sid in INPUT_SERIES[1:]:
        all_dates &= set(inputs[sid].keys())

    all_dates = sorted(all_dates)
    print(f"  Months with all inputs: {len(all_dates)}")

    # Compute total
    totals = []
    for d in all_dates:
        total = sum(inputs[sid][d] for sid in INPUT_SERIES)
        totals.append({"date": d, "value": total})

    return totals


def compute_yoy(totals):
    """
    Compute year-on-year growth from the total series.
    For each month, find the value 12 months prior and calculate
    percentage change.
    """
    # Build a lookup by date
    by_date = {dp["date"]: dp["value"] for dp in totals}

    yoy = []
    for dp in totals:
        d = dp["date"]
        # Find same month, previous year
        if d.month == 2 and d.day == 29:
            prev = date(d.year - 1, 2, 28)
        else:
            try:
                prev = d.replace(year=d.year - 1)
            except ValueError:
                continue

        if prev in by_date and by_date[prev] != 0:
            growth = (dp["value"] / by_date[prev] - 1) * 100
            yoy.append({"date": d, "value": round(growth, 2)})

    return yoy


# ============================================================================
# MAIN
# ============================================================================

def run():
    print("=" * 70)
    print("DERIVED SERIES: PRIVATE SECTOR LOANS")
    print("=" * 70)

    conn = get_db_connection()

    try:
        # Register derived series
        ensure_series_registered(conn, TOTAL_SERIES)
        ensure_series_registered(conn, YOY_SERIES)
        print(f"  Series registered: {TOTAL_SERIES['series_id']}, {YOY_SERIES['series_id']}")
        print()

        # Step 1: Compute total
        print("Computing total private sector loans...")
        totals = compute_total(conn)
        new, skipped = store_derived(conn, TOTAL_SERIES["series_id"], totals)
        print(f"  Stored: {new} new, {skipped} existing")
        print()

        # Step 2: Compute Y-o-Y growth
        print("Computing Y-o-Y growth...")
        yoy = compute_yoy(totals)
        new, skipped = store_derived(conn, YOY_SERIES["series_id"], yoy)
        print(f"  Stored: {new} new, {skipped} existing")
        print(f"  (First Y-o-Y point: {yoy[0]['date']} = {yoy[0]['value']}%)" if yoy else "")
        print()

        print("Done.")

    finally:
        conn.close()


if __name__ == "__main__":
    run()
