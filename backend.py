"""
============================================================================
RECONOMANIA — Backend API (Phase One)
============================================================================

FILE PURPOSE:
    This is the backend web server for RECONOMANIA. It sits between the
    database (where the data lives) and the frontend (what users see in
    their browser). When the frontend needs data to draw a chart, it sends
    a request to this server, which queries the database and sends back
    the data as JSON.

    Think of it as a waiter in a restaurant:
    - The frontend (customer) asks: "I'd like the EUR/RON data, please."
    - The backend (waiter) goes to the database (kitchen) and fetches it.
    - The backend brings it back in a format the frontend can consume (JSON).

WHAT THIS FILE PROVIDES:
    1. GET /api/series/{series_id}
       Returns the data points for a given time series.
       Optional query parameters: start_date, end_date (to filter by date range).
       Example: /api/series/bnr_eurron_daily?start_date=2024-01-01

    2. GET /api/series
       Returns a list of all available time series (their metadata).
       In Phase One, this returns just one series (EUR/RON).

    3. Serves the frontend (HTML, CSS, JS files) as static files.
       When someone visits http://localhost:8000/ in their browser, the
       backend serves the frontend page.

HOW TO RUN:
    From the project directory (~/reconomania), with the virtual environment
    activated:

        uvicorn backend:app --reload

    Then open http://localhost:8000 in your browser.

    - 'uvicorn' is the server that runs the application.
    - 'backend:app' means "in the file called backend.py, find the object
      called 'app'" — that's the FastAPI application defined below.
    - '--reload' means "watch for file changes and restart automatically."
      Useful during development: edit code, save, and the server restarts
      itself. Do NOT use --reload in production.

WHAT TO DO IF IT BREAKS:
    - "Address already in use": another process is using port 8000. Either
      stop that process or run on a different port:
          uvicorn backend:app --reload --port 8001
    - "ModuleNotFoundError: No module named 'fastapi'": your virtual
      environment isn't activated. Run: source venv/bin/activate
    - Database connection errors: make sure PostgreSQL is running:
          sudo service postgresql start

DATE:        March 2026
PHASE:       Phase One
============================================================================
"""

# ============================================================================
# IMPORTS
# ============================================================================

# FastAPI — the web framework. It provides the tools to define API endpoints
# (URLs that return data) and handle HTTP requests/responses.
# We installed this with 'pip install fastapi'.
from fastapi import FastAPI, HTTPException, Query

# StaticFiles — serves files (HTML, CSS, JS) directly to the browser.
# This is how the frontend gets delivered: the browser requests 'index.html',
# and this middleware sends the file.
from fastapi.staticfiles import StaticFiles

# FileResponse — sends a single file as the response. We use this to serve
# the main index.html page when someone visits the root URL (/).
from fastapi.responses import FileResponse

# psycopg2 — connects Python to PostgreSQL (same library the scraper uses).
import psycopg2

# psycopg2.extras — provides RealDictCursor, which returns database rows as
# Python dictionaries (key-value pairs) instead of plain tuples. This makes
# the code much more readable: row['value'] instead of row[3].
import psycopg2.extras

# datetime.date — represents a calendar date (year, month, day) without a
# time component. Used for the date range query parameters.
from datetime import date

# os — for file path operations (checking if the frontend directory exists).
import os

# typing.Optional — a type hint that means "this value can be the specified
# type OR None." Used for optional query parameters.
from typing import Optional

# decimal.Decimal — Python's exact decimal type. PostgreSQL's NUMERIC columns
# return Decimal objects in Python. We need to convert these to floats for
# JSON serialisation (JSON doesn't have a Decimal type).
from decimal import Decimal


# ============================================================================
# CONFIGURATION
# ============================================================================

# Database connection settings — identical to the scraper's config.
# In a larger project, this would be in a shared config file imported by both
# the scraper and the backend. For Phase One, duplication is acceptable.
DB_CONFIG = {
    "dbname": "reconomania",
    # No host specified = Unix socket connection (same as the scraper fix).
}

# Path to the frontend files (HTML, CSS, JS).
# This directory will contain index.html and any other static assets.
# We'll create this in Step 4.
FRONTEND_DIR = "frontend"


# ============================================================================
# APPLICATION SETUP
# ============================================================================

# Create the FastAPI application instance. This is the central object that
# everything attaches to. The 'title' and 'version' appear in the automatic
# API documentation that FastAPI generates (available at /docs).
app = FastAPI(
    title="RECONOMANIA API",
    version="0.1.0",
    description="Financial Reconnaissance for Romania — Data API",
)


# ============================================================================
# DATABASE HELPER
# ============================================================================

def get_db_connection():
    """
    Opens and returns a database connection.

    Uses RealDictCursor so that query results come back as dictionaries.
    Instead of accessing columns by position (row[0], row[1]), we can use
    names (row['observation_date'], row['value']), which is clearer.

    This function is called for each API request. In a high-traffic
    application, you'd use a "connection pool" (a set of pre-opened
    connections that get reused). For Phase One traffic (just you), opening
    a fresh connection per request is perfectly fine.

    Returns:
        A psycopg2 connection object with RealDictCursor as the default cursor.
    """
    return psycopg2.connect(
        **DB_CONFIG,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


# ============================================================================
# HELPER: Convert database rows to JSON-safe format
# ============================================================================

def make_json_safe(row):
    """
    Converts a database row (dictionary) into a format that can be serialised
    as JSON.

    The problem: PostgreSQL returns some Python types that JSON doesn't
    understand natively:
    - Decimal objects (from NUMERIC columns) → must become float
    - date objects → must become strings ("2025-03-14")
    - datetime objects → must become strings ("2025-03-14T12:00:00+00:00")

    This function walks through each value in the row and converts any
    problematic types to JSON-compatible equivalents.

    Args:
        row: A dictionary representing one database row.

    Returns:
        A new dictionary with all values converted to JSON-safe types.
    """
    safe = {}
    for key, val in row.items():
        if isinstance(val, Decimal):
            # Convert Decimal to float. We lose the "exact precision"
            # guarantee here, but JSON doesn't support Decimal, and for
            # display purposes (charts), float precision is sufficient.
            # The database still stores the exact value.
            safe[key] = float(val)
        elif isinstance(val, (date,)):
            # Convert date to ISO 8601 string: "2025-03-14"
            safe[key] = val.isoformat()
        else:
            # Everything else (strings, integers, None) is already JSON-safe.
            safe[key] = val
    return safe


# ============================================================================
# API ENDPOINTS
# ============================================================================
# Each endpoint is a Python function decorated with @app.get() or @app.post().
# The decorator tells FastAPI: "when someone sends a GET request to this URL
# path, run this function and return its result as the HTTP response."
#
# FastAPI automatically:
# - Converts the return value to JSON
# - Generates API documentation at /docs (try it!)
# - Validates query parameters and returns clear error messages


@app.get("/api/series")
def list_series():
    """
    Returns a list of all available time series with their metadata.

    This endpoint answers the question: "What data does RECONOMANIA have?"
    In Phase One, the answer is just one series (EUR/RON). As we add more
    scrapers, this list grows automatically — no code changes needed here.

    URL: GET /api/series
    Response: JSON array of time series metadata objects.
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        # Fetch all rows from the time_series table.
        # ORDER BY series_id ensures consistent ordering.
        cursor.execute(
            "SELECT * FROM time_series ORDER BY series_id"
        )
        rows = cursor.fetchall()

        # Convert each row to a JSON-safe dictionary.
        # fetchall() returns a list of RealDictRow objects (one per row).
        result = [make_json_safe(dict(row)) for row in rows]

        return {"series": result}

    finally:
        # Always close the connection, even if an error occurs.
        conn.close()


@app.get("/api/series/batch")
def get_batch_series(
    ids: str = Query(
        description="Comma-separated list of series IDs to fetch",
    ),
):
    """
    Returns data for multiple time series in one response.
    Used by the interactive loan chart to fetch all 27 loan series at once.

    URL: GET /api/series/batch?ids=bnr_ifmcl_g,bnr_ifmcl_s,bnr_ifmcl_i
    Response: JSON object keyed by series_id, each with date-value arrays.
    """
    series_ids = [s.strip() for s in ids.split(",") if s.strip()]

    if not series_ids:
        raise HTTPException(status_code=400, detail="No series IDs provided")
    if len(series_ids) > 50:
        raise HTTPException(status_code=400, detail="Maximum 50 series per request")

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        result = {}

        for series_id in series_ids:
            cursor.execute(
                """
                SELECT DISTINCT ON (observation_date)
                    observation_date, value
                FROM data_points
                WHERE series_id = %s
                ORDER BY observation_date ASC, recorded_at DESC
                """,
                (series_id,),
            )
            rows = cursor.fetchall()
            result[series_id] = [
                {"date": row["observation_date"].isoformat(), "value": float(row["value"])}
                for row in rows
            ]

        return {"series": result}

    finally:
        conn.close()


@app.get("/api/series/{series_id}")
def get_series_data(
    series_id: str,
    start_date: Optional[date] = Query(
        default=None,
        description="Filter: only return data points on or after this date (YYYY-MM-DD)",
    ),
    end_date: Optional[date] = Query(
        default=None,
        description="Filter: only return data points on or before this date (YYYY-MM-DD)",
    ),
):
    """
    Returns the data points for a specific time series.

    This is the main endpoint — the one the chart will call. It queries the
    database for all data points belonging to the requested series, optionally
    filtered by a date range.

    URL: GET /api/series/{series_id}
    Path parameter:
        series_id — the unique identifier (e.g., 'bnr_eurron_daily')
    Query parameters (optional):
        start_date — earliest date to include (e.g., ?start_date=2024-01-01)
        end_date   — latest date to include (e.g., ?end_date=2024-12-31)

    Response: JSON object with series metadata and an array of data points.

    Example request:
        /api/series/bnr_eurron_daily?start_date=2024-01-01&end_date=2024-12-31

    Example response:
        {
            "series_id": "bnr_eurron_daily",
            "name": "EUR/RON Reference Exchange Rate",
            "units": "RON per 1 EUR",
            "data": [
                {"date": "2024-01-02", "value": 4.9737},
                {"date": "2024-01-03", "value": 4.9741},
                ...
            ]
        }
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        # --- Step 1: Verify the series exists ---
        # Before querying data points, check that the requested series_id
        # actually exists in the time_series table. If it doesn't, return
        # a 404 "Not Found" error.
        cursor.execute(
            "SELECT * FROM time_series WHERE series_id = %s",
            (series_id,),
        )
        series_meta = cursor.fetchone()

        if series_meta is None:
            # HTTPException with status_code=404 tells the browser (or the
            # frontend JavaScript) that the requested resource doesn't exist.
            # This is the standard HTTP way to say "I don't have that."
            raise HTTPException(
                status_code=404,
                detail=f"Series '{series_id}' not found",
            )

        # --- Step 2: Build the data query ---
        # We start with a base query and conditionally add WHERE clauses
        # for the date filters. This avoids writing multiple nearly-identical
        # queries for each combination of filters.
        #
        # The DISTINCT ON clause is PostgreSQL-specific and implements our
        # data versioning strategy: for each observation_date, it returns
        # only the row with the most recent recorded_at. This means if BNR
        # ever revises a historical rate, we automatically serve the latest
        # version without any special logic.
        #
        # How DISTINCT ON works:
        # - "DISTINCT ON (observation_date)" = group by observation_date
        # - "ORDER BY observation_date, recorded_at DESC" = within each group,
        #   sort by recorded_at descending (newest first)
        # - PostgreSQL picks the first row from each group = the newest version

        # We build the query dynamically using a list of conditions and
        # parameters. This is a safe pattern for conditional filtering.
        conditions = ["series_id = %s"]
        params = [series_id]

        if start_date is not None:
            conditions.append("observation_date >= %s")
            params.append(start_date)

        if end_date is not None:
            conditions.append("observation_date <= %s")
            params.append(end_date)

        # Join all conditions with AND.
        # Example result: "series_id = %s AND observation_date >= %s"
        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT DISTINCT ON (observation_date)
                observation_date, value
            FROM data_points
            WHERE {where_clause}
            ORDER BY observation_date ASC, recorded_at DESC
        """

        cursor.execute(query, params)
        rows = cursor.fetchall()

        # --- Step 3: Format the response ---
        # Transform database rows into a clean, frontend-friendly format.
        data = [
            {
                "date": row["observation_date"].isoformat(),
                "value": float(row["value"]),
            }
            for row in rows
        ]

        # Return the series metadata alongside the data points.
        # The frontend needs both: metadata for labels/titles, data for the chart.
        return {
            "series_id": series_meta["series_id"],
            "name": series_meta["name"],
            "units": series_meta["units"],
            "frequency": series_meta["frequency"],
            "temporal_type": series_meta["temporal_type"],
            "source_institution": series_meta["source_institution"],
            "data_points": len(data),
            "data": data,
        }

    finally:
        conn.close()



# ============================================================================
# STATIC FILE SERVING (Frontend)
# ============================================================================
# These lines tell FastAPI to serve the frontend files (HTML, CSS, JS) from
# the 'frontend/' directory. When someone visits http://localhost:8000/,
# they get the index.html page, which then loads the JavaScript that calls
# the API endpoints above to fetch data and draw charts.
#
# The order matters: API routes (@app.get("/api/...")) are defined above,
# so they take priority. The static file mount below is a "catch-all" for
# everything else — it serves files from the frontend directory.

# First, check if the frontend directory exists. If not, create it with a
# placeholder. This prevents the application from crashing if you run the
# backend before creating the frontend files.
if not os.path.exists(FRONTEND_DIR):
    os.makedirs(FRONTEND_DIR, exist_ok=True)


@app.get("/")
def serve_index():
    """
    Serves the main page (index.html) when someone visits the root URL.

    This is what happens when you type http://localhost:8000/ in your browser.
    The browser receives index.html, which contains the HTML structure, and
    references to CSS (for styling) and JavaScript (for charts and data fetching).
    """
    index_path = os.path.join(FRONTEND_DIR, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    else:
        # If the frontend hasn't been built yet, return a helpful message
        # instead of a cryptic error.
        return {"message": "RECONOMANIA API is running. Frontend not yet deployed."}


# Mount the frontend directory for serving static assets (CSS, JS, images).
# 'mount' means: any request to /static/... will look for a matching file
# in the FRONTEND_DIR directory.
# Example: /static/chart.js → frontend/chart.js
if os.path.exists(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")
