-- ============================================================================
-- RECONOMANIA — Database Schema (Phase One)
-- ============================================================================
--
-- FILE PURPOSE:
--   Creates the foundational database tables for the RECONOMANIA platform.
--   This script is run once to set up the database structure. It is safe to
--   run multiple times thanks to "IF NOT EXISTS" clauses — it will not
--   destroy existing data.
--
-- WHAT THIS FILE CREATES:
--   1. time_series     — The metadata registry ("identity card" for each data source)
--   2. data_points     — The actual numerical observations (dates and values)
--   3. scrape_log      — Operational audit trail for every scraper run
--
-- HOW TO RUN THIS FILE:
--   From your WSL terminal, navigate to the directory containing this file
--   and run:
--
--       psql -f 001_create_schema.sql
--
--   This connects to your default PostgreSQL database (configured during
--   Phase Zero) and executes all the SQL statements in order.
--
--   If you want to connect to a specific database instead of the default:
--
--       psql -d reconomania -f 001_create_schema.sql
--
--   (You would first need to create that database with: CREATE DATABASE reconomania;)
--
-- WHAT TO DO IF IT BREAKS:
--   If you see an error, read the message carefully — PostgreSQL error
--   messages are usually clear. Common issues:
--     - "role does not exist": your PostgreSQL user isn't set up correctly.
--       Revisit the Phase Zero PostgreSQL configuration.
--     - "database does not exist": you need to create the database first.
--     - "permission denied": your user doesn't have sufficient privileges.
--
-- DATE:        March 2026
-- PHASE:       Phase One (first data source: BNR EUR/RON daily exchange rate)
-- ============================================================================


-- ============================================================================
-- STEP 0: Create the database (if it doesn't already exist)
-- ============================================================================
-- NOTE: This command cannot run inside a transaction block in some PostgreSQL
-- configurations. If you get an error here, run this line separately:
--
--     createdb reconomania
--
-- from the WSL terminal (not inside psql), then comment out or remove the
-- line below and run the rest of this file with:
--
--     psql -d reconomania -f 001_create_schema.sql
-- ============================================================================

-- We'll skip the CREATE DATABASE here and assume it's been created manually.
-- See instructions above.


-- ============================================================================
-- TABLE 1: time_series (The Metadata Registry)
-- ============================================================================
-- This table implements the "identity card" concept from Section 6.1 of the
-- Concept Paper. Every time series in the entire platform gets exactly one
-- row here. Adding a new data source to RECONOMANIA always starts with
-- inserting a row into this table.
--
-- Right now (Phase One), this table has exactly ONE row: the BNR EUR/RON
-- daily reference exchange rate. But the schema is designed to hold hundreds
-- of series without any structural changes.
--
-- DESIGN DECISIONS:
--
-- 1. series_id is a human-readable TEXT string (like 'bnr_eurron_daily'),
--    not an auto-incrementing number. Why? Because it appears in code,
--    queries, logs, API responses, and file names. "Series 47" means nothing
--    to a human debugging at 2am; "bnr_eurron_daily" is immediately clear.
--    The trade-off is that joins are slightly slower with text keys than
--    integer keys — but with hundreds of series (not millions), this is
--    completely irrelevant. Clarity wins.
--
-- 2. frequency and temporal_type use PostgreSQL's ENUM types. An ENUM is a
--    column that only accepts values from a predefined list — like a dropdown
--    menu in a form. If someone tries to insert frequency='biweekly', the
--    database rejects it. This prevents typos and invalid data at the
--    database level, which is safer than relying on application code alone.
-- ============================================================================

-- First, create the ENUM types. Think of these as custom "allowed values"
-- lists that PostgreSQL enforces automatically.

-- Frequency: how often new data points are published.
-- 'daily' = every business day (like exchange rates)
-- 'monthly' = once per month (like CPI inflation)
-- 'quarterly' = once per quarter (like GDP)
-- 'annual' = once per year (like annual financial reports)
DO $$ BEGIN
    CREATE TYPE series_frequency AS ENUM ('daily', 'monthly', 'quarterly', 'annual');
EXCEPTION
    WHEN duplicate_object THEN NULL;  -- Ignore if it already exists
END $$;

-- Temporal type: what does a data point mean in time?
-- This is the distinction from Section 6.2 of the Concept Paper.
-- 'point_in_time' = a snapshot at a specific moment (e.g., exchange rate on March 16)
-- 'period_average' = an average over a period (e.g., monthly average exchange rate)
-- 'period_total' = a total accumulated over a period (e.g., quarterly GDP)
-- 'end_of_period' = value at the end of a period (e.g., total bank assets at Dec 31)
DO $$ BEGIN
    CREATE TYPE temporal_type AS ENUM ('point_in_time', 'period_average', 'period_total', 'end_of_period');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- Now create the table itself.
CREATE TABLE IF NOT EXISTS time_series (

    -- PRIMARY KEY: The unique internal identifier for this series.
    -- Convention: source_variable_frequency, all lowercase, underscores.
    -- Examples: 'bnr_eurron_daily', 'insse_cpi_monthly', 'bnr_policyrate_daily'
    series_id       TEXT PRIMARY KEY,

    -- Human-readable name, displayed in the UI.
    -- Example: 'EUR/RON Reference Exchange Rate'
    name            TEXT NOT NULL,

    -- Longer description explaining what this series represents.
    -- Example: 'Official EUR/RON reference rate published daily by BNR...'
    description     TEXT,

    -- Which institution publishes this data.
    -- Examples: 'BNR', 'INSSE', 'ASF', 'ECB', 'EBA'
    source_institution TEXT NOT NULL,

    -- How to access the source data: URL, API endpoint, or file path.
    -- For BNR exchange rates, this is the XML feed URL.
    source_url      TEXT,

    -- How often new data points appear (uses the ENUM defined above).
    frequency       series_frequency NOT NULL,

    -- What does each data point represent in time? (uses the ENUM above).
    -- EUR/RON daily rate is 'point_in_time': it's the rate AT that date.
    temporal_type   temporal_type NOT NULL,

    -- The unit of measurement. Free text because units vary wildly.
    -- Examples: 'RON per 1 EUR', 'percent', 'millions EUR', 'index (2015=100)'
    units           TEXT NOT NULL,

    -- When should we expect new data? Free text description.
    -- Examples: 'Every business day by 13:00 EET', '12th of each month'
    -- Used by the monitoring system to detect missing updates.
    expected_update_schedule TEXT,

    -- The earliest date for which data should be collected.
    -- Set once when the scraper is designed. Used for the initial backfill.
    -- For BNR exchange rates, this is 2005-01-03 (earliest available in XML).
    historical_start_date DATE,

    -- The hex colour code this series uses in charts. NULL until the colour
    -- scheme is designed (see Concept Paper Section 8.3, Open Question #5).
    chart_colour    TEXT,

    -- Where this series lives in the navigation hierarchy.
    -- Example: '/fx/eurron' or '/macro/inflation/cpi'
    -- Will be formalised later (Open Question #4).
    topic_path      TEXT,

    -- How the source formats dates, for documentation purposes.
    -- Example: 'YYYY-MM-DD (ISO 8601 in XML attribute)'
    source_date_format TEXT,

    -- Timestamp of the most recent successful data update for this series.
    -- Updated by the scraper after each successful run.
    -- Uses TIMESTAMPTZ (timestamp with time zone), stored internally as UTC.
    last_updated    TIMESTAMPTZ,

    -- When this series was first registered in the system.
    -- DEFAULT NOW() means PostgreSQL automatically fills this in.
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- ============================================================================
-- TABLE 2: data_points (The Actual Data)
-- ============================================================================
-- This is where the numbers live. Each row represents one observation:
-- "On [date], series [X] had value [Y]."
--
-- For the EUR/RON daily rate, expect roughly 250 rows per year (business
-- days only — BNR doesn't publish rates on weekends or holidays).
-- The initial backfill from 2005 will load approximately 5,000+ rows.
--
-- DESIGN DECISIONS:
--
-- 1. DATA VERSIONING (Section 6.3 of the Concept Paper):
--    The Concept Paper requires that every version of every data point is
--    preserved permanently. We handle this with the 'recorded_at' column,
--    which records WHEN we scraped each value. If BNR ever revises a
--    historical rate, we store the new value as a new row with a newer
--    recorded_at timestamp. The old row remains untouched.
--
--    To get the CURRENT (most recent) value for each date, we query:
--      SELECT DISTINCT ON (series_id, observation_date) *
--      FROM data_points
--      ORDER BY series_id, observation_date, recorded_at DESC;
--
--    This PostgreSQL-specific syntax means: "for each unique combination of
--    series and date, give me only the row with the latest recorded_at."
--
-- 2. value uses NUMERIC, not FLOAT. In computing, decimal numbers can be
--    stored two ways:
--    - FLOAT: fast but imprecise. 4.9737 might secretly be 4.973699999997.
--      Fine for games and graphics; dangerous for financial data.
--    - NUMERIC (also called DECIMAL): exact precision. 4.9737 is stored as
--      exactly 4.9737. Slower for heavy computation but perfect for
--      financial values where precision matters.
--    We use NUMERIC because this is a financial data platform.
--
-- 3. The UNIQUE constraint on (series_id, observation_date, recorded_at)
--    prevents accidental duplicate imports. Running the same scraper twice
--    won't create duplicate rows (assuming recorded_at is the same, which
--    it will be within a single scraper run).
-- ============================================================================

CREATE TABLE IF NOT EXISTS data_points (

    -- Auto-incrementing integer primary key. Every row gets a unique number.
    -- We don't use this for lookups (we query by series_id + date), but it
    -- provides a simple, unambiguous row identifier for debugging.
    id              SERIAL PRIMARY KEY,

    -- Which time series this data point belongs to.
    -- REFERENCES time_series(series_id) creates a "foreign key constraint":
    -- PostgreSQL will refuse to insert a data point for a series_id that
    -- doesn't exist in the time_series table. This prevents orphaned data.
    series_id       TEXT NOT NULL REFERENCES time_series(series_id),

    -- The date this observation corresponds to.
    -- For daily EUR/RON: the specific business day (e.g., 2025-03-14).
    -- For monthly CPI: the first day of the month (convention we establish).
    -- For quarterly GDP: the first day of the quarter.
    -- DATE type stores just the date (no time component). Correct for this use.
    observation_date DATE NOT NULL,

    -- The actual numerical value.
    -- NUMERIC type for exact decimal precision (see design note above).
    -- Examples: 4.9737 (EUR/RON rate), 5.25 (interest rate percent)
    value           NUMERIC NOT NULL,

    -- When this value was recorded in our database.
    -- This is the key to data versioning: if the same (series_id, date)
    -- appears multiple times, each with a different recorded_at, the most
    -- recent one is the "current" value and older ones are the revision history.
    -- DEFAULT NOW() means it's automatically set to the current timestamp.
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Which source file this data point was extracted from.
    -- For BNR: 'nbrfxrates2024.xml', 'nbrfxrates.xml', etc.
    -- This is the provenance trail: if a value looks wrong, you can trace
    -- it back to the exact file that was parsed.
    source_file     TEXT,

    -- UNIQUE CONSTRAINT: prevents duplicate entries.
    -- The same series + date + recording timestamp cannot appear twice.
    -- This is the versioning mechanism: a new version has a new recorded_at.
    UNIQUE (series_id, observation_date, recorded_at)
);

-- INDEX: Speeds up the most common query pattern — "give me all data points
-- for series X between date A and date B, most recent version of each."
--
-- An index is like a book's index: instead of reading every page (row) to
-- find what you need, the database can jump directly to the relevant rows.
-- Without this index, every query would scan the entire table. With thousands
-- of rows across many series, this matters.
--
-- We include recorded_at in the index because our versioning query sorts by
-- it to find the most recent version of each data point.
CREATE INDEX IF NOT EXISTS idx_data_points_lookup
    ON data_points (series_id, observation_date, recorded_at DESC);


-- ============================================================================
-- TABLE 3: scrape_log (Operational Audit Trail)
-- ============================================================================
-- Every time a scraper runs — whether it succeeds or fails — it creates a
-- row here. This serves three purposes:
--
-- 1. DEBUGGING: When something goes wrong, the log tells you what happened,
--    when, and what the error was.
-- 2. MONITORING: The admin dashboard (Phase Three) will query this table to
--    show green/amber/red status for each pipeline. Even before Phase Three,
--    you can check this table manually.
-- 3. AUDIT: A complete history of every data collection event. Useful for
--    questions like "when did we last successfully update this series?" or
--    "how many failures have we had this month?"
-- ============================================================================

CREATE TABLE IF NOT EXISTS scrape_log (

    -- Auto-incrementing primary key.
    id              SERIAL PRIMARY KEY,

    -- Which series was this scraper run for.
    -- Foreign key to time_series, same as in data_points.
    series_id       TEXT NOT NULL REFERENCES time_series(series_id),

    -- When did this scraper run start?
    run_started_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- When did it finish? NULL if still running or if it crashed so hard
    -- it couldn't record its own end time (rare but possible).
    run_finished_at TIMESTAMPTZ,

    -- What happened? 'success', 'failure', or 'partial' (some records
    -- succeeded but others failed — possible with large backfills).
    status          TEXT NOT NULL CHECK (status IN ('success', 'failure', 'partial')),

    -- How many records were fetched from the source?
    -- For a BNR yearly XML file, this might be ~250 (business days in a year).
    records_fetched INTEGER DEFAULT 0,

    -- How many of those were genuinely new (not already in the database)?
    -- On a daily update, this should be 1. On initial backfill, it equals
    -- records_fetched. If it's 0 on a daily run, the source hasn't updated yet.
    records_new     INTEGER DEFAULT 0,

    -- How many existing records were updated (value changed at source)?
    -- For exchange rates, this should almost always be 0. For GDP data,
    -- this will be non-zero when revisions are published.
    records_updated INTEGER DEFAULT 0,

    -- If status is 'failure' or 'partial', what went wrong?
    -- Free text. The scraper writes the Python error message here.
    -- NULL when status is 'success'.
    error_message   TEXT,

    -- Path to the archived raw source file (the unaltered original).
    -- Section 7.2 of the Concept Paper requires storing the original file
    -- as an "unalterable receipt." This column records where it was saved.
    -- Example: '/archive/bnr/nbrfxrates2024_20260316T143022.xml'
    source_file_archived TEXT
);

-- INDEX: Speeds up "show me all runs for series X" queries.
-- The admin dashboard will query this frequently.
CREATE INDEX IF NOT EXISTS idx_scrape_log_series
    ON scrape_log (series_id, run_started_at DESC);


-- ============================================================================
-- SEED DATA: Register the first time series
-- ============================================================================
-- This inserts the metadata "identity card" for our first (and currently
-- only) data source: the BNR EUR/RON daily reference exchange rate.
--
-- ON CONFLICT DO NOTHING means: if this series_id already exists (because
-- you ran this script before), skip the insert instead of throwing an error.
-- This makes the script safe to run multiple times.
-- ============================================================================

INSERT INTO time_series (
    series_id,
    name,
    description,
    source_institution,
    source_url,
    frequency,
    temporal_type,
    units,
    expected_update_schedule,
    historical_start_date,
    chart_colour,
    topic_path,
    source_date_format
) VALUES (
    'bnr_eurron_daily',
    'EUR/RON Reference Exchange Rate',
    'Official EUR/RON reference rate calculated and published daily by the '
    'National Bank of Romania (BNR). Published each business day by 13:00 EET. '
    'This is the rate used as a benchmark by the Romanian financial system for '
    'contracts, accounting, and regulatory reporting denominated in or referencing EUR.',
    'BNR',
    'https://www.bnr.ro/nbrfxrates.xml',
    'daily',
    'point_in_time',
    'RON per 1 EUR',
    'Every business day by 13:00 EET. No publication on weekends or Romanian public holidays.',
    '2005-01-03',       -- Earliest date available in BNR yearly XML archives
    NULL,               -- Colour to be decided later (Open Question #5)
    '/fx/eurron',       -- Preliminary topic path
    'YYYY-MM-DD (ISO 8601, in XML Cube date attribute)'
) ON CONFLICT (series_id) DO NOTHING;


-- ============================================================================
-- VERIFICATION
-- ============================================================================
-- After running this script, you can verify everything was created correctly
-- by running these queries in psql:
--
--   \dt                              -- Lists all tables (should show 3)
--   \dT+                             -- Lists all custom types (should show 2 ENUMs)
--   SELECT * FROM time_series;       -- Should show 1 row (bnr_eurron_daily)
--   SELECT COUNT(*) FROM data_points; -- Should show 0 (no data loaded yet)
--   SELECT COUNT(*) FROM scrape_log;  -- Should show 0 (no scraper has run yet)
--
-- ============================================================================
-- END OF SCHEMA
-- ============================================================================
