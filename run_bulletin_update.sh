#!/bin/bash
# ============================================================================
# RECONOMANIA — Daily Bulletin Update Runner
# ============================================================================
#
# Purpose: Check for new BNR Monthly Bulletins, download if available,
#          and run the extraction scrapers.
#
# How it works:
#   1. Counts PDF files in archive BEFORE download
#   2. Runs the download script (discovers + downloads new bulletins)
#   3. Counts PDF files AFTER download
#   4. If the count increased, runs both extraction scrapers in --update mode
#   5. If no new files, exits quietly
#
# This script is safe to run daily. The download script skips existing files,
# and the extraction scrapers use ON CONFLICT DO NOTHING for duplicates.
#
# Setup (cron):
#   crontab -e
#   Add: 0 10 * * * /home/cristi/reconomania/run_bulletin_update.sh >> /home/cristi/reconomania/logs/bulletin_update.log 2>&1
#
# That runs at 10:00 UTC daily. BNR publishes bulletins irregularly
# (~6-8 weeks after reference month), so daily checking is appropriate.
#
# ============================================================================

# Exit on error
set -e

# Configuration
PROJECT_DIR="/home/cristi/reconomania"
VENV_DIR="$PROJECT_DIR/venv"
ARCHIVE_DIR="$PROJECT_DIR/archive/bnr_monthly_bulletin"
LOG_DIR="$PROJECT_DIR/logs"

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Activate virtual environment
cd "$PROJECT_DIR"
source "$VENV_DIR/bin/activate"

# Timestamp
echo ""
echo "========================================"
echo "Bulletin update: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "========================================"

# Count files before download
BEFORE=$(ls -1 "$ARCHIVE_DIR"/*.pdf 2>/dev/null | wc -l)
echo "PDFs before: $BEFORE"

# Run download (discovers listing, downloads any new bulletins)
python "$PROJECT_DIR/scraper_bnr_bulletin_download.py" --download

# Count files after download
AFTER=$(ls -1 "$ARCHIVE_DIR"/*.pdf 2>/dev/null | wc -l)
echo "PDFs after: $AFTER"

if [ "$AFTER" -gt "$BEFORE" ]; then
    NEW_COUNT=$((AFTER - BEFORE))
    echo "New bulletins downloaded: $NEW_COUNT"
    echo ""

    # Run extraction scrapers in update mode (processes latest PDF only)
    echo "Running prudential indicators extractor..."
    python "$PROJECT_DIR/scraper_bnr_bulletin_prudential.py" --update

    echo ""
    echo "Running monetary policy extractor..."
    python "$PROJECT_DIR/scraper_bnr_bulletin_monetary.py" --update

    echo ""
    echo "DONE: New data extracted and loaded."
else
    echo "No new bulletins. Nothing to do."
fi
