## Setup
# Packages
from dotenv import load_dotenv
import json
import logging
import time
import argparse
from datetime import date
from pathlib import Path
import pandas as pd

from src.etl.extract import extract
from src.etl.transform import transform
from src.etl.load import get_azuresqldb_engine‎, load

# Env things for engine creation
load_dotenv()

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("backfill.log"),
    ],
)
log = logging.getLogger(__name__)

# Constants
CHECKPOINT_FILE      = Path("backfill_checkpoint.json")
FAILURES_FILE        = Path("backfill_failures.json")
API_CALLS_PER_MINUTE = 5
API_CALLS_PER_DAY    = 500
SECONDS_PER_CALL     = 15

## Helper fns.
# Get date range fn.
def get_backfill_dates(start_date: str = None, end_date: str = None) -> list[str]:
  '''
  Generate all Sundays between two dates (inclusive);
  default end is the earliest date in the API ("2008-06-15")
  start determined by a checkpoint tracker fn
  '''
  # Get start date using query to database if kept at None
  if start_date is None:
    # Make engine
    engine = get_azuresqldb_engine()
  
    # Connect, get start
    with engine.connect() as conn:
      query = text("SELECT MIN(list_date) FROM weekly_lists")
      min_list_date = conn.execute(query).scalar()
      start_date = (min_list_date + timedelta(days=11)).strftime("%Y-%m-%d")

  # Set end date if end date is None
  end_date = "2008-06-15" if end_date is None else end_date
  
  # Get date range
  pub_dates = pd.date_range(
      start=start_date,
      end=end_date,
      freq=-1 * pd.offsets.Week(weekday=6) # Each Sunday from start to end going backward
  )
  
  # Output
  return [d.strftime('%Y-%m-%d') for d in pub_dates]

# Load Checkpoint fn.
def load_checkpoint() -> set[str]:
  '''
  Loads checkpoint if exists
  '''
  if CHECKPOINT_FILE.exists():
    data = json.loads(CHECKPOINT_FILE.read_text())
    log.info(f"Checkpoint loaded: {len(data['completed'])} completed, "
             f"{len(data.get('failures', {}))} prior failures")
    return set(data["completed"]), data.get("failures", {})
  
  return set(), {}

def save_checkpoint(completed: set[str], failures: dict):
  '''
  Writes completes, failuers to checkpoint file
  '''
  CHECKPOINT_FILE.write_text(
    json.dumps({"completed": sorted(completed), "failures": failures}, indent=2)
  )

# Run pipeline for date fn.
def run_pipeline_for_date(target_date: date, db_retries: int = 3) -> None:
    '''
    Runs ETL for a single date
    '''
    raw  = extract(target_date=target_date)
    data = transform(raw)
  
    for attempt in range(1, db_retries + 1):
        try:
            load(data)
            return
        except Exception as e:
            log.warning(f"  DB attempt {attempt}/{db_retries} failed for {target_date}: {e}")
            if attempt < db_retries:
                time.sleep(5 * attempt) # backoff time increases linearly
            else:
                raise  # re-raise so outer loop records it as a failure

## Main Backfill fn.
def run_backfill(
    dates:       list[date],
    daily_limit: int = API_CALLS_PER_DAY,
    max_retries: int = 3,
) -> None:
    '''
    Sets up checkpoint and logs, then runs backfill
    for all dates specified or found
    '''
    completed, failures = load_checkpoint()
    remaining = [d for d in dates if d.isoformat() not in completed]

    log.info(f"Backfill scope   : {dates[0]} → {dates[-1]} ({len(dates)} Thursdays)")
    log.info(f"Already completed: {len(completed)}")
    log.info(f"Remaining        : {len(remaining)}")

    calls_today = 0

    for d in remaining:
        print(f"========== ATTEMPTING BACKFILL DATE {d} ==========")
        # Daily cap check
        if calls_today >= daily_limit:
            log.warning(
                f"Daily API limit ({daily_limit}) reached after processing "
                f"{calls_today} dates. Stopping cleanly. "
                f"Re-run tomorrow (or trigger manually) to continue from {d}."
            )
            save_checkpoint(completed, failures)
            return  # clean exit; checkpoint means re-run picks up here

        # Sleep before call
        if calls_today > 0:
            time.sleep(SECONDS_PER_CALL)

        # Pipeline by-date function execution
        success = False
        for attempt in range(1, max_retries + 1):
            try:
                run_pipeline_for_date(d)
                completed.add(d.isoformat())
                save_checkpoint(completed, failures)
                calls_today += 1
                log.info(f"✓ {d}  (call {calls_today}/{daily_limit} today)")
                success = True
                break
            except Exception as e:
                log.warning(f"✗ {d} pipeline attempt {attempt}/{max_retries}: {e}")
                if attempt < max_retries:
                    time.sleep(2 ** attempt)

        if not success:
            failures[d.isoformat()] = str(e)
            save_checkpoint(completed, failures)
            log.error(f"PERMANENT FAILURE: {d} — logged to {FAILURES_FILE}")

    # Completion message
    log.info("=" * 60)
    log.info(f"Backfill finished. Completed: {len(completed)}, Failed: {len(failures)}")
    if failures:
        FAILURES_FILE.write_text(json.dumps(failures, indent=2))
        log.error(f"See {FAILURES_FILE} for {len(failures)} permanent failures.")

## Executor
if __name__ == "__main__":
    dates = get_backfill_dates()
    run_backfill(dates)
