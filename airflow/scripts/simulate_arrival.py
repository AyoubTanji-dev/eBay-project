"""
Simulate Data Arrival - Incremental Batch Processor
Reads N rows from the Silver reservoir and outputs them for daily processing.
Keeps track of progress in a state file.
"""

import pandas as pd
import json
import os

# ==============================================================================
# Configuration (Container Paths)
# ==============================================================================
DATA_DIR = "/opt/airflow/data"
SILVER_FILE = f"{DATA_DIR}/silver/silver_data.csv"
DAILY_OUTPUT = f"{DATA_DIR}/staging/daily_input.csv"
STATE_FILE = f"{DATA_DIR}/staging/ingestion_state.json"

BATCH_SIZE = 50000  # Number of rows per "day"

os.makedirs(f"{DATA_DIR}/staging", exist_ok=True)

print("📥 Simulate Data Arrival - Starting...")

# ==============================================================================
# Load State (Where did we stop last time?)
# ==============================================================================
if os.path.exists(STATE_FILE):
    with open(STATE_FILE, 'r') as f:
        state = json.load(f)
    skip_rows = state.get('processed_rows', 0)
    print(f"   Resuming from row {skip_rows}")
else:
    skip_rows = 0
    print("   Starting fresh (no previous state)")

# ==============================================================================
# Read Next Batch
# ==============================================================================
try:
    # Read header + skip already processed rows
    df_batch = pd.read_csv(
        SILVER_FILE,
        skiprows=range(1, skip_rows + 1),  # Skip header=0, then skip processed rows
        nrows=BATCH_SIZE
    )
    
    rows_read = len(df_batch)
    
    if rows_read == 0:
        print("🏁 All data has been processed! Resetting state for next cycle.")
        # Reset state to start over
        with open(STATE_FILE, 'w') as f:
            json.dump({'processed_rows': 0}, f)
        # Create empty file to signal "nothing to do"
        df_batch.to_csv(DAILY_OUTPUT, index=False)
    else:
        print(f"   Read {rows_read} new rows (rows {skip_rows+1} to {skip_rows+rows_read})")
        
        # Save batch to staging
        df_batch.to_csv(DAILY_OUTPUT, index=False)
        print(f"   ✅ Saved to {DAILY_OUTPUT}")
        
        # Update state
        new_state = {'processed_rows': skip_rows + rows_read}
        with open(STATE_FILE, 'w') as f:
            json.dump(new_state, f)
        print(f"   State updated: {new_state}")

except Exception as e:
    print(f"❌ Error: {e}")
    raise

print("🎉 Simulate Arrival Complete!")
