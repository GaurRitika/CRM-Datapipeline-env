"""
Run this script to preview exactly what data your agent sees and what the grader checks.
It shows the BEFORE (dirty) and AFTER (expected clean) for all 3 tasks.

Usage:
    python debug_data.py
"""
from server.data_generator import get_task_data
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 140)
pd.set_option("display.max_colwidth", 30)

DIVIDER = "=" * 120

def preview_task(task_id: str):
    print(f"\n{DIVIDER}")
    print(f"  TASK {task_id.upper()}")
    print(DIVIDER)
    
    data = get_task_data(task_id)
    sources = data["sources"]
    truth = data["hidden_truth"]
    
    print(f"\n📂 SOURCES (what the agent sees — DIRTY data):")
    for name, df in sources.items():
        print(f"\n  Source: '{name}'  ({len(df)} rows, {len(df.columns)} cols)")
        print(f"  Columns: {list(df.columns)}")
        print(df.head(5).to_string(index=False))
        
    print(f"\n🎯 HIDDEN TRUTH (what grader checks — CLEAN data, agent NEVER sees this):")
    for name, df in truth.items():
        print(f"\n  Truth Table: '{name}'  ({len(df)} rows)")
        print(df.head(5).to_string(index=False))
        
    print(f"\n📋 Target Schema:")
    for col, fmt in data["schema"].items():
        print(f"    {col}: {fmt}")

if __name__ == "__main__":
    for tid in ["t1", "t2", "t3"]:
        preview_task(tid)
    
    print(f"\n{DIVIDER}")
    print("  HOW TO CHECK IF AGENT IS CLEANING DATA CORRECTLY")
    print(DIVIDER)
    print("""
1. Start the server:   uvicorn server.app:app --host 0.0.0.0 --port 8080 --reload
2. Run the agent:      python baseline.py
3. Check the score:    curl -X POST http://localhost:8080/grader/t1
                       curl -X POST http://localhost:8080/grader/t2
                       curl -X POST http://localhost:8080/grader/t3

Score = 0.0  → agent submitted without cleaning (bots still in, raw data)
Score = 0.5  → agent partially cleaned (some columns match, still missing data)
Score = 1.0  → agent perfectly matched ground truth

The agent earns reward for:
  - Standardizing email to lowercase
  - Standardizing phone to E.164 format (+1xxxxxxxxxx)
  - Standardizing dates to ISO 8601 (2023-01-01T00:00:00)
  - Dropping bot rows (customer_id = '???')
  - Removing duplicates before submit
""")
