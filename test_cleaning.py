"""
UPLOAD YOUR OWN CSV AND CHECK IF THE AGENT CLEANS IT
=====================================================
Drop any messy CSV into the project folder and this script will:
1. Show you what's wrong with it (dirty report)
2. Apply all pipeline cleaning actions
3. Show you the cleaned result
4. Score it against the auto-generated ground truth

Usage:
    python test_cleaning.py                    # uses auto-generated data (no upload needed)
    python test_cleaning.py myfile.csv t1      # test your own CSV as Task 1 input
"""
import sys
import os
import re
import pandas as pd
import sqlite3

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 140)

def clean_email(s):
    if pd.isna(s): return ""
    return str(s).lower().strip()

def clean_phone(s):
    if pd.isna(s): return ""
    raw = str(s).lower()
    # Remove extensions
    for sep in ["ext", " x ", "x"]:
        if sep in raw:
            raw = raw.split(sep)[0]
    digits = re.sub(r'\D+', '', raw)
    if not digits: return ""
    return "+" + digits

def clean_date(s):
    if pd.isna(s) or str(s).strip() in ["", "None", "nan"]: return ""
    result = pd.to_datetime(s, errors='coerce')
    if pd.isna(result): return ""
    return result.strftime('%Y-%m-%dT00:00:00')

def clean_name(s):
    if pd.isna(s): return ""
    return str(s).strip()

def drop_bots(df):
    mask = (
        df.get("customer_id", pd.Series(["valid"]*len(df))).astype(str).str.strip() != "???" 
    )
    if "email" in df.columns:
        mask &= ~df["email"].astype(str).str.contains("bot|spam|not_an_email", case=False, na=True)
    if "name" in df.columns:
        mask &= ~df["name"].astype(str).str.match(r'^\d+$', na=False)
    return df[mask].copy()

def run_pipeline(df_dirty: pd.DataFrame) -> pd.DataFrame:
    """Apply the full cleaning pipeline to a DataFrame."""
    df = df_dirty.copy()
    
    print("\n📋 DIRTY DATA (first 5 rows):")
    print(df.head(5).to_string(index=False))
    
    print("\n🔍 PROFILE:")
    for col in df.columns:
        nulls = df[col].isnull().sum()
        print(f"  {col}: {df[col].dtype} | nulls={nulls} | sample={df[col].dropna().iloc[0] if not df[col].dropna().empty else 'ALL NULL'}")
    
    # Apply cleaning
    if "email" in df.columns:
        df["email"] = df["email"].apply(clean_email)
    if "phone" in df.columns:
        df["phone"] = df["phone"].apply(clean_phone)
    if "signup_date" in df.columns:
        df["signup_date"] = df["signup_date"].apply(clean_date)
    if "name" in df.columns:
        df["name"] = df["name"].apply(clean_name)
    
    # Drop bots
    before = len(df)
    df = drop_bots(df)
    print(f"\n🤖 Dropped {before - len(df)} bot/spam rows")
    
    # Deduplicate
    if "email" in df.columns:
        before = len(df)
        df.drop_duplicates(subset=["email"], inplace=True)
        print(f"🗑  Removed {before - len(df)} duplicate emails")
    
    print(f"\n✅ CLEANED DATA ({len(df)} rows):")
    print(df.head(5).to_string(index=False))
    return df


def score_against_truth(cleaned_df: pd.DataFrame, task_id: str) -> float:
    """Score the cleaned df against the deterministic ground truth."""
    from server.data_generator import get_task_data
    truth_data = get_task_data(task_id)["hidden_truth"]
    
    # Pick the right truth table
    if task_id == "t1": truth_df = truth_data.get("web_forms")
    elif task_id == "t2": truth_df = truth_data.get("merged_output")
    elif task_id == "t3": truth_df = truth_data.get("merged_output")
    else: return 0.0
    
    if truth_df is None or truth_df.empty or "customer_id" not in cleaned_df.columns:
        print("\n⚠️  Cannot score: 'customer_id' column missing or truth table empty")
        return 0.0
    
    merged = pd.merge(truth_df, cleaned_df.drop_duplicates(subset=["customer_id"]),
                      on="customer_id", suffixes=('_truth', '_agent'), how='left')
    
    match_cols = []
    for col in truth_df.columns:
        if col == "customer_id": continue
        t_col = f"{col}_truth" if f"{col}_truth" in merged.columns else col
        a_col = f"{col}_agent" if f"{col}_agent" in merged.columns else col
        if a_col in merged.columns:
            s_t = merged[t_col].fillna("").astype(str).str.strip().str.lower()
            s_a = merged[a_col].fillna("").astype(str).str.strip().str.lower()
            merged[f"{col}_ok"] = (s_t == s_a)
            match_cols.append(f"{col}_ok")
    
    if not match_cols:
        return 0.0
    
    merged["row_correct"] = merged[match_cols].all(axis=1)
    correct = merged["row_correct"].sum()
    penalty = max(0, len(cleaned_df) - len(truth_df)) * 0.15
    score = max(0.0, min(1.0, correct / len(truth_df) - penalty))
    return score


if __name__ == "__main__":
    csv_path = sys.argv[1] if len(sys.argv) > 1 else None
    task_id  = sys.argv[2] if len(sys.argv) > 2 else "t1"
    
    print("=" * 70)
    print(f"  CRM PIPELINE CLEANER — Task {task_id.upper()}")
    print("=" * 70)
    
    if csv_path:
        print(f"\n📂 Loading your CSV: {csv_path}")
        df_input = pd.read_csv(csv_path)
    else:
        print(f"\n📂 No CSV provided — using auto-generated dirty data for {task_id}")
        from server.data_generator import get_task_data
        data = get_task_data(task_id)
        source_name = list(data["sources"].keys())[0]
        df_input = data["sources"][source_name]
    
    df_clean = run_pipeline(df_input)
    
    print("\n" + "=" * 70)
    print("  SCORING AGAINST GROUND TRUTH")
    print("=" * 70)
    
    try:
        score = score_against_truth(df_clean, task_id)
        grade = "🏆 EXCELLENT" if score >= 0.9 else "✅ GOOD" if score >= 0.7 else "⚠️  PARTIAL" if score >= 0.4 else "❌ POOR"
        print(f"\n  Final Score: {score:.4f}  {grade}")
        print(f"""
  Interpretation:
    1.0  = Perfect — every row matches ground truth exactly
    0.7+ = Good — most rows cleaned correctly
    0.4+ = Partial — some columns wrong (check phone/date format)
    0.0  = Agent failed — bots still in, wrong schema, or missing key
        """)
    except Exception as e:
        print(f"\n  (Scoring unavailable when using custom CSV: {e})")
        print("  Tip: scoring only works against auto-generated truth (run without --csv to compare)")
