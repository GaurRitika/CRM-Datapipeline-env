import pandas as pd
import numpy as np
from faker import Faker
import random

fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

def generate_easy_task():
    """Task 1: Normalize web_forms.csv
    Goal: Lowercase emails, fix date formats, strip whitespace from names, and format phones.
    """
    rows = []
    ground_truth = []
    
    for i in range(50):
        # Truth values
        customer_id = f"CUST_{1000+i}"
        name = fake.name()
        email = fake.email().lower()
        signup_date = fake.date_this_decade()
        signup_date_iso = signup_date.isoformat()
        phone = fake.phone_number()
        # simplified E164 for truth just strip non-digits for this mockup
        phone_clean = ''.join(filter(str.isdigit, phone))
        
        ground_truth.append({
            "customer_id": customer_id,
            "name": name,
            "email": email,
            "signup_date": signup_date_iso,
            "phone": phone_clean
        })
        
        # Messy values
        dirty_name = f"  {name}  " if random.random() > 0.5 else name
        dirty_email = email.upper() if random.random() > 0.5 else email
        dirty_email = f" {dirty_email} " if random.random() > 0.7 else dirty_email
        date_formats = ["%d/%m/%Y", "%b %d %Y", "%m-%d-%y"]
        dirty_date = signup_date.strftime(random.choice(date_formats))
        
        rows.append({
            "customer_id": customer_id,
            "name": dirty_name,
            "email": dirty_email,
            "signup_date": dirty_date,
            "phone": phone
        })
        
    df_messy = pd.DataFrame(rows)
    df_truth = pd.DataFrame(ground_truth)
    
    target_schema = {
        "customer_id": "string",
        "name": "string (stripped)",
        "email": "string (lowercase, stripped)",
        "signup_date": "datetime ISO 8601",
        "phone": "string (digits only)"
    }
    
    return {"web_forms": df_messy}, {"web_forms": df_truth}, target_schema

def generate_medium_task():
    """Task 2: Deduplicate legacy_db.csv"""
    _, truth, schema = generate_easy_task()
    df_truth = truth["web_forms"].copy()
    
    messy_rows = []
    for _, row in df_truth.iterrows():
        # Keep original
        messy_rows.append(row.to_dict())
        
        # 30% chance of a duplicate with slight typo in name/email
        if random.random() > 0.7:
            dup = row.to_dict()
            if random.random() > 0.5:
                dup["name"] = dup["name"].lower() # Case mismatch
            else:
                dup["email"] = dup["email"] + " " # Padding
            messy_rows.append(dup)
            
    df_messy = pd.DataFrame(messy_rows).sample(frac=1).reset_index(drop=True)
    return {"legacy_db": df_messy}, {"legacy_db": df_truth}, schema

def generate_hard_task():
    """Task 3: 3-way merge conflict."""
    # This requires 3 dataframes reflecting partial information
    # For now, we stub this out to build the core environment faster.
    # The true logic will distribute partial rows across 3 DFs.
    _, truth, schema = generate_easy_task()
    df_truth = truth["web_forms"].copy()
    
    df_salesforce = df_truth.sample(frac=0.8).copy()
    df_webleads = df_truth.sample(frac=0.6).copy()
    df_legacy = df_truth.sample(frac=0.9).copy()
    
    # Introduce conflicts that S1 > S2 > S3 should resolve
    # By default, we expect the merge to recreate df_truth
    
    return {
        "salesforce": df_salesforce,
        "web_leads": df_webleads,
        "legacy_db": df_legacy
    }, {"merged_final": df_truth}, schema

def get_task_data(task_id: str):
    if task_id == "t1": return generate_easy_task()
    if task_id == "t2": return generate_medium_task()
    if task_id == "t3": return generate_hard_task()
    raise ValueError(f"Unknown task {task_id}")
