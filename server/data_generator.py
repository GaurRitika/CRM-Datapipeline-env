import pandas as pd
import numpy as np
from faker import Faker
import random

fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

def generate_easy_task():
    """Task 1: Normalize web_forms.csv"""
    rows = []
    ground_truth = []
    
    for i in range(50):
        customer_id = f"CUST_{1000+i}"
        name = fake.name()
        email = fake.email().lower()
        signup_date = fake.date_this_decade()
        phone = fake.phone_number()
        phone_clean = ''.join(filter(str.isdigit, phone))
        
        ground_truth.append({
            "customer_id": customer_id,
            "name": name,
            "email": email,
            "signup_date": signup_date.isoformat(),
            "phone": phone_clean
        })
        
        dirty_name = f"  {name}  " if random.random() > 0.5 else name
        dirty_email = email.upper() if random.random() > 0.5 else email
        date_formats = ["%d/%m/%Y", "%b %d %Y", "%m-%d-%y"]
        dirty_date = signup_date.strftime(random.choice(date_formats))
        
        rows.append({
            "customer_id": customer_id,
            "name": dirty_name,
            "email": dirty_email,
            "signup_date": dirty_date,
            "phone": phone
        })
        
    target_schema = {
        "customer_id": "string",
        "name": "string (stripped)",
        "email": "string (lowercase, stripped)",
        "signup_date": "datetime ISO 8601",
        "phone": "string (digits only)"
    }
    
    return {
        "sources": {"web_forms": pd.DataFrame(rows)},
        "hidden_truth": {"web_forms": pd.DataFrame(ground_truth)},
        "schema": target_schema
    }

def generate_medium_task():
    """Task 2: Deduplicate legacy_db.csv"""
    data = generate_easy_task()
    df_truth = data["hidden_truth"]["web_forms"].copy()
    
    messy_rows = []
    for _, row in df_truth.iterrows():
        messy_rows.append(row.to_dict())
        if random.random() > 0.7:
            dup = row.to_dict()
            dup["name"] = dup["name"].lower() if random.random() > 0.5 else dup["name"]
            messy_rows.append(dup)
            
    df_messy = pd.DataFrame(messy_rows).sample(frac=1).reset_index(drop=True)
    return {
        "sources": {"legacy_db": df_messy},
        "hidden_truth": {"legacy_db": df_truth},
        "schema": data["schema"]
    }

def generate_hard_task():
    """Task 3: Real 3-way merge conflict."""
    truth = []
    salesforce = []
    web_leads = []
    legacy = []
    
    for i in range(50):
        # Master records
        cid = f"CUST_{2000+i}"
        old_cid = f"OLD-{2000+i}"
        real_email = fake.email().lower()
        old_email = fake.email().lower()
        real_phone = fake.phone_number()
        old_phone = fake.phone_number()
        
        # Ground Truth
        truth.append({"customer_id": cid, "email": real_email, "phone": real_phone})
        
        # Salesforce is incredibly rigid. 100% CIDs. 30% missing phones.
        sf_phone = real_phone if random.random() > 0.3 else None
        salesforce.append({"customer_id": cid, "email": real_email, "phone": sf_phone})
        
        # Web leads is fresh. 100% emails. 20% missing IDs. Completely fake phones (users put junk).
        wl_cid = cid if random.random() > 0.2 else None
        web_leads.append({"customer_id": wl_cid, "email": real_email, "phone": old_phone})
        
        # Legacy DB uses old ID format, old email, old phone.
        legacy.append({"legacy_id": old_cid, "contact_email": old_email, "home_phone": old_phone})
        
    df_sf = pd.DataFrame(salesforce).sample(frac=0.9).reset_index(drop=True)
    df_wl = pd.DataFrame(web_leads).sample(frac=0.8).reset_index(drop=True)
    df_leg = pd.DataFrame(legacy).sample(frac=0.85).reset_index(drop=True)
    df_truth = pd.DataFrame(truth)
    
    target_schema = {
        "customer_id": "string",
        "email": "string",
        "phone": "string"
    }
    
    return {
        "sources": {"salesforce": df_sf, "web_leads": df_wl, "legacy_db": df_leg},
        "hidden_truth": {"merged_final": df_truth},
        "schema": target_schema
    }

def get_task_data(task_id: str):
    if task_id == "t1": return generate_easy_task()
    if task_id == "t2": return generate_medium_task()
    if task_id == "t3": return generate_hard_task()
    raise ValueError(f"Unknown task {task_id}")
