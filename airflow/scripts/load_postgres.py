"""
Incremental Load to PostgreSQL
Reads incremental gold files and INSERTs them into the database.
"""

import pandas as pd
import psycopg2
from psycopg2 import sql
import os
import glob

# ==============================================================================
# Configuration (From Environment Variables set by Docker)
# ==============================================================================
DB_CONFIG = {
    'host': os.environ.get('EBAY_DB_HOST', 'host.docker.internal'),
    'port': int(os.environ.get('EBAY_DB_PORT', 5433)),
    'database': os.environ.get('EBAY_DB_NAME', 'ebay_maroc'),
    'user': os.environ.get('EBAY_DB_USER', 'ebay_user'),
    'password': os.environ.get('EBAY_DB_PASSWORD', 'ebay123')
}

DATA_DIR = "/opt/airflow/data"
GOLD_INCR_DIR = f"{DATA_DIR}/gold_incremental"
PROCESSED_DIR = f"{DATA_DIR}/gold_processed"

os.makedirs(PROCESSED_DIR, exist_ok=True)

print("🔌 Incremental Load to PostgreSQL - Starting...")
print(f"   Connecting to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")

# ==============================================================================
# Connect to Database
# ==============================================================================
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("   ✅ Connection established.")
except Exception as e:
    print(f"❌ Connection error: {e}")
    raise

# ==============================================================================
# Process Incremental Market Activity Files
# ==============================================================================
market_files = glob.glob(f"{GOLD_INCR_DIR}/fact_market_activity_*.csv")
print(f"\n📊 Found {len(market_files)} market activity files to load.")

for file_path in market_files:
    print(f"   Loading: {os.path.basename(file_path)}")
    df = pd.read_csv(file_path)
    
    if len(df) == 0:
        print("      (empty file, skipping)")
        continue
    
    # Insert rows
    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO fact_market_activity (date_key, category_key, bucket_key, total_listings, total_revenue)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                row['date_key'],
                row['category_key'] if pd.notna(row['category_key']) else None,
                row['bucket_key'] if pd.notna(row['bucket_key']) else None,
                row['total_listings'],
                row['total_revenue']
            ))
        except Exception as e:
            print(f"      ⚠️ Insert error: {e}")
    
    conn.commit()
    print(f"      ✅ Inserted {len(df)} rows.")
    
    # Move processed file
    os.rename(file_path, f"{PROCESSED_DIR}/{os.path.basename(file_path)}")

# ==============================================================================
# Process Incremental Seller Performance Files
# ==============================================================================
seller_files = glob.glob(f"{GOLD_INCR_DIR}/fact_seller_performance_*.csv")
print(f"\n👤 Found {len(seller_files)} seller performance files to load.")

for file_path in seller_files:
    print(f"   Loading: {os.path.basename(file_path)}")
    df = pd.read_csv(file_path)
    
    if len(df) == 0:
        print("      (empty file, skipping)")
        continue
    
    # Insert rows
    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO fact_seller_performance (date_key, category_key, seller_name, total_listings, total_revenue)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                row['date_key'],
                row['category_key'] if pd.notna(row['category_key']) else None,
                row['seller_name'],
                row['total_listings'],
                row['total_revenue']
            ))
        except Exception as e:
            print(f"      ⚠️ Insert error: {e}")
    
    conn.commit()
    print(f"      ✅ Inserted {len(df)} rows.")
    
    # Move processed file
    os.rename(file_path, f"{PROCESSED_DIR}/{os.path.basename(file_path)}")

# ==============================================================================
# Cleanup
# ==============================================================================
cursor.close()
conn.close()

print("\n🎉 Incremental Load Complete!")
