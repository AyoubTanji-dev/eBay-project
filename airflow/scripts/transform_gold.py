"""
Incremental Silver → Gold Transformation
Processes daily batch and outputs incremental fact files.
"""

import pandas as pd
import os
import datetime

# ==============================================================================
# Configuration (Container Paths)
# ==============================================================================
DATA_DIR = "/opt/airflow/data"
DAILY_INPUT = f"{DATA_DIR}/staging/daily_input.csv"
GOLD_DIR = f"{DATA_DIR}/gold_incremental"

os.makedirs(GOLD_DIR, exist_ok=True)

print("⭐ Incremental Gold Transformation - Starting...")

# ==============================================================================
# Read Daily Batch
# ==============================================================================
if not os.path.exists(DAILY_INPUT):
    print("⚠️ No daily input file found. Nothing to process.")
    exit(0)

df = pd.read_csv(DAILY_INPUT)

if len(df) == 0:
    print("⚠️ Daily input is empty. Nothing to process.")
    exit(0)

print(f"   Processing {len(df)} rows...")

# ==============================================================================
# Build Dimension Mappings (Load existing or create new)
# ==============================================================================
DIM_CAT_FILE = f"{DATA_DIR}/gold/dim_category.csv"
DIM_BUCKET_FILE = f"{DATA_DIR}/gold/dim_price_bucket.csv"

# Load existing dimensions
if os.path.exists(DIM_CAT_FILE):
    df_dim_cat = pd.read_csv(DIM_CAT_FILE)
    cat_map = dict(zip(df_dim_cat['category_name'], df_dim_cat['category_key']))
else:
    cat_map = {}

if os.path.exists(DIM_BUCKET_FILE):
    df_dim_bucket = pd.read_csv(DIM_BUCKET_FILE)
    bucket_map = dict(zip(df_dim_bucket['bucket_name'], df_dim_bucket['bucket_key']))
else:
    bucket_map = {}

# ==============================================================================
# Aggregate: Fact Market Activity
# ==============================================================================
print("   Building fact_market_activity...")
group_market = df.groupby(['listing_year', 'listing_month', 'category_main', 'price_bucket']).agg({
    'price': ['count', 'sum']
}).reset_index()

group_market.columns = ['listing_year', 'listing_month', 'category_main', 'price_bucket', 'total_listings', 'total_revenue']

# Add keys
group_market['date_key'] = group_market.apply(
    lambda r: datetime.date(int(r['listing_year']), int(r['listing_month']), 1), axis=1
)
group_market['category_key'] = group_market['category_main'].map(cat_map)
group_market['bucket_key'] = group_market['price_bucket'].map(bucket_map)

fact_market = group_market[['date_key', 'category_key', 'bucket_key', 'total_listings', 'total_revenue']]
fact_market['total_revenue'] = fact_market['total_revenue'].round(2)

# ==============================================================================
# Aggregate: Fact Seller Performance
# ==============================================================================
print("   Building fact_seller_performance...")
group_seller = df.groupby(['listing_year', 'listing_month', 'category_main', 'seller']).agg({
    'price': ['count', 'sum']
}).reset_index()

group_seller.columns = ['listing_year', 'listing_month', 'category_main', 'seller', 'total_listings', 'total_revenue']

group_seller['date_key'] = group_seller.apply(
    lambda r: datetime.date(int(r['listing_year']), int(r['listing_month']), 1), axis=1
)
group_seller['category_key'] = group_seller['category_main'].map(cat_map)

fact_seller = group_seller[['date_key', 'category_key', 'seller', 'total_listings', 'total_revenue']]
fact_seller = fact_seller.rename(columns={'seller': 'seller_name'})
fact_seller['total_revenue'] = fact_seller['total_revenue'].round(2)

# ==============================================================================
# Save Incremental Files (Timestamped)
# ==============================================================================
timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

fact_market.to_csv(f"{GOLD_DIR}/fact_market_activity_{timestamp}.csv", index=False)
fact_seller.to_csv(f"{GOLD_DIR}/fact_seller_performance_{timestamp}.csv", index=False)

print(f"   ✅ Saved incremental facts with timestamp {timestamp}")
print("🎉 Incremental Gold Transformation Complete!")
