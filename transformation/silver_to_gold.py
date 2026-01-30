"""
Transformation Silver → Gold (Star Schema Version)
Génère des Dimensions et des Faits pour un Dashboard Power BI 100% Dynamique.
"""

import pandas as pd
import os
import datetime

# ==============================================================================
# Configuration
# ==============================================================================
SILVER_FILE = r"C:\Users\pc\Desktop\eBay\eBay-project\data\silver\silver_data.csv"
GOLD_DIR = r"C:\Users\pc\Desktop\eBay\eBay-project\data\gold"

os.makedirs(GOLD_DIR, exist_ok=True)

print("🚀 Démarrage de la transformation Silver → Gold (Star Schema)...")

# ==============================================================================
# 1. Structures de Données (Accumulateurs)
# ==============================================================================
# Sets pour les dimensions
unique_categories = set()
unique_buckets = set()

# Dictionnaires pour les Faits
# Key: (year, month, category, bucket) -> Val: {count, sum_price}
market_agg = {}

# Key: (year, month, category, seller) -> Val: {count, sum_price}
seller_agg = {}

CHUNK_SIZE = 500000
total_rows = 0

print(f"📊 Lecture de: {SILVER_FILE}")

# ==============================================================================
# 2. Lecture et Agrégation
# ==============================================================================
for i, chunk in enumerate(pd.read_csv(SILVER_FILE, chunksize=CHUNK_SIZE)):
    total_rows += len(chunk)
    print(f"   Lot {i+1}: {total_rows:,} lignes traitées...")
    
    # Remplir les sets de dimensions
    unique_categories.update(chunk['category_main'].dropna().unique())
    unique_buckets.update(chunk['price_bucket'].dropna().unique())
    
    # --- Fact 1: Market Activity (Detail: Year, Month, Category, PriceBucket) ---
    # GroupBy pour réduire la taille en mémoire
    group_market = chunk.groupby(['listing_year', 'listing_month', 'category_main', 'price_bucket']).agg({
        'price': ['count', 'sum']
    })
    
    for idx, row in group_market.iterrows():
        # idx = (year, month, category, bucket)
        if idx not in market_agg:
            market_agg[idx] = {'count': 0, 'sum': 0.0}
        market_agg[idx]['count'] += row[('price', 'count')]
        market_agg[idx]['sum'] += row[('price', 'sum')]

    # --- Fact 2: Seller Performance (Detail: Year, Month, Category, Seller) ---
    group_seller = chunk.groupby(['listing_year', 'listing_month', 'category_main', 'seller']).agg({
        'price': ['count', 'sum']
    })
    
    for idx, row in group_seller.iterrows():
        # idx = (year, month, category, seller)
        if idx not in seller_agg:
            seller_agg[idx] = {'count': 0, 'sum': 0.0}
        seller_agg[idx]['count'] += row[('price', 'count')]
        seller_agg[idx]['sum'] += row[('price', 'sum')]

print(f"\n✅ Agrégation terminée. {total_rows:,} lignes traitées.")

# ==============================================================================
# 3. Création des DIMENSIONS
# ==============================================================================

# --- A. DIM_CATEGORY ---
print("\n📦 Création: dim_category.csv")
category_list = sorted(list(unique_categories))
df_dim_category = pd.DataFrame({
    'category_key': range(1, len(category_list) + 1),
    'category_name': category_list
})
# Créer un map pour les FK
cat_map = dict(zip(df_dim_category['category_name'], df_dim_category['category_key']))
df_dim_category.to_csv(f"{GOLD_DIR}/dim_category.csv", index=False)
print(f"   -> {len(df_dim_category)} catégories")

# --- B. DIM_PRICE_BUCKET ---
print("\n💰 Création: dim_price_bucket.csv")
# Ordre logique personnalisé (pas alphabétique)
bucket_order = {
    '0-10': 1,
    '10-50': 2,
    '50-100': 3,
    '100-500': 4,
    '500-1000': 5,
    '1000+': 6
}
bucket_list = sorted(list(unique_buckets), key=lambda x: bucket_order.get(x, 99))

df_dim_bucket = pd.DataFrame({
    'bucket_key': range(1, len(bucket_list) + 1),
    'bucket_name': bucket_list,
    'sort_order': [bucket_order.get(b, 99) for b in bucket_list]
})
bucket_map = dict(zip(df_dim_bucket['bucket_name'], df_dim_bucket['bucket_key']))
df_dim_bucket.to_csv(f"{GOLD_DIR}/dim_price_bucket.csv", index=False)
print(f"   -> {len(df_dim_bucket)} buckets")

# --- C. DIM_TIME ---
print("\n📅 Création: dim_time.csv")
# Générer tous les mois de 2020 à 2025
start_date = datetime.date(2015, 1, 1)
end_date = datetime.date(2030, 12, 1) # Assurer qu'on couvre tout
dates = []
curr = start_date
while curr <= end_date:
    dates.append({
        'date_key': curr, # Format YYYY-MM-DD (1er du mois)
        'year': curr.year,
        'month': curr.month,
        'month_name': curr.strftime('%B'),
        'quarter': (curr.month - 1) // 3 + 1,
        'year_month': curr.strftime('%Y-%m')
    })
    # Passer au mois suivant
    if curr.month == 12:
        curr = datetime.date(curr.year + 1, 1, 1)
    else:
        curr = datetime.date(curr.year, curr.month + 1, 1)

df_dim_time = pd.DataFrame(dates)
df_dim_time.to_csv(f"{GOLD_DIR}/dim_time.csv", index=False)
print(f"   -> {len(df_dim_time)} mois générés")


# ==============================================================================
# 4. Création des FAITS
# ==============================================================================

# --- A. FACT_MARKET_ACTIVITY ---
print("\n📈 Création: fact_market_activity.csv")
fact_market_rows = []
for (year, month, cat, bucket), data in market_agg.items():
    # Créer la date_key (1er du mois) pour joindre avec dim_time
    date_key = datetime.date(year, month, 1)
    
    fact_market_rows.append({
        'date_key': date_key,
        'category_key': cat_map.get(cat),   # FK vers Dim Category
        'bucket_key': bucket_map.get(bucket), # FK vers Dim Price
        'total_listings': data['count'],
        'total_revenue': round(data['sum'], 2)
    })

df_fact_market = pd.DataFrame(fact_market_rows)
df_fact_market.to_csv(f"{GOLD_DIR}/fact_market_activity.csv", index=False)
print(f"   -> {len(df_fact_market)} lignes")


# --- B. FACT_SELLER_PERFORMANCE ---
print("\n👤 Création: fact_seller_performance.csv")
fact_seller_rows = []
for (year, month, cat, seller), data in seller_agg.items():
     # Garder uniquement les vendeurs significatifs par mois (ex: > 0 vente)
     # Ici on garde tout car c'est déjà agrégé
     date_key = datetime.date(year, month, 1)
     
     fact_seller_rows.append({
        'date_key': date_key,
        'category_key': cat_map.get(cat),
        'seller_name': seller, # On garde le nom direct (pas de dim_seller pour simplifier)
        'total_listings': data['count'],
        'total_revenue': round(data['sum'], 2)
     })

df_fact_seller = pd.DataFrame(fact_seller_rows)
# Trier par date et volume
df_fact_seller = df_fact_seller.sort_values(['date_key', 'total_listings'], ascending=[True, False])

df_fact_seller.to_csv(f"{GOLD_DIR}/fact_seller_performance.csv", index=False)
print(f"   -> {len(df_fact_seller)} lignes")

print("\n🎉 Transformation terminée ! Système Star Schema prêt.")
