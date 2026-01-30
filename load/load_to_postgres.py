"""
Chargement des tables GOLD (Star Schema) dans PostgreSQL
"""

import pandas as pd
import psycopg2
from psycopg2 import sql
import os

# ==============================================================================
# Configuration
# ==============================================================================
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'ebay_maroc',
    'user': 'ebay_user',
    'password': 'ebay123'
}

GOLD_DIR = r"C:\Users\pc\Desktop\eBay\eBay-project\data\gold"

# Liste des tables à charger (Ordre important: Dimensions d'abord, puis Faits)
TABLES_ORDER = [
    'dim_time',
    'dim_category',
    'dim_price_bucket',
    'fact_market_activity',
    'fact_seller_performance'
]

print("🚀 Chargement des tables GOLD (Architecture Star Schema)...")

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("✅ Connexion PostgreSQL établie.")
except Exception as e:
    print(f"❌ Erreur connexion: {e}")
    exit(1)

# ==============================================================================
# Chargement
# ==============================================================================

for table_name in TABLES_ORDER:
    file_path = os.path.join(GOLD_DIR, f"{table_name}.csv")
    
    if not os.path.exists(file_path):
        print(f"⚠️ Fichier manquant: {file_path}")
        continue
        
    print(f"\n📦 Traitement de {table_name}...")
    df = pd.read_csv(file_path)
    
    # DROP Table
    cursor.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table_name)))
    
    # CREATE Table (Typage dynamique)
    columns_def = []
    for col in df.columns:
        dtype = df[col].dtype
        if table_name == 'dim_time' and col == 'date_key':
            pg_type = 'DATE' # Force Date type for key
        elif table_name.startswith('fact_') and col == 'date_key':
             pg_type = 'DATE'
        elif 'int' in str(dtype):
            pg_type = 'BIGINT'
        elif 'float' in str(dtype):
             pg_type = 'DOUBLE PRECISION'
        else:
            pg_type = 'TEXT'
        columns_def.append(f'"{col}" {pg_type}')
    
    # Définition des Primary Keys (Optionnel mais propre)
    pk_def = ""
    if table_name == 'dim_time':
        pk_def = ', PRIMARY KEY ("date_key")'
    elif table_name == 'dim_category':
        pk_def = ', PRIMARY KEY ("category_key")'
    elif table_name == 'dim_price_bucket':
        pk_def = ', PRIMARY KEY ("bucket_key")'
        
    create_sql = f'CREATE TABLE "{table_name}" ({", ".join(columns_def)}{pk_def})'
    cursor.execute(create_sql)
    
    # INSERT Data
    if len(df) > 0:
        cols = ', '.join([f'"{c}"' for c in df.columns])
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_sql = f'INSERT INTO "{table_name}" ({cols}) VALUES ({placeholders})'
        
        # Batch execution pour vitesse
        rows = list(df.itertuples(index=False, name=None))
        # Remplacer NaN par None
        batch = []
        for r in rows:
            batch.append(tuple(None if pd.isna(x) else x for x in r))
            
        # Insert par paquets de 1000
        batch_size = 1000
        for i in range(0, len(batch), batch_size):
            cursor.executemany(insert_sql, batch[i:i + batch_size])
            
    conn.commit()
    print(f"   ✅ Chargé {len(df)} lignes.")

# ==============================================================================
# Vérification & Relations (Optionnel - Création d'index pour perfs)
# ==============================================================================
print("\n🔗 Création des index pour la performance...")
try:
    # Index sur les clés étrangères des faits
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_fma_date ON fact_market_activity ("date_key")')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_fma_cat ON fact_market_activity ("category_key")')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_fsp_date ON fact_seller_performance ("date_key")')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_fsp_cat ON fact_seller_performance ("category_key")')
    conn.commit()
    print("   ✅ Index créés.")
except Exception as e:
    print(f"   ⚠️ Erreur index: {e}")

cursor.close()
conn.close()
print("\n🎉 ETL Gold terminé avec succès ! Base prête pour Power BI.")
