"""
Nettoyage des anciennes tables et fichiers (Legacy)
Supprime les tables statiques obsolètes qui ont été remplacées par le Star Schema.
"""

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

# Liste des anciennes tables à supprimer
LEGACY_TABLES = [
    'sales_by_category',
    'sales_by_month',
    'top_sellers',
    'price_distribution',
    'sales_by_category_month',
    'sellers_by_category',
    'sales_by_year',
    'price_by_category'
]

print("🧹 Démarrage du nettoyage des anciennes données...")

# 1. Nettoyage Base de Données
print("\n🔌 Connexion à PostgreSQL...")
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    for table in LEGACY_TABLES:
        print(f"   🗑️ Suppression de la table : {table}")
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table)))
    
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Nettoyage DB terminé.")
    
except Exception as e:
    print(f"❌ Erreur DB: {e}")

# 2. Nettoyage Fichiers CSV
print("\n📂 Nettoyage des fichiers CSV obsolètes...")
for table in LEGACY_TABLES:
    file_path = os.path.join(GOLD_DIR, f"{table}.csv")
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            print(f"   🗑️ Supprimé : {table}.csv")
        except Exception as e:
            print(f"   ⚠️ Erreur suppression fichier {table}.csv : {e}")
    else:
        print(f"   ℹ️ Fichier déjà absent : {table}.csv")

print("\n✨ Grand ménage terminé ! Il ne reste que le Star Schema.")
