"""
Transformation et nettoyage des données eBay Maroc avec PySpark
"""

import os
# Configuration Hadoop pour Windows (évite l'erreur HADOOP_HOME)
os.environ["HADOOP_HOME"] = r"C:\hadoop-3.4.1-src"
os.environ["PATH"] = os.environ["PATH"] + r";C:\hadoop-3.4.1-src\bin"
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, when, count, avg, year, month, dayofmonth,
    to_timestamp, upper, regexp_replace, lit
)
from pyspark.sql.types import DoubleType, TimestampType

# ==============================================================================
# Configuration
# ==============================================================================
INPUT_FILE = "../data/morocco_market_enriched.csv"
OUTPUT_DIR = "../data/silver"

# ==============================================================================
# Session Spark
# ==============================================================================
print("🚀 Démarrage de la transformation...")

spark = SparkSession.builder \
    .appName("eBay Data Transformation") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ==============================================================================
# 1. LECTURE DES DONNÉES
# ==============================================================================
print("\n📂 Lecture des données...")
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(INPUT_FILE)

initial_count = df.count()
print(f"✅ Lignes chargées: {initial_count:,}")

# ==============================================================================
# 2. TRANSFORMATION 1 : Filtrer les lignes valides
# ==============================================================================
print("\n🔧 Transformation 1: Filtrage des lignes valides...")

df_filtered = df.filter(col("market") == "MOROCCO")
df_filtered = df_filtered.filter(col("currency") == "USD")

filtered_count = df_filtered.count()
removed_count = initial_count - filtered_count
print(f"   • Lignes valides: {filtered_count:,}")
print(f"   • Lignes supprimées: {removed_count:,} ({removed_count/initial_count*100:.2f}%)")

# ==============================================================================
# 3. TRANSFORMATION 2 : Convertir price en Double
# ==============================================================================
print("\n🔧 Transformation 2: Conversion du prix...")

df_typed = df_filtered.withColumn("price", col("price").cast(DoubleType()))
df_typed = df_typed.filter(col("price").isNotNull() & (col("price") > 0))

typed_count = df_typed.count()
print(f"   • Lignes avec prix valide: {typed_count:,}")

# ==============================================================================
# 4. TRANSFORMATION 3 : Parsing des dates
# ==============================================================================
print("\n🔧 Transformation 3: Parsing des dates...")

# S'assurer que listing_date est bien un Timestamp
df_dates = df_typed.withColumn(
    "listing_date", 
    col("listing_date").cast(TimestampType())
)

# Filtrer les dates nulles
df_dates = df_dates.filter(col("listing_date").isNotNull())

dates_count = df_dates.count()
print(f"   • Lignes avec date valide: {dates_count:,}")

# ==============================================================================
# 5. TRANSFORMATION 4 : Colonnes dérivées (year, month, price_bucket)
# ==============================================================================
print("\n🔧 Transformation 4: Ajout des colonnes dérivées...")

# Extraire year et month de listing_date
df_derived = df_dates \
    .withColumn("listing_year", year(col("listing_date"))) \
    .withColumn("listing_month", month(col("listing_date"))) \
    .withColumn("listing_day", dayofmonth(col("listing_date")))

# Créer des tranches de prix (price_bucket)
df_derived = df_derived.withColumn(
    "price_bucket",
    when(col("price") < 10, "0-10")
    .when(col("price") < 50, "10-50")
    .when(col("price") < 100, "50-100")
    .when(col("price") < 500, "100-500")
    .when(col("price") < 1000, "500-1000")
    .otherwise("1000+")
)

print(f"   • Colonnes ajoutées: listing_year, listing_month, listing_day, price_bucket")

# ==============================================================================
# 6. TRANSFORMATION 5 : Normalisation des catégories
# ==============================================================================
print("\n🔧 Transformation 5: Normalisation des catégories...")

# Nettoyer les espaces et mettre en majuscules pour uniformiser
df_normalized = df_derived \
    .withColumn("category", trim(col("category"))) \
    .withColumn("category_normalized", upper(trim(col("category"))))

# Créer une catégorie principale (avant le &)
df_normalized = df_normalized.withColumn(
    "category_main",
    trim(regexp_replace(col("category"), "&.*", ""))
)

print(f"   • Colonnes ajoutées: category_normalized, category_main")

# ==============================================================================
# 7. TRANSFORMATION 6 : Nettoyage des colonnes texte
# ==============================================================================
print("\n🔧 Transformation 6: Nettoyage des colonnes texte...")

df_clean = df_normalized \
    .withColumn("seller", trim(col("seller"))) \
    .withColumn("title", trim(col("title")))

df_clean = df_clean.filter(
    (col("seller") != "") & 
    (col("title") != "") & 
    (col("category") != "")
)

clean_count = df_clean.count()
print(f"   • Lignes après nettoyage: {clean_count:,}")

# ==============================================================================
# 8. TRANSFORMATION 7 : Dédoublonnage
# ==============================================================================
print("\n🔧 Transformation 7: Suppression des doublons...")

df_dedupe = df_clean.dropDuplicates()

final_count = df_dedupe.count()
dedupe_removed = clean_count - final_count
print(f"   • Doublons supprimés: {dedupe_removed:,}")
print(f"   • Lignes finales: {final_count:,}")

# ==============================================================================
# 9. CONTRÔLES QUALITÉ (Data Quality Checks)
# ==============================================================================
print("\n" + "=" * 60)
print("🔍 CONTRÔLES QUALITÉ")
print("=" * 60)

# Check 1: Pas de valeurs nulles dans les colonnes critiques
print("\n✓ Check 1: Valeurs nulles dans colonnes critiques")
critical_columns = ["seller", "title", "category", "price", "listing_date"]
for col_name in critical_columns:
    null_count = df_dedupe.filter(col(col_name).isNull()).count()
    status = "✅ OK" if null_count == 0 else f"❌ {null_count} nulls"
    print(f"   • {col_name}: {status}")

# Check 2: Plage de prix valide
print("\n✓ Check 2: Plage de prix")
price_stats = df_dedupe.agg(
    {"price": "min", "price": "max", "price": "avg"}
).collect()[0]
min_price = df_dedupe.agg({"price": "min"}).collect()[0][0]
max_price = df_dedupe.agg({"price": "max"}).collect()[0][0]
avg_price = df_dedupe.agg({"price": "avg"}).collect()[0][0]
print(f"   • Min: ${min_price:.2f}")
print(f"   • Max: ${max_price:.2f}")
print(f"   • Avg: ${avg_price:.2f}")
status = "✅ OK" if min_price > 0 and max_price < 100000 else "⚠️ À vérifier"
print(f"   • Status: {status}")

# Check 3: Années valides
print("\n✓ Check 3: Années de listing")
years = df_dedupe.select("listing_year").distinct().orderBy("listing_year").collect()
year_list = [row[0] for row in years]
print(f"   • Années présentes: {year_list}")
status = "✅ OK" if all(2020 <= y <= 2026 for y in year_list if y) else "⚠️ À vérifier"
print(f"   • Status: {status}")

# Check 4: Distribution des catégories
print("\n✓ Check 4: Nombre de catégories")
cat_count = df_dedupe.select("category").distinct().count()
print(f"   • Catégories uniques: {cat_count}")
status = "✅ OK" if cat_count > 5 else "⚠️ Peu de diversité"
print(f"   • Status: {status}")

# Check 5: Distribution des vendeurs
print("\n✓ Check 5: Nombre de vendeurs")
seller_count = df_dedupe.select("seller").distinct().count()
print(f"   • Vendeurs uniques: {seller_count}")

# ==============================================================================
# 10. RÉSUMÉ DES TRANSFORMATIONS
# ==============================================================================
print("\n" + "=" * 60)
print("📊 RÉSUMÉ DES TRANSFORMATIONS")
print("=" * 60)
print(f"   • Lignes initiales       : {initial_count:,}")
print(f"   • Après filtrage market  : {filtered_count:,}")
print(f"   • Après conversion prix  : {typed_count:,}")
print(f"   • Après parsing dates    : {dates_count:,}")
print(f"   • Après nettoyage texte  : {clean_count:,}")
print(f"   • Après dédoublonnage    : {final_count:,}")
print(f"   • Total supprimé         : {initial_count - final_count:,} ({(initial_count - final_count)/initial_count*100:.2f}%)")

# ==============================================================================
# 11. SCHEMA FINAL
# ==============================================================================
print("\n🔧 Schema final:")
df_dedupe.printSchema()

print("\n👀 Aperçu des données nettoyées:")
df_dedupe.show(5, truncate=30)

# ==============================================================================
# 12. DISTRIBUTION DES NOUVELLES COLONNES
# ==============================================================================
print("\n� Distribution par année:")
df_dedupe.groupBy("listing_year").count().orderBy("listing_year").show()

print("\n� Distribution par tranche de prix:")
df_dedupe.groupBy("price_bucket").count().orderBy("price_bucket").show()

# ==============================================================================
# 13. EXPORT EN CSV (streaming via toLocalIterator)
# ==============================================================================
OUTPUT_FILE = r"C:\Users\pc\Desktop\eBay\eBay-project\data\silver\silver_data.csv"
print(f"\n💾 Export CSV vers: {OUTPUT_FILE}")

import csv

# Écrire l'en-tête
columns = df_dedupe.columns
with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(columns)

# Stream vers fichier CSV
print("   Streaming vers fichier CSV...")

with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    
    batch = []
    count = 0
    BATCH_SIZE = 50000
    
    for row in df_dedupe.toLocalIterator():
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            writer.writerows(batch)
            batch = []
            count += BATCH_SIZE
            if count % 500000 == 0:
                print(f"   Exporté: {count:,} lignes...")
    
    # Écrire le reste
    if batch:
        writer.writerows(batch)
        count += len(batch)

print(f"✅ Export CSV terminé! Total: {count:,} lignes")

# ==============================================================================
# Fin
# ==============================================================================
spark.stop()
print("\n🎉 Transformation terminée avec succès!")
