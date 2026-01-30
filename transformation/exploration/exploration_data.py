"""
EXPLORATION PURE des données eBay Maroc avec PySpark
Aucune transformation - uniquement observation des données brutes
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnull, length
from datetime import datetime

# Configuration
INPUT_FILE = "../data/morocco_market_enriched.csv"
REPORT_FILE = "exploration/exploration_report.txt"

# Créer la session Spark
spark = SparkSession.builder \
    .appName("eBay Data Exploration") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ============================================================
# CLASSE POUR ÉCRIRE DANS FICHIER ET CONSOLE
# ============================================================
class ReportWriter:
    def __init__(self, filename):
        self.file = open(filename, 'w', encoding='utf-8')
        header = f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    RAPPORT D'EXPLORATION DES DONNÉES                         ║
║                           eBay Maroc Dataset                                 ║
║                      Généré le: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}                        ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""
        self.print(header)
    
    def print(self, text=""):
        print(text)
        self.file.write(text + "\n")
    
    def section(self, title, emoji="📊"):
        self.print(f"\n{'='*78}")
        self.print(f"{emoji}  {title}")
        self.print("=" * 78)
    
    def close(self):
        self.print(f"\n{'='*78}")
        self.print("✅ Rapport sauvegardé dans: " + REPORT_FILE)
        self.file.close()

# ============================================================
# EXPLORATION PURE (aucune transformation)
# ============================================================
report = ReportWriter(REPORT_FILE)

# 1. Lecture avec inférence automatique du schema
report.section("CHARGEMENT DES DONNÉES", "📂")
report.print(f"Fichier: {INPUT_FILE}\n")

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(INPUT_FILE)

df.cache()
row_count = df.count()

report.print(f"✅ Lignes: {row_count:,}")
report.print(f"✅ Colonnes: {len(df.columns)}")

# 2. Schema détecté par Spark
report.section("SCHEMA DÉTECTÉ PAR SPARK (types inférés)", "🔧")
report.print("Voici comment Spark a interprété vos données:\n")
for field in df.schema.fields:
    report.print(f"   {field.name:<20} → {str(field.dataType)}")

# 3. Aperçu des données brutes
report.section("APERÇU DES DONNÉES BRUTES (5 lignes)", "👀")
sample = df.limit(5).toPandas()
report.print(sample.to_string(index=False))

# 4. Valeurs nulles
report.section("VALEURS NULLES PAR COLONNE", "❓")
report.print(f"{'Colonne':<20} {'Nulls':>12} {'%':>8}")
report.print("-" * 45)
for column in df.columns:
    nulls = df.filter(isnull(col(column))).count()
    pct = (nulls / row_count) * 100
    report.print(f"{column:<20} {nulls:>12,} {pct:>7.2f}%")

# 5. Valeurs uniques
report.section("VALEURS UNIQUES PAR COLONNE", "�")
report.print(f"{'Colonne':<20} {'Uniques':>15}")
report.print("-" * 40)
for column in df.columns:
    uniques = df.select(column).distinct().count()
    report.print(f"{column:<20} {uniques:>15,}")

# 6. Distribution des catégories
report.section("DISTRIBUTION DES CATÉGORIES", "📦")
cat_dist = df.groupBy("category").count().orderBy(col("count").desc()).limit(20).toPandas()
report.print(cat_dist.to_string(index=False))

# 7. Distribution des vendeurs
report.section("TOP 15 VENDEURS", "👤")
sellers = df.groupBy("seller").count().orderBy(col("count").desc()).limit(15).toPandas()
report.print(sellers.to_string(index=False))

# 8. Exemples de valeurs par colonne
report.section("EXEMPLES DE VALEURS (pour vérifier le format)", "🔍")
for column in df.columns:
    examples = df.select(column).distinct().limit(3).toPandas()[column].tolist()
    report.print(f"   {column}: {examples}")

# 9. Statistiques prix (si numérique)
report.section("STATISTIQUES PRIX (describe)", "💰")
report.print("Statistiques brutes retournées par Spark:\n")
price_stats = df.describe("price").toPandas()
report.print(price_stats.to_string(index=False))

# Fin
df.unpersist()
spark.stop()
report.close()
