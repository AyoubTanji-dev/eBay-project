"""
Script d'enrichissement des données eBay Maroc
Génère ~2Go de données CSV pour un projet Data Engineering avec PySpark
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from tqdm import tqdm
import os

# Configuration
INPUT_FILE = "morocco_market_data.xlsx"
OUTPUT_FILE = "morocco_market_enriched.csv"
TARGET_SIZE_GB = 2.0
BATCH_SIZE = 100000  # Écriture par lots pour performance

# Catégories eBay réalistes
CATEGORIES = [
    "Electronics", "Cell Phones & Accessories", "Computers/Tablets & Networking",
    "Cameras & Photo", "TV, Video & Home Audio", "Video Games & Consoles",
    "Clothing, Shoes & Accessories", "Jewelry & Watches", "Health & Beauty",
    "Home & Garden", "Sporting Goods", "Toys & Hobbies",
    "Automotive", "Business & Industrial", "Musical Instruments & Gear",
    "Books, Movies & Music", "Baby", "Pet Supplies"
]

# Préfixes de produits par catégorie
PRODUCT_TEMPLATES = {
    "Electronics": [
        "LED TV {size}\" {brand} Smart", "Wireless Headphones {brand}",
        "Bluetooth Speaker {brand}", "Power Bank {capacity}mAh",
        "USB Cable Type-C {length}m", "Phone Charger Fast Charging {brand}",
        "HDMI Cable {length}m Gold Plated", "Laptop Stand Aluminum",
        "Webcam HD {resolution}p {brand}", "Gaming Mouse RGB {brand}"
    ],
    "Cell Phones & Accessories": [
        "iPhone {model} {storage}GB {condition}", "Samsung Galaxy {model} {storage}GB",
        "Phone Case for {brand} {model}", "Screen Protector Tempered Glass",
        "Car Phone Holder Magnetic", "Wireless Charger Qi 15W",
        "Earbuds True Wireless {brand}", "SIM Card Adapter Kit",
        "Selfie Stick with Tripod", "Phone Ring Holder 360°"
    ],
    "Computers/Tablets & Networking": [
        "Laptop {brand} {processor} {ram}GB RAM", "iPad {model} {storage}GB",
        "SSD {capacity}GB {brand} Internal", "External Hard Drive {capacity}TB",
        "WiFi Router Dual Band {brand}", "Mechanical Keyboard RGB {brand}",
        "Wireless Mouse Ergonomic", "Laptop Bag {size}\" Professional",
        "USB Hub 7-Port Powered", "Graphics Card {model} {memory}GB"
    ],
    "Clothing, Shoes & Accessories": [
        "Men's T-Shirt {brand} Size {size}", "Women's Dress {style} Size {size}",
        "Running Shoes {brand} Size {size}", "Leather Wallet {brand}",
        "Sunglasses Polarized UV400", "Baseball Cap {brand}",
        "Backpack Travel {brand}", "Wristwatch {type} {brand}",
        "Belt Leather {brand}", "Scarf Cashmere {color}"
    ],
    "Automotive": [
        "Car Air Freshener {scent}", "Dash Cam HD {resolution}p",
        "LED Headlight Bulbs H7", "Car Phone Mount Dashboard",
        "Engine Oil Filter {brand}", "Brake Pads Front {brand}",
        "Windshield Wipers {size}\" Set", "Car Cover Waterproof",
        "Tire Pressure Gauge Digital", "Jump Starter Portable"
    ],
    "Home & Garden": [
        "Coffee Maker {type} {brand}", "Blender 1000W {brand}",
        "Garden Tool Set {pieces}pc", "LED Light Bulbs E27 {watt}W",
        "Cookware Set Stainless Steel", "Vacuum Cleaner Robot {brand}",
        "Iron Steam Professional", "Curtains Blackout {size}cm",
        "Door Mat Anti-Slip", "Plant Pot Ceramic {size}cm"
    ]
}

# Marques réalistes
BRANDS = {
    "Electronics": ["Sony", "LG", "Samsung", "Philips", "JBL", "Anker", "Belkin"],
    "Cell Phones & Accessories": ["Apple", "Samsung", "Huawei", "Xiaomi", "OnePlus", "Oppo"],
    "Computers/Tablets & Networking": ["Dell", "HP", "Lenovo", "ASUS", "Acer", "MSI", "Corsair"],
    "Automotive": ["Bosch", "Michelin", "Castrol", "3M", "Philips"],
    "Home & Garden": ["Tefal", "Philips", "Moulinex", "Braun", "Black+Decker"]
}

# Noms de vendeurs marocains réalistes
MOROCCAN_SELLERS = [
    f"seller_ma_{i:03d}" for i in range(1, 201)
] + [
    "casablanca_tech", "marrakech_electronics", "rabat_deals", "tanger_shop",
    "fes_market", "agadir_store", "morocco_best", "atlas_trading",
    "sahara_electronics", "maghreb_outlet", "medina_shop", "souk_online"
]

def generate_product_title(category):
    """Génère un titre de produit réaliste"""
    templates = PRODUCT_TEMPLATES.get(category, PRODUCT_TEMPLATES["Electronics"])
    template = random.choice(templates)
    
    # Substitutions
    replacements = {
        "{size}": str(random.choice([32, 40, 43, 50, 55, 65, 75, 13, 15, 17, 24, 27])),
        "{brand}": random.choice(BRANDS.get(category, BRANDS["Electronics"])),
        "{capacity}": str(random.choice([10000, 20000, 30000, 50000, 128, 256, 512, 1, 2, 4])),
        "{length}": str(random.choice([1, 2, 3, 5, 10])),
        "{resolution}": str(random.choice([720, 1080, 1440, 2160, 4096])),
        "{model}": random.choice(["11", "12", "13", "14", "15 Pro", "S21", "S22", "S23", "A54", "Note 20"]),
        "{storage}": str(random.choice([64, 128, 256, 512, 1024])),
        "{condition}": random.choice(["New", "Like New", "Refurbished", "Used"]),
        "{processor}": random.choice(["i5", "i7", "Ryzen 5", "Ryzen 7", "M1", "M2"]),
        "{ram}": str(random.choice([4, 8, 16, 32])),
        "{style}": random.choice(["Casual", "Formal", "Evening", "Summer", "Winter"]),
        "{size}": random.choice(["S", "M", "L", "XL", "XXL", "42", "43", "44", "45"]),
        "{type}": random.choice(["Analog", "Digital", "Smart", "Automatic"]),
        "{color}": random.choice(["Black", "White", "Blue", "Red", "Gray", "Brown"]),
        "{scent}": random.choice(["Vanilla", "Lavender", "Ocean", "New Car", "Coffee"]),
        "{watt}": str(random.choice([7, 9, 12, 15, 20])),
        "{pieces}": str(random.choice([5, 10, 12, 15, 20])),
        "{memory}": str(random.choice([4, 6, 8, 12, 16]))
    }
    
    for key, value in replacements.items():
        template = template.replace(key, value)
    
    return template

def generate_price(category):
    """Génère un prix réaliste selon la catégorie"""
    price_ranges = {
        "Electronics": (15, 500),
        "Cell Phones & Accessories": (5, 800),
        "Computers/Tablets & Networking": (30, 2000),
        "Cameras & Photo": (50, 1500),
        "Clothing, Shoes & Accessories": (10, 150),
        "Automotive": (8, 300),
        "Home & Garden": (12, 400),
        "Toys & Hobbies": (5, 100)
    }
    
    min_price, max_price = price_ranges.get(category, (10, 200))
    
    # Distribution log-normale pour plus de réalisme
    price = np.random.lognormal(np.log(min_price + (max_price - min_price) / 3), 0.7)
    price = max(min_price, min(max_price, price))
    
    # Arrondir à .99 ou .00
    if random.random() > 0.3:
        price = int(price) + 0.99
    else:
        price = round(price, 0)
    
    return round(price, 2)

def generate_date_range(start_date, end_date):
    """Génère une date aléatoire dans l'intervalle"""
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    random_seconds = random.randint(0, 24 * 60 * 60)
    return start_date + timedelta(days=random_days, seconds=random_seconds)

def estimate_rows_needed(existing_rows, target_gb):
    """Estime le nombre de lignes nécessaires pour atteindre la taille cible"""
    # Approximation: 1 ligne CSV ~ 80-100 bytes en moyenne
    avg_bytes_per_row = 90
    target_bytes = target_gb * 1024 * 1024 * 1024
    estimated_rows = int(target_bytes / avg_bytes_per_row)
    
    print(f"📊 Lignes existantes: {existing_rows:,}")
    print(f"🎯 Lignes cibles estimées: {estimated_rows:,}")
    print(f"✨ Nouvelles lignes à générer: {estimated_rows - existing_rows:,}")
    
    return estimated_rows

def generate_batch(num_rows, start_date, end_date):
    """Génère un lot de données synthétiques"""
    data = []
    
    for _ in range(num_rows):
        category = random.choice(CATEGORIES)
        seller = random.choice(MOROCCAN_SELLERS)
        
        run_date = datetime.now().date().isoformat()
        collected_at = datetime.now().isoformat()
        listing_date = generate_date_range(start_date, end_date).isoformat()
        
        title = generate_product_title(category)
        price = generate_price(category)
        
        data.append({
            'run_date': run_date,
            'collected_at': collected_at,
            'listing_date': listing_date,
            'seller': seller,
            'title': title,
            'category': category,
            'price': price,
            'currency': 'USD',
            'market': 'MOROCCO'
        })
    
    return pd.DataFrame(data)

def main():
    print("🚀 Démarrage de l'enrichissement des données eBay Maroc")
    print("=" * 60)
    
    # Charger les données existantes
    print(f"📂 Chargement de {INPUT_FILE}...")
    df_existing = pd.read_excel(INPUT_FILE)
    existing_rows = len(df_existing)
    
    print(f"✅ Chargé: {existing_rows:,} lignes")
    print(f"📋 Colonnes: {list(df_existing.columns)}")
    
    # Calculer le nombre de lignes nécessaires
    target_rows = estimate_rows_needed(existing_rows, TARGET_SIZE_GB)
    new_rows_needed = target_rows - existing_rows
    
    # Dates pour la génération
    end_date = datetime.now()
    start_date = end_date - timedelta(days=730)  # 2 ans
    
    # Écrire le fichier par lots
    print(f"\n💾 Génération du fichier CSV...")
    print(f"📝 Fichier de sortie: {OUTPUT_FILE}")
    
    # Écrire d'abord les données existantes
    df_existing.to_csv(OUTPUT_FILE, index=False, mode='w')
    
    # Générer et ajouter les nouvelles données par lots
    total_generated = 0
    
    with tqdm(total=new_rows_needed, desc="🔄 Génération des données", unit=" lignes") as pbar:
        while total_generated < new_rows_needed:
            batch_size = min(BATCH_SIZE, new_rows_needed - total_generated)
            
            # Générer un lot
            df_batch = generate_batch(batch_size, start_date, end_date)
            
            # Ajouter au fichier CSV
            df_batch.to_csv(OUTPUT_FILE, index=False, mode='a', header=False)
            
            total_generated += batch_size
            pbar.update(batch_size)
    
    # Vérification finale
    print("\n✅ Génération terminée!")
    
    file_size_bytes = os.path.getsize(OUTPUT_FILE)
    file_size_gb = file_size_bytes / (1024 ** 3)
    
    print(f"\n📊 Statistiques finales:")
    print(f"  • Fichier: {OUTPUT_FILE}")
    print(f"  • Taille: {file_size_gb:.2f} Go ({file_size_bytes:,} bytes)")
    print(f"  • Lignes totales: {existing_rows + total_generated:,}")
    print(f"  • Lignes originales: {existing_rows:,}")
    print(f"  • Lignes générées: {total_generated:,}")
    
    # Test de lecture
    print("\n🧪 Test de lecture...")
    df_test = pd.read_csv(OUTPUT_FILE, nrows=5)
    print(df_test)
    
    print("\n🎉 Prêt pour PySpark! 🎉")

if __name__ == "__main__":
    main()
