import psycopg2

conn = psycopg2.connect(
    host='localhost',
    port=5433,
    database='ebay_maroc',
    user='ebay_user',
    password='ebay123'
)
cur = conn.cursor()
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
tables = cur.fetchall()
print(f"Tables dans la base: {len(tables)}")
for t in tables:
    cur.execute(f'SELECT COUNT(*) FROM "{t[0]}"')
    print(f"  - {t[0]}: {cur.fetchone()[0]} lignes")
conn.close()
