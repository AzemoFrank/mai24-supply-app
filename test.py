import warnings
warnings.filterwarnings("ignore")
import duckdb
import json
import os

# Connexion à la base de données DuckDB existante
db_path = os.getenv('DUCKDB_PATH', 'data/supply_app.duckdb')
con = duckdb.connect(db_path)

# con.execute("""
#     DROP TABLE  data_scraped_traite_traduit
# """)

df = con.execute("""
    SELECT *
    FROM data_scraped_traite_non_traduit
    LIMIT 5
""").fetch_df()

print(df)

con.close()