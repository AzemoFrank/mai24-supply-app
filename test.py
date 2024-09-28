import warnings
warnings.filterwarnings("ignore")
import duckdb
import json
import os

# Connexion à la base de données DuckDB existante
con = duckdb.connect('supply_app.duckdb')

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