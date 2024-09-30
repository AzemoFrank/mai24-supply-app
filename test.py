import warnings
warnings.filterwarnings("ignore")
import duckdb
import json
import os

# Connexion à la base de données DuckDB existante
db_path = os.getenv('DUCKDB_PATH', 'data/supply_app.duckdb')
con = duckdb.connect(db_path)

con.execute("""
    DROP TABLE  data_scrapped_brut
""")

# df = con.execute("""
#     SELECT *
#     FROM data_scrapped_brut
#     LIMIT 5
# """).fetch_df()

# print(df)

con.close()