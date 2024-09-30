import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import numpy as np
import re
import os
import json
import psutil
from datetime import datetime
from googletrans import Translator
from langdetect import detect
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import duckdb
from tenacity import retry, stop_after_attempt, wait_exponential


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def traduire_texte(translator, text, src, dest):
    return translator.translate(text, src=src, dest=dest).text


def handle_nan_values(df):
    numeric_cols = df.select_dtypes(include=['float64', 'int64', 'int32', 'float32']).columns
    df[numeric_cols] = df[numeric_cols].fillna(0).astype(int)
    df = df.fillna('')
    return df

def f_data_clean_2():
    debut = time.time()
    # max_workers = calculer_max_workers()
    # BATCH_SIZE = max(100, 1000 // max_workers)
    # Taille des lots pour la traduction
    BATCH_SIZE = 200

    json_file_path = os.getenv('SCRAPPER_JSON_OUTPUT_PATH', 'data/scrapping_output.json')
    if not os.path.exists(json_file_path):
        raise FileNotFoundError(f"Le fichier {json_file_path} n'existe pas.")
    
    with open(json_file_path, 'r') as json_file:
        scraping_data = json.load(json_file)
    
    date_scrap_filter = scraping_data.get('latest_date_scrap')
    if not date_scrap_filter:
        raise ValueError("La valeur 'date_scrap_filter' n'a pas ete trouvee dans le fichier JSON.")

    log_file = "/logs/clean_b_logs.txt"
    fichier_c = open(log_file, "a")
    print("------------- début clean data: features engineering -----------------", file=fichier_c)

    db_path = os.getenv('DUCKDB_PATH', 'data/supply_app.duckdb')
    con = duckdb.connect(db_path)

    if con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'data_scraped_traite_non_traduit'").fetchone()[0] > 0:
        df = con.execute("SELECT * FROM data_scraped_traite_non_traduit WHERE date_scrap = ?", [date_scrap_filter]).fetchdf()

        print("Aperçu des donnees non traduit :", df.head(), file=fichier_c)

        # Creation de la table de resultats si elle n'existe pas
        con.execute("""
            CREATE TABLE IF NOT EXISTS data_scraped_traite_traduit (
                categorie_bis VARCHAR,
                companies VARCHAR,
                noms VARCHAR,
                titre_com VARCHAR,
                commentaire VARCHAR,
                reponses VARCHAR,
                notes DOUBLE,
                date_experience DATE,
                date_commentaire DATE,
                site VARCHAR,
                nombre_pages INTEGER,
                date_scrap DATE,
                verified INTEGER,
                annee_experience INTEGER,
                mois_experience INTEGER,
                jour_experience INTEGER,
                annee_commentaire INTEGER,
                mois_commentaire INTEGER,
                jour_commentaire INTEGER,
                leadtime_com_exp INTEGER,
                nombre_caracteres INTEGER,
                nombre_maj INTEGER,
                nombre_car_spe INTEGER,
                caracteres_spe VARCHAR,
                emojis_positifs_count INTEGER,
                emojis_negatifs_count INTEGER,
                commentaire_text VARCHAR,
                langue_bis VARCHAR
            )
        """)

        df['nombre_caracteres'] = df['commentaire'].str.len()
        df['nombre_maj'] = df['commentaire'].str.count(r'[A-Z]')
        df['nombre_car_spe'] = df['commentaire'].str.count(r'[^a-zA-Z0-9\s]')
        df['caracteres_spe'] = df['commentaire'].apply(lambda x: ''.join(re.findall(r'[^a-zA-Z0-9\s]', x)))

        emojis_positifs = ['😀', '😁', '😂', '🤣', '😃', '😄', '😅', '😆', '😇', '😉', '😊', '😋', '😌', '😍', '😎', '😏', '😐', '😑','👍', '👏', '🙌', '🤝', '🙏', '✌️','✌', '🤞', '🤟', '🤘', '🤙', '👌', '👈', '👉', '👆', '👇', '☝️', '✋', '🤚', '🖐️', '🖖','👋', '🤗', '🤩','💖','💓', '💕', '💞', '💘' ,'💗' ,'💝','❤️','🧡' ,'💛' ,'💚', '💙' ,'💜' ,'🤎', '🖤','❤' ,'🤍', '💟', '💫', '💯']
        emojis_negatifs = ['😔', '😕', '😖', '😣', '😢', '😥', '😰', '😨', '😩', '😫', '😤', '😡', '😠', '😈', '👿', '💀', '☠️', '💩', '🤡','👎','👊', '🖕','💔']

        df['emojis_positifs_count'] = df['caracteres_spe'].apply(lambda x: sum(1 for char in x if char in emojis_positifs))
        df['emojis_negatifs_count'] = df['caracteres_spe'].apply(lambda x: sum(1 for char in x if char in emojis_negatifs))

        df['commentaire_text'] = df['commentaire'].str.lower().replace(r'[^a-z0-9\sàáâäçeeêëìíîïñòóôöùúûüýÿ\s,.;\']', ' ', regex=True).str.strip()

        # COMPLEXITE_CIBLE = BATCH_SIZE * 100
        # df_batches = creer_lots_intelligents(df, COMPLEXITE_CIBLE)
        # Traitement par lots pour la traduction
        df_batches = [df[i:i + BATCH_SIZE] for i in range(0, len(df), BATCH_SIZE)]

        translator = Translator()

        def traduire_lot(batch):
            batch['langue_bis'] = batch['commentaire_text'].apply(lambda text: detect(text) if text else 'Non detectee')
            batch = batch[batch['langue_bis'] != 'Non detectee']

            for index, row in batch.iterrows():
                try:
                    if row['langue_bis'] != 'en':
                        row['commentaire_en_bis'] = traduire_texte(translator, row['commentaire_text'], src=row['langue_bis'], dest='en')
                    else:
                        row['commentaire_en_bis'] = row['commentaire_text']
                except Exception as e:
                    log_message = f"Erreur de traduction pour l'index {index}: {e}"
                    print(log_message)
                    print(log_message, df.head(), file=fichier_c)

                    row['commentaire_en_bis'] = row['commentaire_text']
                time.sleep(3)
            
            batch = handle_nan_values(batch)
            return batch
    
        log_message = "Début de la traduction des commentaires."
        print(log_message)
        print(log_message, df.head(), file=fichier_c)

        with ThreadPoolExecutor(max_workers=24) as executor:
            future_to_batch = {executor.submit(traduire_lot, batch): batch for batch in df_batches}
            for future in as_completed(future_to_batch):
                try:
                    result_batch = future.result()
                    # Insertion des donnees nettoyees dans la table
                    con.execute("""
                        INSERT INTO data_scraped_traite_traduit (categorie_bis, companies, noms, titre_com, commentaire, reponses, notes, 
                            date_experience, date_commentaire, site, nombre_pages, date_scrap, verified, annee_experience,
                            mois_experience, jour_experience, annee_commentaire,
                            mois_commentaire, jour_commentaire, leadtime_com_exp,
                            nombre_caracteres, nombre_maj, nombre_car_spe, caracteres_spe,
                            emojis_positifs_count, emojis_negatifs_count, commentaire_text,
                            langue_bis)
                        SELECT 
                                categorie_bis, companies, noms, titre_com, commentaire, reponses, notes, 
                                date_experience, date_commentaire, site, nombre_pages, date_scrap, verified, annee_experience,
                                mois_experience, jour_experience, annee_commentaire,
                                mois_commentaire, jour_commentaire, leadtime_com_exp,
                                nombre_caracteres, nombre_maj, nombre_car_spe, caracteres_spe,
                                emojis_positifs_count, emojis_negatifs_count, commentaire_text,
                                langue_bis
                        FROM result_batch
                                """)
                except Exception as e:
                    log_message = f"{datetime.now()} - Erreur lors de la traduction : {e}\n"
                    print(log_message)
                    print(log_message, df.head(), file=fichier_c)

    sample_data = con.execute("SELECT * FROM data_scraped_traite_traduit WHERE date_scrap = ? ", [date_scrap_filter]).fetchdf().to_dict(orient='records')
    con.close()
                    
    fin = time.time()
    print(f"Fin de l'execution, duree: {round((fin - debut)/60, 2)} minutes.", file=fichier_c)
    print("Le nettoyage des données est terminé", file=fichier_c)
    fichier_c.close()
    return sample_data


if __name__ == "__main__":
    f_data_clean_2()