import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import re, json
from datetime import datetime
import os
import duckdb

def f_data_clean():
    """Fonction pour nettoyer les donnees à partir d'un fichier brut."""
    
     # Lecture du fichier JSON pour obtenir date_scrap_filter
    json_file_path = os.getenv('SCRAPPER_JSON_OUTPUT_PATH', 'data/scrapping_output.json')
    if not os.path.exists(json_file_path):
        raise FileNotFoundError(f"Le fichier {json_file_path} n'existe pas.")
    
    with open(json_file_path, 'r') as json_file:
        scraping_data = json.load(json_file)
    
    date_scrap_filter = scraping_data.get('latest_date_scrap')
    if not date_scrap_filter:
        raise ValueError("La valeur 'date_scrap_filter' n'a pas ete trouvee dans le fichier JSON.")

    log_file = "/app/logs/clean_a_logs.txt"
    fichier_b = open(log_file, "a")
    print("------------- debut clean data----------------- :", file=fichier_b)

    # Fonction pour extraire la note en utilisant une expression regulière
    def extraire_note(description):
        match = re.search(r'Rated\s+(\d+)', str(description))
        return match.group(1) if match else description

    # Fonction pour remplacer certains mots par une chaîne vide
    def remplacer_mots_par_X(phrase, mots_a_remplacer):
        for mot in mots_a_remplacer:
            phrase = phrase.replace(mot, '')
        return phrase

    # Fonction pour convertir la chaîne en date au format "dd/mm/aaaa"
    def convertir_date(chaine):
        try:
            date_obj = datetime.strptime(chaine, "%b %d, %Y")
            return date_obj.strftime("%d/%m/%Y")
        except ValueError:
            return chaine

    def convertir_date2(chaine):
        try:
            date_obj = datetime.strptime(chaine, "%B %d, %Y")
            return date_obj.strftime("%d/%m/%Y")
        except ValueError:
            return chaine

    # Remplacer 'aaaaa pas de commentaire!' par le titre de commentaire
    def replace_value(cell_value, other_value):
        return other_value if cell_value == 'aaaaa pas de commentaire!' else cell_value

    # Si le commentaire est vide (ou au format date) mettre : aaaaa pas de commentaire!.
    def replace_texte(cell_value):
        try:
            pd.to_datetime(cell_value, format='%B %d, %Y')
            return 'aaaaa pas de commentaire!'
        except ValueError:
            return cell_value

    # Connexion à DuckDB
    db_path = os.getenv('DUCKDB_PATH', 'data/supply_app.duckdb')
    con = duckdb.connect(db_path)

    # Verification de l'existence de la table data_scrapped_brut
    if con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'data_scrapped_brut'").fetchone()[0] > 0:

        df = con.execute("SELECT * FROM data_scrapped_brut WHERE date_scrap = ? ", [date_scrap_filter]).fetchdf()
        
        print("Aperçu des donnees brutes :", df.head(), file=fichier_b)

        # Application des fonctions de nettoyage
        df['notes'] = df['notes'].apply(extraire_note)
        df['verified'] = df['date_commentaire'].str.contains('Verified').astype(int)

        # Liste des mots à remplacer
        mots_a_remplacer = ["Reviews", "Date of experience:", '<br/><br/>', '<br/></p>', '</p>', '<br/>']
        mots_a_remplacer_date = ["Verified", "Updated ", "Invited", " ago", "Merged", "Redirected"]

        # Nettoyage des colonnes
        for nom_colonne in df.columns:
            df[nom_colonne] = df[nom_colonne].astype(str)
            df[nom_colonne] = df[nom_colonne].apply(lambda x: remplacer_mots_par_X(x, mots_a_remplacer))

        df['date_commentaire'] = df['date_commentaire'].apply(lambda x: remplacer_mots_par_X(x, mots_a_remplacer_date))

        # Conversion des dates et nettoyage des colonnes
        df['date_commentaire'] = df['date_commentaire'].apply(lambda x: convertir_date(x))
        df['date_experience'] = df['date_experience'].apply(lambda x: convertir_date2(x))

        # Suppression des lignes contenant des chaînes liees au temps dans 'date_commentaire'
        mask = df['date_commentaire'].str.contains(r'\b(days|day|minutes|minute|hours|hour)\b', case=False)
        df = df[~mask]

        # Conversion des types de colonnes
        df["notes"] = pd.to_numeric(df["notes"], errors='coerce')
        df["verified"] = df["verified"].astype(int)
        df["nombre_pages"] = pd.to_numeric(df["nombre_pages"], errors='coerce')
        df["date_experience"] = pd.to_datetime(df["date_experience"], format='%d/%m/%Y', errors='coerce')
        df["date_commentaire"] = pd.to_datetime(df["date_commentaire"], format='%d/%m/%Y', errors='coerce')

        # Creation de nouvelles colonnes basees sur les dates
        df["annee_experience"] = df['date_experience'].dt.year
        df["mois_experience"] = df['date_experience'].dt.month
        df["jour_experience"] = df['date_experience'].dt.day
        df["annee_commentaire"] = df['date_commentaire'].dt.year
        df["mois_commentaire"] = df['date_commentaire'].dt.month
        df["jour_commentaire"] = df['date_commentaire'].dt.day

        # Calcul de l'ecart entre la date d'experience et la date du commentaire
        df['leadtime_com_exp'] = df['date_commentaire'] - df['date_experience']

        # Remplacement des textes vides et des valeurs specifiques
        df['commentaire'] = df['commentaire'].apply(replace_texte)
        df['commentaire'] = df.apply(lambda row: replace_value(row['commentaire'], row['titre_com']), axis=1)

        # Creation de la table de resultats si elle n'existe pas
        con.execute("""
            CREATE TABLE IF NOT EXISTS data_scraped_traite_non_traduit (
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
                leadtime_com_exp INTEGER
            )
        """)

        # Insertion des donnees nettoyees dans la table
        con.execute("INSERT INTO data_scraped_traite_non_traduit SELECT * FROM df")


        print("Le nettoyage des donnees est termine: (b_data_clean):", file=fichier_b)
    else:
        print("La table data_scrapped_brut n'existe pas dans la base de donnees !!!", file=fichier_b)
    
    fichier_b.close()
    con.close()

    return

if __name__ == "__main__":
    f_data_clean()