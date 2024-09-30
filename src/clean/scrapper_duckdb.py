from time import sleep
import re, os, json
from bs4 import BeautifulSoup as bs
import requests
import duckdb
from datetime import datetime

def scrapper_first_phase(url: str, date_actuelle:str):
    """Première phase de scraping pour les colonnes marque, liens_marque, catégorie, reviews, pays."""
    categorie, pays, marque, liens_marque, reviews = [], [], [], [], []
    liste_liens1 = ['bank']
    
    for lien_cc in liste_liens1:
        lien = f"{url}/{lien_cc}?country=FR"
        page = requests.get(lien, verify=False)
        soup = bs(page.content, "lxml")
        
        for X in range(1, 3):
            sleep(0.5)  # Attendre une demi-seconde entre chaque page
            lien2 = f"{url}/{lien_cc}?country=FR&page={X}"
            page = requests.get(lien2, verify=False)
            soup2 = bs(page.content, "lxml")
            soup_marques = soup2.find_all('div', class_="paper_paper__1PY90 paper_outline__lwsUX card_card__lQWDv card_noPadding__D8PcU styles_wrapper__2JOo2")
            
            for lien_m in soup_marques:
                marque.append(lien_m.find('p', class_='typography_heading-xs__jSwUz typography_appearance-default__AAY17').text) #TODO: removed styles_displayName__GOhL2
                liens_marque.append(lien_m.find('a', class_='link_internal__7XN06 link_wrapper__5ZJEx styles_linkWrapper__UWs5j').get('href').replace('/review/', ''))
                reviews.append(lien_m.find('p', class_='typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l styles_ratingText__yQ5S7'))
                categorie.append(lien_cc)
                pays.append("FR")

    # Nettoyage des données
    def extraire_chiffres(texte):
        pattern = r'\|\</span>([0-9,]+)'
        match = re.search(pattern, str(texte))
        if match:
            return float(match.group(1).replace(',', ''))
        elif len(str(texte)) < 8:
            return float(texte.replace(',', ''))
        else:
            return None

    reviews = [extraire_chiffres(review) for review in reviews]

    # Connexion à DuckDB
    db_path = os.getenv('DUCKDB_PATH', 'data/supply_app.duckdb')
    con = duckdb.connect(db_path)

    # Création de la table si elle n'existe pas
    con.execute("""
        CREATE TABLE IF NOT EXISTS avis_trustpilot_liste_liens (
            marque VARCHAR,
            liens_marque VARCHAR,
            categorie VARCHAR,
            reviews FLOAT,
            pays VARCHAR,
            insert_dt DATE
        )
    """)

    # Insertion des données dans DuckDB
    con.execute("""
        INSERT INTO avis_trustpilot_liste_liens (marque, liens_marque, categorie, reviews, pays, insert_dt)
        SELECT * FROM (
            SELECT 
                UNNEST(?) as marque,
                UNNEST(?) as liens_marque,
                UNNEST(?) as categorie,
                UNNEST(?) as reviews,
                UNNEST(?) as pays,
                ? as insert_dt
        )
    """, [marque, liens_marque, categorie, reviews, pays, date_actuelle])

    # Récupération des données pour le retour
    result = con.execute("""
        SELECT * 
        FROM avis_trustpilot_liste_liens
        WHERE insert_dt = ?
        ORDER BY categorie, reviews DESC
    """, [date_actuelle]).fetchdf()

    # Fermeture de la connexion
    con.close()

    return result

def scrapper_second_phase(df_liens_filtre, date_actuelle):
    """Deuxième phase de scraping pour les détails des avis clients."""

    tout, noms, date_commentaire, date_experience, notes, titre_com, companies, reponses = [], [], [], [], [], [], [], []
    commentaire, verified, test, site, nombre_pages, date_reponse, date_rep, categorie_bis = [], [], [], [], [], [], [], []

    for lien_cat in df_liens_filtre['categorie'].unique():
        df_marque = df_liens_filtre.loc[df_liens_filtre['categorie'] == lien_cat]

        for lien_c in df_marque['liens_marque']:
            url_lien = f'https://www.trustpilot.com/review/{lien_c}?page=1'

            try:
                page = requests.get(url_lien, verify=False)
                soup = bs(page.content, "lxml")
            except Exception as e:
                print(f"Une exception s'est produite pour {lien_c}: {e}")
                continue

            pagination_div = soup.find('div', class_='styles_pagination__6VmQv')
            page_numbers = []

            try:
                for item in pagination_div.find_all(['span']):
                    page_numbers.append(item.get_text())
                nb_pages = int(page_numbers[-2])
            except:
                nb_pages = 1

            for X in range(1, nb_pages + 1):
                lien = f'https://www.trustpilot.com/review/{lien_c}?page={X}'
                page = requests.get(lien, verify=False)
                soup = bs(page.content, "lxml")
                avis_clients = soup.find_all('div', attrs={'class': "styles_cardWrapper__LcCPA styles_show__HUXRb styles_reviewCard__9HxJJ"})

                try:
                    company = soup.find('h1', class_='typography_default__hIMlQ typography_appearance-default__AAY17 title_title__i9V__').text.strip()
                except:
                    company = None

                for avis in avis_clients:
                    noms.append(avis.find('span', class_='typography_heading-xxs__QKBS8 typography_appearance-default__AAY17').text.strip() if avis.find('span', class_='typography_heading-xxs__QKBS8 typography_appearance-default__AAY17') else None)
                    titre_com.append(avis.find('h2', class_='typography_heading-s__f7029 typography_appearance-default__AAY17').text.strip() if avis.find('h2', class_='typography_heading-s__f7029 typography_appearance-default__AAY17') else None)
                    commentaire.append(avis.find('p').text.strip() if avis.find('p') else None)
                    reponses.append(avis.find('p', class_='typography_body-m__xgxZ_ typography_appearance-default__AAY17 styles_message__shHhX').text.strip() if avis.find('p', class_='typography_body-m__xgxZ_ typography_appearance-default__AAY17 styles_message__shHhX') else None)
                    notes.append(avis.find('div', class_='star-rating_starRating__4rrcf star-rating_medium__iN6Ty').text.strip() if avis.find('div', class_='star-rating_starRating__4rrcf star-rating_medium__iN6Ty') else None)
                    date_experience.append(avis.find('p', class_='typography_body-m__xgxZ_ typography_appearance-default__AAY17').text.strip() if avis.find('p', class_='typography_body-m__xgxZ_ typography_appearance-default__AAY17') else None)
                    date_commentaire.append(avis.find('div', class_='styles_reviewHeader__iU9Px').text.strip() if avis.find('div', class_='styles_reviewHeader__iU9Px') else None)
                    companies.append(company)
                    site.append(lien)
                    nombre_pages.append(nb_pages)
                    categorie_bis.append(lien_cat)

    # Connexion à DuckDB
    db_path = os.getenv('DUCKDB_PATH', 'data/supply_app.duckdb')
    con = duckdb.connect(db_path)

    # Création de la table si elle n'existe pas
    con.execute("""
        CREATE TABLE IF NOT EXISTS data_scrapped_brut (
            categorie_bis VARCHAR,
            companies VARCHAR,
            noms VARCHAR,
            titre_com VARCHAR,
            commentaire VARCHAR,
            reponses VARCHAR,
            notes VARCHAR,
            date_experience VARCHAR,
            date_commentaire VARCHAR,
            site VARCHAR,
            nombre_pages INTEGER,
            date_scrap DATE
        )
    """)

    # Insertion des données dans DuckDB
    con.execute("""
        INSERT INTO data_scrapped_brut (categorie_bis, companies, noms, titre_com, commentaire, reponses, notes, date_experience, date_commentaire, site, nombre_pages, date_scrap)
        SELECT * FROM (
            SELECT 
                UNNEST(?) as categorie_bis,
                UNNEST(?) as companies,
                UNNEST(?) as noms,
                UNNEST(?) as titre_com,
                UNNEST(?) as commentaire,
                UNNEST(?) as reponses,
                UNNEST(?) as notes,
                UNNEST(?) as date_experience,
                UNNEST(?) as date_commentaire,
                UNNEST(?) as site,
                UNNEST(?) as nombre_pages,
                ? as date_scrap
        )
    """, [categorie_bis, companies, noms, titre_com, commentaire, reponses, notes, date_experience, date_commentaire, site, nombre_pages, date_actuelle])

    # Récupération d'un échantillon pour l'affichage
    df_sample = con.execute("""
        SELECT 
            categorie_bis, companies, noms, titre_com, commentaire, 
            reponses, notes, date_experience, date_commentaire, 
            site, nombre_pages 
        FROM data_scrapped_brut 
        WHERE date_scrap = ? 
        LIMIT 80
    """, [date_actuelle]).fetchdf()

    
    print("------------------------------------------------------------------------")
    print('#### Résultats: données brutes scrapées:', file=open("/app/logs/scrapper_logs.txt", "a"))
    print("Voici le Dataframe des données brutes scrapées (données non traitées). \nD'après ce que nous voyons ci-dessus, les données scrapées nécessitent un traitement supplémentaire avec text mining. Nous allons aussi procéder à la création de nouvelles features engineering.")
    print(con.execute("SELECT categorie_bis, COUNT(*) FROM data_scrapped_brut WHERE date_scrap = ? GROUP BY categorie_bis", [date_actuelle]).fetchdf())
    print(f"La taille du df brut: {con.execute('SELECT COUNT(*) FROM data_scrapped_brut WHERE date_scrap = ? ', [date_actuelle]).fetchone()[0]}")
    print(f"Webscraping terminé le: {date_actuelle}", file=open("/app/logs/scrapper_logs.txt", "a"))
    print(f"Webscraping terminé le: {date_actuelle}")

    # Fermeture de la connexion
    con.close()

    return df_sample.to_dict(orient='records')


def main_scrapper():

    # Vérifier si le fichier supply_app.duckdb existe, sinon le créer
    db_file = os.getenv('DUCKDB_PATH', 'data/supply_app.duckdb')
    if not os.path.exists(db_file):
        print(f"Le fichier {db_file} n'existe pas. Création d'une nouvelle base de données.")
        con = duckdb.connect(db_file)
        con.close()
    
    if not os.path.exists("/app/logs/scrapper_logs.txt"):
        open("/app/logs/scrapper_logs.txt", 'a').close()

    date_actuelle = datetime.now().strftime("%Y-%m-%d")
    
    # Étape 1 : Scraping des données de la première phase
    url = "https://www.trustpilot.com/categories"
    df_liens = scrapper_first_phase(url, date_actuelle)

    # Étape 2 : Scraping des avis clients avec les données obtenues de la première phase
    sample_data = scrapper_second_phase(df_liens, date_actuelle)

    # Écriture de la date_actuelle dans un fichier JSON
    json_file_path = os.getenv('SCRAPPER_JSON_OUTPUT_PATH', 'data/scrapping_output.json')
    
    # Vérifier si le fichier existe déjà
    if os.path.exists(json_file_path):
        # Si le fichier existe, lire son contenu
        with open(json_file_path, 'r') as json_file:
            existing_data = json.load(json_file)
    else:
        # Si le fichier n'existe pas, créer un dictionnaire vide
        existing_data = {}

    # Mettre à jour ou ajouter la clé 'latest_date_scrap'
    existing_data['latest_date_scrap'] = date_actuelle

    # Écrire les données mises à jour dans le fichier JSON
    with open(json_file_path, 'w') as json_file:
        json.dump(existing_data, json_file, indent=4)
    
    return sample_data

if __name__ == "__main__":

    main_scrapper()

