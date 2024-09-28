from time import sleep
import re
from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
from datetime import datetime

def scrape_first_phase(url: str) -> pd.DataFrame:
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
                marque.append(lien_m.find('p', class_='typography_heading-xs__jSwUz typography_appearance-default__AAY17 styles_displayName__GOhL2').text)
                liens_marque.append(lien_m.find('a', class_='link_internal__7XN06 link_wrapper__5ZJEx styles_linkWrapper__UWs5j').get('href'))
                reviews.append(lien_m.find('p', class_='typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l styles_ratingText__yQ5S7'))
                categorie.append(lien_cc)
                pays.append("FR")

    data = {
        'marque': marque,
        'liens_marque': liens_marque,
        'categorie': categorie,
        'reviews': reviews,
        'pays': pays
    }
    
    df_liens = pd.DataFrame(data)

    # Data cleaning
    df_liens['liens_marque'] = df_liens['liens_marque'].str.replace('/review/', '')

    def extraire_chiffres(texte):
        pattern = r'\|\</span>([0-9,]+)'
        match = re.search(pattern, str(texte))
        if match:
            return match.group(1)
        elif len(str(texte)) < 8:
            return texte
        else:
            return None

    df_liens['reviews'] = df_liens['reviews'].apply(extraire_chiffres)
    df_liens['reviews'] = df_liens['reviews'].str.replace(',', '').astype(float)
    df_liens = df_liens.sort_values(by=['categorie', 'reviews'], ascending=[True, False])

    # Enregistrer le dataframe traité en CSV avec horodatage
    timestamp_liens = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file_liens = f'src/data/Avis_trustpilot_liste_liens_{timestamp_liens}.csv'
    df_liens.to_csv(csv_file_liens, index=False)

    # Optionnel : Créer une copie ou un lien symbolique vers un fichier 'latest.csv'
    latest_file_liens = 'src/data/Avis_trustpilot_liste_liens_latest.csv'
    df_liens.to_csv(latest_file_liens, index=False)

    return df_liens

def scrape_second_phase(df_liens_filtré: pd.DataFrame) -> pd.DataFrame:
    """Deuxième phase de scraping pour les détails des avis clients."""
    date_actuelle = datetime.now()

    Data = {}
    tout, noms, date_commentaire, date_experience, notes, titre_com, companies, reponses = [], [], [], [], [], [], [], []
    commentaire, verified, test, site, nombre_pages, date_scrap, date_reponse, date_rep, categorie_bis = [], [], [], [], [], [], [], [], []

    for lien_cat in df_liens_filtré['categorie'].unique():
        df_marque = df_liens_filtré.loc[df_liens_filtré['categorie'] == lien_cat]

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
                    date_scrap.append(date_actuelle.strftime("%d-%m-%Y"))

    data = {
        'categorie_bis': categorie_bis,
        'companies': companies,
        'noms': noms,
        'titre_com': titre_com,
        'commentaire': commentaire,
        'reponses': reponses,
        'notes': notes,
        'date_experience': date_experience,
        'date_commentaire': date_commentaire,
        'site': site,
        'nombre_pages': nombre_pages,
        'date_scrap': date_scrap
    }

    df = pd.DataFrame(data)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = f'src/data/Final_data_scraped_brut_{timestamp}.csv'
    df.to_csv(csv_file, index=False)

    latest_file = 'src/data/Final_data_scraped_brut_latest.csv'
    df.to_csv(latest_file, index=False)
    df_loaded = pd.read_csv(latest_file)

    print("------------------------------------------------------------------------")
    print('#### Résultats: données brutes scrapées:', file=open("src/data/texte.txt", "w"))
    print("Voici le Dataframe des données brutes scrapées (données non traitées). \nD'après ce que nous voyons ci-dessus, les données scrapées nécessitent un traitement supplémentaire avec text mining. Nous allons aussi procéder à la création de nouvelles features engineering.")
    print(df_loaded['categorie_bis'].value_counts())
    print(f"La taille du df brut: {df_loaded.shape}")
    print(f"Webscraping terminé le: {date_actuelle}", file=open("src/data/texte.txt", "w"))
    print(f"Webscraping terminé le: {date_actuelle}")

    df_sample = df.head(80)  # Échantillon des 80 premières lignes
    sample_data = df_sample.to_dict(orient='records')  # Convertir en liste de dictionnaires

    return sample_data


# Exemple d'appel de la fonction
# url = "https://www.trustpilot.com/categories"
# fonction_scraper(url)

if __name__ == "__main__":
    # Étape 1 : Scraping des données de la première phase
    url = "https://www.trustpilot.com/categories"
    df_liens = scrape_first_phase(url)
    # Étape 2 : Scraping des avis clients avec les données obtenues de la première phase
    sample_data = scrape_second_phase(df_liens)