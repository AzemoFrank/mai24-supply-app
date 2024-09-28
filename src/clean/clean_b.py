import pandas as pd
import re
import os
import joblib
from datetime import datetime
from googletrans import Translator
from langdetect import detect
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def f_data_clean_2(sample_size=200):
    # Taille des lots pour la traduction
    BATCH_SIZE = 100

    # Fichier log
    fichier_c = open("src/features/log_clean_c.txt", "a")
    print("------------- début clean data: features engineering -----------------", file=fichier_c)

    # Vérification de la présence du fichier brut
    fichier_brut = 'src/data/Final_data_scraped_traité_non_traduit_latest.csv'

    if not os.path.isfile(fichier_brut):
        print(f"Le fichier {fichier_brut} n'existe pas. Veuillez vérifier le chemin du fichier.", file=fichier_c)
        return []  # Retourner une liste vide si le fichier n'existe pas

    # Chargement du fichier brut non traité
    df_brut = pd.read_csv(fichier_brut)

    # Sélectionner un échantillon des données à traiter
    df = df_brut.sample(n=sample_size, random_state=42).copy()

    # Ajout de nouvelles colonnes avec des caractéristiques de texte
    df['nombre_caractères'] = df['commentaire'].apply(len)
    df['nombre_maj'] = df['commentaire'].apply(lambda x: sum(1 for c in x if c.isupper()))
    df['nombre_car_spé'] = df['commentaire'].apply(lambda x: len([c for c in x if not c.isalnum() and not c.isspace()]))
    df['caractères_spé'] = df['commentaire'].apply(lambda x: [c for c in x if not c.isalnum() and not c.isspace() and c not in ['.', ',', '?', '!']])

    # Liste d'emojis positifs et négatifs
    emojis_positifs = ['😀', '😁', '😂', '🤣', '😃', '😄', '😅', '😆', '😇', '😉', '😊', '😋', '😌', '😍', '😎', '😏', '😐', '😑','👍', '👏', '🙌', '🤝', '🙏', '✌️','✌', '🤞', '🤟', '🤘', '🤙', '👌', '👈', '👉', '👆', '👇', '☝️', '✋', '🤚', '🖐️', '🖖','👋', '🤗', '🤩','💖','💓', '💕', '💞', '💘' ,'💗' ,'💝','❤️','🧡' ,'💛' ,'💚', '💙' ,'💜' ,'🤎', '🖤','❤' ,'🤍', '💟', '💫', '💯']
    emojis_negatifs = ['😔', '😕', '😖', '😣', '😢', '😥', '😰', '😨', '😩', '😫', '😤', '😡', '😠', '😈', '👿', '💀', '☠️', '💩', '🤡','👎','👊', '🖕','💔']

    # Fonction pour compter les emojis
    def compter_emojis(texte, emojis_list):
        return sum(1 for char in texte if char in emojis_list)

    df['emojis_positifs_count'] = df['caractères_spé'].apply(lambda x: compter_emojis(''.join(x), emojis_positifs))
    df['emojis_negatifs_count'] = df['caractères_spé'].apply(lambda x: compter_emojis(''.join(x), emojis_negatifs))

    # Nettoyage des caractères spéciaux
    def remove_special_characters_and_emojis(text):
        text = re.sub(r'[^a-zA-Z0-9\sàáâäçèéêëìíîïñòóôöùúûüýÿ\s,.;\']', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    df['commentaire_text'] = df['commentaire'].apply(remove_special_characters_and_emojis)

    # Conversion en minuscules
    df['commentaire_text'] = df['commentaire_text'].str.lower()

    # Détection de la langue
    def detect_language_safe(text):
        try:
            return detect(text)
        except:
            return 'Non détectée'

    df['langue_bis'] = df['commentaire_text'].apply(lambda text: detect_language_safe(text) if text else 'Non détectée')

    # Suppression des lignes avec langue "Non détectée"
    df = df[df['langue_bis'] != 'Non détectée']

    # Traduction en anglais
    translator = Translator()

    def traduire_lot(batch):
        for index, row in batch.iterrows():
            try:
                if row['langue_bis'] != 'en':
                    row['commentaire_en_bis'] = translator.translate(row['commentaire_text'], src=row['langue_bis'], dest='en').text
                else:
                    row['commentaire_en_bis'] = row['commentaire_text']
            except Exception as e:
                print(f"Erreur de traduction pour l'index {index}: {e}", file=fichier_c)
                row['commentaire_en_bis'] = row['commentaire_text']
            time.sleep(1)  # Pause pour éviter d'atteindre la limite de l'API
        return batch

    # Traitement par lots pour la traduction
    df_batches = [df[i:i + BATCH_SIZE] for i in range(0, len(df), BATCH_SIZE)]

    translated_batches = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(traduire_lot, batch): batch for batch in df_batches}
        for future in as_completed(futures):
            translated_batches.append(future.result())

    # Combinaison de tous les lots traduits
    df_translated = pd.concat(translated_batches)

    # Gérer les valeurs NaN avant la conversion
    def handle_nan_values(df):
        # Remplacer NaN ou chaînes vides par des valeurs par défaut appropriées
        df['notes'] = pd.to_numeric(df['notes'], errors='coerce').fillna(0)  # Convertir en float, remplacer NaN par 0
        df['année_experience'] = pd.to_numeric(df['année_experience'], errors='coerce').fillna(0).astype(int)  # Convertir en int, remplacer NaN par 0
        df['mois_experience'] = pd.to_numeric(df['mois_experience'], errors='coerce').fillna(0).astype(int)
        df['jour_experience'] = pd.to_numeric(df['jour_experience'], errors='coerce').fillna(0).astype(int)
        
        # Convertir la liste de 'caractères_spé' en chaîne de caractères
        df['caractères_spé'] = df['caractères_spé'].apply(lambda x: ''.join(x) if isinstance(x, list) else x)
        
        # Remplacer NaN par une chaîne vide pour les colonnes de type string
        df = df.fillna('')

        # Remplacer NaN par 0 pour les colonnes de type nombre
        numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
        df[numeric_cols] = df[numeric_cols].fillna(0)

        return df

    df_translated = handle_nan_values(df_translated)

    # Sauvegarde du fichier final avec timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = f'src/data/Final_data_scraped_traité_traduit_ok_{timestamp}.csv'
    df_translated.to_csv(csv_file, index=False)

    # Création d'une copie vers 'latest.csv'
    latest_file = 'src/data/Final_data_scraped_traité_traduit_ok_latest.csv'
    df_translated.to_csv(latest_file, index=False)

    # Sauvegarde en tant que fichier `new_data_lib` pour le chargement ultérieur
    new_data_lib = 'src/models/new_data_lib'
    joblib.dump(df_translated, new_data_lib)

    print("Le nettoyage des données est terminé", file=fichier_c)

    # Conversion des données nettoyées en dictionnaire
    sample_data = df_translated.to_dict(orient='records')

    return sample_data

if __name__ == "__main__":
    f_data_clean_2(sample_size=200)
