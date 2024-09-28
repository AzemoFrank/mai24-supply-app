import pandas as pd
import numpy as np
import joblib
from typing import Dict, Tuple
import spacy
from datetime import datetime
from sklearn.preprocessing import LabelEncoder, StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.pipeline import Pipeline
from sklearn.naive_bayes import MultinomialNB
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix, classification_report
import certifi
import ssl

# Configurez le SSL pour utiliser les certificats certifi
ssl._create_default_https_context = ssl.create_default_context(cafile=certifi.where())

# Charger le modèle spaCy
nlp = spacy.load("en_core_web_sm")

def POStagging(commentt: str) -> str:
    if isinstance(commentt, str):  # Assurez-vous que c'est une chaîne de caractères
        doc = nlp(commentt)
        text = [f"{token.text}_{token.pos_}" for token in doc]
        return ' '.join(text)
    else:
        raise ValueError("L'entrée doit être une chaîne de caractères")


def prepare_data() -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, LabelEncoder]:
    """Prépare les données pour l'entraînement et la prédiction"""
    log_file_path = "src/features/resultats_train.txt"
    with open(log_file_path, "a") as fichier_t:
        print("---------------", datetime.now(), "--------------", file=fichier_t)
        
        def importer_df1() -> pd.DataFrame:
            return joblib.load('src/models/data1_lib')
        
        def importer_df_clean() -> pd.DataFrame:
            df_clean_2 = joblib.load("src/models/data_clean_lib")
            return df_clean_2.reset_index(drop=False)
        
        def importer_new_df() -> pd.DataFrame:
            return joblib.load('src/models/new_data_lib')
        
        pipe_text_nb = joblib.load("src/models/pipe_bayes_lib")
        df1 = importer_df1()
        
        # Importer et combiner le nouveau DataFrame avec df1, si nécessaire
        new_df = importer_new_df()
        df_combined = pd.concat([df1, new_df], ignore_index=True)
        
        # Transformation des variables
        numeric_features = [
            'nombre_caractères', 'nombre_maj', 'nombre_car_spé', 
            'emojis_positifs_count', 'emojis_negatifs_count', 
            'nombre_point_intero', 'nombre_point_exclam', 'sentiment_commentaire'
        ]
        numeric_transformer = StandardScaler()

        categorical_features = ['commentaire_clean_pos_tag']
        categorical_transformer = OneHotEncoder(handle_unknown='ignore', categories='auto')
        
        preprocessor = ColumnTransformer(
            transformers=[('num', numeric_transformer, numeric_features)]
        )
        df_clean = preprocessor.fit_transform(df_combined)

        # Suppression des colonnes inutiles
        colonnes_à_supprimer = [
            'categorie_bis', 'verified', 'nombre_caractères', 'nombre_maj', 
            'nombre_car_spé', 'emojis_positifs_count', 'emojis_negatifs_count',
            'nombre_point_intero', 'nombre_point_exclam', 'companies', 'noms', 
            'titre_com', 'commentaire', 'verif_reponses', 'reponses', 
            'date_experience', 'date_commentaire', 'site', 'nombre_pages', 
            'date_scrap', 'année_experience', 'langue', 'mois_experience', 
            'jour_experience', 'année_commentaire', 'mois_commentaire', 
            'jour_commentaire', 'leadtime_com_exp', 'caractères_spé',
            'commentaire_text', 'commentaire_en', 'verif_traduction', 
            'commentaire_en_bis', 'cat_nombre_caractères', 'cat_nombre_maj', 
            'notes', 'sentiment_commentaire', 'commentaire_clean'
        ]
        
        df_clean_2 = importer_df_clean()
        df2 = df_clean_2.drop(columns=colonnes_à_supprimer)
        df2 = df2.drop_duplicates()
        df2 = df2.dropna(subset=['commentaire_clean_pos_tag'])

        # Préparation des données pour le modèle
        encode_y = LabelEncoder()
        x = df2["commentaire_clean_pos_tag"].astype(str)  # Assurez-vous que c'est une chaîne de caractères
        y = encode_y.fit_transform(df2["notes_bis"])
        x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42)
        
        return x_train, x_test, y_train, y_test, encode_y



def train_model() -> Dict[str, str]:
    """Entraîne le modèle et retourne les résultats"""
    log_file_path = "src/features/resultats_train.txt"
    with open(log_file_path, "a") as fichier_t:
        print("---------------", datetime.now(), "--------------", file=fichier_t)
        
        x_train, x_test, y_train, y_test, encode_y = prepare_data()

        # Créer le pipeline avec CountVectorizer et le modèle Naive Bayes
        pipeline = Pipeline([
            ('vect', CountVectorizer()),
            ('clf', MultinomialNB())
        ])
        
        # Entraîner le pipeline
        pipeline.fit(x_train.tolist(), y_train)
        
        # Sauvegarder le pipeline
        joblib.dump(pipeline, "src/models/pipeline_bayes_lib")
        joblib.dump(encode_y, "src/models/encode_y_lib")

        print("------résultats---------", file=fichier_t)
        conf_matrix = confusion_matrix(y_test, pipeline.predict(x_test))
        print("Matrice de confusion:\n", conf_matrix, file=fichier_t)
        score = pipeline.score(x_test, y_test)
        print("Score du modèle:\n", score, file=fichier_t)
        
        report = classification_report(y_test, pipeline.predict(x_test), output_dict=True)
        df_report = pd.DataFrame(report).transpose()
        
        columns_mapping = {
            "precision": "Précision",
            "recall": "Rappel",
            "f1-score": "F1-score",
            "support": "Support"
        }
        df_report.rename(columns=columns_mapping, inplace=True)
        print(df_report, file=fichier_t)
        
        print("---------------------------fin--------------------------\n", file=fichier_t)
        
        return {
            "confusion_matrix": str(conf_matrix.tolist()),  # Convertir en chaîne pour JSON
            "score": str(score),  # Convertir en chaîne pour JSON
            "report": str(df_report.to_dict())  # Convertir en chaîne pour JSON
        }


def commentaire_pred_nb(comment: str) -> str:
    """
    Effectue une prédiction sur le commentaire fourni en utilisant le modèle de Naive Bayes.
    
    Args:
        comment (str): Le commentaire à prédire.
        
    Returns:
        str: La prédiction pour le commentaire.
    """
    # Charger le pipeline et le LabelEncoder
    pipeline = joblib.load("src/models/pipeline_bayes_lib")
    encode_y = joblib.load("src/models/encode_y_lib")
    
    # Préparer le commentaire
    comment_trans = pipeline.named_steps['vect'].transform([POStagging(comment)])
    
    # Prévoir directement
    prediction = pipeline.named_steps['clf'].predict(comment_trans)
    prediction_label = encode_y.inverse_transform(prediction)
    
    return prediction_label[0]

def predict_comment(comment: str, type_acces: str) -> Dict[str, str]:
    """Effectue une prédiction et retourne les résultats en fonction du type d'accès"""
    log_file_path = "src/features/resultats_train.txt"
    
    # Charger le pipeline et le LabelEncoder
    pipeline = joblib.load("src/models/pipeline_bayes_lib")
    encode_y = joblib.load("src/models/encode_y_lib")
    
    # Préparer le texte pour le modèle
    comment_trans = pipeline.named_steps['vect'].transform([POStagging(comment)])
    
    # Effectuer la prédiction
    prediction = pipeline.named_steps['clf'].predict(comment_trans)
    prediction_label = encode_y.inverse_transform(prediction)
    
    with open(log_file_path, "a") as fichier_t:
        print("---------------", datetime.now(), "--------------", file=fichier_t)
        print("------résultats pour: type accès: ", type_acces, "---------", file=fichier_t)
        print("prédiction modèle nb: ", comment, " == ", prediction_label[0], file=fichier_t)
        
        if type_acces in ["admin", "superadmin"]:
            # Log détaillé pour admin/superadmin
            x_train, x_test, y_train, y_test, encode_y = prepare_data()
            x_train_trans = pipeline.named_steps['vect'].transform(x_train.tolist())
            x_test_trans = pipeline.named_steps['vect'].transform(x_test.tolist())
            
            score = pipeline.named_steps['clf'].score(x_test_trans, y_test)
            print("Score du modèle:\n", score, file=fichier_t)
            return {
                "message": "Prédiction terminée avec succès",
                "score": str(score),  # Convertir en chaîne pour JSON
                "prediction": str(prediction_label[0])  # Convertir en chaîne pour JSON
            }
        
        elif type_acces == "user":
            # Log partiel pour les utilisateurs
            score = pipeline.named_steps['clf'].score(comment_trans, [0])  # Vous devriez fournir un vrai ensemble de données pour le score
            print("Score du modèle:\n", score, file=fichier_t)
            print("Il faut avoir un accès admin pour voir le rapport de classification et plus d'analyses :( ", file=fichier_t)
            return {
                "message": "Prédiction terminée, rapport partiel disponible",
                "score": str(score),  # Convertir en chaîne pour JSON
                "prediction": str(prediction_label[0])  # Convertir en chaîne pour JSON
            }
        
        else:
            prediction = "-------------type d'accès erroné!!! \n"
            print(prediction, file=fichier_t)
            return {"message": prediction}


if __name__ == "__main__":
    # Exemple d'appel
    train = train_model()
    predict = predict_comment("df", "superadmin")
    print(train)
