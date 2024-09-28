from fastapi import APIRouter, Depends, HTTPException
from typing import Dict
import json
import os
import json
from datetime import datetime
import pandas as pd
from src.users.models import UserIn, UserOut, Token, UserUpdate, ScraperResponse, CleanResponse, PredictRequest
from src.clean.scrapper_duckdb import main_scrapper
from typing import List
from fastapi.security import OAuth2PasswordRequestForm
from src.utils.train_duckdb import train_model, predict_comment
from src.utils.utils import (
    create_access_token, 
    get_current_user, 
    verify_password, 
    load_users, 
    save_user,
    clean_data,
    hash_password
)
from datetime import timedelta

# Router pour les routes utilisateur
user_routes = APIRouter(tags=['user'])
admin_routes = APIRouter(tags=['admin'])

@user_routes.get("/users", response_model=List[UserOut])
async def get_users(token: str = Depends(get_current_user)):
    current_user = get_current_user(token)
    if current_user.acces != 'superadmin':
        raise HTTPException(status_code=403, detail="Accès non autorisé")
    users = load_users()
    return [UserOut(**user.dict()) for user in users]

@admin_routes.post("/update_access")
async def update_access(user_update: UserUpdate, token: str = Depends(get_current_user)):
    current_user = get_current_user(token)
    if current_user.acces != 'superadmin':
        raise HTTPException(status_code=403, detail="Accès non autorisé")

    users = load_users()
    user_found = False

    for user in users:
        if user.username == user_update.username:
            user.acces = user_update.new_access
            user_found = True
            break

    if not user_found:
        raise HTTPException(status_code=404, detail="Utilisateur non trouvé")

    save_user(user)
    return {"message": "Accès mis à jour avec succès"}

@user_routes.post("/register", response_model=UserOut)
async def register(user: UserIn):
    """Enregistrement des utilisateurs"""
    users = load_users()
    if not users:
        user.acces = 'superadmin'
    else:
        user.acces = 'user'

    hashed_password = hash_password(user.password)
    user_data = user.dict(exclude={"password"})
    user_in_db = UserIn(**user_data, password=hashed_password)
    save_user(user_in_db)
    return UserOut(**user_data)

@user_routes.post("/logout")
async def logout(current_user: UserOut = Depends(get_current_user)):
    """Déconnexion de l'utilisateur"""
    # Ici, vous pouvez ajouter la logique de gestion de session si nécessaire
    return {"message": "Déconnexion réussie"}


@user_routes.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """Obtenir un token d'accès"""
    users = load_users()
    for user in users:
        if user.username == form_data.username and verify_password(form_data.password, user.password):
            token_data = {"sub": user.username}
            access_token = create_access_token(token_data, timedelta(minutes=30))
            return {"access_token": access_token, "token_type": "bearer", "type_acces": user.acces}
    raise HTTPException(status_code=400, detail="Invalid credentials")


@admin_routes.post("/scraper", response_model=ScraperResponse)
async def scraper(current_user: UserOut = Depends(get_current_user)):
    """Scraper les données (réservé aux admins)"""
    if current_user.acces in ["admin", "superadmin"]:
        try:
            sample_data = main_scrapper()
            
            if not sample_data:
                sample_data = []
            
            # Log des données brutes pour débogage
            print("Données de scraping : ", json.dumps(sample_data, indent=2))
            
            # Vérifiez le format des données
            expected_keys = [
                "categorie_bis", "companies", "noms", "titre_com", "commentaire", "reponses", "notes", 
                "date_experience", "date_commentaire", "site", "nombre_pages", "date_scrap"
            ]
            for item in sample_data:
                if not isinstance(item, dict):
                    raise ValueError("Les données de scraping ne sont pas des dictionnaires")
                missing_keys = [key for key in expected_keys if key not in item]
                if missing_keys:
                    raise ValueError(f"Les données de scraping sont manquantes pour les clés: {missing_keys}")

            return ScraperResponse(
                message="Scraping terminé pour https://www.trustpilot.com/categories",
                lien="https://www.trustpilot.com/categories",
                sample=sample_data
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Erreur lors du scraping : {str(e)}")
    raise HTTPException(status_code=403, detail="Accès interdit")


@admin_routes.post("/clean", response_model=CleanResponse)
async def clean(current_user: UserOut = Depends(get_current_user)):
    """Nettoyage des données (réservé aux superadmin)"""
    if current_user.acces == "superadmin":
        try:
            # Appel de la fonction pour nettoyer les données
            cleaned_data = await clean_data()
            # Affichage des données nettoyées dans la console (pour débogage)
            print("Données nettoyées retournées:", cleaned_data)
            # Vérification si les données nettoyées sont disponibles
            if not cleaned_data:
                raise HTTPException(status_code=404, detail="Aucune donnée nettoyée disponible")     
            # Retour des données nettoyées et du message de succès
            return {"message": "Nettoyage terminé", "sample": cleaned_data}
        except Exception as e:
            # Gestion des erreurs internes et levée d'exception
            raise HTTPException(status_code=500, detail=f"Erreur interne: {str(e)}")  
    # Si l'utilisateur n'est pas un superadmin, accès refusé
    raise HTTPException(status_code=403, detail="Accès interdit")


@admin_routes.post("/train", response_model=Dict[str, str], name="Entraîner les données")
async def train(current_user: UserOut = Depends(get_current_user)):
    """Entraînement des données avec des algorithmes et création de pipelines."""
    
    # Création du token d'accès
    token_data = {"sub": current_user.username}
    access_token = create_access_token(token_data, expires_delta=timedelta(minutes=30))

    # Chargement des utilisateurs et vérification des droits d'accès
    users = load_users()
    user = next((user for user in users if user.username == current_user.username), None)

    if not user or user.acces != "superadmin":
        raise HTTPException(status_code=403, detail="Accès interdit")

    try:
        # Appel de la fonction d'entraînement
        resultats = train_model()
        
        # Log message après succès de l'entraînement
        with open("src/features/log_app_api.txt", "a") as fichier:
            print("Entraînement des données terminé avec succès.", file=fichier)
            print("Utilisateur:", current_user.username, "Date:", datetime.now(), file=fichier)

        # Retour des résultats sous forme de JSON
        return {
            "access_token_private": access_token, 
            "token_type": "bearer",
            "confusion_matrix": resultats.get("confusion_matrix", []),
            "score": resultats.get("score", 0),
            "report": resultats.get("report", {})
        }
    except HTTPException as e:
        # Relancer l'exception HTTP si c'est un problème de fichier non trouvé ou accès interdit
        raise e
    except Exception as e:
        # Gestion des erreurs internes
        raise HTTPException(status_code=500, detail=f"Erreur interne: {str(e)}")

@user_routes.post("/predict", response_model=Dict[str, str], name="Prédiction")
async def predict(request: PredictRequest, current_user: UserOut = Depends(get_current_user)) -> Dict[str, str]:
    """Effectue une prédiction en fonction du type d'accès"""
    
    type_acces = current_user.acces
    
    if type_acces not in ["admin", "superadmin"]:
        raise HTTPException(status_code=403, detail="Type d'accès non valide.")
    
    try:
        # Effectuer la prédiction
        resultats = predict_comment(request.comment, type_acces)
        
        return {
            "token_type": "bearer",
            "message": resultats.get("message", "Erreur pendant la prédiction"),
            "score": resultats.get("score", 0),
            "prediction": resultats.get("prediction", "")
        }
    except Exception as e:
        # Gestion des erreurs internes
        raise HTTPException(status_code=500, detail=f"Erreur interne: {str(e)}")
