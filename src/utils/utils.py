import json
import os
from dotenv import load_dotenv
from hashlib import sha256
import pandas as pd
from typing import List, Dict
from datetime import datetime, timedelta
import jwt
from fastapi import HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from src.users.models import UserIn, UserOut, CleanedDataItem
from src.clean.clean_a_duckdb import f_data_clean
from src.clean.clean_b_duckdb import f_data_clean_2
# from utils.train import f_data_train


# Charger les variables d'environnement du fichier .env
load_dotenv()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")

# Obtenir la clé secrète à partir de l'environnement
SECRET_KEY = os.getenv("SECRET_KEY", "default_secret_key")
ALGORITHM = "HS256"
USERS_JSON_FILE_PATH = os.getenv("USERS_JSON_FILE_PATH", "data/users.json")

def hash_password(password: str) -> str:
    return sha256(password.encode()).hexdigest()

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return hash_password(plain_password) == hashed_password

def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def load_users() -> List[UserIn]:
    if not os.path.exists(USERS_JSON_FILE_PATH):
        with open(USERS_JSON_FILE_PATH, "w") as file:
            json.dump([], file)
    
    with open(USERS_JSON_FILE_PATH, "r") as file:
        users_data = json.load(file)
    
    return [UserIn(**user) for user in users_data]

def get_current_user(token: str = Depends(oauth2_scheme)) -> UserOut:
    try:
        # Décodez le token et obtenez le nom d'utilisateur
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        
        if username is None:
            raise HTTPException(status_code=401, detail="Token invalide")
        
        # Chargez tous les utilisateurs
        users = load_users()
        
        # Trouvez l'utilisateur correspondant
        user = next((user for user in users if user.username == username), None)
        
        if user is None:
            raise HTTPException(status_code=401, detail="Utilisateur non trouvé")
        
        return UserOut(**user.dict())  # Assurez-vous que UserOut est correctement défini

    except jwt.JWTError:
        raise HTTPException(status_code=401, detail="Token invalide")

def save_user(user):
    users = load_users()
    users.append(user)
    with open(USERS_JSON_FILE_PATH, "w") as file:
        json.dump([user.dict() for user in users], file)

async def clean_data() -> List[CleanedDataItem]:
    """Fonction pour nettoyer les données."""
    f_data_clean()  # Appelle la première fonction de nettoyage
    cleaned_data = f_data_clean_2()  # Appelle la deuxième fonction et stocke le résultat

    if cleaned_data is None:
        return []  # Retourner une liste vide si aucune donnée n'est retournée

    # Convertir les données nettoyées en liste d'objets CleanedDataItem
    cleaned_items = [CleanedDataItem(**item) for item in cleaned_data]

    print("Données nettoyées:", cleaned_items)  # Ajout de log pour vérifier les données
    return cleaned_items
