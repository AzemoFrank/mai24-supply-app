from pydantic import BaseModel, constr
from typing import List, Dict, Any, Optional, Literal

class UserIn(BaseModel):
    username: str
    first_name: str
    last_name: str
    password: str
    acces: Literal['user', 'admin', 'superadmin'] = 'user'  # Valeur par défaut pour les nouveaux utilisateurs

class UserUpdate(BaseModel):
    username: str
    new_access: str

class UserOut(BaseModel):
    username: str
    first_name: str
    last_name: str
    acces: str

class Token(BaseModel):
    access_token: str
    token_type: str
    type_acces: str

class Lien(BaseModel):
    lien: str

class CleanedDataItem(BaseModel):
    categorie_bis: Optional[str]
    companies: Optional[str]
    noms: Optional[str]
    titre_com: Optional[str]
    commentaire: Optional[str]
    reponses: Optional[str]
    notes: Optional[float]
    date_experience: Optional[str]
    date_commentaire: Optional[str]
    site: Optional[str]
    nombre_pages: Optional[int]
    date_scrap: Optional[str]
    verified: Optional[int]
    année_experience: Optional[int]
    mois_experience: Optional[int]
    jour_experience: Optional[int]
    année_commentaire: Optional[int]
    mois_commentaire: Optional[int]
    jour_commentaire: Optional[int]
    leadtime_com_exp: Optional[str]
    nombre_caractères: Optional[int]
    nombre_maj: Optional[int]
    nombre_car_spé: Optional[int]
    caractères_spé: Optional[str]
    emojis_positifs_count: Optional[int]
    emojis_negatifs_count: Optional[int]
    commentaire_text: Optional[str]
    langue_bis: Optional[str]

class CleanResponse(BaseModel):
    message: str
    sample: List[CleanedDataItem]

class Train(BaseModel):
    train: str
    
class PredictRequest(BaseModel):
    comment: str


class ScraperResponse(BaseModel):
    message: str
    lien: str
    sample: List[Dict[str, Any]]

