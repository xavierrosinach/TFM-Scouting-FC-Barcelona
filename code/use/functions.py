import os
import pandas as pd
import time
import json as jsonlib
import unicodedata
import re
import numpy as np
import requests
from datetime import datetime, timedelta

# Lector de JSON
def json_to_dict(json_path: str) -> dict:
    with open(json_path, "r", encoding="utf-8") as f:
        dict = jsonlib.load(f)
    return dict

# Creación de slug a partir de un string.
def create_slug(text: str) -> str:

    text = text.lower()                                                                                     # Letra minúscula
    text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')        # Eliminación de acentos
    text = re.sub(r"\s+", "_", text)                                                                        # Substitución de espacios por '_'
    text = re.sub(r"[^a-z0-9_]", "", text)                                                                  # Eliminación de carácteres no alfanuméricos
    text = re.sub(r"_+", "_", text).strip("_")
    
    return text

# División segura
def safe_div(num, den, ndigits=4):

    if pd.isna(num) or pd.isna(den) or den == 0:
        return np.nan
    return round(num / den, ndigits)

# A partir de un diccionario en formato JSON, lo guarda.
def safe_json_dump(data: dict, path: str) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            jsonlib.dump(data, f, ensure_ascii=False)
    except Exception:
        try:
            with open(path, "w", encoding="utf-8") as f:
                jsonlib.dump({}, f)
        except Exception:
            pass

# Procede a hacer scraping de un URL de Fotmob y obtiene los datos en formato de diccionario.
def url_to_json(url: str, sleep_time: int = 3) -> dict:
    out = requests.get(url).json()
    time.sleep(sleep_time)                           # Para garantir seguridad
    return out

# Comprueva la antiguidad del archivo, si supera unos días, devuelve True conforme se tiene que actualizar.
def need_to_upload(path: str, total_days: int = 5) -> bool:

    creation_time = os.path.getctime(path)          # Día de creación
    return datetime.now() - datetime.fromtimestamp(creation_time) > timedelta(days = total_days)

# Devuelve el time string del tiempo que se ha tardado
def elapsed_time_str(start_time: float) -> str:

    elapsed_time = time.time() - start_time

    hours = int(elapsed_time // 3600)
    minutes = int((elapsed_time % 3600) // 60)
    seconds = int(elapsed_time % 60)

    if hours > 0:
        time_str = f"{hours} hours {minutes} minutes {seconds} seconds"
    elif minutes > 0:
        time_str = f"{minutes} minutes {seconds} seconds"
    else:
        time_str = f"{elapsed_time:.2f} seconds"

    return time_str
