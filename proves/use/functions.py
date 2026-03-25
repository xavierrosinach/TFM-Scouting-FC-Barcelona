import os
import json as jsonlib
import re
import time
import unicodedata
from datetime import datetime, timedelta
from typing import Any
import random
import string

import numpy as np
import pandas as pd
import requests

# Caràcteres diferentes
REPLACEMENTS = {"À": "A", "Á": "A", "Â": "A", "Ã": "A", "Ä": "A", "Å": "A", "Æ": "AE", "Ç": "C", "È": "E", "É": "E", "Ê": "E", "Ë": "E", "Ė": "E", "Ę": "E", "Ě": "E", "Ì": "I", 
                "Í": "I", "Î": "I", "Ï": "I", "Ñ": "N", "Ń": "N", "Ň": "N", "Ò": "O", "Ó": "O", "Ô": "O", "Õ": "O", "Ö": "O", "Ø": "O", "Œ": "OE", "Š": "S", "Ş": "S", "Ș": "S", 
                "Ț": "T", "Ţ": "T", "Ù": "U", "Ú": "U", "Û": "U", "Ü": "U", "Ý": "Y", "Ÿ": "Y", "Ž": "Z", "Ć": "C", "Č": "C", "Ğ": "G", "Ħ": "H", "Ł": "L", "Đ": "D", "Þ": "TH",
                "à": "a", "á": "a", "â": "a", "ã": "a", "ä": "a", "å": "a", "æ": "ae", "ç": "c", "è": "e", "é": "e", "ê": "e", "ë": "e", "ė": "e", "ę": "e", "ě": "e", "ì": "i", 
                "í": "i", "î": "i", "ï": "i", "ñ": "n", "ń": "n", "ň": "n", "ò": "o", "ó": "o", "ô": "o", "õ": "o", "ö": "o", "ø": "o", "œ": "oe", "š": "s", "ş": "s", "ș": "s",
                "ț": "t", "ţ": "t", "ù": "u", "ú": "u", "û": "u", "ü": "u", "ý": "y", "ÿ": "y", "ž": "z", "ć": "c", "č": "c", "ğ": "g", "ħ": "h", "ł": "l", "đ": "d", "þ": "th",
                "ß": "ss", "Ə": "E", "ə": "a"}

# --------------------------------------------------------------------------------------
# GENERACIÓN DE IDENTIFICADORES - Genera identificadores únicos (n)
# --------------------------------------------------------------------------------------
def generate_unique_ids(n: int, length: int = 5) -> list:
    chars = string.ascii_uppercase + string.digits
    ids = set()

    while len(ids) < n:
        ids.add(''.join(random.choices(chars, k=length)))

    return list(ids)

# --------------------------------------------------------------------------------------
# LECTURA DE JSON - Lee un archivo JSON y devuelve su contenido.
# --------------------------------------------------------------------------------------
def json_to_dict(json_path: str) -> dict:
    
    with open(json_path, "r", encoding="utf-8") as f:
        data = jsonlib.load(f)

    if not isinstance(data, dict):
        raise ValueError(f"El JSON '{json_path}' no contiene un diccionario.")
    return data

# --------------------------------------------------------------------------------------
# CREACIÓN DE SLUGS - Convierte un texto en un slug normalizado.
# --------------------------------------------------------------------------------------
def create_slug(text: str) -> str:
    if text is None:
        return ""

    text = str(text)

    for old, new in REPLACEMENTS.items():
        text = text.replace(old, new)

    # Normalización adicional por si queda algún acento combinable
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")

    text = text.lower()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^a-z0-9_]", "", text)
    text = re.sub(r"_+", "_", text).strip("_")

    return text

# --------------------------------------------------------------------------------------
# DIVISIÓN SEGURA - Realiza una división segura controlando NaN y divisiones por cero.
# --------------------------------------------------------------------------------------
def safe_div(num: Any, den: Any, ndigits: int = 4) -> float:
    
    if pd.isna(num) or pd.isna(den) or den == 0:
        return np.nan

    return round(num / den, ndigits)

# --------------------------------------------------------------------------------------
# GUARDADO SEGURO DE JSON - Guarda un diccionario en formato JSON.
# --------------------------------------------------------------------------------------
def safe_json_dump(data: dict, path: str) -> None:
    
    if not isinstance(data, dict):
        raise TypeError("El parámetro 'data' debe ser un diccionario.")

    out_dir = os.path.dirname(path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        jsonlib.dump(data, f, ensure_ascii=False)

# --------------------------------------------------------------------------------------
# DESCARGA DE JSON DESDE URL - Realiza una petición HTTP GET y devuelve la respuesta en formato JSON.
# --------------------------------------------------------------------------------------
def url_to_json(url: str, sleep_time: int = 3, timeout: int = 30) -> dict:
    
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()

    data = response.json()
    if not isinstance(data, dict):
        raise ValueError(f"La respuesta de '{url}' no contiene un diccionario JSON.")

    time.sleep(sleep_time)
    return data

# --------------------------------------------------------------------------------------
# CONTROL DE ANTIGÜEDAD DE ARCHIVOS - Indica si un archivo debe actualizarse según su antigüedad.
# --------------------------------------------------------------------------------------
def need_to_upload(path: str, total_days: int = 5) -> bool:
    
    if not os.path.exists(path):
        return True

    creation_time = os.path.getctime(path)
    file_age = datetime.now() - datetime.fromtimestamp(creation_time)

    return file_age > timedelta(days=total_days)

# --------------------------------------------------------------------------------------
# TIEMPO DE EJECUCIÓN - Devuelve el time string del tiempo que ha tardado la ejecución.
# --------------------------------------------------------------------------------------
def elapsed_time_str(start_time: float) -> str:

    elapsed_time = time.time() - start_time

    hours = int(elapsed_time // 3600)
    minutes = int((elapsed_time % 3600) // 60)
    seconds = int(elapsed_time % 60)

    if hours > 0:
        time_str = f"{hours} hour(s) {minutes} minute(s) {seconds} second(s)"
    elif minutes > 0:
        time_str = f"{minutes} minute(s) {seconds} second(s)"
    else:
        time_str = f"{elapsed_time:.2f} second(s)"

    return time_str
