import os
import json as jsonlib
import re
import time
import unicodedata
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
import requests

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

    text = str(text).lower()
    text = "".join(c for c in unicodedata.normalize("NFD", text)
                   if unicodedata.category(c) != "Mn")
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
