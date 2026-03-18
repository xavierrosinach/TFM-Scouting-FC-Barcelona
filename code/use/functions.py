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
# LECTURA DE JSON
# --------------------------------------------------------------------------------------

def json_to_dict(json_path: str) -> dict:
    """
    Lee un archivo JSON y devuelve su contenido.

    Parameters
    ----------
    json_path : str
        Ruta del archivo JSON.

    Returns
    -------
    dict
        Contenido del JSON.

    Raises
    ------
    FileNotFoundError
        Si el archivo no existe.
    json.JSONDecodeError
        Si el contenido no es un JSON válido.
    """
    with open(json_path, "r", encoding="utf-8") as f:
        data = jsonlib.load(f)

    if not isinstance(data, dict):
        raise ValueError(f"El JSON '{json_path}' no contiene un diccionario.")
    return data


# --------------------------------------------------------------------------------------
# CREACIÓN DE SLUGS
# --------------------------------------------------------------------------------------

def create_slug(text: str) -> str:
    """
    Convierte un texto en un slug normalizado.

    Parameters
    ----------
    text : str
        Texto de entrada.

    Returns
    -------
    str
        Texto normalizado en minúsculas, sin acentos y separado por '_'.
    """
    if text is None:
        return ""

    text = str(text).lower()
    text = "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^a-z0-9_]", "", text)
    text = re.sub(r"_+", "_", text).strip("_")

    return text


# --------------------------------------------------------------------------------------
# DIVISIÓN SEGURA
# --------------------------------------------------------------------------------------

def safe_div(num: Any, den: Any, ndigits: int = 4) -> float:
    """
    Realiza una división segura controlando NaN y divisiones por cero.

    Parameters
    ----------
    num : Any
        Numerador.
    den : Any
        Denominador.
    ndigits : int, default=4
        Número de decimales del resultado.

    Returns
    -------
    float
        Resultado redondeado o np.nan si no puede calcularse.
    """
    if pd.isna(num) or pd.isna(den) or den == 0:
        return np.nan

    return round(num / den, ndigits)


# --------------------------------------------------------------------------------------
# GUARDADO SEGURO DE JSON
# --------------------------------------------------------------------------------------

def safe_json_dump(data: dict, path: str) -> None:
    """
    Guarda un diccionario en formato JSON.

    Parameters
    ----------
    data : dict
        Diccionario a guardar.
    path : str
        Ruta de salida del archivo JSON.

    Raises
    ------
    TypeError
        Si 'data' no es un diccionario.
    OSError
        Si no se puede escribir el archivo.
    """
    if not isinstance(data, dict):
        raise TypeError("El parámetro 'data' debe ser un diccionario.")

    out_dir = os.path.dirname(path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        jsonlib.dump(data, f, ensure_ascii=False)


# --------------------------------------------------------------------------------------
# DESCARGA DE JSON DESDE URL
# --------------------------------------------------------------------------------------

def url_to_json(url: str, sleep_time: int = 3, timeout: int = 30) -> dict:
    """
    Realiza una petición HTTP GET y devuelve la respuesta en formato JSON.

    Parameters
    ----------
    url : str
        URL de la petición.
    sleep_time : int, default=3
        Tiempo de espera tras la petición.
    timeout : int, default=30
        Tiempo máximo de espera de la petición.

    Returns
    -------
    dict
        Respuesta en formato JSON.

    Raises
    ------
    requests.RequestException
        Si la petición HTTP falla.
    ValueError
        Si la respuesta no es un JSON válido o no es un diccionario.
    """
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()

    data = response.json()
    if not isinstance(data, dict):
        raise ValueError(f"La respuesta de '{url}' no contiene un diccionario JSON.")

    time.sleep(sleep_time)
    return data


# --------------------------------------------------------------------------------------
# CONTROL DE ANTIGÜEDAD DE ARCHIVOS
# --------------------------------------------------------------------------------------

def need_to_upload(path: str, total_days: int = 5) -> bool:
    """
    Indica si un archivo debe actualizarse según su antigüedad.

    Parameters
    ----------
    path : str
        Ruta del archivo.
    total_days : int, default=5
        Antigüedad máxima permitida en días.

    Returns
    -------
    bool
        True si el archivo no existe o supera la antigüedad indicada.
    """
    if not os.path.exists(path):
        return True

    creation_time = os.path.getctime(path)
    file_age = datetime.now() - datetime.fromtimestamp(creation_time)

    return file_age > timedelta(days=total_days)



# # Devuelve el time string del tiempo que se ha tardado
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
