from pathlib import Path
import json
import pandas as pd

# --------------------------------------------------------------------------------------
# RUTAS BASE DEL PROYECTO
# --------------------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[2]
UTILS_DIR = BASE_DIR / "utils"

# --------------------------------------------------------------------------------------
# ARCHIVOS DE CONFIGURACIÓN
# --------------------------------------------------------------------------------------

COMPS_PATH = UTILS_DIR / "comps.csv"
DES_SEASONS_PATH = UTILS_DIR / "des_seasons.json"

# --------------------------------------------------------------------------------------
# CARGA DE DATOS DE CONFIGURACIÓN
# --------------------------------------------------------------------------------------

comps = pd.read_csv(COMPS_PATH, sep=";", encoding="latin1")

with open(DES_SEASONS_PATH, "r", encoding="utf-8") as f:
    desired_seasons = json.load(f)

if not isinstance(desired_seasons, list) or len(desired_seasons) == 0:
    raise ValueError("El archivo 'des_seasons.json' debe contener una lista con al menos una temporada.")

act_season = desired_seasons[0]

# --------------------------------------------------------------------------------------
# ALIAS PARA MANTENER COMPATIBILIDAD CON EL RESTO DEL PROYECTO
# --------------------------------------------------------------------------------------

utils = str(UTILS_DIR)
comps_path = str(COMPS_PATH)