from pathlib import Path
import json
import pandas as pd

# --------------------------------------------------------------------------------------
# RUTA DE LOS DATOS
# --------------------------------------------------------------------------------------

DATA_PATH = r"C:\Users\ASUS\Desktop\data"

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

COMPS = pd.read_csv(COMPS_PATH, sep=";", encoding="latin1")

with open(DES_SEASONS_PATH, "r", encoding="utf-8") as f:
    DES_SEASONS = json.load(f)

if not isinstance(DES_SEASONS, list) or len(DES_SEASONS) == 0:
    raise ValueError("El archivo 'des_seasons.json' esta vacío (se necesita almenos una temporada).")

ACT_SEASON = DES_SEASONS[0]