import os
import pandas as pd
import json

cdir = os.getcwd()                                                                  # Directorio actual de ejecución                                                                            
utils = os.path.join(os.path.abspath(os.path.join(cdir, '..')), 'utils')            # Carpeta con utils

comps_path = os.path.join(utils, 'comps.csv')                                       # Path del archivo CSV con competiciones
comps = pd.read_csv(comps_path, sep=';', encoding='latin1')                         # Lectura del CSV de competiciones

with open(os.path.join(utils, 'des_seasons.json'), 'r', encoding='utf-8') as f:     # Temporadas deseadas
    desired_seasons = json.load(f)
act_season = desired_seasons[0]                                                     # Temporada actual (última temporada)