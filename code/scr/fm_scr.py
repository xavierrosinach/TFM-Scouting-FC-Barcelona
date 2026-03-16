import os
import json as jsonlib
import time
from datetime import datetime, timedelta

from use.config import comps, desired_seasons, act_season
from use.functions import safe_json_dump, url_to_json, need_to_upload, create_slug

# Obtenemos un diccionario JSON con las temporadas disponibles en una liga.
def league_available_seasons(league_code: int, out_path: str) -> dict:

    json_path = os.path.join(out_path, 'available_seasons.json')                                # Nombre del documento
    if os.path.exists(json_path) and not need_to_upload(json_path, total_days=200):             # Comprovamos que no existe y que no se tenga que actualizar
        with open(json_path, "r", encoding="utf-8") as f:
            available_seasons_json = jsonlib.load(f)
        return available_seasons_json

    fotmob_url = f'https://www.fotmob.com/api/leagues?id={league_code}'                         # Scraping si no existe
    available_seasons_json = url_to_json(url=fotmob_url)
    available_seasons = available_seasons_json.get('allAvailableSeasons', [])

    seasons_dict = {}                                   # Añadimos información
    for s in available_seasons:
        if "/" in s:
            start, end = s.split("/")                   # Separación por año
            key_candidate = start[-2:] + end[-2:]
        else:
            s = s[:4]                                   # Sino, primer año
            key_candidate = f"{int(s) % 100:02d}{(int(s)+1) % 100:02d}"
        season_link = f"{fotmob_url}&season={s}"
        seasons_dict[s] = {"key": key_candidate, "link": season_link}

    
    available_seasons_json["allAvailableSeasons"] = seasons_dict                # Substitución y guardado del JSON
    if available_seasons_json.get('allAvailableSeasons'):
        safe_json_dump(data=available_seasons_json, path=json_path)

    return available_seasons_json

# Datos de una temporada
def season_data(seasons_dict: dict, season_key: str, out_path: str) -> dict:

    out_seasons_path = os.path.join(out_path, season_key)               # Carpeta con el output
    os.makedirs(out_seasons_path, exist_ok=True)
    json_path = os.path.join(out_seasons_path, 'info.json')    # Fichero output

    if season_key != act_season:                                            # Temporada que no sea la actual
        if os.path.exists(json_path):                                       # En caso que exista, devolvemos el JSON
            with open(json_path, "r", encoding="utf-8") as f:
                season_json = jsonlib.load(f)
            return season_json
    elif season_key == act_season:                                          # Temporada actual
        if os.path.exists(json_path) and not need_to_upload(json_path):     # En caso que exista, devolvemos el JSON
            with open(json_path, "r", encoding="utf-8") as f:
                season_json = jsonlib.load(f)
            return season_json
    
    if season_key not in seasons_dict.keys():       # Comprovamos que existe en la temporada
        return {}
   
    season_link = seasons_dict[season_key]          # Link y obtenemos información
    season_json = url_to_json(season_link)
    if season_json.get('fixtures'):
        safe_json_dump(data=season_json, path=json_path)
    
    return season_json

# Función principal para la extracción de datos de Fotmob de una liga
def main_fotmob_league_scraping(league_id: int, out_path: str, print_info: bool = True) -> None:
   
    start_time = time.time()   # Inicio del contador

    fm_code = int(comps[comps['id'] == league_id]['fotmob'].iloc[0])        # Código de Fotmob a partir del codigo interno
    league_name = comps[comps['id'] == league_id]['tournament'].iloc[0]     # Nombre de la liga
    league_slug = create_slug(text=league_name)                             # Slug de la liga

    if print_info:
        print('================================================================================')
        print(f'Starting Fotmob scraping ({league_name})')

    out_league_path = os.path.join(out_path, 'fotmob', league_slug)         # Creación de la carpeta de la liga
    os.makedirs(out_league_path, exist_ok=True)

    available_seasons = league_available_seasons(league_code=fm_code, out_path=out_league_path)                 # Temporadas disponibles
    seasons_dict = {v['key']: v['link'] for v in available_seasons.get('allAvailableSeasons', {}).values()}     # Diccionario con las temporadas y enlace
    seasons_dict = {k: v for k, v in seasons_dict.items() if k in desired_seasons}

    for season in seasons_dict.keys():              # Obtención de datos para cada temporada
        season_json = season_data(seasons_dict=seasons_dict, season_key=str(season), out_path=out_league_path)
        if print_info:
            print(f'     - Scraping information for season {season}')
    
    elapsed_time = time.time() - start_time         # Tiempo transcurrido
    if print_info:
        print(f'Finished Fotmob scraping ({league_name}) in {elapsed_time:.2f} seconds')
        print('================================================================================')
