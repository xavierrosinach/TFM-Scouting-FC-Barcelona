import requests
import re
import pandas as pd
import numpy as np
import os
import json as jsonlib
import time
from datetime import datetime, timedelta
import unicodedata

from config import comps, desired_seasons, act_season

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

# Creación de slug a partir de un string.
def create_slug(text: str) -> str:

    text = text.lower()                                                                                     # Letra minúscula
    text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')        # Eliminación de acentos
    text = re.sub(r"\s+", "_", text)                                                                        # Substitución de espacios por '_'
    text = re.sub(r"[^a-z0-9_]", "", text)                                                                  # Eliminación de carácteres no alfanuméricos
    text = re.sub(r"_+", "_", text).strip("_")
    
    return text

# Descargar datos de url de Scoresway en formato JSON
def scrape_json(url: str, referer: str = 'https://www.scoresway.com/', sleep_time: int = 3) -> dict:

    headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
               "referer": referer,
               "accept": "*/*",
               "accept-language": "en-US,en;q=0.9",
               "connection": "keep-alive"}

    with requests.Session() as s:
        try:
            s.get(referer, headers=headers, timeout=20)
        except requests.RequestException:
            pass

        r = s.get(url, headers=headers, timeout=30)
        if r.status_code != 200:
            return {}

        text = r.text.strip()
        match = re.match(r"^[\w$]+\((.*)\)\s*;?\s*$", text, flags=re.DOTALL)        # Extraer JSON interno del callback(...)
        if not match:
            return {}

        json_str = match.group(1)
        time.sleep(sleep_time)                                                      # Tiempo de espera para evitar errores
        return jsonlib.loads(json_str)
    
# Comprueva la antiguidad del archivo, si supera unos días, devuelve True conforme se tiene que actualizar.
def need_to_upload(path: str, total_days: int = 5) -> bool:

    creation_time = os.path.getctime(path)          # Día de creación
    return datetime.now() - datetime.fromtimestamp(creation_time) > timedelta(days = total_days)

# Obtenemos los partidos de una temporada en una liga.
def season_matches(season: str, league_code: int, out_path: str) -> dict:

    out_league_path = os.path.join(out_path, 'info')                    # Entorno de carpetas y ficheros - no creamos hasta asegurar que funciona
    json_path = os.path.join(out_league_path, 'matches.json')

    if season != act_season:
        if os.path.exists(json_path):                                       # Si existe el fichero
            with open(json_path, "r", encoding="utf-8") as f:
                matches_json = jsonlib.load(f)
            return matches_json
    elif season == act_season:
        if os.path.exists(json_path) and not need_to_upload(json_path):     # Si existe el fichero
            with open(json_path, "r", encoding="utf-8") as f:
                matches_json = jsonlib.load(f)
            return matches_json

    if f'scoresway{season}' in comps.columns:
        league_sw = comps[comps['id'] == league_code][f'scoresway{season}'].iloc[0]          # Acceso al link y a la información - Link en el dataframe interno
        matches_url = f'https://api.performfeeds.com/soccerdata/match/ft1tiv1inq7v1sk3y9tv12yh5/?_rt=c&tmcl={league_sw}&live=yes&_pgSz=400&_lcl=en&_fmt=jsonp&sps=widgets&_clbk=cb'
        matches_json = scrape_json(url=matches_url)

        if matches_json.get('match'):                                      # Si hay información, creamos carpeta y guardamos
            os.makedirs(out_league_path, exist_ok=True)
            safe_json_dump(data=matches_json, path=json_path)
            return matches_json
    
    return {}

# Obtenemos la clasificación de una liga
def season_standings(season: str, league_code: int, out_path: str) -> dict:

    out_league_path = os.path.join(out_path, 'info')                    # Entorno de carpetas y ficheros - no creamos hasta asegurar que funciona
    json_path = os.path.join(out_league_path, 'standings.json')

    if season != act_season:
        if os.path.exists(json_path):                                       # Si existe el fichero
            with open(json_path, "r", encoding="utf-8") as f:
                matches_json = jsonlib.load(f)
            return matches_json
    elif season == act_season:
        if os.path.exists(json_path) and not need_to_upload(json_path):     # Si existe el fichero
            with open(json_path, "r", encoding="utf-8") as f:
                matches_json = jsonlib.load(f)
            return matches_json

    if f'scoresway{season}' in comps.columns:
        league_sw = comps[comps['id'] == league_code][f'scoresway{season}'].iloc[0]          # Acceso al link y a la información - Link en el dataframe interno
        standings_url = f'https://api.performfeeds.com/soccerdata/standings/ft1tiv1inq7v1sk3y9tv12yh5/?_rt=c&tmcl={league_sw}&live=yes&_lcl=en&_fmt=jsonp&sps=widgets&_clbk=cb'
        standings_json = scrape_json(url=standings_url)

        if standings_json.get('stage'):                                      # Si hay información, creamos carpeta y guardamos
            os.makedirs(out_league_path, exist_ok=True)
            safe_json_dump(data=standings_json, path=json_path)
            return standings_json
    
    return {}

# Obtenemos las plantillas de una liga
def season_squads(season: str, league_code: int, out_path: str) -> dict:

    out_league_path = os.path.join(out_path, 'info')                    # Entorno de carpetas y ficheros - no creamos hasta asegurar que funciona
    json_path = os.path.join(out_league_path, 'squads.json')

    if season != act_season:
        if os.path.exists(json_path):                                       # Si existe el fichero
            with open(json_path, "r", encoding="utf-8") as f:
                matches_json = jsonlib.load(f)
            return matches_json
    elif season == act_season:
        if os.path.exists(json_path) and not need_to_upload(json_path):     # Si existe el fichero
            with open(json_path, "r", encoding="utf-8") as f:
                matches_json = jsonlib.load(f)
            return matches_json

    if f'scoresway{season}' in comps.columns:
        league_sw = comps[comps['id'] == league_code][f'scoresway{season}'].iloc[0]          # Acceso al link y a la información - Link en el dataframe interno
        squads_url = f'https://api.performfeeds.com/soccerdata/squads/ft1tiv1inq7v1sk3y9tv12yh5/?_rt=c&tmcl={league_sw}&_pgSz=200&_lcl=en&_fmt=jsonp&sps=widgets&_clbk=cb'
        squads_json = scrape_json(url=squads_url)

        if squads_json.get('squad'):                                      # Si hay información, creamos carpeta y guardamos
            os.makedirs(out_league_path, exist_ok=True)
            safe_json_dump(data=squads_json, path=json_path)
            return squads_json
    
    return {}

# Obtención de las estadísticas de un partido
def match_stats(match_id: str, out_path: str) -> dict:

    out_league_path = os.path.join(out_path, 'matches')                 # Entorno de carpetas output
    os.makedirs(out_league_path, exist_ok=True)
    json_path = os.path.join(out_league_path, f'{match_id}.json')

    if os.path.exists(json_path) and os.path.getsize(json_path) > 0:    # Si existe el fichero
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                match_json = jsonlib.load(f)
            if isinstance(match_json, dict) and match_json.get('matchInfo'):
                return match_json
        except Exception:
            try:
                os.remove(json_path)
            except OSError:
                pass
    
    stats_url = f'https://api.performfeeds.com/soccerdata/matchstats/ft1tiv1inq7v1sk3y9tv12yh5/{match_id}?_rt=c&_lcl=en&_fmt=jsonp&sps=widgets&_clbk=cb'        # URL y información del partido
    stats_json = scrape_json(stats_url) or {}   

    if isinstance(stats_json, dict) and stats_json.get('matchInfo'):        # Guardamos si tiene estadísticas
        safe_json_dump(data=stats_json, path=json_path)
        return stats_json
    
    return {}

# Función principal para la extracción de datos de Scoresway de una liga
def main_scoresway_league_scraping(league_id:int, out_path:str, matches_to_proc:int=None, print_info:bool=True) -> None:

    start_time = time.time()   # Inicio del contador

    league_name = comps[comps['id']==league_id]['tournament'].iloc[0]     # Nombre de la liga
    league_slug = create_slug(text=league_name)                           # Slug de la liga

    if print_info:
        print('================================================================================')
        print(f'Starting Scoresway scraping ({league_name})')

    out_league_path = os.path.join(out_path, 'scoresway', league_slug)    # Creación de la carpeta de la liga
    os.makedirs(out_league_path, exist_ok=True)

    for season in desired_seasons:
        season_path = os.path.join(out_league_path, season)                # Path de la temporada

        matches_json = season_matches(season=season, league_code=league_id, out_path=season_path)       # Partidos de la temporada
        standings_json = season_standings(season=season, league_code=league_id, out_path=season_path)   # Clasificación de la temporada
        season_json = season_squads(season=season, league_code=league_id, out_path=season_path)         # Plantillas de la temporada

        if print_info and matches_json:
            print(f'     - Scraping information for season {season}')

        if matches_json:
            played_matches = {m.get('matchInfo', {}).get('id'):f"{m.get('matchInfo', {}).get('contestant', [{}])[0].get('name','')}-{m.get('matchInfo', {}).get('contestant', [{},{}])[1].get('name','')}".lower().replace(' ', '-')
                              for m in matches_json.get('match', []) if m.get('liveData', {}).get('matchDetails', {}).get('matchStatus')=='Played'}   # Partidos jugados

            match_ids = list(played_matches.keys())
            if matches_to_proc is not None:
                match_ids = match_ids[:matches_to_proc]   # Limitar al máximo indicado

            total_matches = len(match_ids)

            for i, match_id in enumerate(match_ids, start=1):
                if print_info:
                    print(f'          - Scraping information for match {match_id} ({i}/{total_matches})')

                match_stats_json = match_stats(match_id=match_id, out_path=season_path)

    elapsed_time = time.time() - start_time
    if print_info:
        print(f'Finished Scoresway scraping ({league_name}) in {elapsed_time:.2f} seconds')
        print('================================================================================')