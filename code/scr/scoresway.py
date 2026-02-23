import requests
import re
import pandas as pd
import numpy as np
import os
import json as jsonlib
import time
from datetime import datetime, timedelta

# Obtenemos el CSV con competiciones
cdir = os.getcwd()
utils = os.path.join(os.path.abspath(os.path.join(cdir, '..', '..')), 'utils')
comps = pd.read_csv(os.path.join(utils, 'comps.csv'), sep=';')

# Links Scoresway
sw_links = pd.read_csv(os.path.join(utils, 'sw_urls.csv'), sep=';')

# JSON con temporadas deseadas
with open(os.path.join(utils, 'des_seasons.json'), 'r', encoding='utf-8') as f:
    desired_seasons = jsonlib.load(f)

# Descargar datos de url de Scoresway
def scrape_json(url: str, referer: str = 'https://www.scoresway.com/', sleep_time: int = 3, print_info: bool = True) -> dict:

    if print_info: 
        print(f'Scraping {url}')

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

        # Extraer JSON interno del callback(...)
        match = re.match(r"^[\w$]+\((.*)\)\s*;?\s*$", text, flags=re.DOTALL)
        if not match:
            return {}

        json_str = match.group(1)

        time.sleep(sleep_time)

        return jsonlib.loads(json_str)
    
# Comprueva que el guardado de un fichero ha sucedido X días antes o no
def need_to_upload(path: str, total_days: int = 5) -> bool:                 # Si a los cinco días no se ha actualizado, lo actualizamos

    # Día de creación
    creation_time = os.path.getctime(path)
    return datetime.now() - datetime.fromtimestamp(creation_time) > timedelta(days = total_days)

# Obtenemos los partidos de una temporada en una liga
def season_matches(season: str, league_code: int, out_path: str) -> dict:

    # Entorno de carpetas output
    out_league_path = os.path.join(out_path, str(league_code), season)
    os.makedirs(out_league_path, exist_ok=True)
    json_path = os.path.join(out_league_path, f'Matches{season}.json')

    # Si existe el fichero
    if os.path.exists(json_path) and not need_to_upload(json_path):
        with open(json_path, "r", encoding="utf-8") as f:
            matches_json = jsonlib.load(f)
        return matches_json

    # Obtenemos el link y obtenemos la información
    matches_url = sw_links[sw_links['id'] == league_code][f'match{season}'].iloc[0]
    matches_json = scrape_json(url=matches_url)

    if matches_json.get('match'):
        with open(json_path, "w", encoding="utf-8") as f:
            jsonlib.dump(matches_json, f)
        return matches_json
    
    return {}

# Obtenemos la clasificación de la liga
def season_standings(season: str, league_code: int, out_path: str) -> dict:

    # Entorno de carpetas output
    out_league_path = os.path.join(out_path, str(league_code), season)
    os.makedirs(out_league_path, exist_ok=True)
    json_path = os.path.join(out_league_path, f'Standings{season}.json')

    # Si existe el fichero
    if os.path.exists(json_path) and not need_to_upload(json_path):
        with open(json_path, "r", encoding="utf-8") as f:
            standings_json = jsonlib.load(f)
        return standings_json

    # Obtenemos el link y obtenemos la información
    standings_url = sw_links[sw_links['id'] == league_code][f'standings{season}'].iloc[0]
    standings_json = scrape_json(url=standings_url)

    if standings_json.get('stage'):
        with open(json_path, "w", encoding="utf-8") as f:
            jsonlib.dump(standings_json, f)
        return standings_json
    
    return {}

# Obtenemos la información de un partido
def match_data(season: str, league_code: int, match_id: str, out_path: str) -> dict:

    # Entorno de carpetas output
    out_league_path = os.path.join(out_path, str(league_code), season, 'matches_info')
    os.makedirs(out_league_path, exist_ok=True)
    json_path = os.path.join(out_league_path, f'{match_id}.json')

    # Si existe el fichero
    if os.path.exists(json_path) and os.path.getsize(json_path) > 0:
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
    
    # URL y información del partido
    match_url = f'https://api.performfeeds.com/soccerdata/match/ft1tiv1inq7v1sk3y9tv12yh5/{match_id}?_rt=c&live=yes&_lcl=en&_fmt=jsonp&sps=widgets&_clbk=cb'
    match_json = scrape_json(match_url) or {}

    if isinstance(match_json, dict) and match_json.get('matchInfo'):
        with open(json_path, "w", encoding="utf-8") as f:
            jsonlib.dump(match_json, f, ensure_ascii=False)
        return match_json
    
    return {}

# Dado el codigo de una liga, scraping de toda la información
def scrape_league_data(league_id: int, out_path: str, available_sw_seasons: list = ['2425', '2526']) -> None:

    for season in available_sw_seasons:

        # Obtención de los partidos y la clasificación de la temporada
        matches_json = season_matches(season=season, league_code=league_id, out_path=out_path)
        standings_json = season_standings(season=season, league_code=league_id, out_path=out_path)

        # Partidos jugados
        played_matches = {m.get('matchInfo', {}).get('id'): f"{m.get('matchInfo', {}).get('contestant', [{}])[0].get('name','')}-{m.get('matchInfo', {}).get('contestant', [{},{}])[1].get('name','')}".lower().replace(' ', '-')
                        for m in matches_json.get('match', []) if m.get('liveData', {}).get('matchDetails', {}).get('matchStatus') == 'Played'}
        
        # Para cada partido
        for match_id in played_matches.keys():
            match_json = match_data(season=season, league_code=league_id, match_id=match_id, out_path=out_path)