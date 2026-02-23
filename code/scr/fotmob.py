import requests
import pandas as pd
import numpy as np
import os
import json as jsonlib
import time

# Obtenemos el CSV con competiciones
cdir = os.getcwd()
utils = os.path.join(os.path.abspath(os.path.join(cdir, '..', '..')), 'utils')
comps = pd.read_csv(os.path.join(utils, 'comps.csv'), sep=';')

# JSON con temporadas deseadas
with open(os.path.join(utils, 'des_seasons.json'), 'r', encoding='utf-8') as f:
    desired_seasons = jsonlib.load(f)

# Convertir un URL a JSON
def url_to_json(url: str, sleep_time: int = 3, print_info: bool = True) -> dict:

    if print_info: 
        print(f'Scraping {url}')

    out = requests.get(url).json()
    time.sleep(sleep_time)                           # Para garantir seguridad
    return out

# Obtenemos un diccionario JSON con las temporadas disponibles en una liga
def league_available_seasons(league_code: int, out_path: str) -> dict:

    # Entorno de carpetas output
    out_league_path = os.path.join(out_path, str(league_code))
    os.makedirs(out_league_path, exist_ok=True)
    json_path = os.path.join(out_league_path, 'AvailableSeasons.json')

    # Si existe el fichero
    if os.path.exists(json_path):
        with open(json_path, "r", encoding="utf-8") as f:
            available_seasons_json = jsonlib.load(f)
        return available_seasons_json

    # Si no existe entramos en el proceso de scraping
    fotmob_url = f'https://www.fotmob.com/api/leagues?id={league_code}'

    # Leer JSON y obtener lista con temporadas disponibles
    available_seasons_json = url_to_json(url=fotmob_url)
    available_seasons = available_seasons_json.get('allAvailableSeasons', [])

    # Diccionario para añadir info
    seasons_dict = {}

    # Para cada temporada, obtenemos el link
    for s in available_seasons:

        # Obtenemos el link a partir de sus años
        if "/" in s:
            start, end = s.split("/")           # Separación por año
            key_candidate = start[-2:] + end[-2:]
        else:
            s = s[:4]                           # Sino, primer año
            key_candidate = f"{int(s) % 100:02d}{(int(s)+1) % 100:02d}"

        season_link = f"{fotmob_url}&season={s}"

        # Guardamos temporada en key y link
        seasons_dict[s] = {"key": key_candidate, "link": season_link}

    # Substituimos dentro del JSON original
    available_seasons_json["allAvailableSeasons"] = seasons_dict

    # Guardado en JSON
    if available_seasons_json.get('allAvailableSeasons'):
        with open(json_path, "w", encoding="utf-8") as f:
            jsonlib.dump(available_seasons_json, f)
    
    return available_seasons_json

# A partir de la clave de una temporada obtenemos el JSON con su información
def season_data(seasons_dict: dict, season_key: str, league_code: int, out_path: str) -> dict:

    # Entorno de carpetas output
    out_league_path = os.path.join(out_path, str(league_code))
    os.makedirs(out_league_path, exist_ok=True)
    json_path = os.path.join(out_league_path, f'Season{season_key}.json')

    # Si existe el fichero
    if os.path.exists(json_path):
        with open(json_path, "r", encoding="utf-8") as f:
            season_json = jsonlib.load(f)
        return season_json
    
    # Si no existe, comprovamos
    if season_key not in seasons_dict.keys():
        return {}
    else:
        season_link = seasons_dict[season_key]

    # Leemos el link y guardado en JSON
    season_json = url_to_json(season_link)

    if season_json.get('fixtures'):
        with open(json_path, "w", encoding="utf-8") as f:
            jsonlib.dump(season_json, f)
    
    return season_json

# Obtención de los datos de un partido usando su ID
def match_data(matches_dict: dict, match_id: str, league_code: int, out_path: str) -> dict:

    # Entorno de carpetas output
    out_league_path = os.path.join(out_path, str(league_code), 'matches')
    os.makedirs(out_league_path, exist_ok=True)
    json_path = os.path.join(out_league_path, f'Match{match_id}.json')

    # Si existe el fichero
    if os.path.exists(json_path):
        with open(json_path, "r", encoding="utf-8") as f:
            match_json = jsonlib.load(f)
        return match_json
    
    # Comprovamos que el ID esta entre los partidos
    if match_id not in matches_dict.keys():
        return {}
    else:
        match_link = matches_dict[match_id]
        print(match_id, json_path)

    # Leemos el link y guardado en JSON
    match_json = url_to_json(url=match_link)
    with open(json_path, "w", encoding="utf-8") as f:
        jsonlib.dump(match_json, f)
    
    return match_json

# Dado el codigo de una liga, scraping de toda la información
def scrape_league_data(league_id: int, out_path: str) -> None:

    # Obtenemos el codigo de fotmob
    fm_code = int(comps[comps['id'] == league_id]['fotmob'].iloc[0])

    # Competiciones disponibles y diccionario para obtener los valores
    available_seasons = league_available_seasons(league_code=fm_code, out_path=out_path)
    seasons_dict = {v['key']: v['link'] for v in available_seasons.get('allAvailableSeasons', {}).values()}
    seasons_dict = {k: v for k, v in seasons_dict.items() if k in desired_seasons}
    
    # Fichero de temporada a partir de la key
    for season in seasons_dict.keys():
        season_json = season_data(seasons_dict=seasons_dict, season_key=str(season), league_code=fm_code, out_path=out_path)

    # Partidos y diccionario con los IDs
    matches = season_json.get('fixtures', {}).get('allMatches', {})
    dict_matches_urls = {match['id']: f'https://www.fotmob.com/api/matchDetails?matchId={match['id']}'
                        for match in matches if match.get('status', {}).get('finished', False)}