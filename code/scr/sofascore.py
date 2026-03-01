import pandas as pd
import numpy as np
import os
import json as jsonlib
import time
import subprocess                           # Para ejecutar processos en R
from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Obtenemos el CSV con competiciones
cdir = os.getcwd()
utils = os.path.join(os.path.abspath(os.path.join(cdir, '..', '..')), 'utils')
comps = pd.read_csv(os.path.join(utils, 'comps.csv'), sep=';')

# JSON con temporadas deseadas
with open(os.path.join(utils, 'des_seasons.json'), 'r', encoding='utf-8') as f:
    desired_seasons = jsonlib.load(f)

# Creación driver
options = Options()
options.add_argument("--headless=new")
driver = webdriver.Chrome(options=options)
driver.get("https://www.google.com")

# Convertir un URL a JSON
def page_scraper(url: str, sleep_time: int = 3, timeout: int = 10, print_info: bool = True) -> dict:

    if print_info: 
        print(f'Scraping {url}')

    # Usando el driver, entramos a la página - definimos un timeout en segundos por si se tarda
    driver.get(url)
    pre = WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.TAG_NAME, "pre")))

    # Convertimos los datos en formato JSON
    data_json = jsonlib.loads(pre.text)
    time.sleep(sleep_time)
    return(data_json)

# Comprueva que el guardado de un fichero ha sucedido X días antes o no
def need_to_upload(path: str, total_days: int = 5) -> bool:                 # Si a los cinco días no se ha actualizado, lo actualizamos

    if not os.path.exists(path):
        return True

    # Día de creación
    creation_time = os.path.getctime(path)
    return datetime.now() - datetime.fromtimestamp(creation_time) > timedelta(days = total_days)

# Obtenemos un diccionario JSON con las temporadas disponibles en una liga
def league_available_seasons(league_code: int, out_path: str) -> dict:

    # Entorno de carpetas output
    json_path = os.path.join(out_path, 'available_seasons.json')

    # Si existe el fichero
    if os.path.exists(json_path):
        with open(json_path, "r", encoding="utf-8") as f:
            available_seasons_json = jsonlib.load(f)
        return available_seasons_json

    # Si no existe entramos en el proceso de scraping
    fotmob_url = f'https://api.sofascore.com/api/v1/unique-tournament/{league_code}/seasons/'

    # Leer JSON y obtener lista con temporadas disponibles
    available_seasons_json = page_scraper(url=fotmob_url)

    # Guardado en JSON si existe 'seasons'
    if available_seasons_json.get('seasons'):
        with open(json_path, "w", encoding="utf-8") as f:
            jsonlib.dump(available_seasons_json, f)
    
    return available_seasons_json

# A partir de la clave de una temporada obtenemos el JSON con su información
def season_data(seasons_dict: dict, season_key: str, league_code: int, out_path: str) -> list:
    
    i = 0
    list_match_dict = []

    if season_key not in seasons_dict:
        return list_match_dict
    
    # Entorno de carpetas output
    out_season_path = os.path.join(out_path, 'info', 'matches')
    os.makedirs(out_season_path, exist_ok=True)

    # Mientras se pueda scrapear
    while True:

        # Path
        json_path = os.path.join(out_season_path, f'{i}.json')

        # Si existe el fichero
        if os.path.exists(json_path) and not need_to_upload(json_path):
            with open(json_path, "r", encoding="utf-8") as f:
                season_json = jsonlib.load(f)
            list_match_dict.append(season_json)
            i += 1

        # Si no existe el fichero
        else:
            season_id = seasons_dict[season_key]
            matches_url = f'https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{season_id}/events/last/{i}'
            matches_json = page_scraper(url=matches_url)

            if not matches_json.get('events', []):
                return list_match_dict
            else:
                # Guardamos
                with open(json_path, "w", encoding="utf-8") as f:
                    jsonlib.dump(matches_json, f)
    
                # Añadimos a la lista
                list_match_dict.append(matches_json)
                i += 1

# Obtención de las tablas de la liga (total, home y away)
def season_standings(seasons_dict: dict, season_key: str, league_code: int, out_path: str) -> dict:

    # Validación
    if season_key not in seasons_dict:
        return {}
    
    # Carpeta output
    out_season_path = os.path.join(out_path, 'info')
    os.makedirs(out_season_path, exist_ok=True)

    # Path cache
    standings_path = os.path.join(out_season_path, f"standings.json")

    # Si existe y no hace falta actualizar
    if os.path.exists(standings_path) and not need_to_upload(path=standings_path):
        with open(standings_path, "r", encoding="utf-8") as f:
            return jsonlib.load(f)

    season_id = seasons_dict[season_key]

    urls = {"total": f"https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{season_id}/standings/total",
            "home":  f"https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{season_id}/standings/home",
            "away":  f"https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{season_id}/standings/away"}

    standings = {}

    for key, url_to_scrape in urls.items():
        standings_json = page_scraper(url=url_to_scrape)
        standings[key] = standings_json if standings_json.get("standings") else {}

    # Guardar 1 vez el dict completo
    with open(standings_path, "w", encoding="utf-8") as f:
        jsonlib.dump(standings, f, ensure_ascii=False, indent=2)

    return standings

# Obtención de las tablas de la liga (total, home y away)
def season_information(seasons_dict: dict, season_key: str, league_code: int, out_path: str) -> dict:

    list_information = {}

    if season_key not in seasons_dict:
        return list_information
    
    # Carpeta output
    out_season_path = os.path.join(out_path, 'info')
    os.makedirs(out_season_path, exist_ok=True)

    info_dict = {'player': f'https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{seasons_dict[season_key]}/players',
                 'team': f'https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{seasons_dict[season_key]}/teams',
                 'venue': f'https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{seasons_dict[season_key]}/venues'}
    
    for info_key in info_dict.keys():

        # Path  
        info_path = os.path.join(out_season_path, f"{info_key}.json")

        # Si existe y no hace falta actualizar
        if os.path.exists(info_path) and not need_to_upload(path=info_path):
            with open(info_path, "r", encoding="utf-8") as f:
                info_scraped = jsonlib.load(f)
            list_information[info_key] = (info_scraped)
        
        else: 
            page_to_scrape = info_dict[info_key]
            info_scraped = page_scraper(url=page_to_scrape)

            # Obtenemos la información
            with open(info_path, "w", encoding="utf-8") as f:
                jsonlib.dump(info_scraped, f, ensure_ascii=False, indent=2)
            list_information[info_key] = (info_scraped)
    
    return list_information

# Scraping de un simple partido
def match_scraping(matches_dict: dict, match_id: int, out_path: str) -> dict:

    if match_id not in matches_dict.keys():
        return {}

    # Carpeta output
    out_season_path = os.path.join(out_path, 'matches', 'match')
    os.makedirs(out_season_path, exist_ok=True)

    # Comprovamos que no existe
    final_path = os.path.join(out_season_path, f'{match_id}.json')
    if os.path.exists(final_path):
        with open(final_path, "r", encoding="utf-8") as f:
            return jsonlib.load(f)
        
    # Vamos a scrapear uno a uno, si en alguno hay error, devolveremos vacío -> no seguiremos si hay error
    match_info_url = f'https://api.sofascore.com/api/v1/event/{match_id}'
    match_info_json = page_scraper(url=match_info_url)
    match_lineups_url = f'https://api.sofascore.com/api/v1/event/{match_id}/lineups'
    match_lineups_json = page_scraper(url=match_lineups_url)
    match_stats_url = f'https://api.sofascore.com/api/v1/event/{match_id}/statistics'
    match_stats_json = page_scraper(url=match_stats_url)
    match_shotmap_url = f'https://api.sofascore.com/api/v1/event/{match_id}/shotmap'
    match_shotmap_json = page_scraper(url=match_shotmap_url)
    match_graph_url = f'https://api.sofascore.com/api/v1/event/{match_id}/graph'
    match_graph_json = page_scraper(url=match_graph_url)
    match_incidents_url = f'https://api.sofascore.com/api/v1/event/{match_id}/incidents'
    match_incidents_json = page_scraper(url=match_incidents_url)

    # Comrovamos que sea correcto
    if match_info_json.get('event') and match_lineups_json.get('confirmed') and match_stats_json.get('statistics') and match_shotmap_json.get('shotmap') and match_graph_json.get('graphPoints') and match_incidents_json.get('incidents'):

        # Diccionario y lo guardamos
        full_match_info = {'match': match_info_json, 'lineups': match_lineups_json, 'statistics': match_stats_json, 'shotmap': match_shotmap_json, 'graph': match_graph_json, 'incidents': match_incidents_json}
        with open(final_path, "w", encoding="utf-8") as f:
            jsonlib.dump(full_match_info, f, ensure_ascii=False, indent=2)
        return full_match_info
    
    return {}    

# Ejecución de un proceso en R para la descarga de la imagen de un jugador, equipo, manager, o estadio
def image_downloader(type: str, id: int, out_path: str, sleep_time: int = 3, rscript_path: str = r"C:\Program Files\R\R-4.4.1\bin\x64\Rscript.exe", rfile: str = 'sofascore_images.R') -> None:

    # Script en la misma carpeta
    r_script = os.path.join(cdir, rfile)

    # Acceso a la carpeta de output
    out_season_path = os.path.join(out_path, 'images')
    os.makedirs(out_season_path, exist_ok=True)

    # Según el tipo, el URL y el output será diferente
    if type == 'player':
        out_dir = os.path.join(out_season_path, 'player')
        os.makedirs(out_dir, exist_ok=True)
        image_url = f"https://img.sofascore.com/api/v1/player/{id}/image"
        out_file = os.path.join(out_dir, f'{id}.png')        
    elif type == 'team':
        out_dir = os.path.join(out_season_path, 'team')
        os.makedirs(out_dir, exist_ok=True)
        image_url = f"https://img.sofascore.com/api/v1/team/{id}/image"
        out_file = os.path.join(out_dir, f'{id}.png') 
    elif type == 'manager':
        out_dir = os.path.join(out_season_path, 'manager')
        os.makedirs(out_dir, exist_ok=True)
        image_url = f"https://img.sofascore.com/api/v1/manager/{id}/image"
        out_file = os.path.join(out_dir, f'{id}.png') 
    elif type == 'venue':
        out_dir = os.path.join(out_season_path, 'venue')
        os.makedirs(out_dir, exist_ok=True)
        image_url = f"https://img.sofascore.com/api/v1/venue/{id}/image"
        out_file = os.path.join(out_dir, f'{id}.png') 

    # Run si no existe el file
    if not os.path.exists(out_file) and need_to_upload(path=out_file, total_days=90):       # Cada tres meses actualizamos fotos
        cmd = [rscript_path, r_script, image_url, os.path.join(out_file)]
        res = subprocess.run(cmd, text=True, capture_output=True)

        time.sleep(sleep_time)

# Obtener información sobre un jugador, equipo, manager o estadio
def obtain_information(type: str, id: int, season_key: str, league_code: int, out_season_path: str) -> dict:

    # Según el tipo, el URL y el output será diferente
    if type == 'player':
        info_path = f'https://api.sofascore.com/api/v1/player/{id}'
        out_dir = os.path.join(out_season_path, 'info', 'player')
        os.makedirs(out_dir, exist_ok=True)
        out_json = os.path.join(out_dir, f'{id}.json')
    elif type == 'team':
        info_path = f'https://api.sofascore.com/api/v1/team/{id}'
        out_dir = os.path.join(out_season_path, 'info', 'team')
        os.makedirs(out_dir, exist_ok=True)
        out_json = os.path.join(out_dir, f'{id}.json')
    elif type == 'manager':
        info_path = f'https://api.sofascore.com/api/v1/manager/{id}'
        out_dir = os.path.join(out_season_path, 'info', 'manager')
        os.makedirs(out_dir, exist_ok=True)
        out_json = os.path.join(out_dir, f'{id}.json')        
    elif type == 'venue':
        info_path = f'https://api.sofascore.com/api/v1/venue/{id}'
        out_dir = os.path.join(out_season_path, 'info', 'venue')
        os.makedirs(out_dir, exist_ok=True)
        out_json = os.path.join(out_dir, f'{id}.json')

    # Si existe, lo devolvemos, sino scrapeamos
    if os.path.exists(out_json) and not need_to_upload(path=out_json, total_days=30):           # Cada mes actualizamos información
        with open(out_json, "r", encoding="utf-8") as f:
            return jsonlib.load(f)
    else:
        info_json = page_scraper(url=info_path)
        if info_json.get(type):
            with open(out_json, "w", encoding="utf-8") as f:
                jsonlib.dump(info_json, f, ensure_ascii=False, indent=2)
        return info_json
    
# Dado el codigo de una liga, scraping de toda la información
def scrape_league_data(league_id: int, out_path: str) -> None:

    # Obtenemos el codigo de sofascore
    ss_code = int(comps[comps['id'] == league_id]['sofascore'].iloc[0])

    # Obtenemos el nombre de la liga
    league_name = comps[comps['id'] == league_id]['tournament'].iloc[0]
    league_slug = league_name.lower().replace(' ', '-')
  
    # Path de la liga
    out_league_path = os.path.join(out_path, league_slug)
    os.makedirs(out_league_path, exist_ok=True)

    # Competiciones disponibles y diccionario para obtener los valores
    available_seasons = league_available_seasons(league_code=ss_code, out_path=out_league_path)
    seasons_dict = {v['year']: v['id'] for v in available_seasons.get('seasons', {})}
    seasons_dict = {k.replace('/', ''): v for k, v in seasons_dict.items()}
    seasons_dict = {k: v for k, v in seasons_dict.items() if k in desired_seasons}


    # Para cada temporada
    for season_key in seasons_dict.keys():

         # Path de la season
        out_season_path = os.path.join(out_league_path, season_key)
        os.makedirs(out_season_path, exist_ok=True)
        print(out_season_path)

        # Obtenemos los datos de una temporada - partidos jugados
        season_data_list = season_data(seasons_dict=seasons_dict, season_key=season_key, league_code=ss_code, out_path=out_season_path)
        season_stand = season_standings(seasons_dict=seasons_dict, season_key=season_key, league_code=ss_code, out_path=out_season_path)
        season_info = season_information(seasons_dict=seasons_dict, season_key=season_key, league_code=ss_code, out_path=out_season_path)

        # Diccionario con los partidos, jugadores, equipos y estadios
        dict_matches = {match['id']: match['slug'] for events in season_data_list for match in events.get('events', []) if match.get('status', {}).get('description') == 'Ended'}
        dict_players = {player['playerId']: player['playerName'].lower().replace(' ', '-') for player in season_info['player']['players']}
        dict_teams = {team['id']: team['slug'] for team in season_info['team']['teams']}
        dict_venues = {venue['id']: venue['slug'] for venue in season_info['venue']['venues']}

        # Para cada partido
        for match_id in dict_matches.keys():

            # Extracción de información de un partido
            match_info = match_scraping(matches_dict=dict_matches, match_id=match_id, out_path=out_season_path)

            # IDs de los jugadores que han jugado almenos un minuto en el partido -> los que tienen estadísticas
            home_players_ids = {p.get('player', {}).get('id', 0): p.get('player', {}).get('slug', '') for p in match_info.get('lineups', {}).get('home', {}).get('players', []) if p.get('statistics', {}).get('minutesPlayed', 0) > 0}
            away_players_ids = {p.get('player', {}).get('id', 0): p.get('player', {}).get('slug', '') for p in match_info.get('lineups', {}).get('away', {}).get('players', []) if p.get('statistics', {}).get('minutesPlayed', 0) > 0}
            players_ids = home_players_ids | away_players_ids

            # Diccionario con los managers del partido
            home_manager = {match_info.get('match', {}).get('event', {}).get('homeTeam', {}).get('manager', {}).get('id', 0): match_info.get('match', {}).get('event', {}).get('homeTeam', {}).get('manager', {}).get('slug', '')}
            away_manager = {match_info.get('match', {}).get('event', {}).get('awayTeam', {}).get('manager', {}).get('id', 0): match_info.get('match', {}).get('event', {}).get('awayTeam', {}).get('manager', {}).get('slug', '')}
            managers_ids = home_manager | away_manager

            # Para manager en el partido, obtener información de los managers y fotografias
            for manager in managers_ids.keys():
                manager_info = obtain_information(type='manager', id=manager, season_key=season_key, league_code=ss_code, out_season_path=out_season_path)
                image_downloader(type='manager', id=manager, out_path=out_season_path)

        # Para cada jugador de todos los disponibles, obtenemos información y fotografías, al igual que con equipos y venues
        for player in dict_players.keys():
            player_info = obtain_information(type='player', id=player, season_key=season_key, league_code=ss_code, out_season_path=out_season_path)
            image_downloader(type='player', id=player, out_path=out_season_path)

        for team in dict_teams.keys():
            team_info = obtain_information(type='team', id=team, season_key=season_key, league_code=ss_code, out_season_path=out_season_path)
            image_downloader(type='team', id=team, out_path=out_season_path)

        for venue in dict_venues.keys():
            venue_info = obtain_information(type='venue', id=venue, season_key=season_key, league_code=ss_code, out_season_path=out_season_path)
            image_downloader(type='venue', id=venue, out_path=out_season_path)