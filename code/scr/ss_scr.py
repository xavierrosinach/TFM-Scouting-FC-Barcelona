import os
import json as jsonlib
import time
import subprocess                           # Para ejecutar processos en R

from selenium import webdriver                              # Librería y extensiones de Selenium para 
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from contextlib import contextmanager

from use.config import comps, desired_seasons, act_season
from use.functions import safe_json_dump, create_slug, need_to_upload, elapsed_time_str

# Función para suprimir stderr (logs internos de Chrome)
@contextmanager
def suppress_stderr():
    old_stderr_fd = os.dup(2)
    devnull = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull, 2)
    os.close(devnull)
    try:
        yield
    finally:
        os.dup2(old_stderr_fd, 2)
        os.close(old_stderr_fd)

# Evita que Chrome escriba logs
os.environ["CHROME_LOG_FILE"] = os.devnull

# Opciones de Chrome
options = Options()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--disable-software-rasterizer")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--no-sandbox")
options.add_argument("--log-level=3")
options.add_argument("--silent")
options.add_argument("--disable-logging")
options.add_argument("--disable-webgl")
options.add_argument("--disable-features=Vulkan")
options.add_argument("--use-angle=swiftshader")
options.add_argument("--use-gl=swiftshader")
options.add_experimental_option("excludeSwitches", ["enable-logging"])

# Servicio de ChromeDriver
service = Service(log_path=os.devnull)

# Creación del driver sin mostrar logs
with suppress_stderr():
    driver = webdriver.Chrome(service=service, options=options)
driver.get("https://www.google.com")

# Convertir un URL de Sofascore a JSON
def page_scraper(url: str, sleep_time: int = 3, timeout: int = 10) -> dict:

    # Usando el driver, entramos a la página - definimos un timeout en segundos por si se tarda
    driver.get(url)
    pre = WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.TAG_NAME, "pre")))

    # Convertimos los datos en formato JSON
    data_json = jsonlib.loads(pre.text)
    time.sleep(sleep_time)
    return(data_json)

# Obtenemos un diccionario JSON con las temporadas disponibles en una liga
def league_available_seasons(league_code: int, out_path: str) -> dict:

    json_path = os.path.join(out_path, 'available_seasons.json')        # Path del archivo

    if os.path.exists(json_path) and not need_to_upload(json_path, total_days=200):     # Observamos que no existe y que no necesita actualizarse
        with open(json_path, "r", encoding="utf-8") as f:
            available_seasons_json = jsonlib.load(f)
        return available_seasons_json

    
    url = f'https://api.sofascore.com/api/v1/unique-tournament/{league_code}/seasons/'      # Scraping si no existe
    available_seasons_json = page_scraper(url=url)
    if available_seasons_json.get('seasons'):                                               # Guardado
        safe_json_dump(data=available_seasons_json, path=json_path)
        return available_seasons_json

    return {}

# A partir de la clave de una temporada obtenemos el JSON con su información
def season_data(seasons_dict: dict, season_key: str, league_code: int, out_path: str) -> dict:
    
    i = 0
    list_match_dict = {}

    if season_key not in seasons_dict:                              # Aseguramos
        return list_match_dict
    
    out_season_path = os.path.join(out_path, 'info', 'matches')     # Entorno output
    os.makedirs(out_season_path, exist_ok=True)

    
    while True:     # Mientras se pueda scrapear - en este caso se tienen los partidos en distintos ficheros - si nos da error vamos a parar
        json_path = os.path.join(out_season_path, f'{i}.json')      # Path y comprovación de existencia
        
        if season_key != act_season:
            if os.path.exists(json_path):
                with open(json_path, "r", encoding="utf-8") as f:
                    season_json = jsonlib.load(f)
                list_match_dict[i] = season_json
                i += 1
            else:
                season_id = seasons_dict[season_key]
                matches_url = f'https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{season_id}/events/last/{i}'    # Enlace y scrapeo
                matches_json = page_scraper(url=matches_url)

                if not matches_json.get('events', []):      # Comprobamos que es valido y guardamos
                    return list_match_dict
                else:
                    safe_json_dump(data=matches_json, path=json_path)
                    list_match_dict[i] = matches_json
                    i += 1
            
        elif season_key == act_season:                                          # Solo si es la temporada actual comprovamos que no haya datos a actualizar
            if os.path.exists(json_path) and not need_to_upload(json_path):
                with open(json_path, "r", encoding="utf-8") as f:
                    season_json = jsonlib.load(f)
                list_match_dict[i] = season_json
                i += 1
            else:
                season_id = seasons_dict[season_key]
                matches_url = f'https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{season_id}/events/last/{i}'    # Enlace y scrapeo
                matches_json = page_scraper(url=matches_url)

                if not matches_json.get('events', []):      # Comprobamos que es valido y guardamos
                    return list_match_dict
                else:
                    safe_json_dump(data=matches_json, path=json_path)
                    list_match_dict[i] = matches_json
                    i += 1        

# Obtención de las tablas de la liga (total, home y away)
def season_standings(seasons_dict: dict, season_key: str, league_code: int, out_path: str) -> dict:

    if season_key not in seasons_dict:      # Validación
        return {}
    
    out_season_path = os.path.join(out_path, 'info')        # Carpeta output y path con la información
    os.makedirs(out_season_path, exist_ok=True)
    standings_path = os.path.join(out_season_path, "standings.json")

    if season_key != act_season:
        if os.path.exists(standings_path):
            with open(standings_path, "r", encoding="utf-8") as f:
                return jsonlib.load(f)
    elif season_key == act_season:
        if os.path.exists(standings_path) and not need_to_upload(path=standings_path):      # Observamos que no exista - solo pendiente de actualizar en la temporada actual
            with open(standings_path, "r", encoding="utf-8") as f:
                return jsonlib.load(f)

    season_id = seasons_dict[season_key]
    urls = {"total": f"https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{season_id}/standings/total",            # Obtención de URLs
            "home":  f"https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{season_id}/standings/home",
            "away":  f"https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{season_id}/standings/away"}

    standings = {}
    for key, url_to_scrape in urls.items():
        standings_json = page_scraper(url=url_to_scrape)
        standings[key] = standings_json if standings_json.get("standings") else {}

    safe_json_dump(data=standings, path=standings_path)         # Guardado del diccionario completo
    return standings

# Obtención de información de jugadores, equipos, y estadios
def season_information(seasons_dict: dict, season_key: str, league_code: int, out_path: str) -> dict:

    list_information = {}
    if season_key not in seasons_dict:      # Comprovación
        return list_information
    
    out_season_path = os.path.join(out_path, 'info')        # Carpeta output y enlaces
    os.makedirs(out_season_path, exist_ok=True)
    info_dict = {'player': f'https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{seasons_dict[season_key]}/players',
                 'team': f'https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{seasons_dict[season_key]}/teams',
                 'venue': f'https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{seasons_dict[season_key]}/venues'}
    
    for info_key in info_dict.keys():
        info_path = os.path.join(out_season_path, f"{info_key}.json")

        if season_key != act_season:                                        # Diferenciación entre temporada actual y temporada anterior
            if os.path.exists(info_path):
                with open(info_path, "r", encoding="utf-8") as f:
                    info_scraped = jsonlib.load(f)
                list_information[info_key] = (info_scraped)
            else: 
                page_to_scrape = info_dict[info_key]
                info_scraped = page_scraper(url=page_to_scrape)
                safe_json_dump(data=info_scraped, path=info_path)
                list_information[info_key] = (info_scraped)

        elif season_key == act_season:
            if os.path.exists(info_path) and not need_to_upload(path=info_path):
                with open(info_path, "r", encoding="utf-8") as f:
                    info_scraped = jsonlib.load(f)
                list_information[info_key] = (info_scraped)
            else: 
                page_to_scrape = info_dict[info_key]
                info_scraped = page_scraper(url=page_to_scrape)
                safe_json_dump(data=info_scraped, path=info_path)
                list_information[info_key] = (info_scraped)
    
    return list_information

# Ejecución de un proceso en R para la descarga de la imagen de un jugador, equipo, manager, o estadio
def image_downloader(type: str, id: int, out_path: str, sleep_time: int = 3, rscript_path: str = r"C:\Program Files\R\R-4.4.1\bin\x64\Rscript.exe", r_script: str = 'ss_pict.R') -> None:

    out_season_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(out_path))), 'images')
    os.makedirs(out_season_path, exist_ok=True)
                                  
    out_dir = os.path.join(out_season_path, type)           # Diferenciamos variables según el tipo
    os.makedirs(out_dir, exist_ok=True)
    image_url = f"https://img.sofascore.com/api/v1/{type}/{id}/image"
    out_file = os.path.join(out_dir, f'{id}.png')

    if not os.path.exists(out_file):                                                    # Si no existe la imagen ejecutamos el codigo de obtención
        cmd = [rscript_path, r_script, image_url, os.path.join(out_file)]
        res = subprocess.run(cmd, text=True, capture_output=True)
        time.sleep(sleep_time)
    elif os.path.exists(out_file) and need_to_upload(path=out_file, total_days=90):     # Cada tres meses actualizamos fotos
        cmd = [rscript_path, r_script, image_url, os.path.join(out_file)]
        res = subprocess.run(cmd, text=True, capture_output=True)
        time.sleep(sleep_time)

# Obtener información sobre un jugador, equipo, manager o estadio
def obtain_information(type: str, id: int, out_season_path: str) -> None:

    info_path = f'https://api.sofascore.com/api/v1/{type}/{id}'         # Diferenciación de variables según el tipo
    out_dir = os.path.join(out_season_path, 'info', type)
    os.makedirs(out_dir, exist_ok=True)
    out_json = os.path.join(out_dir, f'{id}.json')

    if not os.path.exists(out_json) and need_to_upload(path=out_json, total_days=30):           # Cada mes actualizamos información
        info_json = page_scraper(url=info_path)
        if info_json.get(type):
            safe_json_dump(data=info_json, path=out_json)
    
# Scraping de un simple partido
def match_scraping(matches_dict: dict, match_id: int, out_path: str) -> dict:

    if match_id not in matches_dict.keys():     # Comprovamos
        return {}

    out_season_path = os.path.join(out_path, 'matches')    # Entorno de carpetas y comprovación de que existe
    os.makedirs(out_season_path, exist_ok=True)
    final_path = os.path.join(out_season_path, f'{match_id}.json')
    if os.path.exists(final_path):
        try:
            with open(final_path, "r", encoding="utf-8") as f:
                return jsonlib.load(f)
        except:
            return {}
        
    match_info_url = f'https://api.sofascore.com/api/v1/event/{match_id}'                   # Información general del partido
    match_info_json = page_scraper(url=match_info_url)
    match_lineups_url = f'https://api.sofascore.com/api/v1/event/{match_id}/lineups'        # Alineaciones
    match_lineups_json = page_scraper(url=match_lineups_url)
    match_stats_url = f'https://api.sofascore.com/api/v1/event/{match_id}/statistics'       # Estadísticas generales del equipo
    match_stats_json = page_scraper(url=match_stats_url)

    if match_info_json.get('event') and match_lineups_json.get('confirmed') and match_stats_json.get('statistics'):     # Comprovación de datos y guardado
        full_match_info = {'match': match_info_json, 'lineups': match_lineups_json, 'statistics': match_stats_json}
        safe_json_dump(data=full_match_info, path=final_path)
        return full_match_info
    
    return {}    

# Función principal para la extracción de datos de Sofascore de una liga
def main_sofascore_league_scraping(league_id:int, out_path:str, scrape_images:bool=True, matches_to_proc:int=None, print_info:bool=True) -> None:

    start_time = time.time()   # Inicio del contador

    league_name = comps[comps['id']==league_id]['tournament'].iloc[0]     # Nombre de la liga
    league_slug = create_slug(text=league_name)                           # Slug de la liga
    ss_code = int(comps[comps['id'] == league_id]['sofascore'].iloc[0])   # Codigo de Sofascore

    if print_info:
        print(f'Starting Sofascore scraping ({league_name})')

    out_league_path = os.path.join(out_path, 'sofascore', league_slug)    # Creación de la carpeta de la liga
    os.makedirs(out_league_path, exist_ok=True)

    available_seasons = league_available_seasons(league_code=ss_code, out_path=out_league_path)     # Obtención de las competiciones disponibles
    seasons_dict = {v['year']: v['id'] for v in available_seasons.get('seasons', {})}
    seasons_dict = {k.replace('/', ''): v for k, v in seasons_dict.items()}
    seasons_dict = {k: v for k, v in seasons_dict.items() if k in desired_seasons}

    for season_key in seasons_dict.keys():                                  # Entramos a los datos de cada temporada

        if print_info:
            print(f'     - Scraping information for season {season_key}')

        out_season_path = os.path.join(out_league_path, season_key)         # Creación del path si no existe
        os.makedirs(out_season_path, exist_ok=True)

        season_data_dict = season_data(seasons_dict=seasons_dict, season_key=season_key, league_code=ss_code, out_path=out_season_path)             # Scrapeo de datos de los partidos
        season_stand = season_standings(seasons_dict=seasons_dict, season_key=season_key, league_code=ss_code, out_path=out_season_path)            # Tablas de clasificación
        season_info = season_information(seasons_dict=seasons_dict, season_key=season_key, league_code=ss_code, out_path=out_season_path)           # Información

        dict_matches = {match['id']: match['slug'] for events in season_data_dict.values() for match in events.get('events', []) if match.get('status', {}).get('description') == 'Ended'}
        dict_players = {player['playerId']: player['playerName'].lower().replace(' ', '-') for player in season_info['player']['players']}
        dict_teams = {team['id']: team['slug'] for team in season_info['team']['teams']}
        dict_venues = {venue['id']: venue['slug'] for venue in season_info['venue']['venues']}

        players_ids = list(dict_players.keys())[:matches_to_proc] if matches_to_proc is not None else list(dict_players.keys())         # Limitamos también jugadores, equipos y estadios
        teams_ids = list(dict_teams.keys())[:matches_to_proc] if matches_to_proc is not None else list(dict_teams.keys())
        venues_ids = list(dict_venues.keys())[:matches_to_proc] if matches_to_proc is not None else list(dict_venues.keys())
    
        for player in players_ids:
            obtain_information(type='player', id=player, out_season_path=out_season_path)
            if scrape_images:
                image_downloader(type='player', id=player, out_path=out_season_path)
        for team in teams_ids:
            obtain_information(type='team', id=team, out_season_path=out_season_path)
            if scrape_images:
                image_downloader(type='team', id=team, out_path=out_season_path)
        for venue in venues_ids:        # No hace falta extraer información de los estadios porque ya la tenemos
            if scrape_images:
                image_downloader(type='venue', id=venue, out_path=out_season_path)

        match_ids = list(dict_matches.keys())           # Limitamos si queremos scrapear menos información
        if matches_to_proc is not None:
            match_ids = match_ids[:matches_to_proc]

        total_matches = len(match_ids)
        i = 1

        for match_id in match_ids:
            if print_info:
                print(f'          - Scraping information for match {match_id} ({i}/{total_matches})')
            i += 1

            match_info = match_scraping(matches_dict=dict_matches, match_id=match_id, out_path=out_season_path)     # Scraping de información
            
            home_manager = {match_info.get('match', {}).get('event', {}).get('homeTeam', {}).get('manager', {}).get('id', 0): match_info.get('match', {}).get('event', {}).get('homeTeam', {}).get('manager', {}).get('slug', '')}
            away_manager = {match_info.get('match', {}).get('event', {}).get('awayTeam', {}).get('manager', {}).get('id', 0): match_info.get('match', {}).get('event', {}).get('awayTeam', {}).get('manager', {}).get('slug', '')}
            managers_ids = home_manager | away_manager          # Diccionario con informaicón de los managers
            for manager in managers_ids.keys():
                obtain_information(type='manager', id=manager, out_season_path=out_season_path)
                if scrape_images:
                    image_downloader(type='manager', id=manager, out_path=out_season_path)

    if print_info:
        print(f'Finished Sofascore scraping ({league_name}) in {elapsed_time_str(start_time=start_time)}')