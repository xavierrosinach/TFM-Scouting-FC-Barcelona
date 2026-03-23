import os
import json as jsonlib
import subprocess
import time
from contextlib import contextmanager

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from use.config import COMPS, DES_SEASONS, ACT_SEASON, DATA_PATH
from use.functions import json_to_dict, safe_json_dump, create_slug, need_to_upload, elapsed_time_str

# --------------------------------------------------------------------------------------
# CONFIGURACIÓN GLOBAL DE CHROME / SELENIUM
# --------------------------------------------------------------------------------------
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

os.environ["CHROME_LOG_FILE"] = os.devnull

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

service = Service(log_path=os.devnull)

with suppress_stderr():
    driver = webdriver.Chrome(service=service, options=options)
driver.get("https://www.google.com")

# --------------------------------------------------------------------------------------
# SCRAPER BASE DE PÁGINAS JSON DE SOFASCORE - Accede a una URL con Selenium y devuelve el contenido JSON renderizado.
# --------------------------------------------------------------------------------------
def ss_page_scraper(url: str, sleep_time: int = 3, timeout: int = 10) -> dict:
   
    driver.get(url)
    pre = WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.TAG_NAME, "pre")))

    data_json = jsonlib.loads(pre.text)
    time.sleep(sleep_time)

    return data_json

# --------------------------------------------------------------------------------------
# TEMPORADAS DISPONIBLES - Obtiene las temporadas disponibles de una liga en Sofascore.
# --------------------------------------------------------------------------------------
def ss_league_available_seasons(league_ss_code: int) -> dict:
    
    url = f"https://api.sofascore.com/api/v1/unique-tournament/{league_ss_code}/seasons/"
    available_seasons_json = ss_page_scraper(url=url)

    if available_seasons_json.get("seasons"):
        return available_seasons_json

    return {}

# --------------------------------------------------------------------------------------
# PARTIDOS DE UNA TEMPORADA - Obtiene los partidos de una temporada de Sofascore, almacenados por bloques.
# --------------------------------------------------------------------------------------
def ss_season_matches_scraper(season_key: str, ss_season_code: int, ss_league_code: int, season_slug: str, raw_matches_path: str) -> dict:

    os.makedirs(raw_matches_path, exist_ok=True)        # Si no existe la carpeta de output la creamos
    is_current_season = season_key == ACT_SEASON        # Booleano para saber si estamos tratando la temporada actual o una pasada

    block_idx = 0
    blocks_dict = {}
    while True:                                         # Mientras podamos scrapear, lo hacemos - bucle ya que no se sabe el total de paginas que vamos a encontrar
        out_path = os.path.join(raw_matches_path, f'{season_slug}_{block_idx}.json')        # Fichero JSON de output

        # Caso de que existe
        if os.path.exists(out_path):
            with open(out_path, "r", encoding="utf-8") as f:
                partial_matches_dict = jsonlib.load(f)
            blocks_dict[block_idx] = partial_matches_dict
            block_idx += 1
            continue
        else:
            url = (f"https://api.sofascore.com/api/v1/unique-tournament/{ss_league_code}/season/{ss_season_code}/events/last/{block_idx}")
            partial_matches_dict = ss_page_scraper(url=url)              # Scrapeo

            if not partial_matches_dict.get("events", []):               # En caso de que no tenga datos, hacemos break del bucle
                break
            else:                                                        # En caso de que sí, guardamos
                safe_json_dump(data=partial_matches_dict, path=out_path)
                blocks_dict[block_idx] = partial_matches_dict
                block_idx + 1
    
    # Una vez terminado el bucle, si nuestra temporada es la actual, comprovaremos el índice pasado de partidos, por si se han añadido
    if is_current_season:
        out_path = os.path.join(raw_matches_path, f'{season_slug}_{block_idx - 1}.json')
        url = (f"https://api.sofascore.com/api/v1/unique-tournament/{ss_league_code}/season/{ss_season_code}/events/last/{block_idx - 1}")
        partial_matches_dict = ss_page_scraper(url=url)
        safe_json_dump(data=partial_matches_dict, path=out_path)
        blocks_dict[block_idx] = partial_matches_dict

    return blocks_dict

# --------------------------------------------------------------------------------------
# INFORMACIÓN DE JUGADORES, EQUIPOS Y ESTADIOS - Obtiene información general de jugadores, equipos y estadios de una temporada.
# --------------------------------------------------------------------------------------
def ss_season_info_scraper(season_key: str, ss_season_code: int, ss_league_code: int, season_slug: str, raw_info_path: str) -> dict:

    os.makedirs(raw_info_path, exist_ok=True)
    is_current_season = season_key == ACT_SEASON
    info_dict = {}

    # Diccionario con los URLs a scrapear
    info_urls = {"player": f"https://api.sofascore.com/api/v1/unique-tournament/{ss_league_code}/season/{ss_season_code}/players",
                 "team": f"https://api.sofascore.com/api/v1/unique-tournament/{ss_league_code}/season/{ss_season_code}/teams",
                 "venue": f"https://api.sofascore.com/api/v1/unique-tournament/{ss_league_code}/season/{ss_season_code}/venues"}
    
    # Para cada link, comprovamos que no existe y lo scrapeamos
    for info_key, url in info_urls.items():
        json_path = os.path.join(raw_info_path, f"{season_slug}_{info_key}.json")

        # Comprovamos si existe
        if os.path.exists(json_path):
            if is_current_season:
                if not need_to_upload(path=json_path, total_days=10):           # Si es la temporada actual, comprovamos que no se tenga que actualizar
                    with open(json_path, "r", encoding="utf-8") as f:
                        info_dict[info_key] = jsonlib.load(f)
                else:
                    info_scraped = ss_page_scraper(url=url)                     # En caso que se tenga que actualizar, scrapeamos
                    safe_json_dump(data=info_scraped, path=json_path)
                    info_dict[info_key] = info_scraped
            else:
                with open(json_path, "r", encoding="utf-8") as f:
                        info_dict[info_key] = jsonlib.load(f)
                        
        else:
            info_scraped = ss_page_scraper(url=url)
            safe_json_dump(data=info_scraped, path=json_path)
            info_dict[info_key] = info_scraped

    return info_dict

# --------------------------------------------------------------------------------------
# INFORMACIÓN INDIVIDUAL DE ENTIDADES - Obtiene información individual de player, team, o manager
# --------------------------------------------------------------------------------------
def ss_obtain_information(type: str, id: int, out_path: str) -> dict:

    os.makedirs(out_path, exist_ok=True)                                # Comprovamos que no existe
    info_url = f"https://api.sofascore.com/api/v1/{type}/{id}"          # URL

    out_json = os.path.join(out_path, f"{id}.json")

    # Cada 30 dias actualizamos la información
    if os.path.exists(out_json) and not need_to_upload(path=out_json, total_days=30):
        with open(out_json, "r", encoding="utf-8") as f:
            return jsonlib.load(f)
    
    else:
        info_json = ss_page_scraper(url=info_url)               # Scraping de la información
        if info_json.get(type):                                 # Si hay información, lo guardamos
            safe_json_dump(data=info_json, path=out_json)

# --------------------------------------------------------------------------------------
# DESCARGA DE IMÁGENES MEDIANTE SCRIPT EN R - Descarga la imagen de una entidad de Sofascore usando un script de R.
# --------------------------------------------------------------------------------------
def ss_image_downloader(type: str, id: int, out_path: str, sleep_time: int = 3, rscript_path: str = r"C:\Program Files\R\R-4.4.1\bin\x64\Rscript.exe", r_script: str = "use/picture_scraper.R",) -> None:

    os.makedirs(out_path, exist_ok=True)                                    # Creación de la carpeta de salida
    image_url = f"https://img.sofascore.com/api/v1/{type}/{id}/image"       # URL de la imagen
    out_file = os.path.join(out_path, f"{id}.png")                          # Output de la imagen en formato PNF

    # Si no existe o se necesita actualizar (rango de 90 días) ejecutamos el codigo R
    if (not os.path.exists(out_file) or need_to_upload(path=out_file, total_days=90)):
        cmd = [rscript_path, r_script, image_url, out_file]
        subprocess.run(cmd, text=True, capture_output=True)
        time.sleep(sleep_time)

# --------------------------------------------------------------------------------------
# SCRAPING DE UN PARTIDO - Obtiene la información completa de un partido.
# --------------------------------------------------------------------------------------
def ss_match_scraping(match_id: int, out_path: str) -> dict:

    os.makedirs(out_path, exist_ok=True)        # Creamos la carpeta si no existe

    # Obtenemos el URL de distintos ámbitos
    dict_urls = {"info": f"https://api.sofascore.com/api/v1/event/{match_id}",
                 "lineups": f"https://api.sofascore.com/api/v1/event/{match_id}/lineups",
                 "stats": f"https://api.sofascore.com/api/v1/event/{match_id}/statistics"}
    
    # Claves a comprovar para guardar JSONS
    dict_info_to_check = {"info": "event",
                          "lineups": "confirmed",
                          "stats": "statistics"}
    
    out_dict = {}
    
    # Para cada tipo de información
    for type_info in dict_urls.keys():

        json_path = os.path.join(out_path, f'{match_id}_{type_info}.json')
        if os.path.exists(json_path):
            with open(json_path, "r", encoding="utf-8") as f:
                out_dict[type_info] = jsonlib.load(f)
        
        else:
            url = dict_urls[type_info]          # URL
            info_json = ss_page_scraper(url)    # Scraping

            # Comprovamos que tiene información a dentro usando el diccionario creado
            if info_json.get(dict_info_to_check[type_info]):
               out_dict[type_info] = info_json
               safe_json_dump(data=info_json, path=json_path)

    return out_dict

# --------------------------------------------------------------------------------------
# SCRAPING PRINCIPAL DE LIGA EN SOFASCORE - Ejecuta el scraping completo de una liga en Sofascore.
# --------------------------------------------------------------------------------------
def ss_main_league_scraping(league_id: id, scrape_images: bool = True, print_info: bool = True) -> None: 

    start_time = time.time()

    # Definición del slug de liga - para crear carpetas
    league_str = COMPS[COMPS["id"] == league_id]["tournament"].iloc[0]
    league_slug = create_slug(text=league_str)

    if print_info:
        print(f"Starting the Sofascore scraping process of {league_str}.")

    # Definición de carpetas de output y creación
    raw_data_path = os.path.join(DATA_PATH, "raw")
    info_raw_data_path = os.path.join(raw_data_path, "info")
    matches_raw_data_path = os.path.join(raw_data_path, "matches")
    images_raw_data_path = os.path.join(raw_data_path, "images")
    available_seasons_raw_data_path = os.path.join(raw_data_path, "available_seasons")

    # Creamos todas las carpetas
    for path in [raw_data_path, info_raw_data_path, matches_raw_data_path, images_raw_data_path, available_seasons_raw_data_path]:
        os.makedirs(path, exist_ok=True)

    league_code = int(COMPS[COMPS["tournament"] == league_str]["sofascore"].iloc[0])                    # ID de la liga en Sofascore

    # Obtención del JSON de temporadas disponibles
    av_seasons_path = os.path.join(available_seasons_raw_data_path, f'{league_slug}.json')              # Temporadas disponibles de la liga

    # Comprovamos que el path existe se necesita actualizar
    if (os.path.exists(av_seasons_path) or not need_to_upload(path=av_seasons_path, total_days=30)):
        av_seasons_dict = json_to_dict(json_path=av_seasons_path)
    else:
        av_seasons_dict = ss_league_available_seasons(league_ss_code=league_code)
        safe_json_dump(data=av_seasons_dict, path=av_seasons_path)
    
    # Diccionario con las temporadas y el ID de Sofascore - Solo tratamos aquellas temporadas deseadas
    seasons_dict = {season_data["year"].replace("/", ""): season_data["id"] 
                    for season_data in av_seasons_dict.get("seasons", []) 
                    if season_data.get("year", "").replace("/", "") in DES_SEASONS}

    season_counter = 1
    total_seasons = len(DES_SEASONS)

    # Para cada temporada, extracción de datos
    for season_key in seasons_dict.keys():

        if print_info:
            print(f"     [{season_counter}/{total_seasons}] Starting the scraping of the season {season_key} of {league_str}.")
            season_counter += 1

        season_slug = f'{league_slug}_{season_key}'     # Slug de la temporada a partir del de la liga
        ss_season_code = seasons_dict[season_key]       # Codigo de la temporada en Sofascore

        if print_info:
            print(f"          1. Season matches scraping of {league_str}.")

        # Obtenemos una lista de diccionarios con partidos que se van a procesar
        season_matches_dict = ss_season_matches_scraper(season_key=season_key, ss_season_code=ss_season_code, ss_league_code=league_code,
                                                        season_slug=season_slug, raw_matches_path=os.path.join(info_raw_data_path, "ss_matches"))
        
        # Diccionario con todos los partidos jugados
        dict_matches = {match["id"]: match["slug"] for events in season_matches_dict.values()
                        for match in events.get("events", []) if match.get("status", {}).get("description") == "Ended"}
        
        if print_info:
            print(f"          2. Season information scraping of {league_str}.")
        
        # Obtenemos el diccionario con información de jugadores, equipos, y estadios
        season_information_dict = ss_season_info_scraper(season_key=season_key, ss_season_code=ss_season_code, ss_league_code=league_code,
                                                        season_slug=season_slug, raw_info_path=os.path.join(info_raw_data_path, "ss_info"))
        
        # Diccionario con todos los jugadores, equipos y estadios
        dict_players = {player["playerId"]: player["playerName"].lower().replace(" ", "-") 
                        for player in season_information_dict.get("player", {}).get("players", [])}
        dict_teams = {team["id"]: team["slug"] 
                    for team in season_information_dict.get("team", {}).get("teams", [])}
        dict_venues = {venue["id"]: venue["slug"] 
                    for venue in season_information_dict.get("venue", {}).get("venues", [])}
        
        if print_info:
            print(f"          3. Matches data scraping of {league_str}.")
        
        manager_ids = []        # Lista con los entrenadores de los equipos para posteriormente extraer información

        match_counter = 1
        total_matches = len(dict_matches.keys())

        # Para cada partido, obtenemos su información
        for match_id in dict_matches.keys():

            if print_info:
                print(f"               [{match_counter}/{total_matches}] Scraping of match {match_id} of {league_str}.")
                match_counter += 1

            match_info = ss_match_scraping(match_id=match_id, out_path=os.path.join(matches_raw_data_path, season_slug))

            # Obtenemos managers y los añadimos a la lista
            home_manager_id = (match_info.get("info", {}).get("event", {}).get("homeTeam", {}).get("manager", {}).get("id"))
            away_manager_id = (match_info.get("info", {}).get("event", {}).get("awayTeam", {}).get("manager", {}).get("id"))
            manager_ids.extend([manager_id for manager_id in [home_manager_id, away_manager_id] if manager_id is not None])
        
        if print_info:
            print(f"          4. Players information scraping of {league_str}.")
        
        player_counter = 1
        total_players = len(dict_players.keys())
        
        # Para cada jugador, obtenemos información
        for player_id in dict_players.keys():
            
            if print_info:
                print(f"               [{player_counter}/{total_players}] Scraping of player {player_id} of {league_str}.")
                player_counter += 1

            ss_obtain_information(type="player", id=player_id, out_path=os.path.join(info_raw_data_path, "ss_players"))
            if scrape_images:
                ss_image_downloader(type="player", id=player_id, out_path=os.path.join(images_raw_data_path, "players"))

        if print_info:
            print(f"          5. Teams information scraping of {league_str}.")

        team_counter = 1
        total_teams = len(dict_teams.keys())
        
        # Obtenemos información para cada equipo - y imagen
        for team_id in dict_teams.keys():
            
            if print_info:
                print(f"               [{team_counter}/{total_teams}] Scraping of team {team_id} of {league_str}.")
                team_counter += 1

            ss_obtain_information(type="team", id=team_id, out_path=os.path.join(info_raw_data_path, "ss_teams"))
            if scrape_images:
                ss_image_downloader(type="team", id=team_id, out_path=os.path.join(images_raw_data_path, "teams"))

        if print_info:
            print(f"          6. Managers information scraping of {league_str}.")

        manager_counter = 1
        total_managers = len(set(manager_ids))

        # Para cada entrenador - usamos set para evitar duplicados
        for manager_id in sorted(set(manager_ids)):

            if print_info:
                print(f"               [{manager_counter}/{total_managers}] Scraping of manager {manager_id} of {league_str}.")
                manager_counter += 1

            ss_obtain_information(type="manager", id=manager_id, out_path=os.path.join(info_raw_data_path, "ss_managers"))
            if scrape_images:
                ss_image_downloader(type="manager", id=team_id, out_path=os.path.join(images_raw_data_path, "managers"))

        if print_info:
            print(f"          7. Venues information scraping of {league_str}.")
        
        venues_counter = 1
        total_venues = len(dict_venues.keys())

        # Para cada estadio, obtenemos su imagen
        if scrape_images:
            for venue_id in dict_venues.keys():
                ss_image_downloader(type="venue", id=venue_id, out_path=os.path.join(images_raw_data_path, "venues"))   

            if print_info:
                print(f"               [{venues_counter}/{total_venues}] Scraping of venue {venue_id} of {league_str}.")
                venues_counter += 1     

    if print_info:
        print(f"Finished the Sofascore scraping process of {league_str} in {elapsed_time_str(start_time=start_time)}.")