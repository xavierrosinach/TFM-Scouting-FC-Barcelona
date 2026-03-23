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

from use.config import comps, desired_seasons, act_season
from use.functions import safe_json_dump, create_slug, need_to_upload, elapsed_time_str

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
def page_scraper(url: str, sleep_time: int = 3, timeout: int = 10) -> dict:
   
    driver.get(url)
    pre = WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.TAG_NAME, "pre")))

    data_json = jsonlib.loads(pre.text)
    time.sleep(sleep_time)

    return data_json

# --------------------------------------------------------------------------------------
# TEMPORADAS DISPONIBLES - Obtiene las temporadas disponibles de una liga en Sofascore.
# --------------------------------------------------------------------------------------
def league_available_seasons(league_code: int, out_path: str) -> dict:
    
    json_path = os.path.join(out_path, "available_seasons.json")

    if os.path.exists(json_path) and not need_to_upload(json_path, total_days=200):
        with open(json_path, "r", encoding="utf-8") as f:
            return jsonlib.load(f)

    url = f"https://api.sofascore.com/api/v1/unique-tournament/{league_code}/seasons/"
    available_seasons_json = page_scraper(url=url)

    if available_seasons_json.get("seasons"):
        safe_json_dump(data=available_seasons_json, path=json_path)
        return available_seasons_json

    return {}

# --------------------------------------------------------------------------------------
# DATOS DE TEMPORADA: PARTIDOS - Obtiene los partidos de una temporada de Sofascore, almacenados por bloques.
# --------------------------------------------------------------------------------------
def season_data_scraper(seasons_dict: dict, season_key: str, league_code: int, out_path: str) -> dict:
    
    if season_key not in seasons_dict:
        return {}

    season_id = seasons_dict[season_key]
    out_matches_path = os.path.join(out_path, "info", "matches")
    os.makedirs(out_matches_path, exist_ok=True)

    is_current_season = season_key == act_season
    blocks_dict = {}
    block_idx = 0

    while True:
        json_path = os.path.join(out_matches_path, f"{block_idx}.json")

        if os.path.exists(json_path):
            if not is_current_season or not need_to_upload(json_path):
                with open(json_path, "r", encoding="utf-8") as f:
                    season_json = jsonlib.load(f)
                blocks_dict[block_idx] = season_json
                block_idx += 1
                continue

        matches_url = (f"https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{season_id}/events/last/{block_idx}")
        matches_json = page_scraper(url=matches_url)

        if not matches_json.get("events", []):
            break

        safe_json_dump(data=matches_json, path=json_path)
        blocks_dict[block_idx] = matches_json
        block_idx += 1

    return blocks_dict

# --------------------------------------------------------------------------------------
# TABLAS DE CLASIFICACIÓN - Obtiene las clasificaciones total/home/away de una temporada.
# --------------------------------------------------------------------------------------
def season_standings(seasons_dict: dict, season_key: str, league_code: int, out_path: str) -> dict:
    
    if season_key not in seasons_dict:
        return {}

    out_info_path = os.path.join(out_path, "info")
    os.makedirs(out_info_path, exist_ok=True)

    standings_path = os.path.join(out_info_path, "standings.json")
    is_current_season = season_key == act_season

    if os.path.exists(standings_path):
        if not is_current_season or not need_to_upload(path=standings_path):
            with open(standings_path, "r", encoding="utf-8") as f:
                return jsonlib.load(f)

    season_id = seasons_dict[season_key]
    urls = {"total": f"https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{season_id}/standings/total",
            "home": f"https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{season_id}/standings/home",
            "away": f"https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{season_id}/standings/away"}

    standings = {}
    for key, url_to_scrape in urls.items():
        standings_json = page_scraper(url=url_to_scrape)
        standings[key] = standings_json if standings_json.get("standings") else {}

    safe_json_dump(data=standings, path=standings_path)
    return standings

# --------------------------------------------------------------------------------------
# INFORMACIÓN DE JUGADORES, EQUIPOS Y ESTADIOS - Obtiene información general de jugadores, equipos y estadios de una temporada.
# --------------------------------------------------------------------------------------
def season_information(seasons_dict: dict, season_key: str, league_code: int, out_path: str) -> dict:
    
    if season_key not in seasons_dict:
        return {}

    out_info_path = os.path.join(out_path, "info")
    os.makedirs(out_info_path, exist_ok=True)

    season_id = seasons_dict[season_key]
    info_urls = {"player": f"https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{season_id}/players",
                 "team": f"https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{season_id}/teams",
                 "venue": f"https://api.sofascore.com/api/v1/unique-tournament/{league_code}/season/{season_id}/venues"}

    is_current_season = season_key == act_season
    info_dict = {}

    for info_key, url in info_urls.items():
        info_path = os.path.join(out_info_path, f"{info_key}.json")

        if os.path.exists(info_path):
            if not is_current_season or not need_to_upload(path=info_path):
                with open(info_path, "r", encoding="utf-8") as f:
                    info_dict[info_key] = jsonlib.load(f)
                continue

        info_scraped = page_scraper(url=url)
        safe_json_dump(data=info_scraped, path=info_path)
        info_dict[info_key] = info_scraped

    return info_dict

# --------------------------------------------------------------------------------------
# DESCARGA DE IMÁGENES MEDIANTE SCRIPT EN R - Descarga la imagen de una entidad de Sofascore usando un script de R.
# --------------------------------------------------------------------------------------
def image_downloader(type: str, id: int, out_path: str, sleep_time: int = 3, rscript_path: str = r"C:\Program Files\R\R-4.4.1\bin\x64\Rscript.exe", r_script: str = "ss_pict.R",) -> None:

    out_images_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(out_path))),
        "images"
    )
    os.makedirs(out_images_path, exist_ok=True)

    out_type_path = os.path.join(out_images_path, type)
    os.makedirs(out_type_path, exist_ok=True)

    image_url = f"https://img.sofascore.com/api/v1/{type}/{id}/image"
    out_file = os.path.join(out_type_path, f"{id}.png")

    must_download = (not os.path.exists(out_file) or need_to_upload(path=out_file, total_days=90))

    if must_download:
        cmd = [rscript_path, r_script, image_url, out_file]
        subprocess.run(cmd, text=True, capture_output=True)
        time.sleep(sleep_time)

# --------------------------------------------------------------------------------------
# INFORMACIÓN INDIVIDUAL DE ENTIDADES - Obtiene información individual de player, team, manager o venue.
# --------------------------------------------------------------------------------------
def obtain_information(type: str, id: int, out_season_path: str) -> dict:

    info_url = f"https://api.sofascore.com/api/v1/{type}/{id}"
    out_dir = os.path.join(out_season_path, "info", type)
    os.makedirs(out_dir, exist_ok=True)

    out_json = os.path.join(out_dir, f"{id}.json")

    if os.path.exists(out_json) and not need_to_upload(path=out_json, total_days=30):
        with open(out_json, "r", encoding="utf-8") as f:
            return jsonlib.load(f)

    info_json = page_scraper(url=info_url)

    if info_json.get(type):
        safe_json_dump(data=info_json, path=out_json)

    return info_json

# --------------------------------------------------------------------------------------
# SCRAPING DE UN PARTIDO - Obtiene la información completa de un partido.
# --------------------------------------------------------------------------------------
def match_scraping(matches_dict: dict, match_id: int, out_path: str) -> dict:
    
    if match_id not in matches_dict:
        return {}

    out_matches_path = os.path.join(out_path, "matches")
    os.makedirs(out_matches_path, exist_ok=True)

    final_path = os.path.join(out_matches_path, f"{match_id}.json")

    if os.path.exists(final_path):
        with open(final_path, "r", encoding="utf-8") as f:
            return jsonlib.load(f)
  
    match_info_url = f"https://api.sofascore.com/api/v1/event/{match_id}"
    match_lineups_url = f"https://api.sofascore.com/api/v1/event/{match_id}/lineups"
    match_stats_url = f"https://api.sofascore.com/api/v1/event/{match_id}/statistics"

    match_info_json = page_scraper(url=match_info_url)
    match_lineups_json = page_scraper(url=match_lineups_url)
    match_stats_json = page_scraper(url=match_stats_url)

    if (match_info_json.get("event") and match_lineups_json.get("confirmed") and match_stats_json.get("statistics")):
        full_match_info = {"match": match_info_json, "lineups": match_lineups_json, "statistics": match_stats_json}
        safe_json_dump(data=full_match_info, path=final_path)
        return full_match_info

    return {}

# --------------------------------------------------------------------------------------
# SCRAPING PRINCIPAL DE LIGA EN SOFASCORE - Ejecuta el scraping completo de una liga en Sofascore.
# --------------------------------------------------------------------------------------
def main_sofascore_league_scraping(league_id: int, out_path: str, scrape_images: bool = True, matches_to_proc: int = None, print_info: bool = True) -> None:
    
    start_time = time.time()

    comp_row = comps.loc[comps["id"] == league_id]
    if comp_row.empty:
        raise ValueError(f"No existe ninguna liga con id={league_id} en comps.csv.")

    league_name = comp_row["tournament"].iloc[0]
    league_slug = create_slug(text=league_name)
    ss_code = int(comp_row["sofascore"].iloc[0])

    if print_info:
        print(f"Starting Sofascore scraping ({league_name})")

    out_league_path = os.path.join(out_path, "sofascore", league_slug)
    os.makedirs(out_league_path, exist_ok=True)

    available_seasons = league_available_seasons(league_code=ss_code, out_path=out_league_path)
    seasons_dict = {season_data["year"].replace("/", ""): season_data["id"] for season_data in available_seasons.get("seasons", []) if season_data.get("year", "").replace("/", "") in desired_seasons}

    for season_key in seasons_dict:
        if print_info:
            print(f"     - Scraping information for season {season_key}")

        out_season_path = os.path.join(out_league_path, season_key)
        os.makedirs(out_season_path, exist_ok=True)

        season_data_dict = season_data_scraper(seasons_dict=seasons_dict, season_key=season_key, league_code=ss_code, out_path=out_season_path)
        print("season data")
        season_standings(seasons_dict=seasons_dict, season_key=season_key, league_code=ss_code, out_path=out_season_path)
        print("season standings")
        season_info = season_information(seasons_dict=seasons_dict, season_key=season_key, league_code=ss_code, out_path=out_season_path)
        print("season information")

        dict_matches = {match["id"]: match["slug"] for events in season_data_dict.values() for match in events.get("events", []) if match.get("status", {}).get("description") == "Ended"}
        dict_players = {player["playerId"]: player["playerName"].lower().replace(" ", "-") for player in season_info.get("player", {}).get("players", [])}
        dict_teams = {team["id"]: team["slug"] for team in season_info.get("team", {}).get("teams", [])}
        dict_venues = {venue["id"]: venue["slug"] for venue in season_info.get("venue", {}).get("venues", [])}

        player_ids = list(dict_players.keys())
        team_ids = list(dict_teams.keys())
        venue_ids = list(dict_venues.keys())
        match_ids = list(dict_matches.keys())

        if matches_to_proc is not None:
            player_ids = player_ids[:matches_to_proc]
            team_ids = team_ids[:matches_to_proc]
            venue_ids = venue_ids[:matches_to_proc]
            match_ids = match_ids[:matches_to_proc]

        for player_id in player_ids:
            obtain_information(type="player", id=player_id, out_season_path=out_season_path)
            if scrape_images:
                image_downloader(type="player", id=player_id, out_path=out_season_path)

        for team_id in team_ids:
            obtain_information(type="team", id=team_id, out_season_path=out_season_path)
            if scrape_images:
                image_downloader(type="team", id=team_id, out_path=out_season_path)

        for venue_id in venue_ids:
            if scrape_images:
                image_downloader(type="venue", id=venue_id, out_path=out_season_path)

        total_matches = len(match_ids)
        for idx, match_id in enumerate(match_ids, start=1):
            if print_info:
                print(f"          - Scraping information for match {match_id} ({idx}/{total_matches})")

            match_info = match_scraping(matches_dict=dict_matches, match_id=match_id, out_path=out_season_path)

            home_manager_id = (match_info.get("match", {}).get("event", {}).get("homeTeam", {}).get("manager", {}).get("id"))
            away_manager_id = (match_info.get("match", {}).get("event", {}).get("awayTeam", {}).get("manager", {}).get("id"))
            manager_ids = [manager_id for manager_id in [home_manager_id, away_manager_id] if manager_id is not None]

            for manager_id in manager_ids:
                obtain_information(type="manager", id=manager_id, out_season_path=out_season_path)
                if scrape_images:
                    image_downloader(type="manager", id=manager_id, out_path=out_season_path)

    if print_info:
        print(f"Finished Sofascore scraping ({league_name}) in {elapsed_time_str(start_time=start_time)}")
