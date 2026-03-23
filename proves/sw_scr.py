import requests
import re
import os
import json as jsonlib
import time
from datetime import datetime, timedelta

from use.config import COMPS, DES_SEASONS, ACT_SEASON, DATA_PATH
from use.functions import safe_json_dump, create_slug, need_to_upload, elapsed_time_str

# --------------------------------------------------------------------------------------
# SCRAPING DE DATOS - Descarga datos del URL de Scoresway en formato JSON.
# --------------------------------------------------------------------------------------
def sw_scrape_json(url: str, referer: str = 'https://www.scoresway.com/', sleep_time: int = 3) -> dict:

    headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
               "referer": referer, "accept": "*/*", "accept-language": "en-US,en;q=0.9", "connection": "keep-alive"}

    with requests.Session() as s:
        try:
            s.get(referer, headers=headers, timeout=20)
        except requests.RequestException:
            pass

        r = s.get(url, headers=headers, timeout=30)
        if r.status_code != 200:
            return {}

        text = r.text.strip()
        match = re.match(r"^[\w$]+\((.*)\)\s*;?\s*$", text, flags=re.DOTALL)
        if not match:
            return {}

        json_str = match.group(1)
        time.sleep(sleep_time)
        return jsonlib.loads(json_str)

# --------------------------------------------------------------------------------------
# PARTIDOS - Obtiene los partidos de una temporada.
# --------------------------------------------------------------------------------------
def sw_season_matches(league_id: str, season_key: str, season_slug: str, out_path: str) -> dict:

    os.makedirs(out_path, exist_ok=True)                                # Creación del path si no existe
    json_path = os.path.join(out_path, f'{season_slug}_matches.json')   # Path del json

    # Miramos si la temporada es actual o no
    if season_key != ACT_SEASON:
        if os.path.exists(json_path):
            with open(json_path, "r", encoding="utf-8") as f:
                matches_json = jsonlib.load(f)
            return matches_json
        
        else:
            matches_url = f'https://api.performfeeds.com/soccerdata/match/ft1tiv1inq7v1sk3y9tv12yh5/?_rt=c&tmcl={league_id}&live=yes&_pgSz=400&_lcl=en&_fmt=jsonp&sps=widgets&_clbk=cb'
            matches_json = sw_scrape_json(url=matches_url)
            if matches_json.get('match'):
                safe_json_dump(data=matches_json, path=json_path)
                return matches_json

    else:
        if os.path.exists(json_path) and not need_to_upload(json_path):         # Miramos si se debe actualizar en caso de que sea la temporada actual y exista
            with open(json_path, "r", encoding="utf-8") as f:
                matches_json = jsonlib.load(f)
            return matches_json
        
        else:
            matches_url = f'https://api.performfeeds.com/soccerdata/match/ft1tiv1inq7v1sk3y9tv12yh5/?_rt=c&tmcl={league_id}&live=yes&_pgSz=400&_lcl=en&_fmt=jsonp&sps=widgets&_clbk=cb'
            matches_json = sw_scrape_json(url=matches_url)
            if matches_json.get('match'):
                safe_json_dump(data=matches_json, path=json_path)
                return matches_json

    return {}

# --------------------------------------------------------------------------------------
# PLANTILLAS - Obtiene las plantillas de una liga en una temporada.
# --------------------------------------------------------------------------------------
def sw_season_squads(league_id: str, season_key: str, season_slug: str, out_path: str) -> dict:

    os.makedirs(out_path, exist_ok=True)                                # Creación del path si no existe
    json_path = os.path.join(out_path, f'{season_slug}_squads.json')   # Path del json

    # Miramos si la temporada es actual o no
    if season_key != ACT_SEASON:
        if os.path.exists(json_path):
            with open(json_path, "r", encoding="utf-8") as f:
                squads_json = jsonlib.load(f)
            return squads_json
        
        else:
            squads_url = f'https://api.performfeeds.com/soccerdata/squads/ft1tiv1inq7v1sk3y9tv12yh5/?_rt=c&tmcl={league_id}&_pgSz=200&_lcl=en&_fmt=jsonp&sps=widgets&_clbk=cb'
            squads_json = sw_scrape_json(url=squads_url)
            if squads_json.get('match'):
                safe_json_dump(data=squads_json, path=json_path)
                return squads_json

    else:
        if os.path.exists(json_path) and not need_to_upload(json_path):         # Miramos si se debe actualizar en caso de que sea la temporada actual y exista
            with open(json_path, "r", encoding="utf-8") as f:
                squads_json = jsonlib.load(f)
            return squads_json
        
        else:
            squads_url = f'https://api.performfeeds.com/soccerdata/squads/ft1tiv1inq7v1sk3y9tv12yh5/?_rt=c&tmcl={league_id}&_pgSz=200&_lcl=en&_fmt=jsonp&sps=widgets&_clbk=cb'
            squads_json = sw_scrape_json(url=squads_url)
            if squads_json.get('squad'):
                safe_json_dump(data=squads_json, path=json_path)
                return squads_json

    return {}

# --------------------------------------------------------------------------------------
# ESTADÍSTICAS DE UN PARTIDO - Obtiene las estadísticas de un partido
# --------------------------------------------------------------------------------------
def sw_match_stats(match_id: str, out_path: str) -> None:

    os.makedirs(out_path, exist_ok=True)
    json_path = os.path.join(out_path, f'{match_id}.json')

    if os.path.exists(json_path):       # Si existe, continuamos
        return
    else:
        stats_url = f'https://api.performfeeds.com/soccerdata/matchstats/ft1tiv1inq7v1sk3y9tv12yh5/{match_id}?_rt=c&_lcl=en&_fmt=jsonp&sps=widgets&_clbk=cb'
        stats_json = sw_scrape_json(url=stats_url)
        safe_json_dump(data=stats_json, path=json_path)

# --------------------------------------------------------------------------------------
# SCRAPING PRINCIPAL DE LIGA EN SCORESWAY - Ejecuta el scraping completo de una liga en Scoresway.
# --------------------------------------------------------------------------------------
def sw_main_league_scraping(league_id: id, print_info: bool = True) -> None:

    start_time = time.time()

    # Definición del slug de liga - para crear carpetas
    league_str = COMPS[COMPS["id"] == league_id]["tournament"].iloc[0]
    league_slug = create_slug(text=league_str)

    if print_info:
        print(f"Starting the Scoresway scraping process of {league_str}.")

    # Definición de carpetas de output y creación
    raw_data_path = os.path.join(DATA_PATH, "raw")
    info_raw_data_path = os.path.join(raw_data_path, "info")
    matches_raw_data_path = os.path.join(raw_data_path, "matches")

    # Creamos todas las carpetas
    for path in [raw_data_path, info_raw_data_path, matches_raw_data_path]:
        os.makedirs(path, exist_ok=True)

    # Obtenemos los identificadores de la liga según temporada, y los añadimos a un diccionairo
    league_code_2425 = str(COMPS[COMPS["tournament"] == league_str]["scoresway2425"].iloc[0])
    league_code_2526 = str(COMPS[COMPS["tournament"] == league_str]["scoresway2526"].iloc[0])
    league_id_dict = {"2425": league_code_2425, "2526": league_code_2526}

    season_counter = 1
    total_seasons = len(league_id_dict.keys())

    # Para cada temporada en desired seasons:
    for season_key in DES_SEASONS:

        # Comprovamos que existe
        if season_key not in league_id_dict.keys():
            continue
        else:
            sw_season_code = league_id_dict[season_key]

        if print_info:
            print(f"     [{season_counter}/{total_seasons}] Starting the scraping of the season {season_key} of {league_str}.")
            season_counter += 1

        season_slug = f'{league_slug}_{season_key}'     # Slug de la temporada a partir del de la liga

        if print_info:
                print(f"          1. Season matches scraping of {league_str}.")

        # Scraping de los partidos (calendario) de la temporada
        season_matches = sw_season_matches(league_id=league_id_dict[season_key], season_key=season_key, season_slug=season_slug,
                                        out_path=os.path.join(info_raw_data_path, 'sw_info'))
        
        if print_info:
                print(f"          2. Season squads scraping of {league_str}.")
        
        # Scraping de las plantillas
        season_squads = sw_season_squads(league_id=league_id_dict[season_key], season_key=season_key, season_slug=season_slug,
                                        out_path=os.path.join(info_raw_data_path, 'sw_info'))

        # Para los partidos de la temporada
        if season_matches:

            if print_info:
                print(f"          3. Season individual matches scraping of {league_str}.")

            played_matches = {m.get('matchInfo', {}).get('id'):f"{m.get('matchInfo', {}).get('contestant', [{}])[0].get('name','')}-{m.get('matchInfo', {}).get('contestant', [{},{}])[1].get('name','')}".lower().replace(' ', '-')
                            for m in season_matches.get('match', []) if m.get('liveData', {}).get('matchDetails', {}).get('matchStatus')=='Played'}
            
            match_counter = 1
            total_matches = len(played_matches.keys())

            for match_id in played_matches.keys():
                
                if print_info:
                    print(f"               [{match_counter}/{total_matches}] Scraping of match {match_id} of {league_str}.")
                    match_counter += 1

                sw_match_stats(match_id=match_id, out_path=os.path.join(matches_raw_data_path, season_slug))    # Scraping de información del partido

    if print_info:
        print(f"Finished the Scoresway scraping process of {league_str} in {elapsed_time_str(start_time=start_time)}.")