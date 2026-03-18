import os
import json as jsonlib
import time

from use.config import comps, desired_seasons, act_season
from use.functions import safe_json_dump, url_to_json, need_to_upload, create_slug


# --------------------------------------------------------------------------------------
# TEMPORADAS DISPONIBLES DE UNA LIGA
# --------------------------------------------------------------------------------------

def league_available_seasons(league_code: int, out_path: str) -> dict:
    """
    Obtiene las temporadas disponibles de una liga en Fotmob y las guarda en disco.

    Parameters
    ----------
    league_code : int
        Identificador de la liga en Fotmob.
    out_path : str
        Carpeta de salida de la liga.

    Returns
    -------
    dict
        JSON con la información de temporadas disponibles.
    """
    json_path = os.path.join(out_path, "available_seasons.json")

    if os.path.exists(json_path) and not need_to_upload(json_path, total_days=200):
        with open(json_path, "r", encoding="utf-8") as f:
            return jsonlib.load(f)

    fotmob_url = f"https://www.fotmob.com/api/leagues?id={league_code}"
    available_seasons_json = url_to_json(url=fotmob_url)
    available_seasons = available_seasons_json.get("allAvailableSeasons", [])

    seasons_dict = {}
    for season in available_seasons:
        if "/" in season:
            start, end = season.split("/")
            key_candidate = start[-2:] + end[-2:]
        else:
            season_year = season[:4]
            key_candidate = f"{int(season_year) % 100:02d}{(int(season_year) + 1) % 100:02d}"

        season_link = f"{fotmob_url}&season={season}"
        seasons_dict[season] = {
            "key": key_candidate,
            "link": season_link
        }

    available_seasons_json["allAvailableSeasons"] = seasons_dict

    if seasons_dict:
        safe_json_dump(data=available_seasons_json, path=json_path)

    return available_seasons_json


# --------------------------------------------------------------------------------------
# DATOS DE UNA TEMPORADA
# --------------------------------------------------------------------------------------

def season_data(seasons_dict: dict, season_key: str, out_path: str) -> dict:
    """
    Obtiene los datos de una temporada concreta de Fotmob.

    Parameters
    ----------
    seasons_dict : dict
        Diccionario con claves de temporada y URL asociada.
    season_key : str
        Clave de temporada a procesar.
    out_path : str
        Carpeta de salida de la liga.

    Returns
    -------
    dict
        JSON con la información de la temporada.
    """
    if season_key not in seasons_dict:
        return {}

    out_season_path = os.path.join(out_path, season_key)
    os.makedirs(out_season_path, exist_ok=True)

    json_path = os.path.join(out_season_path, "info.json")

    is_current_season = season_key == act_season
    if os.path.exists(json_path):
        if not is_current_season or not need_to_upload(json_path):
            with open(json_path, "r", encoding="utf-8") as f:
                return jsonlib.load(f)

    season_link = seasons_dict[season_key]
    season_json = url_to_json(season_link)

    if season_json.get("fixtures"):
        safe_json_dump(data=season_json, path=json_path)

    return season_json


# --------------------------------------------------------------------------------------
# SCRAPING PRINCIPAL DE UNA LIGA EN FOTMOB
# --------------------------------------------------------------------------------------

def main_fotmob_league_scraping(league_id: int, out_path: str, print_info: bool = True) -> None:
    """
    Ejecuta el scraping de Fotmob para una liga concreta.

    Parameters
    ----------
    league_id : int
        Identificador interno de la liga.
    out_path : str
        Carpeta raíz de salida de datos raw.
    print_info : bool, default=True
        Indica si se muestran mensajes de progreso.

    Returns
    -------
    None
    """
    start_time = time.time()

    comp_row = comps.loc[comps["id"] == league_id]
    if comp_row.empty:
        raise ValueError(f"No existe ninguna liga con id={league_id} en comps.csv.")

    fm_code = int(comp_row["fotmob"].iloc[0])
    league_name = comp_row["tournament"].iloc[0]
    league_slug = create_slug(text=league_name)

    if print_info:
        print("================================================================================")
        print(f"Starting Fotmob scraping ({league_name})")

    out_league_path = os.path.join(out_path, "fotmob", league_slug)
    os.makedirs(out_league_path, exist_ok=True)

    available_seasons = league_available_seasons(league_code=fm_code, out_path=out_league_path)
    seasons_dict = {
        season_data["key"]: season_data["link"]
        for season_data in available_seasons.get("allAvailableSeasons", {}).values()
        if season_data.get("key") in desired_seasons
    }

    for season_key in seasons_dict:
        season_data(seasons_dict=seasons_dict, season_key=season_key, out_path=out_league_path)

        if print_info:
            print(f"     - Scraping information for season {season_key}")

    elapsed_time = time.time() - start_time

    if print_info:
        print(f"Finished Fotmob scraping ({league_name}) in {elapsed_time:.2f} seconds")
        print("================================================================================")