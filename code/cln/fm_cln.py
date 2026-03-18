import os
import time
import pandas as pd
import numpy as np

from use.config import comps
from use.functions import json_to_dict, create_slug


# --------------------------------------------------------------------------------------
# LIMPIEZA DE INFORMACIÓN DE TEMPORADA
# --------------------------------------------------------------------------------------

def clean_season_information(
    season_info: dict,
    season_key: str,
    league_slug: str,
    league_name: str,
    season_out_path: str
) -> None:
    """
    Procesa el JSON de una temporada de Fotmob y genera los CSVs correspondientes.
    """

    os.makedirs(season_out_path, exist_ok=True)

    # ----------------------------------------------------------------------------------
    # INFO GENERAL DE LA LIGA
    # ----------------------------------------------------------------------------------

    details = season_info.get("details")

    if details:
        info_df = pd.DataFrame([{
            "fm_id": details.get("id", np.nan),
            "type": details.get("type", np.nan),
            "season": season_key,
            "slug": league_slug,
            "name": league_name,
            "short_name": details.get("shortName", np.nan),
            "country": details.get("country", np.nan),
            "gender": details.get("gender", np.nan),
            "league_color": details.get("leagueColor", np.nan),
        }])

        info_df.to_csv(
            os.path.join(season_out_path, "info.csv"),
            index=False,
            sep=";"
        )

    # ----------------------------------------------------------------------------------
    # CLASIFICACIONES
    # ----------------------------------------------------------------------------------

    table_data = season_info.get("table")

    if isinstance(table_data, list) and len(table_data) > 0:
        table = table_data[0].get("data", {}).get("table", {})

        if table:
            filters = ["all", "home", "away", "form", "xg"]

            standings_path = os.path.join(season_out_path, "standings")
            os.makedirs(standings_path, exist_ok=True)

            for f in filters:
                part_table = table.get(f)

                if part_table:
                    table_df = pd.DataFrame(part_table)

                    table_df.to_csv(
                        os.path.join(standings_path, f"{f}.csv"),
                        index=False,
                        sep=";"
                    )

    # ----------------------------------------------------------------------------------
    # PARTIDOS
    # ----------------------------------------------------------------------------------

    matches = season_info.get("fixtures", {}).get("allMatches")

    if matches:
        matches_list = [{
            "round": match.get("round", np.nan),
            "round_name": match.get("roundName", np.nan),
            "match_id": match.get("id", np.nan),
            "home_team": match.get("home", {}).get("name", np.nan),
            "home_team_id": match.get("home", {}).get("id", np.nan),
            "away_team": match.get("away", {}).get("name", np.nan),
            "away_team_id": match.get("away", {}).get("id", np.nan),
            "time": match.get("status", {}).get("utcTime", np.nan),
            "score_str": match.get("status", {}).get("scoreStr", np.nan),
        } for match in matches]

        matches_df = pd.DataFrame(matches_list)

        matches_df.to_csv(
            os.path.join(season_out_path, "matches.csv"),
            index=False,
            sep=";"
        )


# --------------------------------------------------------------------------------------
# CLEANING PRINCIPAL DE LIGA
# --------------------------------------------------------------------------------------

def main_fotmob_league_cleaning(
    league_id: int,
    out_path: str,
    print_info: bool = True
) -> None:
    """
    Ejecuta el proceso de limpieza de datos de Fotmob para una liga.
    """

    start_time = time.time()

    comp_row = comps.loc[comps["id"] == league_id]
    if comp_row.empty:
        raise ValueError(f"No existe ninguna liga con id={league_id} en comps.csv.")

    league_name = comp_row["tournament"].iloc[0]
    league_slug = create_slug(text=league_name)

    league_raw_path = os.path.join(out_path, "fotmob", league_slug)
    league_clean_path = os.path.join(out_path, "fotmob", league_slug).replace("raw", "clean")

    os.makedirs(league_clean_path, exist_ok=True)

    if print_info:
        print("================================================================================")
        print(f"Starting Fotmob cleaning ({league_name})")

    seasons_to_proc = [
        s for s in os.listdir(league_raw_path)
        if os.path.isdir(os.path.join(league_raw_path, s))
    ]

    for season in seasons_to_proc:
        season_info_path = os.path.join(league_raw_path, season, "info.json")

        if not os.path.exists(season_info_path):
            continue

        season_info = json_to_dict(json_path=season_info_path)

        clean_season_information(
            season_info=season_info,
            season_key=season,
            league_slug=league_slug,
            league_name=league_name,
            season_out_path=os.path.join(league_clean_path, season),
        )

        if print_info:
            print(f"     - Information cleaned for season {season}")

    elapsed_time = time.time() - start_time

    if print_info:
        print(f"Finished Fotmob cleaning ({league_name}) in {elapsed_time:.2f} seconds")
        print("================================================================================")