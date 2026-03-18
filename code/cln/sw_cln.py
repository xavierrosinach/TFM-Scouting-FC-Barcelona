import os
import time
import pandas as pd
import numpy as np

from use.config import comps
from use.functions import json_to_dict, create_slug, elapsed_time_str

# --------------------------------------------------------------------------------------
# PROCESADO DE PARTIDOS - Procesa el JSON de partidos de Scoresway
# --------------------------------------------------------------------------------------
def matches_proc(matches_json_path: str, df_output_path: str) -> pd.DataFrame:
    
    if not os.path.exists(matches_json_path):
        return pd.DataFrame()

    matches_data = json_to_dict(json_path=matches_json_path).get("matches")
    if not matches_data:
        return pd.DataFrame()

    matches_list = [{"match_id": match.get("id", np.nan), "home_team": match.get("home_team", {}).get("name", np.nan), "away_team": match.get("away_team", {}).get("name", np.nan),
                     "home_score": match.get("scores", {}).get("home_score", np.nan), "away_score": match.get("scores", {}).get("away_score", np.nan), "status": match.get("status", np.nan),
                     "date": match.get("date", np.nan), "venue": match.get("venue", {}).get("name", np.nan), "competition": match.get("competition", {}).get("name", np.nan)} for match in matches_data]
    matches_df = pd.DataFrame(matches_list)
    matches_df.to_csv(df_output_path, index=False, sep=";")

    return matches_df

# --------------------------------------------------------------------------------------
# PROCESADO DE ESTADÍSTICAS DE PARTIDO - Procesa estadísticas de un partido individual.
# --------------------------------------------------------------------------------------
def match_stats_proc(match_json_path: str) -> pd.DataFrame:

    if not os.path.exists(match_json_path):
        return pd.DataFrame()

    match_data = json_to_dict(json_path=match_json_path)

    stats = match_data.get("statistics")
    if not stats:
        return pd.DataFrame()

    stats_df = pd.DataFrame({"ha": ["h", "a"]})

    for stat in stats:
        stat_name = stat.get("type")
        values = stat.get("values")

        if stat_name and values and len(values) == 2:
            stats_df[stat_name] = values

    return stats_df

# --------------------------------------------------------------------------------------
# PROCESADO DE TODOS LOS PARTIDOS - Procesa todos los partidos individuales.
# --------------------------------------------------------------------------------------
def all_matches_proc(matches_dir_path: str, df_output_path: str, stats_output_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    
    matches_list = []
    stats_list = []

    if not os.path.exists(matches_dir_path):
        return pd.DataFrame(), pd.DataFrame()

    for match_file in os.listdir(matches_dir_path):
        match_path = os.path.join(matches_dir_path, match_file)
        match_data = json_to_dict(match_path)

        match_info = match_data.get("match")
        if not match_info:
            continue

        match_id = match_info.get("id")

        matches_list.append({"match_id": match_id, "home_team": match_info.get("home_team", {}).get("name", np.nan), "away_team": match_info.get("away_team", {}).get("name", np.nan),
                             "home_score": match_info.get("scores", {}).get("home_score", np.nan), "away_score": match_info.get("scores", {}).get("away_score", np.nan),
                             "date": match_info.get("date", np.nan), "status": match_info.get("status", np.nan)})

        stats_df = match_stats_proc(match_json_path=match_path)
        if not stats_df.empty:
            stats_df.insert(0, "match_id", match_id)
            stats_list.append(stats_df)

    matches_df = pd.DataFrame(matches_list)
    stats_df = pd.concat(stats_list, ignore_index=True) if stats_list else pd.DataFrame()

    matches_df.to_csv(df_output_path, index=False, sep=";")
    stats_df.to_csv(stats_output_path, index=False, sep=";")

    return matches_df, stats_df

# --------------------------------------------------------------------------------------
# CLEANING PRINCIPAL - Ejecuta el proceso de limpieza de Scoresway.
# --------------------------------------------------------------------------------------
def main_scoresway_league_cleaning(league_id: int, out_path: str, print_info: bool = True) -> None:
    
    start_time = time.time()

    comp_row = comps.loc[comps["id"] == league_id]
    if comp_row.empty:
        raise ValueError(f"No existe ninguna liga con id={league_id}")

    league_name = comp_row["tournament"].iloc[0]
    league_slug = create_slug(league_name)

    league_raw_path = os.path.join(out_path, "scoresway", league_slug)
    league_clean_path = league_raw_path.replace("raw", "clean")
    os.makedirs(league_clean_path, exist_ok=True)

    if print_info:
        print(f"Starting Scoresway cleaning ({league_name})")

    if not os.path.exists(league_raw_path):
        raise FileNotFoundError(f"No existe la ruta raw: {league_raw_path}")

    seasons_to_proc = [season for season in os.listdir(league_raw_path) if os.path.isdir(os.path.join(league_raw_path, season))]

    for season in seasons_to_proc:
        season_raw_path = os.path.join(league_raw_path, season)
        season_clean_path = os.path.join(league_clean_path, season)

        os.makedirs(season_clean_path, exist_ok=True)

        matches_proc(matches_json_path=os.path.join(season_raw_path, "matches.json"), df_output_path=os.path.join(season_clean_path, "matches.csv"))
        all_matches_proc(matches_dir_path=os.path.join(season_raw_path, "matches"), df_output_path=os.path.join(season_clean_path, "matches_detailed.csv"), stats_output_path=os.path.join(season_clean_path, "stats.csv"))

        if print_info:
            print(f"     - Season processed: {season}")

    if print_info:
        print(f"Finished Scoresway cleaning ({league_name}) in {elapsed_time_str(start_time=start_time)}")