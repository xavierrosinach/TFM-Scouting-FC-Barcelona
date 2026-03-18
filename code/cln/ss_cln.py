import os
import time
from typing import Tuple

import numpy as np
import pandas as pd

from use.config import comps
from use.functions import json_to_dict, create_slug, elapsed_time_str

# --------------------------------------------------------------------------------------
# PROCESADO DE TABLAS DE CLASIFICACIÓN - Procesa el JSON de clasificaciones de Sofascore y guarda las tablas disponibles.
# --------------------------------------------------------------------------------------
def standings_tables_proc(standings_path: str, standings_output_path: str) -> list:
    
    if not os.path.exists(standings_path):
        return []

    os.makedirs(standings_output_path, exist_ok=True)
    standings_data = json_to_dict(json_path=standings_path)

    list_return = []

    for standing_type in ["total", "home", "away"]:
        standings_part = (
            standings_data.get(standing_type, {})
            .get("standings", [{}])[0]
            .get("rows")
        )

        if not standings_part:
            continue

        list_info = [{"team_id": team.get("team", {}).get("id", np.nan), "team": team.get("team", {}).get("name", np.nan), "position": team.get("position", np.nan),
                      "promotion": team.get("promotion", {}).get("text", np.nan), "matches": team.get("matches", np.nan), "wins": team.get("wins", np.nan),
                      "losses": team.get("losses", np.nan), "draws": team.get("draws", np.nan), "scores_for": team.get("scoresFor", np.nan), 
                      "scores_against": team.get("scoresAgainst", np.nan), "points": team.get("points", np.nan)} for team in standings_part]
        standings_df = pd.DataFrame(list_info)
        list_return.append(standings_df)
        standings_df.to_csv(os.path.join(standings_output_path, f"{standing_type}.csv"), index=False, sep=";")

    return list_return

# --------------------------------------------------------------------------------------
# PROCESADO DE JUGADORES - Procesa el listado general de jugadores y lo enriquece con la información individual.
# --------------------------------------------------------------------------------------
def players_proc(players_json_path: str, players_dir_path: str, df_output_path: str) -> pd.DataFrame:
    
    if not os.path.exists(players_json_path):
        return pd.DataFrame()

    all_players_data = json_to_dict(json_path=players_json_path).get("players")
    if not all_players_data:
        return pd.DataFrame()

    players_df = pd.DataFrame(all_players_data)

    players_info = []
    if os.path.exists(players_dir_path):
        for player_file in os.listdir(players_dir_path):
            single_player_data = (json_to_dict(json_path=os.path.join(players_dir_path, player_file)).get("player"))

            if not single_player_data:
                continue

            positions_detailed = single_player_data.get("positionsDetailed", [])
            first_position = positions_detailed[0] if len(positions_detailed) > 0 else np.nan
            second_position = positions_detailed[1] if len(positions_detailed) > 1 else np.nan
            third_position = positions_detailed[2] if len(positions_detailed) > 2 else np.nan

            players_info.append({"playerId": single_player_data.get("id", np.nan), "shortName": single_player_data.get("shortName", np.nan), "first_position": first_position,
                                 "second_position": second_position, "third_position": third_position, "shirt_num": single_player_data.get("shirtNumber", np.nan),
                                 "height": single_player_data.get("height", np.nan), "pref_foot": single_player_data.get("preferredFoot", np.nan),
                                 "date_birth": single_player_data.get("dateOfBirthTimestamp", np.nan), "country": single_player_data.get("country", {}).get("name", np.nan),
                                 "contract_until": single_player_data.get("contractUntilTimestamp", np.nan), "market_value": single_player_data.get("proposedMarketValue", np.nan)})

    if players_info:
        players_more_info_df = pd.DataFrame(players_info)
        players_df = players_df.merge(players_more_info_df, how="left", on="playerId")

    players_df.to_csv(df_output_path, index=False, sep=";")
    return players_df

# --------------------------------------------------------------------------------------
# PROCESADO DE EQUIPOS - Procesa la información individual de equipos.
# --------------------------------------------------------------------------------------
def teams_proc(teams_dir_path: str, df_output_path: str) -> pd.DataFrame:
    
    if not os.path.exists(teams_dir_path):
        return pd.DataFrame()

    teams_info = []

    for team_file in os.listdir(teams_dir_path):
        single_team_data = json_to_dict(os.path.join(teams_dir_path, team_file)).get("team")

        if not single_team_data:
            continue

        teams_info.append({"team_id": single_team_data.get("id", np.nan), "name": single_team_data.get("name", np.nan), "short_name": single_team_data.get("shortName", np.nan),
                           "full_name": single_team_data.get("fullName", np.nan), "manager": single_team_data.get("manager", {}).get("id", np.nan),
                           "venue": single_team_data.get("venue", {}).get("id", np.nan), "country": single_team_data.get("country", {}).get("name", np.nan),
                           "foundation_date": single_team_data.get("foundationDateTimestamp", np.nan), "primary_colour": single_team_data.get("teamColors", {}).get("primary", np.nan),
                           "secondary_colour": single_team_data.get("teamColors", {}).get("secondary", np.nan), "text_colour": single_team_data.get("teamColors", {}).get("text", np.nan)})

    teams_df = pd.DataFrame(teams_info)
    teams_df.to_csv(df_output_path, index=False, sep=";")
    return teams_df

# --------------------------------------------------------------------------------------
# PROCESADO DE ESTADIOS - Procesa el JSON general de estadios.
# --------------------------------------------------------------------------------------
def venues_proc(venues_json_path: str, df_output_path: str) -> pd.DataFrame:
    
    if not os.path.exists(venues_json_path):
        return pd.DataFrame()

    venues_data = json_to_dict(json_path=venues_json_path).get("venues")
    if not venues_data:
        return pd.DataFrame()

    venues_info = [{"venue_id": venue.get("id", np.nan), "name": venue.get("name", np.nan), "capacity": venue.get("capacity", np.nan), "city": venue.get("city", {}).get("name", np.nan),
                    "latitude": venue.get("venueCoordinates", {}).get("latitude", np.nan), "longitude": venue.get("venueCoordinates", {}).get("longitude", np.nan)} for venue in venues_data]
    venues_df = pd.DataFrame(venues_info)
    venues_df.to_csv(df_output_path, index=False, sep=";")
    return venues_df

# --------------------------------------------------------------------------------------
# PROCESADO DE MANAGERS - Procesa la información individual de managers.
# --------------------------------------------------------------------------------------
def managers_proc(managers_dir_path: str, df_output_path: str) -> pd.DataFrame:
    
    if not os.path.exists(managers_dir_path):
        return pd.DataFrame()

    managers_info = []

    for manager_file in os.listdir(managers_dir_path):
        manager_data = json_to_dict(json_path=os.path.join(managers_dir_path, manager_file)).get("manager")

        if not manager_data:
            continue

        managers_info.append({"id": manager_data.get("id", np.nan), "name": manager_data.get("name", np.nan), "short_name": manager_data.get("shortName", np.nan),
                              "country": manager_data.get("country", {}).get("name", np.nan), "date_birth": manager_data.get("dateOfBirthTimestamp", np.nan),
                              "matches": manager_data.get("performance", {}).get("total", np.nan), "wins": manager_data.get("performance", {}).get("wins", np.nan),
                              "draws": manager_data.get("performance", {}).get("draws", np.nan), "losses": manager_data.get("performance", {}).get("losses", np.nan),
                              "goals_scored": manager_data.get("performance", {}).get("goalsScored", np.nan), "goals_conceded": manager_data.get("performance", {}).get("goalsConceded", np.nan),
                              "points": manager_data.get("performance", {}).get("totalPoints", np.nan)})

    managers_df = pd.DataFrame(managers_info)
    managers_df.to_csv(df_output_path, index=False, sep=";")
    return managers_df

# --------------------------------------------------------------------------------------
# INFORMACIÓN BÁSICA DE PARTIDO - Extrae la información principal de un partido.
# --------------------------------------------------------------------------------------
def match_info_proc(match_data: dict) -> pd.DataFrame:
    
    match_info = match_data.get("match", {}).get("event")
    if not match_info:
        return pd.DataFrame()

    return pd.DataFrame([{"match_id": match_info.get("id", np.nan), "round": match_info.get("roundInfo", {}).get("round", np.nan), "winner": match_info.get("winnerCode", np.nan),
                          "attendance": match_info.get("attendance", np.nan), "venue": match_info.get("venue", {}).get("id", np.nan), "referee": match_info.get("referee", {}).get("name", np.nan),
                          "home_team": match_info.get("homeTeam", {}).get("id", np.nan), "away_team": match_info.get("awayTeam", {}).get("id", np.nan), "home_score": match_info.get("homeScore", {}).get("display", np.nan),
                          "away_score": match_info.get("awayScore", {}).get("display", np.nan), "date_time": match_info.get("startTimestamp", np.nan)}])

# --------------------------------------------------------------------------------------
# ALINEACIÓN DE UN EQUIPO - Procesa la alineación y estadísticas de los jugadores de un equipo.
# --------------------------------------------------------------------------------------
def single_team_lineups(team_lineups: dict) -> Tuple[str, pd.DataFrame]:
    
    if not team_lineups:
        return np.nan, pd.DataFrame()

    formation = team_lineups.get("formation", np.nan)
    players = team_lineups.get("players")
    players_list = []

    if players:
        for player in players:
            player_id = player.get("player", {}).get("id")
            starter = not player.get("substitute", True)

            if not player_id:
                continue

            player_statistics = player.get("statistics", {}).copy()
            player_statistics.pop("ratingVersions", None)
            player_statistics.pop("statisticsType", None)

            players_list.append({"player_id": player_id, "starter": starter, **player_statistics})

    if not players_list:
        return formation, pd.DataFrame()

    lineups_df = pd.DataFrame(players_list)

    if "minutesPlayed" in lineups_df.columns:
        lineups_df = lineups_df[lineups_df["minutesPlayed"].notna()].copy()

    return formation, lineups_df

# --------------------------------------------------------------------------------------
# ESTADÍSTICAS DE EQUIPO EN PARTIDO - Procesa las estadísticas de equipo de un partido.
# --------------------------------------------------------------------------------------
def match_stats_proc(match_data: dict) -> pd.DataFrame:

    teams_stats = match_data.get("statistics", {}).get("statistics")
    if not teams_stats:
        return pd.DataFrame()

    statistics_groups = teams_stats[0].get("groups", [])
    statistics_df = pd.DataFrame({"ha": ["h", "a"]})

    for group in statistics_groups:
        group_stats = group.get("statisticsItems", [])

        for stat in group_stats:
            stat_name = stat.get("name")
            if stat_name:
                statistics_df[stat_name] = [stat.get("homeValue"), stat.get("awayValue")]

    return statistics_df

# --------------------------------------------------------------------------------------
# PROCESADO DE TODOS LOS PARTIDOS - Procesa todos los partidos scrapeados de una temporada.
# --------------------------------------------------------------------------------------
def all_matches_proc(league_raw_matches_path: str, league_clean_matches_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    
    list_matches_dfs = []
    list_lineups_dfs = []
    list_stats_dfs = []

    if not os.path.exists(league_raw_matches_path):
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    for match_file in os.listdir(league_raw_matches_path):
        match_data = json_to_dict(json_path=os.path.join(league_raw_matches_path, match_file))

        match_info_df = match_info_proc(match_data=match_data)
        if match_info_df.empty:
            continue

        list_matches_dfs.append(match_info_df)

        match_id = match_info_df["match_id"].iloc[0]
        home_team = match_info_df["home_team"].iloc[0]
        away_team = match_info_df["away_team"].iloc[0]

        match_lineups = match_data.get("lineups", {})
        home_formation, home_df = single_team_lineups(match_lineups.get("home"))
        away_formation, away_df = single_team_lineups(match_lineups.get("away"))

        if not home_df.empty:
            home_df.insert(0, "match_id", match_id)
            home_df.insert(1, "team_id", home_team)
            home_df.insert(2, "opponent_team_id", away_team)
            home_df.insert(3, "ha", "h")

        if not away_df.empty:
            away_df.insert(0, "match_id", match_id)
            away_df.insert(1, "team_id", away_team)
            away_df.insert(2, "opponent_team_id", home_team)
            away_df.insert(3, "ha", "a")

        lineups_parts = [df for df in [home_df, away_df] if not df.empty]
        if lineups_parts:
            lineups_df = pd.concat(lineups_parts, ignore_index=True)
            list_lineups_dfs.append(lineups_df)

        match_stats_df = match_stats_proc(match_data=match_data)
        if not match_stats_df.empty:
            match_stats_df.insert(0, "match_id", match_id)
            match_stats_df.insert(1, "team_id", [home_team, away_team])
            match_stats_df.insert(2, "opponent_team_id", [away_team, home_team])
            list_stats_dfs.append(match_stats_df)

    all_matches_df = pd.concat(list_matches_dfs, ignore_index=True) if list_matches_dfs else pd.DataFrame()
    all_lineups_df = pd.concat(list_lineups_dfs, ignore_index=True) if list_lineups_dfs else pd.DataFrame()
    all_stats_df = pd.concat(list_stats_dfs, ignore_index=True) if list_stats_dfs else pd.DataFrame()

    all_matches_df.to_csv(os.path.join(league_clean_matches_path, "matches.csv"), index=False, sep=";")
    all_lineups_df.to_csv(os.path.join(league_clean_matches_path, "lineups.csv"), index=False, sep=";")
    all_stats_df.to_csv(os.path.join(league_clean_matches_path, "statistics.csv"), index=False, sep=";")

    return all_matches_df, all_lineups_df, all_stats_df

# --------------------------------------------------------------------------------------
# CLEANING PRINCIPAL DE LIGA - Ejecuta el proceso de limpieza de datos de Sofascore para una liga.
# --------------------------------------------------------------------------------------
def main_sofascore_league_cleaning(league_id: int, out_path: str, print_info: bool = True) -> None:
    
    start_time = time.time()

    comp_row = comps.loc[comps["id"] == league_id]
    if comp_row.empty:
        raise ValueError(f"No existe ninguna liga con id={league_id} en comps.csv.")

    league_name = comp_row["tournament"].iloc[0]
    league_slug = create_slug(text=league_name)

    league_raw_path = os.path.join(out_path, "sofascore", league_slug)
    league_clean_path = league_raw_path.replace("raw", "clean")
    os.makedirs(league_clean_path, exist_ok=True)

    if print_info:
        print(f"Starting Sofascore cleaning ({league_name})")

    if not os.path.exists(league_raw_path):
        raise FileNotFoundError(f"No existe la ruta raw de Sofascore: {league_raw_path}")

    seasons_to_proc = [season for season in os.listdir(league_raw_path) if os.path.isdir(os.path.join(league_raw_path, season))]

    for season in seasons_to_proc:
        league_raw_info_path = os.path.join(league_raw_path, season, "info")
        league_raw_matches_path = os.path.join(league_raw_path, season, "matches")

        league_clean_info_path = os.path.join(league_clean_path, season, "info")
        os.makedirs(league_clean_info_path, exist_ok=True)

        league_clean_matches_path = os.path.join(league_clean_path, season, "matches")
        os.makedirs(league_clean_matches_path, exist_ok=True)

        standings_tables_proc(standings_path=os.path.join(league_raw_info_path, "standings.json"), standings_output_path=os.path.join(league_clean_info_path, "standings"))
        players_proc(players_json_path=os.path.join(league_raw_info_path, "player.json"), players_dir_path=os.path.join(league_raw_info_path, "player"), df_output_path=os.path.join(league_clean_info_path, "players.csv"))
        teams_proc(teams_dir_path=os.path.join(league_raw_info_path, "team"), df_output_path=os.path.join(league_clean_info_path, "teams.csv"))
        venues_proc(venues_json_path=os.path.join(league_raw_info_path, "venue.json"), df_output_path=os.path.join(league_clean_info_path, "venues.csv"))
        managers_proc(managers_dir_path=os.path.join(league_raw_info_path, "manager"), df_output_path=os.path.join(league_clean_info_path, "managers.csv"))
        all_matches_proc(league_raw_matches_path=league_raw_matches_path, league_clean_matches_path=league_clean_matches_path)

        if print_info:
            print(f"     - Information cleaned for season {season}")

    if print_info:
        print(f"Finished Sofascore cleaning ({league_name}) in {elapsed_time_str(start_time=start_time)}")
