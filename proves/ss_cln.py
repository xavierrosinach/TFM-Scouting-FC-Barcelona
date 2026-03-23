import os
import pandas as pd
import numpy as np

from use.config import DATA_PATH, COMPS, DES_SEASONS
from use.functions import json_to_dict, create_slug, need_to_upload, elapsed_time_str

# Estructura de carpetas
RAW_DATA_PATH = os.path.join(DATA_PATH, "raw")
BRONZE_DATA_PATH = os.path.join(DATA_PATH, "bronze")
os.makedirs(BRONZE_DATA_PATH, exist_ok=True)

# --------------------------------------------------------------------------------------
# LIMPIEZA DE DATAFRAME DE MANAGERS - Sofascore
# --------------------------------------------------------------------------------------
def ss_managers_clean() -> pd.DataFrame:

    # Path de carpeta con datos de entrenadores
    ss_managers_path = os.path.join(RAW_DATA_PATH, "info", "ss_managers")

    # Lista para concatenar información
    managers_info = []

    # Para cada entrenador
    for manager in os.listdir(ss_managers_path):
        manager_data = json_to_dict(json_path=os.path.join(ss_managers_path, manager)).get('manager')

        # Añadimos datos si podemos
        if manager_data:
            try:
                managers_info.append({"id": manager_data.get("id", np.nan), 
                                      "name": manager_data.get("name", np.nan), 
                                      "short_name": manager_data.get("shortName", np.nan),
                                      "country": manager_data.get("country", {}).get("name", np.nan), 
                                      "date_birth": manager_data.get("dateOfBirthTimestamp", np.nan),
                                      "matches": manager_data.get("performance", {}).get("total", np.nan), 
                                      "wins": manager_data.get("performance", {}).get("wins", np.nan),
                                      "draws": manager_data.get("performance", {}).get("draws", np.nan), 
                                      "losses": manager_data.get("performance", {}).get("losses", np.nan),
                                      "goals_scored": manager_data.get("performance", {}).get("goalsScored", np.nan), 
                                      "goals_conceded": manager_data.get("performance", {}).get("goalsConceded", np.nan),
                                      "points": manager_data.get("performance", {}).get("totalPoints", np.nan)})
            except:
                continue

    # Limpieza básica del Dataframe
    man_df = pd.DataFrame(managers_info)
    man_df.columns = ["IdSS", "Name", "ShortName", "Country", "DateBirth", "Matches", "Wins", "Draws", "Losses", "GoalsFor", "GoalsAgainst", "Points"]
    man_df.insert(1, "Slug", man_df["Name"].apply(create_slug))

    # Fecha de nacimiento y columnas integer
    man_df["DateBirth"] = pd.to_datetime(man_df["DateBirth"], unit="s", errors="coerce").dt.strftime("%d/%m/%Y")
    for col in ["Matches", "Wins", "Draws", "Losses", "GoalsFor", "GoalsAgainst", "Points"]:
        man_df[col] = pd.to_numeric(man_df[col], errors="coerce").astype("Int64")

    return man_df.drop_duplicates().sort_values(by="Slug")

# --------------------------------------------------------------------------------------
# PROCESADO DE ESTADIOS - Procesado de estadios de Sofascore
# --------------------------------------------------------------------------------------
def ss_venues_clean() -> pd.DataFrame:

    # Path de carpeta con datos de estadios
    ss_info_path = os.path.join(RAW_DATA_PATH, "info", "ss_info")

    # Para cada fichero, comprovamos que acaba en "venue.json"
    venues_files = [f for f in os.listdir(ss_info_path) if f.endswith("venue.json")]

    # Lista para concatenar información
    venues_info = []

    # Concatenamos entrenadores
    for file in venues_files:
        venues_data = json_to_dict(json_path=os.path.join(ss_info_path, file)).get("venues")
        if venues_data:
            for venue in venues_data:
                venues_info.append({"venue_id": venue.get("id", np.nan), 
                                    "name": venue.get("name", np.nan), 
                                    "capacity": venue.get("capacity", np.nan), 
                                    "city": venue.get("city", {}).get("name", np.nan),
                                    "latitude": venue.get("venueCoordinates", {}).get("latitude", np.nan), 
                                    "longitude": venue.get("venueCoordinates", {}).get("longitude", np.nan)})
                
    # Limpieza del dataframe
    venue_df = pd.DataFrame(venues_info)
    venue_df.columns = ["IdSS", "Name", "Capacity", "City", "Latitude", "Longitude"]
    venue_df.insert(1, "Slug", venue_df["Name"].apply(create_slug))
    venue_df["Capacity"] = venue_df["Capacity"].astype("Int64")

    return venue_df.drop_duplicates().sort_values(by="Slug")

# --------------------------------------------------------------------------------------
# LIMPIEZA DE DATAFRAME DE EQUIPOS - Sofascore
# --------------------------------------------------------------------------------------
def ss_teams_clean() -> pd.DataFrame:

    # Path de carpeta con datos de equipos
    ss_teams_path = os.path.join(RAW_DATA_PATH, "info", "ss_teams")

    # Lista para concatenar información
    teams_info = []

    # Para cada entrenador
    for team in os.listdir(ss_teams_path):
        single_team_data = json_to_dict(json_path=os.path.join(ss_teams_path, team)).get("team")
        
        if single_team_data:
            teams_info.append({"team_id": single_team_data.get("id", np.nan), 
                               "name": single_team_data.get("name", np.nan), 
                               "short_name": single_team_data.get("shortName", np.nan),
                               "full_name": single_team_data.get("fullName", np.nan), 
                               "manager": single_team_data.get("manager", {}).get("id", np.nan),
                               "venue": single_team_data.get("venue", {}).get("id", np.nan), 
                               "country": single_team_data.get("country", {}).get("name", np.nan),
                               "foundation_date": single_team_data.get("foundationDateTimestamp", np.nan), 
                               "primary_colour": single_team_data.get("teamColors", {}).get("primary", np.nan),
                               "secondary_colour": single_team_data.get("teamColors", {}).get("secondary", np.nan), 
                               "text_colour": single_team_data.get("teamColors", {}).get("text", np.nan)})

    # Limpieza del dataframe
    teams_df = pd.DataFrame(teams_info)
    teams_df.columns = ["IdSS", "Name", "ShortName", "LongName", "Manager", "Venue", "Country", "FoundationDate", "PrimaryColour", "SecondaryColour", "TextColour"]
    teams_df.insert(1, "Slug", teams_df["Name"].apply(create_slug))
    teams_df["FoundationDate"] = pd.to_datetime(teams_df["FoundationDate"], unit="s", errors="coerce").dt.strftime("%d/%m/%Y")

    return teams_df.drop_duplicates().sort_values(by="Slug")

# --------------------------------------------------------------------------------------
# LIMPIEZA DE DATAFRAME DE EQUIPOS - Sofascore
# --------------------------------------------------------------------------------------
def ss_matches_clean() -> pd.DataFrame:

    # Path de carpeta con datos de partidos
    ss_matches_path = os.path.join(RAW_DATA_PATH, "matches")
    all_matches_info = []

    # Para cada liga y temporada, procesaremos partidos
    for league in COMPS['tournament'].unique():
        for season_key in DES_SEASONS:
            league_slug = create_slug(league)
            season_slug = f"{league_slug}_{season_key}"

            season_matches_path = os.path.join(ss_matches_path, season_slug)
            if os.path.exists(season_matches_path):
                season_matches = [f for f in os.listdir(season_matches_path) if f.endswith("info.json")]
                for match in season_matches:
                    dict_info = json_to_dict(json_path=os.path.join(season_matches_path, match)).get("event")
                    if dict_info:
                        all_matches_info.append({"match_id": dict_info.get("id", np.nan), 
                                                 "league": league_slug,
                                                 "season": season_key,
                                                 "round": dict_info.get("roundInfo", {}).get("round", np.nan), 
                                                 "winner": dict_info.get("winnerCode", np.nan),
                                                 "attendance": dict_info.get("attendance", np.nan), 
                                                 "venue": dict_info.get("venue", {}).get("id", np.nan), 
                                                 "referee": dict_info.get("referee", {}).get("name", np.nan),
                                                 "home_team": dict_info.get("homeTeam", {}).get("id", np.nan), 
                                                 "away_team": dict_info.get("awayTeam", {}).get("id", np.nan), 
                                                 "home_score": dict_info.get("homeScore", {}).get("display", np.nan),
                                                 "away_score": dict_info.get("awayScore", {}).get("display", np.nan), 
                                                 "home_manager": dict_info.get("homeTeam", {}).get("manager", {}).get("id", np.nan),
                                                 "away_manager": dict_info.get("homeTeam", {}).get("manager", {}).get("id", np.nan),
                                                 "date_time": dict_info.get("startTimestamp", np.nan)})

    # Limpieza del dataframe
    matches_df = pd.DataFrame(all_matches_info)
    matches_df.columns = ["IdSS", "League", "Season", "Round", "Winner", "Attendance", "Venue", "Referee", "HomeTeam", "AwayTeam", 
                          "HomeScore", "AwayScore", "HomeManager", "AwayManager", "DateTime"]

    # Otras transformaciones
    matches_df["Winner"] = np.where(matches_df["Winner"] == 1, "Home", np.where(matches_df["Winner"] == 2, "Away", "X"))
    for col in ["Attendance", "HomeScore", "AwayScore"]:
        matches_df[col] = matches_df[col].astype("Int64")

    # Ordenado antes de convertir horario
    matches_df = matches_df.sort_values(by=["League", "Season", "DateTime"])

    # Conversión a horario
    matches_df["DateTime"] = pd.to_datetime(matches_df["DateTime"], unit="s")
    matches_df["Date"] = matches_df["DateTime"].dt.strftime("%d/%m/%Y")
    matches_df["Time"] = matches_df["DateTime"].dt.strftime("%H:%M")
    matches_df = matches_df.drop(columns=["DateTime"])

    return matches_df.drop_duplicates()     

# --------------------------------------------------------------------------------------
# LIMPIEZA DE DATAFRAME DE JUGADORES - Sofascore
# --------------------------------------------------------------------------------------
def ss_players_clean() -> pd.DataFrame:

    # Path de carpeta con datos de jugadores
    ss_info_path = os.path.join(RAW_DATA_PATH, "info", "ss_info")
    all_players_info = []

    # Para cada liga y temporada, procesaremos información de jugadores
    for league in COMPS['tournament'].unique():
        for season_key in DES_SEASONS:
            league_slug = create_slug(league)
            season_slug = f"{league_slug}_{season_key}"

            season_matches_path = os.path.join(ss_info_path, f"{season_slug}_player.json")
            if os.path.exists(season_matches_path):
                players_dict = json_to_dict(json_path=season_matches_path).get("players")
                all_players_info.extend(players_dict)

    # Limpieza básica del primer dataframe
    players_df = pd.DataFrame(all_players_info)
    players_df = players_df.drop(columns=["position", "teamName"])
    players_df.columns = ["IdSS", "Name", "Team"]
    players_df = players_df.drop_duplicates(subset="IdSS").sort_values(by="Name")

    # Tratado de cada jugador individualmente - obtener más información
    ss_players_path = os.path.join(RAW_DATA_PATH, "info", "ss_players")
    players_to_proc = players_df["IdSS"].unique().tolist()

    players_more_info = []

    # Para cada jugador
    for player_id in players_to_proc:   
        player_path = os.path.join(ss_players_path, f"{player_id}.json")
        if os.path.exists(player_path):
            single_player_data = json_to_dict(json_path=player_path).get("player")
            if single_player_data:

                # Tratado de posiciones (hay distintas)
                positions_detailed = single_player_data.get("positionsDetailed", [])
                first_position = positions_detailed[0] if len(positions_detailed) > 0 else np.nan
                second_position = positions_detailed[1] if len(positions_detailed) > 1 else np.nan
                third_position = positions_detailed[2] if len(positions_detailed) > 2 else np.nan

                # Añadir más información
                players_more_info.append({"IdSS": single_player_data.get("id", np.nan), 
                                        "shortName": single_player_data.get("shortName", np.nan), 
                                        "first_position": first_position,
                                        "second_position": second_position, 
                                        "third_position": third_position, 
                                        "shirt_num": single_player_data.get("shirtNumber", np.nan),
                                        "height": single_player_data.get("height", np.nan), 
                                        "pref_foot": single_player_data.get("preferredFoot", np.nan),
                                        "date_birth": single_player_data.get("dateOfBirthTimestamp", np.nan), 
                                        "country": single_player_data.get("country", {}).get("name", np.nan),
                                        "contract_until": single_player_data.get("contractUntilTimestamp", np.nan), 
                                        "market_value": single_player_data.get("proposedMarketValue", np.nan)})

    # Transformado a Df y merge con el otro dataframe
    players_more_info_df = pd.DataFrame(players_more_info)
    players_df = players_df.merge(players_more_info_df, how="left", on="IdSS")
    players_df.columns = ["IdSS", "Name", "Team", "ShortName", "FirstPosition", "SecondPosition", "ThirdPosition", "ShirtNum", "Height", "PrefFoot", "DateBirth", "Country", "ContractUntil", "MarketValue"]

    # Tratado de columnas
    players_df.insert(1, "Slug", players_df["Name"].apply(create_slug))
    players_df["ShirtNum"] = players_df["ShirtNum"].astype("Int64")
    players_df["Height"] = players_df["Height"].astype("Int64")
    players_df["MarketValue"] = players_df["MarketValue"].astype("Int64")

    # Fechas
    players_df["DateBirth"] = pd.to_datetime(players_df["DateBirth"], unit="s", errors="coerce").dt.strftime("%d/%m/%Y")
    players_df["ContractUntil"] = pd.to_datetime(players_df["ContractUntil"], unit="s", errors="coerce").dt.strftime("%d/%m/%Y")

    return players_df.drop_duplicates().sort_values(by="Slug")