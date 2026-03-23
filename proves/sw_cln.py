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
# LIMPIEZA DE DATAFRAME DE MANAGERS - Scoresway
# --------------------------------------------------------------------------------------
def sw_managers_clean() -> pd.DataFrame:

    # Path de carpeta con datos de entrenadores
    sw_managers_path = os.path.join(RAW_DATA_PATH, "info", "sw_info")

    # Para cada fichero, comprovamos que acaba en "squads.json"
    squads_files = [f for f in os.listdir(sw_managers_path) if f.endswith("squads.json")]

    # Lista para concatenar información
    managers_info = []

    # Concatenamos entrenadores
    for file in squads_files:
        squad_data = json_to_dict(json_path=os.path.join(sw_managers_path, file)).get("squad")

        if squad_data:
            for squad in squad_data:
                squad_persons = squad.get("person")
                if squad_persons:
                    for person in squad_persons:
                        if person.get("type") and person.get("type") != "player":
                            managers_info.append(person)

    # Limpieza del dataframe
    man_df = pd.DataFrame(managers_info)
    man_df = man_df.drop(columns=["gender", "nationalityId", "placeOfBirth", "startDate", "active", "endDate", "secondNationalityId", "secondNationality", "knownName", "shirtNumber"])
    man_df.columns = ["IdSW", "FirstName", "LastName", "ShortFirstName", "ShortLastName", "MatchName", "Country", "Type"]
    man_df.insert(1, "Name", man_df["FirstName"] + " " + man_df["LastName"])
    man_df.insert(2, "ShortName", man_df["ShortFirstName"] + " " + man_df["ShortLastName"])
    man_df.insert(1, "Slug", man_df["ShortName"].apply(create_slug))

    return man_df.drop_duplicates().sort_values(by="Slug")

# --------------------------------------------------------------------------------------
# LIMPIEZA DE DATAFRAME DE EQUIPOS - Scoresway
# --------------------------------------------------------------------------------------
def sw_teams_clean() -> pd.DataFrame:

    # Path de carpeta con datos de equipos
    sw_info_path = os.path.join(RAW_DATA_PATH, "info", "sw_info")

    # Para cada fichero, comprovamos que acaba en "squads.json"
    squads_files = [f for f in os.listdir(sw_info_path) if f.endswith("squads.json")]

    # Lista para concatenar información
    teams_info = []

    # Concatenamos equipos
    for file in squads_files:
        squad_data = json_to_dict(json_path=os.path.join(sw_info_path, file)).get("squad")
        if squad_data:
            for squad in squad_data:
                squad.pop("person", None)      # Sacamos los jugadores

                # Información sobre las camisetas de los jugadores
                kits_info = squad.get("teamKits", {}).get("kit")
                kits = {"home_col_1": np.nan, "home_col_2": np.nan, "home_shorts": np.nan, 
                        "away_col_1": np.nan, "away_col_2": np.nan, "away_shorts": np.nan}
                if kits_info:
                    for kit in kits_info:
                        if kit.get("type") == "Home":
                            kits["home_col_1"] = kit.get("shirtColour1", np.nan)
                            kits["home_col_2"] = kit.get("shirtColour2", np.nan)
                            kits["home_shorts"] = kit.get("shortsColour1", np.nan)
                        elif kit.get("type") == "Away":
                            kits["away_col_1"] = kit.get("shirtColour1", np.nan)
                            kits["away_col_2"] = kit.get("shirtColour2", np.nan)
                            kits["away_shorts"] = kit.get("shortsColour1", np.nan)

                # Añadimos información
                teams_info.append({'id': squad.get("contestantId", np.nan),
                                'name': squad.get("contestantName", np.nan),
                                "shortName": squad.get("contestantShortName", np.nan),
                                "clubName": squad.get("contestantClubName", np.nan),
                                "code": squad.get("contestantCode", np.nan),
                                'homeKitCol1': kits["home_col_1"],
                                'homeKitCol2': kits["home_col_2"],
                                'homeShorts': kits["home_shorts"],
                                'awayKitCol1': kits["away_col_1"],
                                'awayKitCol2': kits["away_col_2"],
                                'awayShorts': kits["away_shorts"]})
                
    # Limpieza del dataframe
    teams_df = pd.DataFrame(teams_info)
    teams_df.columns = ["IdSW", "LongName", "ShortName", "Name", "Abbreviation", "HomeKitCol1", "HomeKitCol2", "HomeShortsCol", "AwayKitCol1", "AwayKitCol2", "AwayShortsCol"]
    teams_df.insert(1, "Slug", teams_df["Name"].apply(create_slug))

    return teams_df.drop_duplicates().sort_values(by="Slug")

# --------------------------------------------------------------------------------------
# LIMPIEZA DE DATAFRAME DE EQUIPOS - Scoresway
# --------------------------------------------------------------------------------------
def sw_matches_clean() -> pd.DataFrame:

    # Path de carpeta con datos de entrenadores
    sw_matches_path = os.path.join(RAW_DATA_PATH, "info", "sw_info")
    all_matches_info = []

    # Para cada liga y temporada, procesaremos partidos
    for league in COMPS['tournament'].unique():
        for season_key in DES_SEASONS:
            league_slug = create_slug(league)
            season_slug = f"{league_slug}_{season_key}"

            # Leemos la información si existe
            season_matches_path = os.path.join(sw_matches_path, f"{season_slug}_matches.json")
            if os.path.exists(season_matches_path):
                matches_dict = json_to_dict(json_path=season_matches_path).get("match")
                if matches_dict:
                    for match in matches_dict:
                        match_info = match.get("matchInfo")
                        if match_info:
                            all_matches_info.append({"match_id": match_info.get("id", np.nan), 
                                                    "league": league_slug,
                                                    "season": season_key,
                                                    "date": match_info.get("date", np.nan), 
                                                    "time": match_info.get("time", np.nan),
                                                    "homeTeam": match_info.get("contestant", [])[0].get("id", np.nan),
                                                    "awayTeam": match_info.get("contestant", [])[1].get("id", np.nan)})

    # Transformación del dataframe
    matches_df = pd.DataFrame(all_matches_info)
    matches_df.columns = ["IdSW", "League", "Season", "Date_", "Time_", "HomeTeam", "AwayTeam"]

    # Tratado de las horas null
    matches_df["Time_"] = matches_df["Time_"].fillna("00:00:00Z")
    matches_df["Time_"] = matches_df["Time_"].replace("", "00:00:00Z")

    # Datetime y orden
    matches_df["Datetime"] = pd.to_datetime(matches_df["Date_"].str.replace("Z", "", regex=False) + " " + matches_df["Time_"].str.replace("Z", "", regex=False), utc=True)
    matches_df = matches_df[matches_df["Datetime"] <= pd.Timestamp.now(tz="Europe/Madrid")]         # Filtrado de fechas para no tener partidos futuros
    matches_df = matches_df.sort_values(by=["League", "Season", "Datetime"])
    matches_df["Date"] = matches_df["Datetime"].dt.strftime("%d/%m/%Y")
    matches_df["Time"] = matches_df["Datetime"].dt.strftime("%H:%M")
    matches_df = matches_df.drop(columns=["Datetime", "Date_", "Time_"])

    return matches_df.drop_duplicates()

# --------------------------------------------------------------------------------------
# LIMPIEZA DE DATAFRAME DE JUGADORES - Scoresway
# --------------------------------------------------------------------------------------
def sw_players_clean() -> pd.DataFrame:

    # Path de carpeta con información
    sw_info_path = os.path.join(RAW_DATA_PATH, "info", "sw_info")

    # Para cada fichero, comprovamos que acaba en "squads.json"
    squads_files = [f for f in os.listdir(sw_info_path) if f.endswith("squads.json")]

    # Lista para añadir los jugadores
    players_info = []

    # Concatenación de información
    for squad in squads_files:
        squad_data = json_to_dict(json_path=os.path.join(sw_info_path, squad)).get('squad')
        if squad_data:
            for squad in squad_data:
                squad_persons = squad.get("person")
                if squad_persons:
                    for person in squad_persons:
                        if person.get("type") and person.get("type") == "player":
                            players_info.append(person)

    # Limpieza del dataframe
    players_df = pd.DataFrame(players_info)
    players_df = players_df.drop(columns=["gender", "nationalityId", "placeOfBirth", "startDate", "active", "endDate", "secondNationalityId", "secondNationality", "knownName", "type"])
    players_df.columns = ["IdSW", "FirstName", "LastName", "ShortFirstName", "ShortLastName", "MatchName", "Country", "Position", "ShirtNumber"]
    players_df.insert(1, "Name", players_df["FirstName"] + " " + players_df["LastName"])
    players_df.insert(2, "ShortName", players_df["ShortFirstName"] + " " + players_df["ShortLastName"])
    players_df.insert(1, "Slug", players_df["ShortName"].apply(create_slug))
    players_df["ShirtNumber"] = players_df["ShirtNumber"].astype("Int64")

    return players_df.drop_duplicates().sort_values(by="Slug")