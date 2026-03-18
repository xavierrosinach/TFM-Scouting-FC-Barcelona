import os
import re
import json as jsonlib
import shutil
import time
import warnings
from typing import Tuple

import numpy as np
import pandas as pd
from PIL import Image
from rapidfuzz import process, fuzz

from use.config import comps, desired_seasons, utils
from use.functions import create_slug, safe_div, elapsed_time_str

warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

# --------------------------------------------------------------------------------------
# LECTURA DE DATOS DE FOTMOB
# --------------------------------------------------------------------------------------
def read_fotmob_data(fotmob_clean_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    if not os.path.exists(fotmob_clean_path):
        return None, None, None, None, None, None, None

    standings_path = os.path.join(fotmob_clean_path, "standings")
    info_path = os.path.join(fotmob_clean_path, "info.csv")
    matches_path = os.path.join(fotmob_clean_path, "matches.csv")

    st_all_path = os.path.join(standings_path, "all.csv")
    st_home_path = os.path.join(standings_path, "home.csv")
    st_away_path = os.path.join(standings_path, "away.csv")
    st_form_path = os.path.join(standings_path, "form.csv")
    st_xg_path = os.path.join(standings_path, "xg.csv")

    info_df = pd.read_csv(info_path, sep=";") if os.path.exists(info_path) else None
    matches_df = pd.read_csv(matches_path, sep=";") if os.path.exists(matches_path) else None
    all_st_df = pd.read_csv(st_all_path, sep=";") if os.path.exists(st_all_path) else None
    home_st_df = pd.read_csv(st_home_path, sep=";") if os.path.exists(st_home_path) else None
    away_st_df = pd.read_csv(st_away_path, sep=";") if os.path.exists(st_away_path) else None
    form_st_df = pd.read_csv(st_form_path, sep=";") if os.path.exists(st_form_path) else None
    xg_st_df = pd.read_csv(st_xg_path, sep=";") if os.path.exists(st_xg_path) else None

    return info_df, matches_df, all_st_df, home_st_df, away_st_df, form_st_df, xg_st_df

# --------------------------------------------------------------------------------------
# OBTENCIÓN DE EQUIPOS DE FOTMOB
# --------------------------------------------------------------------------------------
def obtain_fotmob_teams(matches_df: pd.DataFrame, all_st_df: pd.DataFrame, home_st_df: pd.DataFrame, away_st_df: pd.DataFrame, form_st_df: pd.DataFrame, xg_st_df: pd.DataFrame) -> list:

    teams_list = []

    if matches_df is not None and not matches_df.empty:
        if "home_team" in matches_df.columns:
            teams_list.extend(matches_df["home_team"].dropna().tolist())
        if "away_team" in matches_df.columns:
            teams_list.extend(matches_df["away_team"].dropna().tolist())

    for df in [all_st_df, home_st_df, away_st_df, form_st_df, xg_st_df]:
        if df is not None and not df.empty and "name" in df.columns:
            teams_list.extend(df["name"].dropna().tolist())

    return sorted(set(teams_list))

# --------------------------------------------------------------------------------------
# LECTURA DE DATOS DE SCORESWAY
# --------------------------------------------------------------------------------------
def read_scoresway_data(scoresway_clean_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    
    if not os.path.exists(scoresway_clean_path):
        return (None,) * 18

    info_path = os.path.join(scoresway_clean_path, "info")
    matches_dir_path = os.path.join(scoresway_clean_path, "matches")
    standings_path = os.path.join(info_path, "standings")

    managers_path = os.path.join(info_path, "managers.csv")
    matches_path = os.path.join(info_path, "matches.csv")
    players_path = os.path.join(info_path, "players.csv")
    teams_path = os.path.join(info_path, "teams.csv")

    st_total_path = os.path.join(standings_path, "total.csv")
    st_home_path = os.path.join(standings_path, "home.csv")
    st_away_path = os.path.join(standings_path, "away.csv")
    st_httotal_path = os.path.join(standings_path, "half-time-total.csv")
    st_hthome_path = os.path.join(standings_path, "half-time-home.csv")
    st_htaway_path = os.path.join(standings_path, "half-time-away.csv")
    st_formhome_path = os.path.join(standings_path, "form-home.csv")
    st_formaway_path = os.path.join(standings_path, "form-away.csv")
    st_overunder_path = os.path.join(standings_path, "over-under.csv")
    st_attendance_path = os.path.join(standings_path, "attendance.csv")

    matches_info_path = os.path.join(matches_dir_path, "info.csv")
    matches_player_stats_path = os.path.join(matches_dir_path, "player_stats.csv")
    matches_referees_path = os.path.join(matches_dir_path, "referees.csv")
    matches_team_stats_path = os.path.join(matches_dir_path, "team_stats.csv")

    managers_df = pd.read_csv(managers_path, sep=";") if os.path.exists(managers_path) else None
    matches_df = pd.read_csv(matches_path, sep=";") if os.path.exists(matches_path) else None
    players_df = pd.read_csv(players_path, sep=";") if os.path.exists(players_path) else None
    teams_df = pd.read_csv(teams_path, sep=";") if os.path.exists(teams_path) else None
    total_st_df = pd.read_csv(st_total_path, sep=";") if os.path.exists(st_total_path) else None
    home_st_df = pd.read_csv(st_home_path, sep=";") if os.path.exists(st_home_path) else None
    away_st_df = pd.read_csv(st_away_path, sep=";") if os.path.exists(st_away_path) else None
    httotal_st_df = pd.read_csv(st_httotal_path, sep=";") if os.path.exists(st_httotal_path) else None
    hthome_st_df = pd.read_csv(st_hthome_path, sep=";") if os.path.exists(st_hthome_path) else None
    htaway_st_df = pd.read_csv(st_htaway_path, sep=";") if os.path.exists(st_htaway_path) else None
    formhome_st_df = pd.read_csv(st_formhome_path, sep=";") if os.path.exists(st_formhome_path) else None
    formaway_st_df = pd.read_csv(st_formaway_path, sep=";") if os.path.exists(st_formaway_path) else None
    overunder_st_df = pd.read_csv(st_overunder_path, sep=";") if os.path.exists(st_overunder_path) else None
    attendance_st_df = pd.read_csv(st_attendance_path, sep=";") if os.path.exists(st_attendance_path) else None
    matches_info_df = pd.read_csv(matches_info_path, sep=";") if os.path.exists(matches_info_path) else None
    matches_player_stats_df = pd.read_csv(matches_player_stats_path, sep=";") if os.path.exists(matches_player_stats_path) else None
    matches_team_stats_df = pd.read_csv(matches_team_stats_path, sep=";") if os.path.exists(matches_team_stats_path) else None
    matches_referees_df = pd.read_csv(matches_referees_path, sep=";") if os.path.exists(matches_referees_path) else None

    return (managers_df, matches_df, players_df, teams_df, total_st_df, home_st_df, away_st_df, httotal_st_df, hthome_st_df, htaway_st_df, formhome_st_df, formaway_st_df,
            overunder_st_df, attendance_st_df, matches_info_df, matches_player_stats_df, matches_team_stats_df, matches_referees_df)

# --------------------------------------------------------------------------------------
# LECTURA DE DATOS DE SOFASCORE
# --------------------------------------------------------------------------------------
def read_sofascore_data(sofascore_clean_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    if not os.path.exists(sofascore_clean_path):
        return (None,) * 10

    info_path = os.path.join(sofascore_clean_path, "info")
    matches_dir_path = os.path.join(sofascore_clean_path, "matches")
    standings_path = os.path.join(info_path, "standings")

    managers_path = os.path.join(info_path, "managers.csv")
    venues_path = os.path.join(info_path, "venues.csv")
    players_path = os.path.join(info_path, "players.csv")
    teams_path = os.path.join(info_path, "teams.csv")

    st_total_path = os.path.join(standings_path, "total.csv")
    st_home_path = os.path.join(standings_path, "home.csv")
    st_away_path = os.path.join(standings_path, "away.csv")

    matches_info_path = os.path.join(matches_dir_path, "matches.csv")
    matches_lineups_path = os.path.join(matches_dir_path, "lineups.csv")
    matches_statistics_path = os.path.join(matches_dir_path, "statistics.csv")

    managers_df = pd.read_csv(managers_path, sep=";") if os.path.exists(managers_path) else None
    players_df = pd.read_csv(players_path, sep=";") if os.path.exists(players_path) else None
    teams_df = pd.read_csv(teams_path, sep=";") if os.path.exists(teams_path) else None
    venues_df = pd.read_csv(venues_path, sep=";") if os.path.exists(venues_path) else None
    total_st_df = pd.read_csv(st_total_path, sep=";") if os.path.exists(st_total_path) else None
    home_st_df = pd.read_csv(st_home_path, sep=";") if os.path.exists(st_home_path) else None
    away_st_df = pd.read_csv(st_away_path, sep=";") if os.path.exists(st_away_path) else None
    matches_info_df = pd.read_csv(matches_info_path, sep=";") if os.path.exists(matches_info_path) else None
    matches_lineups_df = pd.read_csv(matches_lineups_path, sep=";") if os.path.exists(matches_lineups_path) else None
    matches_statistics_df = pd.read_csv(matches_statistics_path, sep=";") if os.path.exists(matches_statistics_path) else None

    return (managers_df, players_df, teams_df, venues_df, total_st_df, home_st_df, away_st_df, matches_info_df, matches_lineups_df, matches_statistics_df)

# --------------------------------------------------------------------------------------
# MATCH FUZZY DE EQUIPOS
# --------------------------------------------------------------------------------------
def match_teams(fm_list: list, sw_list: list, ss_list: list, threshold: int = 30) -> pd.DataFrame:

    rows = []
    for team in fm_list:
        match_sw, score_sw, _ = process.extractOne(team, sw_list, scorer=fuzz.token_sort_ratio) if sw_list else ("", 0, "")
        match_ss, score_ss, _ = process.extractOne(team, ss_list, scorer=fuzz.token_sort_ratio) if ss_list else ("", 0, "")

        rows.append({"fotmob": team, "scoresway": match_sw if score_sw >= threshold else None, "sofascore": match_ss if score_ss >= threshold else None})

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["team", "fotmob", "scoresway", "sofascore"])

    df.insert(0, "team", df["fotmob"].combine_first(df["sofascore"]).combine_first(df["scoresway"]))
    return df

# --------------------------------------------------------------------------------------
# MATCH FUZZY DE JUGADORES / MANAGERS
# --------------------------------------------------------------------------------------
def match_players(sw_list: list, ss_list: list, threshold: int = 10) -> pd.DataFrame:

    sw_list = sw_list if sw_list is not None else []
    ss_list = ss_list if ss_list is not None else []

    if not ss_list and not sw_list:
        return pd.DataFrame(columns=["player", "sofascore", "scoresway"])

    if not ss_list:
        df = pd.DataFrame({"scoresway": sw_list})
        df["sofascore"] = np.nan
        df.insert(0, "player", df["scoresway"])
        return df

    rows = []
    for player in ss_list:
        match_sw, score_sw, _ = process.extractOne(player, sw_list, scorer=fuzz.token_sort_ratio) if sw_list else ("", 0, "")
        rows.append({"sofascore": player, "scoresway": match_sw if score_sw >= threshold else np.nan})

    df = pd.DataFrame(rows)
    df.insert(0, "player", df["sofascore"].combine_first(df["scoresway"]))
    return df

# --------------------------------------------------------------------------------------
# UNIFICACIÓN DE EQUIPOS - Información.
# --------------------------------------------------------------------------------------
def unify_teams_info(matched_teams: pd.DataFrame, sw_teams_df: pd.DataFrame = None, ss_teams_df: pd.DataFrame = None) -> pd.DataFrame:

    sw_to_team = matched_teams.set_index("scoresway")["team"].dropna().to_dict() if "scoresway" in matched_teams.columns else {}
    ss_to_team = matched_teams.set_index("sofascore")["team"].dropna().to_dict() if "sofascore" in matched_teams.columns else {}

    dfs = []

    if sw_teams_df is not None and not sw_teams_df.empty:
        sw_teams_df = sw_teams_df.copy()
        sw_teams_df["TeamName"] = sw_teams_df["club_name"].map(sw_to_team)
        sw_teams_df = sw_teams_df.rename(columns={c: f"{c}_sw" for c in sw_teams_df.columns if c != "TeamName"})
        dfs.append(sw_teams_df)

    if ss_teams_df is not None and not ss_teams_df.empty:
        ss_teams_df = ss_teams_df.copy()
        ss_teams_df["TeamName"] = ss_teams_df["name"].map(ss_to_team)
        ss_teams_df = ss_teams_df.rename(columns={c: f"{c}_ss" for c in ss_teams_df.columns if c != "TeamName"})
        dfs.append(ss_teams_df)

    if len(dfs) == 0:
        return matched_teams[["team"]].rename(columns={"team": "TeamName"}).drop_duplicates().reset_index(drop=True)

    if len(dfs) == 1:
        teams_df = dfs[0]
    else:
        teams_df = pd.merge(dfs[0], dfs[1], on="TeamName", how="outer")

    teams_df = (matched_teams[["team"]].rename(columns={"team": "TeamName"}).drop_duplicates().merge(teams_df, how="left", on="TeamName"))
    return teams_df

# --------------------------------------------------------------------------------------
# UNIFICACIÓN DE EQUIPOS - Limpieza.
# --------------------------------------------------------------------------------------
def clean_unified_teams(df: pd.DataFrame) -> pd.DataFrame:

    list_columns = df.columns
    df_cleaned = pd.DataFrame()

    df_cleaned["Name"] = df["TeamName"]
    df_cleaned["FullName"] = df["name_sw"] if "name_sw" in list_columns else df["name_ss"] if "name_ss" in list_columns else np.nan
    df_cleaned["ShortName"] = df["short_name_ss"] if "short_name_ss" in list_columns else np.nan
    df_cleaned["Code"] = df["code_sw"] if "code_sw" in list_columns else np.nan
    df_cleaned["Country"] = df["country_ss"] if "country_ss" in list_columns else np.nan
    df_cleaned["FoundationDate"] = df["foundation_date_ss"] if "foundation_date_ss" in list_columns else np.nan
    df_cleaned["VenueCodeSS"] = df["venue_ss"] if "venue_ss" in list_columns else np.nan
    df_cleaned["ManagerCodeSS"] = df["manager_ss"] if "manager_ss" in list_columns else np.nan
    df_cleaned["PrimaryColour"] = df["primary_colour_ss"] if "primary_colour_ss" in list_columns else np.nan
    df_cleaned["SecondaryColour"] = df["secondary_colour_ss"] if "secondary_colour_ss" in list_columns else np.nan
    df_cleaned["TextColour"] = df["text_colour_ss"] if "text_colour_ss" in list_columns else np.nan

    df_cleaned["IdSS"] = df["team_id_ss"] if "team_id_ss" in list_columns else np.nan
    df_cleaned["IdSW"] = df["sw_id_sw"] if "sw_id_sw" in list_columns else np.nan

    df_cleaned["FoundationDate"] = pd.to_datetime(df_cleaned["FoundationDate"], unit="s", errors="coerce").dt.strftime("%d/%m/%Y")
    df_cleaned["VenueCodeSS"] = pd.to_numeric(df_cleaned["VenueCodeSS"], errors="coerce").astype("Int64")
    df_cleaned["ManagerCodeSS"] = pd.to_numeric(df_cleaned["ManagerCodeSS"], errors="coerce").astype("Int64")
    df_cleaned["IdSS"] = pd.to_numeric(df_cleaned["IdSS"], errors="coerce").astype("Int64")
    df_cleaned["IdSW"] = pd.to_numeric(df_cleaned["IdSW"], errors="coerce").astype("Int64")

    df_cleaned.insert(0, "Slug", df_cleaned["Name"].apply(create_slug))
    return df_cleaned

# --------------------------------------------------------------------------------------
# UNIFICACIÓN DE EQUIPOS - Creación del Dataframe.
# --------------------------------------------------------------------------------------
def create_teams_info_df(matched_teams: pd.DataFrame, sw_teams_df: pd.DataFrame = None, ss_teams_df: pd.DataFrame = None) -> pd.DataFrame:
    raw_unified_teams_df = unify_teams_info(matched_teams=matched_teams, sw_teams_df=sw_teams_df, ss_teams_df=ss_teams_df)
    teams_df = clean_unified_teams(df=raw_unified_teams_df)
    return teams_df

# --------------------------------------------------------------------------------------
# UNIFICACIÓN DE JUGADORES - Información.
# --------------------------------------------------------------------------------------
def unify_players_info(team: str, matched_players: pd.DataFrame, ss_df: pd.DataFrame = None, sw_df: pd.DataFrame = None) -> pd.DataFrame:

    sw_to_player = matched_players.set_index("scoresway")["player"].dropna().to_dict() if "scoresway" in matched_players.columns else {}
    ss_to_player = matched_players.set_index("sofascore")["player"].dropna().to_dict() if "sofascore" in matched_players.columns else {}

    dfs = []

    if ss_df is not None and not ss_df.empty:
        ss_df = ss_df.copy()
        ss_df["PlayerName"] = ss_df["playerName_ss"].map(ss_to_player)
        dfs.append(ss_df)

    if sw_df is not None and not sw_df.empty:
        sw_df = sw_df.copy()
        sw_df["PlayerName"] = sw_df["match_name_sw"].map(sw_to_player)
        dfs.append(sw_df)

    if len(dfs) == 0:
        players_df = matched_players[["player"]].rename(columns={"player": "PlayerName"}).drop_duplicates().reset_index(drop=True)
    elif len(dfs) == 1:
        players_df = dfs[0]
    else:
        players_df = pd.merge(dfs[0], dfs[1], on="PlayerName", how="outer")

    players_df = (matched_players[["player"]].rename(columns={"player": "PlayerName"}).drop_duplicates().merge(players_df, how="left", on="PlayerName"))
    players_df.insert(0, "Team", create_slug(text=team))

    return players_df

# --------------------------------------------------------------------------------------
# UNIFICACIÓN DE JUGADORES - Limpieza.
# --------------------------------------------------------------------------------------
def clean_unified_players(df: pd.DataFrame) -> pd.DataFrame:

    list_columns = df.columns
    df_cleaned = pd.DataFrame()

    df_cleaned["Name"] = df["PlayerName"]
    df_cleaned["Team"] = df["Team"]
    df_cleaned["ShortName"] = df["shortName_ss"] if "shortName_ss" in list_columns else df["match_name_sw"] if "match_name_sw" in list_columns else np.nan
    df_cleaned["FirstName"] = df["first_name_sw"] if "first_name_sw" in list_columns else np.nan
    df_cleaned["SecondName"] = df["last_name_sw"] if "last_name_sw" in list_columns else np.nan
    df_cleaned["ShortFirstName"] = df["short_first_name_sw"] if "short_first_name_sw" in list_columns else np.nan
    df_cleaned["ShortSecondName"] = df["short_last_name_sw"] if "short_last_name_sw" in list_columns else np.nan
    df_cleaned["Country"] = df["country_ss"] if "country_ss" in list_columns else df["nationality_sw"] if "nationality_sw" in list_columns else np.nan
    df_cleaned["ShirtNumber"] = df["shirt_num_ss"] if "shirt_num_ss" in list_columns else df["shirt_number_sw"] if "shirt_number_sw" in list_columns else np.nan
    df_cleaned["PrefFoot"] = df["pref_foot_ss"] if "pref_foot_ss" in list_columns else np.nan
    df_cleaned["Height"] = df["height_ss"] if "height_ss" in list_columns else np.nan
    df_cleaned["DateBirth"] = df["date_birth_ss"] if "date_birth_ss" in list_columns else np.nan
    df_cleaned["FirstPosition"] = df["first_position_ss"] if "first_position_ss" in list_columns else np.nan
    df_cleaned["SecondPosition"] = df["second_position_ss"] if "second_position_ss" in list_columns else np.nan
    df_cleaned["ThirdPosition"] = df["third_position_ss"] if "third_position_ss" in list_columns else np.nan
    df_cleaned["MarketValue"] = df["market_value_ss"] if "market_value_ss" in list_columns else np.nan
    df_cleaned["ContractUntil"] = df["contract_until_ss"] if "contract_until_ss" in list_columns else np.nan

    df_cleaned["IdSS"] = df["playerId_ss"] if "playerId_ss" in list_columns else np.nan
    df_cleaned["IdSW"] = df["id_sw"] if "id_sw" in list_columns else np.nan

    df_cleaned["DateBirth"] = pd.to_datetime(df_cleaned["DateBirth"], unit="s", errors="coerce").dt.strftime("%d/%m/%Y")
    df_cleaned["ContractUntil"] = pd.to_datetime(df_cleaned["ContractUntil"], unit="s", errors="coerce").dt.strftime("%d/%m/%Y")
    df_cleaned["ShirtNumber"] = pd.to_numeric(df_cleaned["ShirtNumber"], errors="coerce").astype("Int64")
    df_cleaned["Height"] = pd.to_numeric(df_cleaned["Height"], errors="coerce").astype("Int64")
    df_cleaned["MarketValue"] = pd.to_numeric(df_cleaned["MarketValue"], errors="coerce").astype("Int64")
    df_cleaned["IdSS"] = pd.to_numeric(df_cleaned["IdSS"], errors="coerce").astype("Int64")
    df_cleaned["IdSW"] = pd.to_numeric(df_cleaned["IdSW"], errors="coerce").astype("Int64")

    df_cleaned.insert(0, "Slug", df_cleaned["Name"].apply(create_slug))
    return df_cleaned.sort_values(by="ShirtNumber", na_position="last").reset_index(drop=True)

# --------------------------------------------------------------------------------------
# UNIFICACIÓN DE JUGADORES - Creación del Dataframe.
# --------------------------------------------------------------------------------------
def create_players_info_df(matched_teams: pd.DataFrame, sw_players_df: pd.DataFrame, ss_players_df: pd.DataFrame) -> pd.DataFrame:

    sw_to_team = matched_teams.set_index("longname_scoresway")["team"].dropna().to_dict() if "longname_scoresway" in matched_teams.columns else {}
    ss_to_team = matched_teams.set_index("sofascore")["team"].dropna().to_dict() if "sofascore" in matched_teams.columns else {}

    if sw_players_df is not None and not sw_players_df.empty:
        sw_players_df = sw_players_df.copy()
        sw_players_df["TeamName"] = sw_players_df["team"].map(sw_to_team)
        sw_players_df = sw_players_df.rename(columns={c: f"{c}_sw" for c in sw_players_df.columns if c != "TeamName"})

    if ss_players_df is not None and not ss_players_df.empty:
        ss_players_df = ss_players_df.copy()
        ss_players_df["TeamName"] = ss_players_df["teamName"].map(ss_to_team)
        ss_players_df = ss_players_df.rename(columns={c: f"{c}_ss" for c in ss_players_df.columns if c != "TeamName"})

    list_teams = []

    for team in matched_teams["team"].dropna().tolist():
        sw_players_df_ = sw_players_df.loc[sw_players_df["TeamName"] == team] if sw_players_df is not None else None
        ss_players_df_ = ss_players_df.loc[ss_players_df["TeamName"] == team] if ss_players_df is not None else None

        if sw_players_df_ is not None and sw_players_df_.empty:
            sw_players_df_ = None
        if ss_players_df_ is not None and ss_players_df_.empty:
            ss_players_df_ = None

        players_names_sw = sw_players_df_["match_name_sw"].dropna().unique().tolist() if sw_players_df_ is not None else []
        players_names_ss = ss_players_df_["playerName_ss"].dropna().unique().tolist() if ss_players_df_ is not None else []

        matched_players = match_players(sw_list=players_names_sw, ss_list=players_names_ss)
        unified_players_df = unify_players_info(team=team, matched_players=matched_players, ss_df=ss_players_df_, sw_df=sw_players_df_)
        cleaned_players_df = clean_unified_players(df=unified_players_df)
        list_teams.append(cleaned_players_df)

    return pd.concat(list_teams, ignore_index=True) if list_teams else pd.DataFrame()

# --------------------------------------------------------------------------------------
# UNIFICACIÓN DE MANAGERS - Información.
# --------------------------------------------------------------------------------------
def unify_managers_info(matched_managers: pd.DataFrame, ss_df: pd.DataFrame = None, sw_df: pd.DataFrame = None) -> pd.DataFrame:

    sw_to_player = matched_managers.set_index("scoresway")["player"].dropna().to_dict() if "scoresway" in matched_managers.columns else {}
    ss_to_player = matched_managers.set_index("sofascore")["player"].dropna().to_dict() if "sofascore" in matched_managers.columns else {}

    dfs = []

    if ss_df is not None and not ss_df.empty:
        ss_df = ss_df.copy()
        ss_df["ManagerName"] = ss_df["name"].map(ss_to_player)
        ss_df = ss_df.rename(columns={c: f"{c}_ss" for c in ss_df.columns if c != "ManagerName"})
        dfs.append(ss_df)

    if sw_df is not None and not sw_df.empty:
        sw_df = sw_df.copy()
        sw_df["ManagerName"] = sw_df["manager_name"].map(sw_to_player)
        sw_df = sw_df.rename(columns={c: f"{c}_sw" for c in sw_df.columns if c != "ManagerName"})
        dfs.append(sw_df)

    if len(dfs) == 0:
        managers_df = matched_managers[["player"]].rename(columns={"player": "ManagerName"}).drop_duplicates().reset_index(drop=True)
    elif len(dfs) == 1:
        managers_df = dfs[0]
    else:
        managers_df = pd.merge(dfs[0], dfs[1], on="ManagerName", how="outer")

    managers_df = (matched_managers[["player"]].rename(columns={"player": "ManagerName"}).drop_duplicates().merge(managers_df, how="left", on="ManagerName"))
    return managers_df

# --------------------------------------------------------------------------------------
# UNIFICACIÓN DE MANAGERS - Limpieza.
# --------------------------------------------------------------------------------------
def clean_unified_managers(df: pd.DataFrame) -> pd.DataFrame:

    list_columns = df.columns
    df_cleaned = pd.DataFrame()

    df_cleaned["Name"] = df["ManagerName"]
    df_cleaned["ShortName"] = df["short_name_ss"] if "short_name_ss" in list_columns else df["match_name_sw"] if "match_name_sw" in list_columns else np.nan
    df_cleaned["FirstName"] = df["first_name_sw"] if "first_name_sw" in list_columns else np.nan
    df_cleaned["SecondName"] = df["last_name_sw"] if "last_name_sw" in list_columns else np.nan
    df_cleaned["ShortFirstName"] = df["short_first_name_sw"] if "short_first_name_sw" in list_columns else np.nan
    df_cleaned["ShortSecondName"] = df["short_last_name_sw"] if "short_last_name_sw" in list_columns else np.nan
    df_cleaned["Country"] = df["country_ss"] if "country_ss" in list_columns else df["nationality_sw"] if "nationality_sw" in list_columns else np.nan
    df_cleaned["DateBirth"] = df["date_birth_ss"] if "date_birth_ss" in list_columns else np.nan
    df_cleaned["Position"] = df["type_sw"] if "type_sw" in list_columns else np.nan
    df_cleaned["Matches"] = df["matches_ss"] if "matches_ss" in list_columns else np.nan
    df_cleaned["Wins"] = df["wins_ss"] if "wins_ss" in list_columns else np.nan
    df_cleaned["Draws"] = df["draws_ss"] if "draws_ss" in list_columns else np.nan
    df_cleaned["Losses"] = df["losses_ss"] if "losses_ss" in list_columns else np.nan
    df_cleaned["GoalsFor"] = df["goals_scored_ss"] if "goals_scored_ss" in list_columns else np.nan
    df_cleaned["GoalsAgainst"] = df["goals_conceded_ss"] if "goals_conceded_ss" in list_columns else np.nan
    df_cleaned["Points"] = df["points_ss"] if "points_ss" in list_columns else np.nan

    df_cleaned["IdSS"] = df["id_ss"] if "id_ss" in list_columns else np.nan
    df_cleaned["IdSW"] = df["id_sw"] if "id_sw" in list_columns else np.nan

    df_cleaned["DateBirth"] = pd.to_datetime(df_cleaned["DateBirth"], unit="s", errors="coerce").dt.strftime("%d/%m/%Y")
    for col in ["Matches", "Wins", "Draws", "Losses", "GoalsFor", "GoalsAgainst", "Points", "IdSS", "IdSW"]:
        df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors="coerce").astype("Int64")

    df_cleaned.insert(0, "Slug", df_cleaned["Name"].apply(create_slug))
    return df_cleaned

# --------------------------------------------------------------------------------------
# UNIFICACIÓN DE MANAGERS - Creación del Dataframe.
# --------------------------------------------------------------------------------------
def create_managers_info_df(teams_df: pd.DataFrame, sw_managers_df: pd.DataFrame, ss_managers_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:

    if sw_managers_df is not None and not sw_managers_df.empty:
        sw_managers_df = sw_managers_df.copy()
        sw_managers_df["manager_name"] = (sw_managers_df["short_first_name"].fillna("") + " " + sw_managers_df["short_last_name"].fillna("")).str.strip()
        list_sw = sw_managers_df["manager_name"].dropna().tolist()
    else:
        list_sw = []

    list_ss = ss_managers_df["name"].dropna().tolist() if ss_managers_df is not None and not ss_managers_df.empty else []

    matched_managers = match_players(sw_list=list_sw, ss_list=list_ss)
    managers_df = unify_managers_info(matched_managers=matched_managers, sw_df=sw_managers_df, ss_df=ss_managers_df)
    managers_df = clean_unified_managers(df=managers_df)

    managers_name_dict = managers_df.set_index("IdSS")["Slug"].dropna().to_dict() if not managers_df.empty else {}
    teams_df = teams_df.copy()
    teams_df["ManagerCodeSS"] = teams_df["ManagerCodeSS"].map(managers_name_dict)
    teams_df = teams_df.rename(columns={"ManagerCodeSS": "Manager"})

    return teams_df, managers_df

# --------------------------------------------------------------------------------------
# ESTADIOS
# --------------------------------------------------------------------------------------
def create_venues_info_df(teams_df: pd.DataFrame, ss_venues_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:

    if ss_venues_df is None or ss_venues_df.empty:
        return teams_df, pd.DataFrame()

    venues_df = ss_venues_df.copy()
    venues_df.columns = ["IdSS", "Name", "Capacity", "City", "Latitude", "Longitude"]
    venues_df = venues_df[["Name", "Capacity", "City", "Latitude", "Longitude", "IdSS"]]
    venues_df["IdSS"] = pd.to_numeric(venues_df["IdSS"], errors="coerce").astype("Int64")
    venues_df.insert(0, "Slug", venues_df["Name"].apply(create_slug))

    venues_name_dict = venues_df.set_index("IdSS")["Slug"].dropna().to_dict()
    teams_df = teams_df.copy()
    teams_df["VenueCodeSS"] = teams_df["VenueCodeSS"].map(venues_name_dict)
    teams_df = teams_df.rename(columns={"VenueCodeSS": "Venue"})

    return teams_df, venues_df

# --------------------------------------------------------------------------------------
# PARTIDOS - Limpieza del Dataframe.
# --------------------------------------------------------------------------------------
def clean_unified_matches(df: pd.DataFrame) -> pd.DataFrame:

    list_columns = df.columns
    df_cleaned = pd.DataFrame()

    df_cleaned["Slug"] = df["Slug"]
    df_cleaned["Round"] = df["round_ss"] if "round_ss" in list_columns else df["week_sw"] if "week_sw" in list_columns else np.nan
    df_cleaned["Date_"] = df["date_time_ss"] if "date_time_ss" in list_columns else np.nan
    df_cleaned["HomeTeam"] = df["HomeTeam_ss"] if "HomeTeam_ss" in list_columns else df["HomeTeam_sw"]
    df_cleaned["AwayTeam"] = df["AwayTeam_ss"] if "AwayTeam_ss" in list_columns else df["AwayTeam_sw"]
    df_cleaned["Winner"] = df["winner_ss"] if "winner_ss" in list_columns else np.nan
    df_cleaned["HomeScore"] = df["home_score_ss"] if "home_score_ss" in list_columns else np.nan
    df_cleaned["AwayScore"] = df["away_score_ss"] if "away_score_ss" in list_columns else np.nan
    df_cleaned["HomeScoreHT"] = df["ht_home_score_sw"] if "ht_home_score_sw" in list_columns else np.nan
    df_cleaned["AwayScoreHT"] = df["ht_away_score_sw"] if "ht_away_score_sw" in list_columns else np.nan
    df_cleaned["Attendance"] = df["attendance_ss"] if "attendance_ss" in list_columns else df["attendance_sw"] if "attendance_sw" in list_columns else np.nan

    df_cleaned["IdSS"] = df["match_id_ss"] if "match_id_ss" in list_columns else np.nan
    df_cleaned["IdSW"] = df["match_id_sw"] if "match_id_sw" in list_columns else np.nan

    dt_series = pd.to_datetime(df_cleaned["Date_"], unit="s", errors="coerce")
    df_cleaned.insert(2, "Date", dt_series.dt.strftime("%d/%m/%Y"))
    df_cleaned.insert(3, "Time", dt_series.dt.strftime("%H:%M"))
    df_cleaned = df_cleaned.drop(columns=["Date_"])

    for col in ["IdSS", "IdSW", "Round", "HomeScore", "AwayScore", "HomeScoreHT", "AwayScoreHT", "Attendance"]:
        df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors="coerce").astype("Int64")

    df_cleaned["Winner"] = np.select([df_cleaned["Winner"] == 1, df_cleaned["Winner"] == 2, df_cleaned["Winner"] == 3], [df_cleaned["HomeTeam"], df_cleaned["AwayTeam"], "x"], default=np.nan)
    return df_cleaned

# --------------------------------------------------------------------------------------
# PARTIDOS - Creación del Dataframe de todos los partidos.
# --------------------------------------------------------------------------------------
def create_matches_info_df(teams_df: pd.DataFrame, ss_matches_info_df: pd.DataFrame, sw_matches_info_df: pd.DataFrame) -> pd.DataFrame:

    if ss_matches_info_df is None or ss_matches_info_df.empty:
        return pd.DataFrame()

    ss = ss_matches_info_df.copy()
    teams_names_dict_ss = teams_df.set_index("IdSS")["Slug"].dropna().to_dict()
    ss["HomeTeam"] = ss["home_team"].map(teams_names_dict_ss)
    ss["AwayTeam"] = ss["away_team"].map(teams_names_dict_ss)
    ss = ss.rename(columns={c: f"{c}_ss" for c in ss.columns})
    ss["Slug"] = ss["HomeTeam_ss"] + "_" + ss["AwayTeam_ss"]

    if sw_matches_info_df is not None and not sw_matches_info_df.empty:
        sw = sw_matches_info_df.copy()
        teams_names_dict_sw = teams_df.set_index("IdSW")["Slug"].dropna().to_dict()
        sw["HomeTeam"] = sw["home_team_id"].map(teams_names_dict_sw)
        sw["AwayTeam"] = sw["away_team_id"].map(teams_names_dict_sw)
        sw = sw.rename(columns={c: f"{c}_sw" for c in sw.columns})
        sw["Slug"] = sw["HomeTeam_sw"] + "_" + sw["AwayTeam_sw"]

        unified_matches_df = ss.merge(sw, how="left", on="Slug")
    else:
        unified_matches_df = ss

    return clean_unified_matches(df=unified_matches_df)

# --------------------------------------------------------------------------------------
# CLASIFICACIONES - Limpieza.
# --------------------------------------------------------------------------------------
def clean_standing(df: pd.DataFrame, rank_status: bool = True) -> pd.DataFrame:

    df_cleaned = pd.DataFrame()
    df_cleaned["Team"] = df["Team"]

    cols_map = {"Rank": ["rank_sw", "position_ss", "idx_fm"], "Matches": ["played_fm", "matches_ss", "matchesPlayed_sw"], "Wins": ["wins_fm", "wins_ss", "matchesWon_sw"],
                "Losses": ["losses_fm", "losses_ss", "matchesLost_sw"], "Draws": ["draws_fm", "draws_ss", "matchesDrawn_sw"], "Points": ["pts_fm", "points_ss", "points_sw"],
                "GoalsFor": ["scores_for_ss", "goalsFor_sw"], "GoalsAgainst": ["scores_against_ss", "goalsAgainst_sw"]}

    for new_col, possible_cols in cols_map.items():
        existing_cols = [c for c in possible_cols if c in df.columns]
        if existing_cols:
            df_cleaned[new_col] = df[existing_cols].bfill(axis=1).iloc[:, 0]
        else:
            df_cleaned[new_col] = np.nan

    num_cols = ["Rank", "Matches", "Wins", "Losses", "Draws", "GoalsFor", "GoalsAgainst", "Points"]
    for col in num_cols:
        df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors="coerce").astype("Int64")

    df_cleaned["GoalDiff"] = df_cleaned["GoalsFor"] - df_cleaned["GoalsAgainst"]
    df_cleaned["Team"] = df_cleaned["Team"].apply(create_slug)

    if rank_status:
        df_cleaned.insert(2, "Status", df["promotion_ss"] if "promotion_ss" in df.columns else np.nan)

    return df_cleaned.sort_values(by="Rank", na_position="last").reset_index(drop=True)

# --------------------------------------------------------------------------------------
# CLASIFICACIONES - Unificación de las tablas.
# --------------------------------------------------------------------------------------
def unified_standings_tables(matched_teams: pd.DataFrame, fm: pd.DataFrame, ss: pd.DataFrame, sw: pd.DataFrame, rank_status: bool = True) -> pd.DataFrame:

    dfs = []

    if fm is not None and not fm.empty:
        fm = fm.copy()
        fm_dict = matched_teams.set_index("fotmob")["team"].dropna().to_dict()
        fm["Team"] = fm["name"].map(fm_dict)
        fm = fm.rename(columns={c: f"{c}_fm" for c in fm.columns if c != "Team"})
        dfs.append(fm)

    if ss is not None and not ss.empty:
        ss = ss.copy()
        ss_dict = matched_teams.set_index("sofascore")["team"].dropna().to_dict()
        ss["Team"] = ss["team"].map(ss_dict)
        ss = ss.rename(columns={c: f"{c}_ss" for c in ss.columns if c != "Team"})
        dfs.append(ss)

    if sw is not None and not sw.empty:
        sw = sw.copy()
        sw_dict = matched_teams.set_index("scoresway")["team"].dropna().to_dict()
        sw["Team"] = sw["contestantClubName"].map(sw_dict)
        sw = sw.rename(columns={c: f"{c}_sw" for c in sw.columns if c != "Team"})
        dfs.append(sw)

    if len(dfs) == 0:
        unified_standing = pd.DataFrame(columns=["Team"])
    elif len(dfs) == 1:
        unified_standing = dfs[0]
    else:
        unified_standing = dfs[0]
        for df_ in dfs[1:]:
            unified_standing = unified_standing.merge(df_, on="Team", how="outer")

    return clean_standing(unified_standing, rank_status=rank_status)

# --------------------------------------------------------------------------------------
# CLASIFICACIONES - Clasificación esperada.
# --------------------------------------------------------------------------------------
def create_expected_standing_table(matched_teams: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:

    if df is None or df.empty:
        return pd.DataFrame()

    df_clean = df.copy()
    drop_cols = ["shortName", "id", "pageUrl", "ongoing", "played", "wins", "draws", "losses", "scoresStr", "goalConDiff", "pts", "qualColor", "teamId", "teamName", "idx"]
    df_clean = df_clean.drop(columns=[c for c in drop_cols if c in df_clean.columns], errors="ignore")

    fm_dict = matched_teams.set_index("fotmob")["team"].dropna().to_dict()
    df_clean["Team"] = df_clean["name"].map(fm_dict)

    df_clean.columns = ["name", "ExpectedGoalsFor", "ExpectedGoalsAgainst", "ExpectedPoints", "Rank", "ExpectedGoalsForDiff", "ExpectedGoalsAgainstDiff", "ExpectedPointsDiff", "ExpectedRank", "ExpectedRankDiff", "Team"]
    df_clean = df_clean[["Team", "Rank", "ExpectedRank", "ExpectedRankDiff", "ExpectedPoints", "ExpectedPointsDiff", "ExpectedGoalsFor", "ExpectedGoalsAgainst", "ExpectedGoalsAgainstDiff", "ExpectedPointsDiff"]]
    df_clean["Team"] = df_clean["Team"].apply(create_slug)

    return df_clean.sort_values(by="ExpectedRank", na_position="last").reset_index(drop=True)

# --------------------------------------------------------------------------------------
# GOLES - Añadimos los goles en caso de que no se hayan añadido en el Dataframe.
# --------------------------------------------------------------------------------------
def add_goals(matches_df: pd.DataFrame, team_stats_df: pd.DataFrame) -> pd.DataFrame:

    if matches_df is None or matches_df.empty or team_stats_df is None or team_stats_df.empty:
        return team_stats_df

    goals_df = matches_df[["Slug", "HomeScore", "AwayScore"]]
    team_stats_df = team_stats_df.merge(goals_df, left_on="Match", right_on="Slug", how="left")

    team_stats_df["Goals"] = np.where(team_stats_df["HomeAway"] == "h", team_stats_df["HomeScore"], np.where(team_stats_df["HomeAway"] == "a", team_stats_df["AwayScore"], np.nan))
    team_stats_df["GoalsConceded"] = np.where(team_stats_df["HomeAway"] == "h", team_stats_df["AwayScore"], np.where(team_stats_df["HomeAway"] == "a", team_stats_df["HomeScore"], np.nan))

    team_stats_df["Goals"] = pd.to_numeric(team_stats_df["Goals"], errors="coerce").astype("Int64")
    team_stats_df["GoalsConceded"] = pd.to_numeric(team_stats_df["GoalsConceded"], errors="coerce").astype("Int64")

    return team_stats_df.drop(columns=["HomeScore", "AwayScore", "Slug"])

# --------------------------------------------------------------------------------------
# ESTADÍSTICAS DE EQUIPO
# --------------------------------------------------------------------------------------
def team_stats_proc(df: pd.DataFrame, managers_dict: dict, cols_map: dict, cols_order: list) -> pd.DataFrame:

    if df is None or df.empty:
        return pd.DataFrame(columns=cols_order)

    df_cleaned = pd.DataFrame()
    list_columns = df.columns

    df_cleaned["Match"] = df["MatchSlug"]
    df_cleaned["Team"] = df["Team"]
    df_cleaned["Opponent"] = df["Opponent"] if "Opponent" in list_columns else np.nan

    df_cleaned["HomeAway"] = df["ha_ss"] if "ha_ss" in list_columns else df["ha_sw"] if "ha_sw" in list_columns else np.nan
    df_cleaned["Kit"] = df["kit_sw"] if "kit_sw" in list_columns else np.nan
    df_cleaned["Formation"] = df["formation_sw"] if "formation_sw" in list_columns else np.nan
    df_cleaned["Manager"] = df["manager_sw"].map(managers_dict) if "manager_sw" in list_columns else np.nan
    df_cleaned["AverageAge"] = df["average_age_sw"] if "average_age_sw" in list_columns else np.nan

    for col, possible_cols in cols_map.items():
        existing_cols = [c for c in possible_cols if c in list_columns]
        if existing_cols:
            df_cleaned[col] = df[existing_cols].bfill(axis=1).iloc[:, 0]
        else:
            df_cleaned[col] = np.nan

    not_integer_cols = ["Match", "Team", "Opponent", "HomeAway", "Kit", "Formation", "Manager", "AverageAge", "ExpectedGoals", "GoalsPrevented"]
    for c in df_cleaned.columns:
        if c not in not_integer_cols:
            df_cleaned[c] = pd.to_numeric(df_cleaned[c], errors="coerce").astype("Int64")

    not_fill_na_cols = ["Match", "Team", "Opponent", "HomeAway", "Kit", "Formation", "Manager", "AverageAge"]
    fill_na_cols = [c for c in df_cleaned.columns if c not in not_fill_na_cols]
    df_cleaned[fill_na_cols] = df_cleaned[fill_na_cols].fillna(0)

    df_cleaned["Formation"] = df_cleaned["Formation"].apply(lambda x: "-".join(re.sub(r"[^0-9]", "", str(x).replace(".0", ""))) if pd.notna(x) and re.sub(r"[^0-9]", "", str(x).replace(".0", "")) != "" else np.nan)

    for col in cols_order:
        if col not in df_cleaned.columns:
            df_cleaned[col] = np.nan

    return df_cleaned[cols_order]

# --------------------------------------------------------------------------------------
# ESTADÍSTICAS DE JUGADOR
# --------------------------------------------------------------------------------------
def player_stats_proc(df: pd.DataFrame, cols_map: dict, cols_order: list, positions_dict: dict) -> pd.DataFrame:

    if df is None or df.empty:
        return pd.DataFrame(columns=cols_order)

    df_cleaned = pd.DataFrame()
    list_columns = df.columns

    df_cleaned["Match"] = df["MatchSlug"]
    df_cleaned["Team"] = df["Team"]
    df_cleaned["Opponent"] = df["Opponent"] if "Opponent" in list_columns else np.nan
    df_cleaned["Player"] = df["Player"]

    df_cleaned["HomeAway"] = df["ha_ss"] if "ha_ss" in list_columns else df["ha_sw"] if "ha_sw" in list_columns else np.nan
    df_cleaned["Starter"] = df["starter_ss"] if "starter_ss" in list_columns else np.nan
    df_cleaned["ShirtNumber"] = df["shirtNumber_sw"] if "shirtNumber_sw" in list_columns else np.nan
    df_cleaned["Position"] = df["position_sw"] if "position_sw" in list_columns else ""
    df_cleaned["PositionSide"] = df["positionSide_sw"] if "positionSide_sw" in list_columns else ""
    df_cleaned["SubPosition"] = df["subPosition_sw"] if "subPosition_sw" in list_columns else ""

    df_cleaned["Position"] = np.where(df_cleaned["Position"] == "Substitute", df_cleaned["SubPosition"], df_cleaned["Position"].fillna("") + " " + df_cleaned["PositionSide"].fillna(""))
    df_cleaned["Position"] = df_cleaned["Position"].replace("", "Undefined")
    df_cleaned["Position"] = df_cleaned["Position"].map(positions_dict).fillna("Undefined")

    for col, possible_cols in cols_map.items():
        existing_cols = [c for c in possible_cols if c in list_columns]
        if existing_cols:
            df_cleaned[col] = df[existing_cols].bfill(axis=1).iloc[:, 0]
        else:
            df_cleaned[col] = np.nan

    not_integer_cols = ["Match", "Team", "Opponent", "Player", "HomeAway", "Starter", "Position", "PositionSide", "SubPosition", "Rating", "ExpectedAssists", "TotalBallCarriesDistance", "TotalProgression", 
                        "BestBallCarryProgression", "TotalProgressiveBallCarriesDistance", "PassValue", "DribbleValue", "DefensiveValue", "ExpectedGoals", "ShotValue", "ExpectedGoalsOnTarget", 
                        "KeeperSaveValue", "GoalkeeperValue", "GoalsPrevented"]
    for c in df_cleaned.columns:
        if c not in not_integer_cols:
            df_cleaned[c] = pd.to_numeric(df_cleaned[c], errors="coerce").astype("Int64")

    not_fill_na_cols = ["Match", "Team", "Opponent", "Player", "HomeAway", "Starter", "Position", "PositionSide", "SubPosition", "ShirtNumber"]
    fill_na_cols = [c for c in df_cleaned.columns if c not in not_fill_na_cols]
    df_cleaned[fill_na_cols] = df_cleaned[fill_na_cols].fillna(0)

    for col in cols_order:
        if col not in df_cleaned.columns:
            df_cleaned[col] = np.nan

    return df_cleaned[cols_order]

# --------------------------------------------------------------------------------------
# UNIFICACIÓN DE ESTADÍSTICAS DE PARTIDO
# --------------------------------------------------------------------------------------
def matches_proc(matches_df: pd.DataFrame, players_df: pd.DataFrame, teams_df: pd.DataFrame, managers_df: pd.DataFrame, sw_team: pd.DataFrame, ss_team: pd.DataFrame, sw_player: pd.DataFrame, ss_player: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:

    ss_team_dict = teams_df.set_index("IdSS")["Slug"].dropna().to_dict() if not teams_df.empty else {}
    sw_team_dict = teams_df.set_index("IdSW")["Slug"].dropna().to_dict() if not teams_df.empty else {}
    ss_player_dict = players_df.set_index("IdSS")["Slug"].dropna().to_dict() if not players_df.empty else {}
    sw_player_dict = players_df.set_index("IdSW")["Slug"].dropna().to_dict() if not players_df.empty else {}
    sw_managers_dict = managers_df.set_index("IdSW")["Slug"].dropna().to_dict() if not managers_df.empty and "IdSW" in managers_df.columns else {}

    teams_stats_list = []
    players_stats_list = []

    with open(os.path.join(utils, "team_stats_proc", "cols_map.json"), "r", encoding="utf-8") as f:
        teams_cols_map = jsonlib.load(f)
    with open(os.path.join(utils, "team_stats_proc", "cols_order.json"), "r", encoding="utf-8") as f:
        teams_cols_order = jsonlib.load(f)
    with open(os.path.join(utils, "player_stats_proc", "cols_map.json"), "r", encoding="utf-8") as f:
        players_cols_map = jsonlib.load(f)
    with open(os.path.join(utils, "player_stats_proc", "cols_order.json"), "r", encoding="utf-8") as f:
        players_cols_order = jsonlib.load(f)
    with open(os.path.join(utils, "player_stats_proc", "positions_map.json"), "r", encoding="utf-8") as f:
        positions_dict = jsonlib.load(f)

    if matches_df is None or matches_df.empty:
        return pd.DataFrame(columns=teams_cols_order), pd.DataFrame(columns=players_cols_order)

    for _, row in matches_df.iterrows():
        id_ss = row["IdSS"]
        id_sw = row["IdSW"]
        slug = row["Slug"]

        dfs_teams = []
        dfs_players = []

        ss_part_team = ss_team[ss_team["match_id"] == id_ss] if ss_team is not None and pd.notna(id_ss) else None
        if ss_part_team is not None and not ss_part_team.empty:
            ss_part_team = ss_part_team.copy().rename(columns={c: f"{c}_ss" for c in ss_part_team.columns})
            ss_part_team.insert(0, "MatchSlug", slug)
            ss_part_team.insert(1, "Team", ss_part_team["team_id_ss"].map(ss_team_dict))
            ss_part_team.insert(2, "Opponent", ss_part_team["opponent_team_id_ss"].map(ss_team_dict))
            dfs_teams.append(ss_part_team)

        sw_part_team = sw_team[sw_team["match_id"] == id_sw] if sw_team is not None and pd.notna(id_sw) else None
        if sw_part_team is not None and not sw_part_team.empty:
            sw_part_team = sw_part_team.copy().rename(columns={c: f"{c}_sw" for c in sw_part_team.columns})
            sw_part_team.insert(0, "MatchSlug", slug)
            sw_part_team.insert(1, "Team", sw_part_team["team_id_sw"].map(sw_team_dict))
            sw_part_team.insert(2, "Opponent", np.nan)
            dfs_teams.append(sw_part_team)

        ss_part_player = ss_player[ss_player["match_id"] == id_ss] if ss_player is not None and pd.notna(id_ss) else None
        if ss_part_player is not None and not ss_part_player.empty:
            ss_part_player = ss_part_player.copy().rename(columns={c: f"{c}_ss" for c in ss_part_player.columns})
            ss_part_player.insert(0, "MatchSlug", slug)
            ss_part_player.insert(1, "Team", ss_part_player["team_id_ss"].map(ss_team_dict))
            ss_part_player.insert(2, "Opponent", ss_part_player["opponent_team_id_ss"].map(ss_team_dict))
            ss_part_player.insert(3, "Player", ss_part_player["player_id_ss"].map(ss_player_dict))
            dfs_players.append(ss_part_player)

        sw_part_player = sw_player[sw_player["match_id"] == id_sw] if sw_player is not None and pd.notna(id_sw) else None
        if sw_part_player is not None and not sw_part_player.empty:
            sw_part_player = sw_part_player.copy().rename(columns={c: f"{c}_sw" for c in sw_part_player.columns})
            sw_part_player.insert(0, "MatchSlug", slug)
            sw_part_player.insert(1, "Team", sw_part_player["team_id_sw"].map(sw_team_dict))
            sw_part_player.insert(2, "Opponent", np.nan)
            sw_part_player.insert(3, "Player", sw_part_player["playerId_sw"].map(sw_player_dict))
            dfs_players.append(sw_part_player)

        if len(dfs_teams) == 2:
            raw_team_stats_df = dfs_teams[0].merge(dfs_teams[1], how="outer", on=["MatchSlug", "Team"], suffixes=("", "_dup"))
            raw_team_stats_df["Opponent"] = raw_team_stats_df["Opponent"].combine_first(raw_team_stats_df.get("Opponent_dup"))
            raw_team_stats_df = raw_team_stats_df.drop(columns=[c for c in raw_team_stats_df.columns if c.endswith("_dup")], errors="ignore")
        elif len(dfs_teams) == 1:
            raw_team_stats_df = dfs_teams[0]
        else:
            raw_team_stats_df = None

        if len(dfs_players) == 2:
            raw_player_stats_df = dfs_players[0].merge(dfs_players[1], how="outer", on=["MatchSlug", "Team", "Player"], suffixes=("", "_dup"))
            raw_player_stats_df["Opponent"] = raw_player_stats_df["Opponent"].combine_first(raw_player_stats_df.get("Opponent_dup"))
            raw_player_stats_df = raw_player_stats_df.drop(columns=[c for c in raw_player_stats_df.columns if c.endswith("_dup")], errors="ignore")
        elif len(dfs_players) == 1:
            raw_player_stats_df = dfs_players[0]
        else:
            raw_player_stats_df = None

        clean_team_stats_df = team_stats_proc(df=raw_team_stats_df, managers_dict=sw_managers_dict, cols_map=teams_cols_map, cols_order=teams_cols_order)
        clean_player_stats_df = player_stats_proc(df=raw_player_stats_df, cols_map=players_cols_map, cols_order=players_cols_order, positions_dict=positions_dict)

        if not clean_team_stats_df.empty:
            teams_stats_list.append(clean_team_stats_df)
        if not clean_player_stats_df.empty:
            players_stats_list.append(clean_player_stats_df)

    team_stats_df = pd.concat(teams_stats_list, ignore_index=True) if teams_stats_list else pd.DataFrame(columns=teams_cols_order)
    player_stats_df = pd.concat(players_stats_list, ignore_index=True) if players_stats_list else pd.DataFrame(columns=players_cols_order)

    return team_stats_df, player_stats_df

# --------------------------------------------------------------------------------------
# ESTADÍSTICAS DE TEMPORADA POR EQUIPO
# --------------------------------------------------------------------------------------
def season_stats_team(teams_df: pd.DataFrame, team_stats_df: pd.DataFrame) -> pd.DataFrame:

    if teams_df is None or teams_df.empty or team_stats_df is None or team_stats_df.empty:
        return pd.DataFrame()

    teams_season_df_list = []
    for team in teams_df["Slug"].dropna().unique().tolist():
        single_team_stats = team_stats_df[team_stats_df["Team"] == team]
        matches = len(single_team_stats)

        if matches == 0:
            continue

        df = pd.DataFrame()
        df["Team"] = [team]
        df["Matches"] = matches
        formations = single_team_stats["Formation"].mode()
        df["Formation"] = formations.iloc[0] if len(formations) > 0 else np.nan

        cols_to_sum = ["Goals", "GoalsConceded", "ExpectedGoals", "GoalsPrevented", "OwnGoals", "GoalAssist", "CleanSheet", "TotalShots", "ShotsOnTarget", "ShotsOffTarget", "BlockedShots", 
                       "ShotsInsideBox", "ShotsOutsideBox", "HitWoodwork", "BigChances", "TouchesInPenaltyArea", "ThroughBalls", "Crosses", "Dribbles", "BallPossession", "Passes", 
                       "AccuratePasses", "LongBalls", "FinalThirdEntries", "FinalThirdPhase", "CornerKicks", "LostCorners", "ThrowIns", "Offsides", "Fouls", "FoulsWon", "FoulsLost",
                       "YellowCards", "RedCards", "SecondYellow", "Tackles", "TacklesWon", "TotalTackles", "Interceptions", "Recoveries", "Clearances", "Duels", "GroundDuels", "AerialDuels", 
                       "Dispossessed", "FouledFinalThird", "ErrorsLeadToShot", "ErrorsLeadToGoal", "GoalkeeperSaves", "TotalSaves", "BigSaves", "Punches", "HighClaims", "GoalKicks", 
                       "PenaltySaves", "PenaltyWon", "PenaltyConceded", "PenaltyFaced", "PenGoalsConceded", "SubsMade", "SubsGoals"]

        for col in cols_to_sum:
            df[col] = single_team_stats[col].sum() if col in single_team_stats.columns else 0

        df["GoalsPerMatch"] = safe_div(df["Goals"].sum(), matches)
        df["GoalsConcededsPerMatch"] = safe_div(df["GoalsConceded"].sum(), matches)
        df["ExpectedGoalsPerMatch"] = safe_div(df["ExpectedGoals"].sum(), matches)
        df["GoalsPreventedPerMatch"] = safe_div(df["GoalsPrevented"].sum(), matches)

        df["ShotAccuracy"] = safe_div(df["ShotsOnTarget"].sum(), df["TotalShots"].sum())
        df["ShotOffTargetRate"] = safe_div(df["ShotsOffTarget"].sum(), df["TotalShots"].sum())
        df["BlockedShotRate"] = safe_div(df["BlockedShots"].sum(), df["TotalShots"].sum())
        df["GoalConversion"] = safe_div(df["Goals"].sum(), df["TotalShots"].sum())
        df["OnTargetConversion"] = safe_div(df["Goals"].sum(), df["ShotsOnTarget"].sum())
        df["BigChanceRate"] = safe_div(df["BigChances"].sum(), df["TotalShots"].sum())
        df["BigChanceConversion"] = safe_div(df["Goals"].sum(), df["BigChances"].sum())
        df["BoxShotRate"] = safe_div(df["ShotsInsideBox"].sum(), df["TotalShots"].sum())
        df["OutsideShotRate"] = safe_div(df["ShotsOutsideBox"].sum(), df["TotalShots"].sum())
        df["XGPerShot"] = safe_div(df["ExpectedGoals"].sum(), df["TotalShots"].sum())
        df["GoalsMinusXG"] = df["Goals"].sum() - df["ExpectedGoals"].sum()

        df["PassAccuracy"] = safe_div(df["AccuratePasses"].sum(), df["Passes"].sum())
        df["LongBallsPerMatch"] = safe_div(df["LongBalls"].sum(), matches)
        df["CrossesPerMatch"] = safe_div(df["Crosses"].sum(), matches)
        df["FinalThirdEntriesPerMatch"] = safe_div(df["FinalThirdEntries"].sum(), matches)
        df["TouchesInPenaltyAreaPerMatch"] = safe_div(df["TouchesInPenaltyArea"].sum(), matches)
        df["ThroughBallsPerMatch"] = safe_div(df["ThroughBalls"].sum(), matches)
        df["DribblesPerMatch"] = safe_div(df["Dribbles"].sum(), matches)

        df["GoalsConcededPerMatch"] = safe_div(df["GoalsConceded"].sum(), matches)
        df["CleanSheetRate"] = safe_div(df["CleanSheet"].sum(), matches)
        df["TackleSuccess"] = safe_div(df["TacklesWon"].sum(), df["Tackles"].sum())
        df["InterceptionsPerMatch"] = safe_div(df["Interceptions"].sum(), matches)
        df["RecoveriesPerMatch"] = safe_div(df["Recoveries"].sum(), matches)
        df["ClearancesPerMatch"] = safe_div(df["Clearances"].sum(), matches)
        df["DuelsPerMatch"] = safe_div(df["Duels"].sum(), matches)
        df["GroundDuelsPerMatch"] = safe_div(df["GroundDuels"].sum(), matches)
        df["AerialDuelsPerMatch"] = safe_div(df["AerialDuels"].sum(), matches)
        df["ErrorsLeadToShotRate"] = safe_div(df["ErrorsLeadToShot"].sum(), matches)
        df["ErrorsLeadToGoalRate"] = safe_div(df["ErrorsLeadToGoal"].sum(), matches)

        df["PenaltySaveRate"] = safe_div(df["PenaltySaves"].sum(), df["PenaltyFaced"].sum())
        df["GoalkeeperSavesPerMatch"] = safe_div(df["GoalkeeperSaves"].sum(), matches)
        df["TotalSavesPerMatch"] = safe_div(df["TotalSaves"].sum(), matches)

        df["FoulsPerMatch"] = safe_div(df["Fouls"].sum(), matches)
        df["FoulsWonPerMatch"] = safe_div(df["FoulsWon"].sum(), matches)
        df["FoulsLostPerMatch"] = safe_div(df["FoulsLost"].sum(), matches)
        df["YellowCardsPerMatch"] = safe_div(df["YellowCards"].sum(), matches)
        df["RedCardsPerMatch"] = safe_div(df["RedCards"].sum(), matches)
        df["OffsidesPerMatch"] = safe_div(df["Offsides"].sum(), matches)

        df["GoalDifference"] = df["Goals"].sum() - df["GoalsConceded"].sum()
        teams_season_df_list.append(df)

    return pd.concat(teams_season_df_list, ignore_index=True) if teams_season_df_list else pd.DataFrame()

# --------------------------------------------------------------------------------------
# ESTADÍSTICAS DE TEMPORADA POR JUGADOR
# --------------------------------------------------------------------------------------
def season_stats_player(players_df: pd.DataFrame, player_stats_df: pd.DataFrame) -> pd.DataFrame:

    if players_df is None or players_df.empty or player_stats_df is None or player_stats_df.empty:
        return pd.DataFrame()

    players_season_df_list = []
    for _, row in players_df.iterrows():
        player = row["Slug"]

        single_player_stats = player_stats_df[player_stats_df["Player"] == player]
        matches = len(single_player_stats)
        if matches == 0:
            continue

        minutes = single_player_stats["MinutesPlayed"].sum() if "MinutesPlayed" in single_player_stats.columns else 0
        avg_minutes = round(single_player_stats["MinutesPlayed"].mean(), 4) if "MinutesPlayed" in single_player_stats.columns and pd.notna(single_player_stats["MinutesPlayed"].mean()) else np.nan
        per90_factor = safe_div(90, minutes)

        df = pd.DataFrame()
        df["Player"] = [player]
        df["Team"] = row["Team"]
        df["ShirtNumber"] = row["ShirtNumber"]
        positions = single_player_stats["Position"].mode() if "Position" in single_player_stats.columns else pd.Series([], dtype="object")
        df["Position"] = positions.iloc[0] if len(positions) > 0 else np.nan
        df["Matches"] = matches
        df["MatchesStarter"] = single_player_stats["Starter"].sum() if "Starter" in single_player_stats.columns else 0
        df["MatchesBench"] = df["Matches"] - df["MatchesStarter"]
        df["StartsRate"] = safe_div(df["MatchesStarter"].sum(), df["Matches"].sum())
        df["MinutesPlayed"] = minutes
        df["MinutesPerMatch"] = avg_minutes
        df["AvgRating"] = round(single_player_stats["Rating"].mean(), 4) if "Rating" in single_player_stats.columns else np.nan

        cols_to_sum = ["Touches", "Rating", "PossessionLost", "Passes", "AccuratePasses", "LongBalls", "AccurateLongBalls", "AccurateOwnHalfPasses", "TotalOwnHalfPasses", "TotalOppositionHalfPasses", 
                       "AccurateOppositionHalfPasses", "Crosses", "AccurateCrosses", "KeyPasses", "GoalAssist", "ExpectedAssists", "BallCarriesCount", "TotalBallCarriesDistance", "TotalProgression", 
                       "BestBallCarryProgression", "ProgressiveBallCarriesCount", "TotalProgressiveBallCarriesDistance", "TotalShots", "ShotsOnTarget", "ShotsOffTarget", "BlockedShots",
                       "ExpectedGoals", "ExpectedGoalsOnTarget", "BigChanceCreated", "BigChanceMissed", "Goals", "HitWoodwork", "ShotValue", "DuelsWon", "DuelsLost", "AerialWon", "AerialLost", 
                       "Tackles", "TacklesWon", "Interceptions", "Recoveries", "Clearances", "LastManTackle", "OutfielderBlocks", "ErrorsLeadToShot", "ErrorsLeadToGoal", "Saves", "SavedShotsFromInsideTheBox", 
                       "GoalsPrevented", "KeeperSaveValue", "GoalkeeperValue", "Punches", "HighClaims", "ClearanceOffLine", "KeeperSweeperActions", "AccurateKeeperSweeperActions", "GoalKicks",
                       "GoalsConceded", "CleanSheet", "Fouls", "WasFouled", "Offsides", "YellowCards", "RedCards", "SecondYellow", "PenaltyWon", "PenaltyConceded", "PenaltyFaced", "PenaltySave", 
                       "PenaltyMiss", "PenGoalsConceded", "OwnGoals", "CrossNotClaimed", "ThrowIns", "CornerKicks", "LostCorners"]

        for c in cols_to_sum:
            total_value = single_player_stats[c].sum() if c in single_player_stats.columns else 0
            df[c] = total_value
            df[f"{c}Per90"] = total_value * per90_factor if pd.notna(per90_factor) else np.nan

        df["PassAccuracy"] = safe_div(df["AccuratePasses"].sum(), df["Passes"].sum())
        df["LongBallAccuracy"] = safe_div(df["AccurateLongBalls"].sum(), df["LongBalls"].sum())
        df["OwnHalfPassAccuracy"] = safe_div(df["AccurateOwnHalfPasses"].sum(), df["TotalOwnHalfPasses"].sum())
        df["OppositionHalfPassAccuracy"] = safe_div(df["AccurateOppositionHalfPasses"].sum(), df["TotalOppositionHalfPasses"].sum())
        df["CrossAccuracy"] = safe_div(df["AccurateCrosses"].sum(), df["Crosses"].sum())

        df["KeyPassesPerPass"] = safe_div(df["KeyPasses"].sum(), df["Passes"].sum())
        df["ExpectedAssistsPerKeyPass"] = safe_div(df["ExpectedAssists"].sum(), df["KeyPasses"].sum())
        df["ProgressiveCarriesShare"] = safe_div(df["ProgressiveBallCarriesCount"].sum(), df["BallCarriesCount"].sum())
        df["AvgCarryDistance"] = safe_div(df["TotalBallCarriesDistance"].sum(), df["BallCarriesCount"].sum())
        df["AvgProgressionPerCarry"] = safe_div(df["TotalProgression"].sum(), df["BallCarriesCount"].sum())
        df["AvgProgressiveCarryDistance"] = safe_div(df["TotalProgressiveBallCarriesDistance"].sum(), df["ProgressiveBallCarriesCount"].sum())

        df["ShotAccuracy"] = safe_div(df["ShotsOnTarget"].sum(), df["TotalShots"].sum())
        df["ShotOffTargetRate"] = safe_div(df["ShotsOffTarget"].sum(), df["TotalShots"].sum())
        df["BlockedShotRate"] = safe_div(df["BlockedShots"].sum(), df["TotalShots"].sum())
        df["GoalConversion"] = safe_div(df["Goals"].sum(), df["TotalShots"].sum())
        df["OnTargetConversion"] = safe_div(df["Goals"].sum(), df["ShotsOnTarget"].sum())
        df["XGPerShot"] = safe_div(df["ExpectedGoals"].sum(), df["TotalShots"].sum())
        df["GoalsMinusXG"] = round(df["Goals"].sum() - df["ExpectedGoals"].sum(), 4) if pd.notna(df["Goals"].sum()) and pd.notna(df["ExpectedGoals"].sum()) else np.nan
        df["BigChanceMissRate"] = safe_div(df["BigChanceMissed"].sum(), df["BigChanceMissed"].sum() + df["Goals"].sum())
        df["BigChanceCreateToAssist"] = safe_div(df["GoalAssist"].sum(), df["BigChanceCreated"].sum())
        df["DuelWinRate"] = safe_div(df["DuelsWon"].sum(), df["DuelsWon"].sum() + df["DuelsLost"].sum())
        df["AerialWinRate"] = safe_div(df["AerialWon"].sum(), df["AerialWon"].sum() + df["AerialLost"].sum())

        df["TackleSuccess"] = safe_div(df["TacklesWon"].sum(), df["Tackles"].sum())
        df["RecoveriesPerTouch"] = safe_div(df["Recoveries"].sum(), df["Touches"].sum())
        df["InterceptionsPlusRecoveries"] = df["Interceptions"].sum() + df["Recoveries"].sum()
        df["DefensiveActions"] = df["Tackles"].sum() + df["Interceptions"].sum() + df["Recoveries"].sum() + df["Clearances"].sum() + df["OutfielderBlocks"].sum()

        df["FoulsPerWasFouled"] = safe_div(df["Fouls"].sum(), df["WasFouled"].sum())
        df["PossessionLostPerTouch"] = safe_div(df["PossessionLost"].sum(), df["Touches"].sum())

        df["SaveRate"] = safe_div(df["Saves"].sum(), df["Saves"].sum() + df["GoalsConceded"].sum())
        df["PenaltySaveRate"] = safe_div(df["PenaltySave"].sum(), df["PenaltyFaced"].sum())
        df["GoalsConcededPerSave"] = safe_div(df["GoalsConceded"].sum(), df["Saves"].sum())

        df["GoalContributions"] = df["Goals"].sum() + df["GoalAssist"].sum()
        df["GoalContributionsPerMatch"] = safe_div(df["GoalContributions"].sum(), df["Matches"].sum())
        df["GoalContributionsPerStart"] = safe_div(df["GoalContributions"].sum(), df["MatchesStarter"].sum())

        players_season_df_list.append(df)

    return pd.concat(players_season_df_list, ignore_index=True) if players_season_df_list else pd.DataFrame()

# --------------------------------------------------------------------------------------
# IMÁGENES
# --------------------------------------------------------------------------------------
def images_proc(players_df: pd.DataFrame, teams_df: pd.DataFrame, managers_df: pd.DataFrame, venues_df: pd.DataFrame, images_path: str, processed_data_path: str) -> None:

    default_images_path = os.path.join(utils, "default_images")
    default_player = os.path.join(default_images_path, "player.png")
    default_team = os.path.join(default_images_path, "team.png")
    default_manager = os.path.join(default_images_path, "manager.png")
    default_venue = os.path.join(default_images_path, "venue.png")

    players_images_path = os.path.join(images_path, "player")
    teams_images_path = os.path.join(images_path, "team")
    managers_images_path = os.path.join(images_path, "manager")
    venues_images_path = os.path.join(images_path, "venue")

    out_images_path = os.path.join(processed_data_path, "images")
    os.makedirs(out_images_path, exist_ok=True)

    out_players_images = os.path.join(out_images_path, "player")
    out_teams_images = os.path.join(out_images_path, "team")
    out_managers_images = os.path.join(out_images_path, "manager")
    out_venues_images = os.path.join(out_images_path, "venue")

    for p in [out_players_images, out_teams_images, out_managers_images, out_venues_images]:
        os.makedirs(p, exist_ok=True)

    def copy_valid_or_default(source_path: str, target_path: str, default_path: str) -> None:
        valid_image = False
        if os.path.exists(source_path):
            try:
                with Image.open(source_path) as img:
                    img.verify()
                valid_image = True
            except Exception:
                valid_image = False

        shutil.copy2(source_path if valid_image else default_path, target_path)

    if players_df is not None and not players_df.empty:
        for _, row in players_df.iterrows():
            input_image_path = os.path.join(players_images_path, f"{row['IdSS']}.png")
            output_image_path = os.path.join(out_players_images, f"{row['Slug']}.png")
            copy_valid_or_default(input_image_path, output_image_path, default_player)

    if managers_df is not None and not managers_df.empty:
        for _, row in managers_df.iterrows():
            input_image_path = os.path.join(managers_images_path, f"{row['IdSS']}.png")
            output_image_path = os.path.join(out_managers_images, f"{row['Slug']}.png")
            copy_valid_or_default(input_image_path, output_image_path, default_manager)

    if teams_df is not None and not teams_df.empty:
        for _, row in teams_df.iterrows():
            input_image_path = os.path.join(teams_images_path, f"{row['IdSS']}.png")
            output_image_path = os.path.join(out_teams_images, f"{row['Slug']}.png")
            copy_valid_or_default(input_image_path, output_image_path, default_team)

    if venues_df is not None and not venues_df.empty:
        for _, row in venues_df.iterrows():
            input_image_path = os.path.join(venues_images_path, f"{row['IdSS']}.png")
            output_image_path = os.path.join(out_venues_images, f"{row['Slug']}.png")
            copy_valid_or_default(input_image_path, output_image_path, default_venue)

# --------------------------------------------------------------------------------------
# UNIFICACIÓN DE UNA TEMPORADA
# --------------------------------------------------------------------------------------
def season_data_unification(fotmob_clean_path: str, scoresway_clean_path: str, sofascore_clean_path: str, print_info: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    fm_info_df, fm_matches_df, fm_all_st_df, fm_home_st_df, fm_away_st_df, fm_form_st_df, fm_xg_st_df = read_fotmob_data(fotmob_clean_path=fotmob_clean_path)
    fotmob_teams = obtain_fotmob_teams(matches_df=fm_matches_df, all_st_df=fm_all_st_df, home_st_df=fm_home_st_df, away_st_df=fm_away_st_df, form_st_df=fm_form_st_df, xg_st_df=fm_xg_st_df)

    (sw_managers_df, sw_matches_df, sw_players_df, sw_teams_df, sw_total_st_df, sw_home_st_df, sw_away_st_df,
     sw_httotal_st_df, sw_hthome_st_df, sw_htaway_st_df, sw_formhome_st_df, sw_formaway_st_df, sw_overunder_st_df,
     sw_attendance_st_df, sw_matches_info_df, sw_matches_player_stats_df, sw_matches_team_stats_df, sw_matches_referees_df) = read_scoresway_data(scoresway_clean_path=scoresway_clean_path)

    scoresway_teams = sorted(sw_teams_df["club_name"].dropna().unique().tolist()) if sw_teams_df is not None and not sw_teams_df.empty else []

    (ss_managers_df, ss_players_df, ss_teams_df, ss_venues_df, ss_total_st_df, ss_home_st_df, ss_away_st_df,
     ss_matches_info_df, ss_matches_lineups_df, ss_matches_statistics_df) = read_sofascore_data(sofascore_clean_path=sofascore_clean_path)

    sofascore_teams = sorted(ss_teams_df["name"].dropna().unique().tolist()) if ss_teams_df is not None and not ss_teams_df.empty else []

    matched_teams = match_teams(fm_list=fotmob_teams, sw_list=scoresway_teams, ss_list=sofascore_teams)
    if sw_teams_df is not None and not sw_teams_df.empty:
        sw_long_name_dict = sw_teams_df.set_index("club_name")["name"].dropna().to_dict()
        matched_teams["longname_scoresway"] = matched_teams["scoresway"].map(sw_long_name_dict)
    else:
        matched_teams["longname_scoresway"] = np.nan

    teams_df = create_teams_info_df(matched_teams=matched_teams, sw_teams_df=sw_teams_df, ss_teams_df=ss_teams_df)
    players_df = create_players_info_df(matched_teams=matched_teams, sw_players_df=sw_players_df, ss_players_df=ss_players_df)
    teams_df, managers_df = create_managers_info_df(teams_df=teams_df, sw_managers_df=sw_managers_df, ss_managers_df=ss_managers_df)
    teams_df, venues_df = create_venues_info_df(teams_df=teams_df, ss_venues_df=ss_venues_df)
    matches_df = create_matches_info_df(teams_df=teams_df, ss_matches_info_df=ss_matches_info_df, sw_matches_info_df=sw_matches_info_df)

    if print_info:
        print("        - League information unified")

    all_standings = unified_standings_tables(matched_teams=matched_teams, fm=fm_all_st_df, ss=ss_total_st_df, sw=sw_total_st_df)
    home_standings = unified_standings_tables(matched_teams=matched_teams, fm=fm_home_st_df, ss=ss_home_st_df, sw=sw_home_st_df, rank_status=False)
    away_standings = unified_standings_tables(matched_teams=matched_teams, fm=fm_away_st_df, ss=ss_away_st_df, sw=sw_away_st_df, rank_status=False)
    half_time_standings = unified_standings_tables(matched_teams=matched_teams, fm=None, ss=None, sw=sw_httotal_st_df, rank_status=False)
    expected_standings = create_expected_standing_table(matched_teams=matched_teams, df=fm_xg_st_df)

    if print_info:
        print("        - Standings tables unified")

    team_stats_df, player_stats_df = matches_proc(matches_df=matches_df, players_df=players_df, teams_df=teams_df, managers_df=managers_df, sw_team=sw_matches_team_stats_df, ss_team=ss_matches_statistics_df,
                                                  sw_player=sw_matches_player_stats_df, ss_player=ss_matches_lineups_df)

    if not team_stats_df.empty and "Goals" in team_stats_df.columns and (team_stats_df["Goals"] == 0).all():
        team_stats_df = add_goals(matches_df=matches_df, team_stats_df=team_stats_df)

    if print_info:
        print("        - Teams and players stats in the matches")

    team_stats_season_df = season_stats_team(teams_df=teams_df, team_stats_df=team_stats_df)
    player_stats_season_df = season_stats_player(players_df=players_df, player_stats_df=player_stats_df)

    if print_info:
        print("        - Full season teams and players stats")

    return (teams_df, players_df, managers_df, venues_df, all_standings, home_standings, away_standings, half_time_standings, expected_standings, team_stats_df, player_stats_df, team_stats_season_df, player_stats_season_df)

# --------------------------------------------------------------------------------------
# UNIFICADOR COMPLETO DE LIGA - Función principal.
# --------------------------------------------------------------------------------------
def league_data_unification(league_id: int, raw_data_path: str, clean_data_path: str, processed_data_path: str, print_info: bool = True) -> None:
    comp_row = comps.loc[comps["id"] == league_id]
    if comp_row.empty:
        raise ValueError(f"No existe ninguna liga con id={league_id} en comps.csv.")

    league_name = comp_row["tournament"].iloc[0]
    league_slug = create_slug(text=league_name)

    start_time = time.time()

    if print_info:
        print(f"Starting data unification ({league_name})")

    images_path = os.path.join(raw_data_path, "images")
    processed_league_path = os.path.join(processed_data_path, league_slug)
    os.makedirs(processed_league_path, exist_ok=True)

    processed_all_path = os.path.join(processed_league_path, "All")
    os.makedirs(processed_all_path, exist_ok=True)

    teams_list, players_list, managers_list, venues_list = [], [], [], []
    all_standings_list, home_standings_list, away_standings_list, half_time_standings_list, expected_standings_list = [], [], [], [], []
    team_stats_df_list, player_stats_df_list, team_stats_season_df_list, player_stats_season_df_list = [], [], [], []

    for season_key in desired_seasons:
        if print_info:
            print(f"     - Data unification of season {season_key}")

        processed_season_path = os.path.join(processed_league_path, season_key)
        os.makedirs(processed_season_path, exist_ok=True)

        fotmob_clean_path = os.path.join(clean_data_path, "fotmob", league_slug, season_key)
        scoresway_clean_path = os.path.join(clean_data_path, "scoresway", league_slug, season_key)
        sofascore_clean_path = os.path.join(clean_data_path, "sofascore", league_slug, season_key)

        (teams_df, players_df, managers_df, venues_df, all_standings, 
         home_standings, away_standings, half_time_standings, expected_standings, 
         team_stats_df, player_stats_df, team_stats_season_df, player_stats_season_df) = season_data_unification(fotmob_clean_path=fotmob_clean_path, scoresway_clean_path=scoresway_clean_path,
                                                                                                                 sofascore_clean_path=sofascore_clean_path, print_info=print_info)

        images_proc(players_df=players_df, managers_df=managers_df, teams_df=teams_df, venues_df=venues_df, images_path=images_path, processed_data_path=processed_data_path)
        if print_info:
            print("        - Images processed")

        dfs = [teams_df, players_df, managers_df, venues_df, all_standings, home_standings, away_standings, half_time_standings, expected_standings, team_stats_df, player_stats_df, team_stats_season_df, player_stats_season_df]
        for df in dfs:
            if df is not None and not df.empty:
                df.drop(columns=["IdSS", "IdFM", "IdSW"], errors="ignore", inplace=True)
                if "League" not in df.columns:
                    df.insert(0, "League", league_slug)
                if "Season" not in df.columns:
                    df.insert(1, "Season", season_key)

        info_path = os.path.join(processed_season_path, "info")
        standings_path = os.path.join(processed_season_path, "standings")
        stats_path = os.path.join(processed_season_path, "statistics")
        os.makedirs(info_path, exist_ok=True)
        os.makedirs(standings_path, exist_ok=True)
        os.makedirs(stats_path, exist_ok=True)

        teams_list.append(teams_df)
        teams_df.to_csv(os.path.join(info_path, "team.csv"), index=False, sep=";")
        players_list.append(players_df)
        players_df.to_csv(os.path.join(info_path, "player.csv"), index=False, sep=";")
        managers_list.append(managers_df)
        managers_df.to_csv(os.path.join(info_path, "manager.csv"), index=False, sep=";")
        venues_list.append(venues_df)
        venues_df.to_csv(os.path.join(info_path, "venue.csv"), index=False, sep=";")

        all_standings_list.append(all_standings)
        all_standings.to_csv(os.path.join(standings_path, "all.csv"), index=False, sep=";")
        home_standings_list.append(home_standings)
        home_standings.to_csv(os.path.join(standings_path, "home.csv"), index=False, sep=";")
        away_standings_list.append(away_standings)
        away_standings.to_csv(os.path.join(standings_path, "away.csv"), index=False, sep=";")
        half_time_standings_list.append(half_time_standings)
        half_time_standings.to_csv(os.path.join(standings_path, "half_time.csv"), index=False, sep=";")
        expected_standings_list.append(expected_standings)
        expected_standings.to_csv(os.path.join(standings_path, "expected.csv"), index=False, sep=";")

        team_stats_df_list.append(team_stats_df)
        team_stats_df.to_csv(os.path.join(stats_path, "team_match.csv"), index=False, sep=";")
        player_stats_df_list.append(player_stats_df)
        player_stats_df.to_csv(os.path.join(stats_path, "player_match.csv"), index=False, sep=";")
        team_stats_season_df_list.append(team_stats_season_df)
        team_stats_season_df.to_csv(os.path.join(stats_path, "team_season.csv"), index=False, sep=";")
        player_stats_season_df_list.append(player_stats_season_df)
        player_stats_season_df.to_csv(os.path.join(stats_path, "player_season.csv"), index=False, sep=";")

    info_path = os.path.join(processed_all_path, "info")
    standings_path = os.path.join(processed_all_path, "standings")
    stats_path = os.path.join(processed_all_path, "statistics")
    os.makedirs(info_path, exist_ok=True)
    os.makedirs(standings_path, exist_ok=True)
    os.makedirs(stats_path, exist_ok=True)

    teams_df = pd.concat(teams_list, ignore_index=True) if teams_list else pd.DataFrame()
    teams_df.to_csv(os.path.join(info_path, "team.csv"), index=False, sep=";")
    players_df = pd.concat(players_list, ignore_index=True) if players_list else pd.DataFrame()
    players_df.to_csv(os.path.join(info_path, "player.csv"), index=False, sep=";")
    managers_df = pd.concat(managers_list, ignore_index=True) if managers_list else pd.DataFrame()
    managers_df.to_csv(os.path.join(info_path, "manager.csv"), index=False, sep=";")
    venues_df = pd.concat(venues_list, ignore_index=True) if venues_list else pd.DataFrame()
    venues_df.to_csv(os.path.join(info_path, "venue.csv"), index=False, sep=";")

    all_standings = pd.concat(all_standings_list, ignore_index=True) if all_standings_list else pd.DataFrame()
    all_standings.to_csv(os.path.join(standings_path, "all.csv"), index=False, sep=";")
    home_standings = pd.concat(home_standings_list, ignore_index=True) if home_standings_list else pd.DataFrame()
    home_standings.to_csv(os.path.join(standings_path, "home.csv"), index=False, sep=";")
    away_standings = pd.concat(away_standings_list, ignore_index=True) if away_standings_list else pd.DataFrame()
    away_standings.to_csv(os.path.join(standings_path, "away.csv"), index=False, sep=";")
    half_time_standings = pd.concat(half_time_standings_list, ignore_index=True) if half_time_standings_list else pd.DataFrame()
    half_time_standings.to_csv(os.path.join(standings_path, "half_time.csv"), index=False, sep=";")
    expected_standings = pd.concat(expected_standings_list, ignore_index=True) if expected_standings_list else pd.DataFrame()
    expected_standings.to_csv(os.path.join(standings_path, "expected.csv"), index=False, sep=";")

    team_stats_df = pd.concat(team_stats_df_list, ignore_index=True) if team_stats_df_list else pd.DataFrame()
    team_stats_df.to_csv(os.path.join(stats_path, "team_match.csv"), index=False, sep=";")
    player_stats_df = pd.concat(player_stats_df_list, ignore_index=True) if player_stats_df_list else pd.DataFrame()
    player_stats_df.to_csv(os.path.join(stats_path, "player_match.csv"), index=False, sep=";")
    team_stats_season_df = pd.concat(team_stats_season_df_list, ignore_index=True) if team_stats_season_df_list else pd.DataFrame()
    team_stats_season_df.to_csv(os.path.join(stats_path, "team_season.csv"), index=False, sep=";")
    player_stats_season_df = pd.concat(player_stats_season_df_list, ignore_index=True) if player_stats_season_df_list else pd.DataFrame()
    player_stats_season_df.to_csv(os.path.join(stats_path, "player_season.csv"), index=False, sep=";")

    if print_info:
        print(f"Finished data unification ({league_name}) in {elapsed_time_str(start_time=start_time)}")