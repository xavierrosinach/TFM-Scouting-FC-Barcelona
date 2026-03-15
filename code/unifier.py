import os
import pandas as pd
import numpy as np
import json as jsonlib
from typing import Tuple
import unicodedata
import re
from rapidfuzz import process, fuzz
import warnings
import shutil
from PIL import Image
import time

warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

from config import comps, desired_seasons, utils

# Creación de slug a partir de un string.
def create_slug(text: str) -> str:
    text = text.lower()                                                                                     # Letra minúscula
    text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')        # Eliminación de acentos
    text = re.sub(r"\s+", "_", text)                                                                        # Substitución de espacios por '_'
    text = re.sub(r"[^a-z0-9_]", "", text)                                                                  # Eliminación de carácteres no alfanuméricos
    text = re.sub(r"_+", "_", text).strip("_")
    return text

# División segura
def safe_div(num, den, ndigits=4):

    if pd.isna(num) or pd.isna(den) or den == 0:
        return np.nan
    return round(num / den, ndigits)

# Lectura de los datos de Fotmob
def read_fotmob_data(fotmob_clean_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    if not os.path.exists(fotmob_clean_path):
        return None, None, None, None, None, None, None

    standings_path = os.path.join(fotmob_clean_path, 'standings')       # Carpeta con tablas de clasificación
    info_path = os.path.join(fotmob_clean_path, 'info.csv')             # Tabla de información
    matches_path = os.path.join(fotmob_clean_path, 'matches.csv')       # Tabla con partidos jugados

    st_all_path = os.path.join(standings_path, 'all.csv')               # Tablas con las distintas tablas de classificación
    st_home_path = os.path.join(standings_path, 'home.csv')
    st_away_path = os.path.join(standings_path, 'away.csv')
    st_form_path = os.path.join(standings_path, 'form.csv')
    st_xg_path = os.path.join(standings_path, 'xg.csv')

    info_df = pd.read_csv(info_path, sep=';') if os.path.exists(info_path) else None               # Lectura de todos los dataframes si existen
    matches_df = pd.read_csv(matches_path, sep=';') if os.path.exists(matches_path) else None
    all_st_df = pd.read_csv(st_all_path, sep=';') if os.path.exists(st_all_path) else None
    home_st_df = pd.read_csv(st_home_path, sep=';') if os.path.exists(st_home_path) else None
    away_st_df = pd.read_csv(st_away_path, sep=';') if os.path.exists(st_away_path) else None
    form_st_df = pd.read_csv(st_form_path, sep=';') if os.path.exists(st_form_path) else None
    xg_st_df = pd.read_csv(st_xg_path, sep=';') if os.path.exists(st_xg_path) else None
    
    return info_df, matches_df, all_st_df, home_st_df, away_st_df, form_st_df, xg_st_df

# A partir de los datos de Fotmob, obtenemos los nombres de los equipos
def obtain_fotmob_teams(matches_df: pd.DataFrame, all_st_df: pd.DataFrame, home_st_df: pd.DataFrame, away_st_df: pd.DataFrame, form_st_df: pd.DataFrame, xg_st_df: pd.DataFrame) -> list:

    teams_list = []

    if matches_df is not None:                                  # Equipos local y visitante en los partidos
        teams_list.extend(matches_df['home_team'].tolist())
        teams_list.extend(matches_df['away_team'].tolist())

    if all_st_df is not None:                                   # Distintos equipos en la tabla de clasificación
        teams_list.extend(all_st_df['name'].tolist())
    if home_st_df is not None:
        teams_list.extend(home_st_df['name'].tolist())
    if away_st_df is not None:
        teams_list.extend(away_st_df['name'].tolist())
    if form_st_df is not None:
        teams_list.extend(form_st_df['name'].tolist())
    if xg_st_df is not None:
        teams_list.extend(xg_st_df['name'].tolist())

    return sorted(set(teams_list))              # Sin duplicados y ordenado

# Lectura de los datos de Scoresway
def read_scoresway_data(scoresway_clean_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not os.path.exists(scoresway_clean_path):
        return None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None

    info_path = os.path.join(scoresway_clean_path, 'info')                # Carpeta con información
    matches_dir_path = os.path.join(scoresway_clean_path, 'matches')          # Tabla con datos de partidos
    standings_path = os.path.join(info_path, 'standings')      # Tablas de clasificación

    managers_path = os.path.join(info_path, 'managers.csv')               # Dataframe con información variada
    matches_path = os.path.join(info_path, 'matches.csv')
    players_path = os.path.join(info_path, 'players.csv')
    teams_path = os.path.join(info_path, 'teams.csv')

    st_total_path = os.path.join(standings_path, 'total.csv')            # Distintas tablas de clasificación
    st_home_path = os.path.join(standings_path, 'home.csv')
    st_away_path = os.path.join(standings_path, 'away.csv')
    st_httotal_path = os.path.join(standings_path, 'half-time-total.csv')
    st_hthome_path = os.path.join(standings_path, 'half-time-home.csv')
    st_htaway_path = os.path.join(standings_path, 'half-time-away.csv')
    st_formhome_path = os.path.join(standings_path, 'form-home.csv')
    st_formaway_path = os.path.join(standings_path, 'form-away.csv')
    st_overunder_path = os.path.join(standings_path, 'over-under.csv')
    st_attendance_path = os.path.join(standings_path, 'attendance.csv')

    matches_info_path = os.path.join(matches_dir_path, 'info.csv')      # Información y estadísticas sobre partidos
    matches_player_stats_path = os.path.join(matches_dir_path, 'player_stats.csv')
    matches_referees_path = os.path.join(matches_dir_path, 'referees.csv')
    matches_team_stats_path = os.path.join(matches_dir_path, 'team_stats.csv')

    managers_df = pd.read_csv(managers_path, sep=';') if os.path.exists(managers_path) else None        # Lectura de todos los CSVs si no existen
    matches_df = pd.read_csv(matches_path, sep=';') if os.path.exists(matches_path) else None
    players_df = pd.read_csv(players_path, sep=';') if os.path.exists(players_path) else None
    teams_df = pd.read_csv(teams_path, sep=';') if os.path.exists(teams_path) else None
    total_st_df = pd.read_csv(st_total_path, sep=';') if os.path.exists(st_total_path) else None
    home_st_df = pd.read_csv(st_home_path, sep=';') if os.path.exists(st_home_path) else None
    away_st_df = pd.read_csv(st_away_path, sep=';') if os.path.exists(st_away_path) else None
    httotal_st_df = pd.read_csv(st_httotal_path, sep=';') if os.path.exists(st_httotal_path) else None
    hthome_st_df = pd.read_csv(st_hthome_path, sep=';') if os.path.exists(st_hthome_path) else None
    htaway_st_df = pd.read_csv(st_htaway_path, sep=';') if os.path.exists(st_htaway_path) else None
    formhome_st_df = pd.read_csv(st_formhome_path, sep=';') if os.path.exists(st_formhome_path) else None
    formaway_st_df = pd.read_csv(st_formaway_path, sep=';') if os.path.exists(st_formaway_path) else None
    overunder_st_df = pd.read_csv(st_overunder_path, sep=';') if os.path.exists(st_overunder_path) else None
    attendance_st_df = pd.read_csv(st_attendance_path, sep=';') if os.path.exists(st_attendance_path) else None
    matches_info_df = pd.read_csv(matches_info_path, sep=';') if os.path.exists(matches_info_path) else None
    matches_player_stats_df = pd.read_csv(matches_player_stats_path, sep=';') if os.path.exists(matches_player_stats_path) else None
    matches_referees_df = pd.read_csv(matches_referees_path, sep=';') if os.path.exists(matches_referees_path) else None
    matches_team_stats_df = pd.read_csv(matches_team_stats_path, sep=';') if os.path.exists(matches_team_stats_path) else None

    return managers_df, matches_df, players_df, teams_df, total_st_df, home_st_df, away_st_df, httotal_st_df, hthome_st_df, htaway_st_df, formhome_st_df, formaway_st_df, overunder_st_df, attendance_st_df, matches_info_df, matches_player_stats_df, matches_team_stats_df, matches_referees_df

# Lectura de los datos de Sofascore
def read_sofascore_data(sofascore_clean_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not os.path.exists(sofascore_clean_path):
        return None, None, None, None, None, None, None, None, None, None

    info_path = os.path.join(sofascore_clean_path, 'info')                      # Carpeta con información
    matches_dir_path = os.path.join(sofascore_clean_path, 'matches')            # Tabla con datos de partidos
    standings_path = os.path.join(info_path, 'standings')                       # Tablas de clasificación

    managers_path = os.path.join(info_path, 'managers.csv')               # Dataframe con información variada
    venues_path = os.path.join(info_path, 'venues.csv')
    players_path = os.path.join(info_path, 'players.csv')
    teams_path = os.path.join(info_path, 'teams.csv')

    st_total_path = os.path.join(standings_path, 'total.csv')            # Distintas tablas de clasificación
    st_home_path = os.path.join(standings_path, 'home.csv')
    st_away_path = os.path.join(standings_path, 'away.csv')

    matches_info_path = os.path.join(matches_dir_path, 'matches.csv')      # Información y estadísticas sobre partidos
    matches_lineups_path = os.path.join(matches_dir_path, 'lineups.csv')
    matches_team_stats_path = os.path.join(matches_dir_path, 'statistics.csv')

    managers_df = pd.read_csv(managers_path, sep=';') if os.path.exists(managers_path) else None        # Lectura de todos los CSVs si no existen
    players_df = pd.read_csv(players_path, sep=';') if os.path.exists(players_path) else None
    teams_df = pd.read_csv(teams_path, sep=';') if os.path.exists(teams_path) else None
    venues_df = pd.read_csv(venues_path, sep=';') if os.path.exists(venues_path) else None
    total_st_df = pd.read_csv(st_total_path, sep=';') if os.path.exists(st_total_path) else None
    home_st_df = pd.read_csv(st_home_path, sep=';') if os.path.exists(st_home_path) else None
    away_st_df = pd.read_csv(st_away_path, sep=';') if os.path.exists(st_away_path) else None
    matches_info_df = pd.read_csv(matches_info_path, sep=';') if os.path.exists(matches_info_path) else None
    matches_lineups_df = pd.read_csv(matches_lineups_path, sep=';') if os.path.exists(matches_lineups_path) else None
    matches_statistics_df = pd.read_csv(matches_team_stats_path, sep=';') if os.path.exists(matches_team_stats_path) else None

    return managers_df, players_df, teams_df, venues_df, total_st_df, home_st_df, away_st_df, matches_info_df, matches_lineups_df, matches_statistics_df

# A partir de las listas de las fuentes de los equipos, obtenemos un dataframe con los equipos relacionados
def match_teams(fm_list: list, sw_list: list, ss_list: list, threshold: int = 30) -> pd.DataFrame:

    rows = []
    for team in fm_list:
        match2, score2, _ = process.extractOne(team, sw_list, scorer=fuzz.token_sort_ratio) if len(sw_list) > 0 else ('', 0, '')
        match3, score3, _ = process.extractOne(team, ss_list, scorer=fuzz.token_sort_ratio) if len(ss_list) > 0 else ('', 0, '')
        rows.append({"fotmob": team, "scoresway": match2 if score2 >= threshold else None, "sofascore": match3 if score3 >= threshold else None})

    df = pd.DataFrame(rows)
    df.insert(0, 'team', df['fotmob'].combine_first(df['sofascore']).combine_first(df['scoresway']))        # Columna con el nombre que le vamos a poner al equipo - por prioridad
    return df

# A partir de las listas de las fuentes de los jugadores, obtenemos un dataframe con los jugadores
def match_players(sw_list: list, ss_list: list, threshold: int = 10) -> pd.DataFrame:

    sw_list = sw_list if sw_list is not None else []
    ss_list = ss_list if ss_list is not None else []

    if len(ss_list) == 0 and len(sw_list) == 0:
        return pd.DataFrame(columns=['player', 'sofascore', 'scoresway'])

    if len(ss_list) == 0:
        df = pd.DataFrame({'scoresway': sw_list})
        df['sofascore'] = np.nan
        df.insert(0, 'player', df['scoresway'])
        return df

    rows = []
    for player in ss_list:
        match, score, _ = process.extractOne(player, sw_list, scorer=fuzz.token_sort_ratio) if len(sw_list) > 0 else ('', 0, '')
        rows.append({'sofascore': player, 'scoresway': match if score >= threshold else np.nan})

    df = pd.DataFrame(rows)
    df.insert(0, 'player', df['sofascore'].combine_first(df['scoresway']))

    return df

# Unificación de información del partido
def unify_teams_info(matched_teams: pd.DataFrame, sw_teams_df: pd.DataFrame = None, ss_teams_df: pd.DataFrame = None) -> pd.DataFrame:

    sw_to_team = matched_teams.set_index('scoresway')['team'].dropna().to_dict()        # Mapeamos equipos
    ss_to_team = matched_teams.set_index('sofascore')['team'].dropna().to_dict()

    dfs = []
    if sw_teams_df is not None:                 # Para cada dataframe, si no es vacio, no concatenaremos
        sw_teams_df = sw_teams_df.copy()
        sw_teams_df['TeamName'] = sw_teams_df['club_name'].map(sw_to_team)
        sw_teams_df = sw_teams_df.rename(columns={c: f"{c}_sw" for c in sw_teams_df.columns if c != 'TeamName'})
        dfs.append(sw_teams_df)

    if ss_teams_df is not None:
        ss_teams_df = ss_teams_df.copy()
        ss_teams_df['TeamName'] = ss_teams_df['name'].map(ss_to_team)
        ss_teams_df = ss_teams_df.rename(columns={c: f"{c}_ss" for c in ss_teams_df.columns if c != 'TeamName'})
        dfs.append(ss_teams_df)

    if len(dfs) == 0:
        return matched_teams[['team']].rename(columns={'team': 'TeamName'}).drop_duplicates().reset_index(drop=True)        # En caso que no haya ningún dataframe, añadiremos un dataframe con los nombres de los equipos
    if len(dfs) == 1:
        teams_df = dfs[0]
    else:
        teams_df = pd.merge(dfs[0], dfs[1], on='TeamName', how='outer')

    teams_df = (matched_teams[['team']].rename(columns={'team': 'TeamName'}).drop_duplicates().merge(teams_df, how='left', on='TeamName'))      # Dataframe final
    return teams_df

# Limpieza del dataframe unificado de equipos
def clean_unified_teams(df: pd.DataFrame) -> pd.DataFrame:

    list_columns = df.columns
    df_cleaned = pd.DataFrame()     # Dataframe vacío - ahora vamos a añadir columnas

    df_cleaned['Name'] = df['TeamName']                                         # Vamos añadiendo columnas si existen
    df_cleaned['FullName'] = df['name_sw'] if 'name_sw' in list_columns else df['name_ss'] if 'name_ss' in list_columns else np.nan
    df_cleaned['ShortName'] = df['short_name_ss'] if 'short_name_ss' in list_columns else np.nan
    df_cleaned['Code'] = df['code_sw'] if 'code_sw' in list_columns else np.nan
    df_cleaned['Country'] = df['country_ss'] if 'country_ss' in list_columns else np.nan
    df_cleaned['FoundationDate'] = df['foundation_date_ss'] if 'foundation_date_ss' in list_columns else np.nan
    df_cleaned['VenueCodeSS'] = df['venue_ss'] if 'venue_ss' in list_columns else np.nan
    df_cleaned['ManagerCodeSS'] = df['manager_ss'] if 'manager_ss' in list_columns else np.nan
    df_cleaned['PrimaryColour'] = df['primary_colour_ss'] if 'primary_colour_ss' in list_columns else np.nan
    df_cleaned['SecondaryColour'] = df['secondary_colour_ss'] if 'secondary_colour_ss' in list_columns else np.nan
    df_cleaned['TextColour'] = df['text_colour_ss'] if 'text_colour_ss' in list_columns else np.nan

    df_cleaned['IdSS'] = df['team_id_ss'] if 'team_id_ss' in list_columns else np.nan      # IDs de sofascore y de scoresway
    df_cleaned['IdSW'] = df['sw_id_sw'] if 'sw_id_sw' in list_columns else np.nan

    df_cleaned['FoundationDate'] = pd.to_datetime(df_cleaned['FoundationDate'], unit='s', errors='coerce').dt.strftime('%d/%m/%Y')
    df_cleaned['VenueCodeSS'] = pd.to_numeric(df_cleaned['VenueCodeSS'], errors='coerce').astype('Int64')
    df_cleaned['ManagerCodeSS'] = pd.to_numeric(df_cleaned['ManagerCodeSS'], errors='coerce').astype('Int64')

    df_cleaned.insert(0, 'Slug', df_cleaned['Name'].apply(create_slug))         # Añadimos slug como indice

    return df_cleaned

# Información equipos
def create_teams_info_df(matched_teams: pd.DataFrame, sw_teams_df: pd.DataFrame = None, ss_teams_df: pd.DataFrame = None) -> pd.DataFrame:

    raw_unified_teams_df = unify_teams_info(matched_teams=matched_teams, sw_teams_df=sw_teams_df, ss_teams_df=ss_teams_df)          # Unificación de los equipos en un dataframe conjunto
    teams_df = clean_unified_teams(df=raw_unified_teams_df)                                                                         # Limpieza del dataframe
    return teams_df

# Unificamos información de los jugadores de un único equipo
def unify_players_info(team: str, matched_players: pd.DataFrame, ss_df: pd.DataFrame = None, sw_df: pd.DataFrame = None) -> pd.DataFrame:

    sw_to_player = matched_players.set_index('scoresway')['player'].dropna().to_dict()
    ss_to_player = matched_players.set_index('sofascore')['player'].dropna().to_dict()

    dfs = []

    if ss_df is not None and not ss_df.empty:
        ss_df = ss_df.copy()
        ss_df['PlayerName'] = ss_df['playerName_ss'].map(ss_to_player)
        dfs.append(ss_df)

    if sw_df is not None and not sw_df.empty:
        sw_df = sw_df.copy()
        sw_df['PlayerName'] = sw_df['match_name_sw'].map(sw_to_player)
        dfs.append(sw_df)

    if len(dfs) == 0:
        players_df = (matched_players[['player']].rename(columns={'player':'PlayerName'}).drop_duplicates().reset_index(drop=True))
    elif len(dfs) == 1:
        players_df = dfs[0]
    else:
        players_df = pd.merge(dfs[0], dfs[1], on='PlayerName', how='outer')

    players_df = (matched_players[['player']].rename(columns={'player':'PlayerName'}).drop_duplicates().merge(players_df, how='left', on='PlayerName'))
    players_df.insert(0, 'Team', create_slug(text=team))

    return players_df

# Limpieza del dataframe unificado de jugadores
def clean_unified_players(df: pd.DataFrame) -> pd.DataFrame:

    list_columns = df.columns
    df_cleaned = pd.DataFrame()     # Dataframe vacío - ahora vamos a añadir columnas

    df_cleaned['Name'] = df['PlayerName']                                         # Vamos añadiendo columnas si existen
    df_cleaned['Team'] = df['Team']
    df_cleaned['ShortName'] = df['shortName_ss'] if 'shortName_ss' in list_columns else df['match_name_sw'] if 'match_name_sw' in list_columns else np.nan
    df_cleaned['FirstName'] = df['first_name_sw'] if 'first_name_sw' in list_columns else np.nan
    df_cleaned['SecondName'] = df['last_name_sw'] if 'last_name_sw' in list_columns else np.nan
    df_cleaned['ShortFirstName'] = df['short_first_name_sw'] if 'short_first_name_sw' in list_columns else np.nan
    df_cleaned['ShortSecondName'] = df['short_last_name_sw'] if 'short_last_name_sw' in list_columns else np.nan
    df_cleaned['Country'] = df['country_ss'] if 'country_ss' in list_columns else df['nationality_sw'] if 'nationality_sw' in list_columns else np.nan
    df_cleaned['ShirtNumber'] = df['shirt_num_ss'] if 'shirt_num_ss' in list_columns else df['shirt_number_sw'] if 'shirt_number_sw' in list_columns else np.nan
    df_cleaned['PrefFoot'] = df['pref_foot_ss'] if 'pref_foot_ss' in list_columns else np.nan
    df_cleaned['Height'] = df['height_ss'] if 'height_ss' in list_columns else np.nan
    df_cleaned['DateBirth'] = df['date_birth_ss'] if 'date_birth_ss' in list_columns else np.nan
    df_cleaned['FirstPosition'] = df['first_position_ss'] if 'first_position_ss' in list_columns else np.nan
    df_cleaned['SecondPosition'] = df['second_position_ss'] if 'second_position_ss' in list_columns else np.nan
    df_cleaned['ThirdPosition'] = df['third_position_ss'] if 'third_position_ss' in list_columns else np.nan
    df_cleaned['MarketValue'] = df['market_value_ss'] if 'market_value_ss' in list_columns else np.nan
    df_cleaned['ContractUntil'] = df['contract_until_ss'] if 'contract_until_ss' in list_columns else np.nan

    df_cleaned['IdSS'] = df['playerId_ss'] if 'playerId_ss' in list_columns else np.nan      # IDs de sofascore y de scoresway
    df_cleaned['IdSW'] = df['id_sw'] if 'id_sw' in list_columns else np.nan

    df_cleaned['DateBirth'] = pd.to_datetime(df_cleaned['DateBirth'], unit='s', errors='coerce').dt.strftime('%d/%m/%Y')
    df_cleaned['ContractUntil'] = pd.to_datetime(df_cleaned['ContractUntil'], unit='s', errors='coerce').dt.strftime('%d/%m/%Y')
    df_cleaned['ShirtNumber'] = pd.to_numeric(df_cleaned['ShirtNumber'], errors='coerce').astype('Int64')
    df_cleaned['Height'] = pd.to_numeric(df_cleaned['Height'], errors='coerce').astype('Int64')
    df_cleaned['MarketValue'] = pd.to_numeric(df_cleaned['MarketValue'], errors='coerce').astype('Int64')
    df_cleaned['IdSS'] = pd.to_numeric(df_cleaned['IdSS'], errors='coerce').astype('Int64')

    df_cleaned.insert(0, 'Slug', df_cleaned['Name'].apply(create_slug))         # Añadimos slug como indice

    return df_cleaned.sort_values(by='ShirtNumber')

# Unificación total de los datos de información de jugadores
def create_players_info_df(matched_teams: pd.DataFrame, sw_players_df: pd.DataFrame, ss_players_df: pd.DataFrame) -> pd.DataFrame:

    sw_to_team = matched_teams.set_index('longname_scoresway')['team'].dropna().to_dict()        # Mapeamos equipos
    ss_to_team = matched_teams.set_index('sofascore')['team'].dropna().to_dict()

    if sw_players_df is not None:
        sw_players_df = sw_players_df.copy()
        sw_players_df['TeamName'] = sw_players_df['team'].map(sw_to_team)
        sw_players_df = sw_players_df.rename(columns={c: f"{c}_sw" for c in sw_players_df.columns if c != 'TeamName'})

    if ss_players_df is not None:
        ss_players_df = ss_players_df.copy()
        ss_players_df['TeamName'] = ss_players_df['teamName'].map(ss_to_team)
        ss_players_df = ss_players_df.rename(columns={c: f"{c}_ss" for c in ss_players_df.columns if c != 'TeamName'})

    list_teams = []

    for team in matched_teams['team'].tolist():

        sw_players_df_ = sw_players_df.loc[sw_players_df['TeamName'] == team] if sw_players_df is not None else None
        ss_players_df_ = ss_players_df.loc[ss_players_df['TeamName'] == team] if ss_players_df is not None else None

        if sw_players_df_ is not None and sw_players_df_.empty:
            sw_players_df_ = None
        if ss_players_df_ is not None and ss_players_df_.empty:
            ss_players_df_ = None

        players_names_sw = sw_players_df_['match_name_sw'].dropna().unique().tolist() if sw_players_df_ is not None else []
        players_names_ss = ss_players_df_['playerName_ss'].dropna().unique().tolist() if ss_players_df_ is not None else []

        matched_players = match_players(sw_list=players_names_sw, ss_list=players_names_ss)
        unified_players_df = unify_players_info(team=team, matched_players=matched_players, ss_df=ss_players_df_, sw_df=sw_players_df_)
        cleaned_players_df = clean_unified_players(df=unified_players_df)
        list_teams.append(cleaned_players_df)

    return pd.concat(list_teams, ignore_index=True)

# Unificamos información de los managers
def unify_managers_info(matched_managers: pd.DataFrame, ss_df: pd.DataFrame = None, sw_df: pd.DataFrame = None) -> pd.DataFrame:

    sw_to_player = matched_managers.set_index('scoresway')['player'].dropna().to_dict()
    ss_to_player = matched_managers.set_index('sofascore')['player'].dropna().to_dict()

    dfs = []

    if ss_df is not None and not ss_df.empty:
        ss_df = ss_df.copy()
        ss_df['ManagerName'] = ss_df['name'].map(ss_to_player)
        ss_df = ss_df.rename(columns={c: f"{c}_ss" for c in ss_df.columns if c != 'ManagerName'})
        dfs.append(ss_df)

    if sw_df is not None and not sw_df.empty:
        sw_df = sw_df.copy()
        sw_df['ManagerName'] = sw_df['manager_name'].map(sw_to_player)
        sw_df = sw_df.rename(columns={c: f"{c}_sw" for c in sw_df.columns if c != 'ManagerName'})
        dfs.append(sw_df)

    if len(dfs) == 0:
        managers_df = (matched_managers[['player']].rename(columns={'player':'ManagerName'}).drop_duplicates().reset_index(drop=True))
    elif len(dfs) == 1:
        managers_df = dfs[0]
    else:
        managers_df = pd.merge(dfs[0], dfs[1], on='ManagerName', how='outer')

    managers_df = (matched_managers[['player']].rename(columns={'player':'ManagerName'}).drop_duplicates().merge(managers_df, how='left', on='ManagerName'))

    if sw_df is not None:
        return pd.concat([managers_df, sw_df], ignore_index=True)
    return managers_df

# Limpieza del dataframe unificado de entrenadores
def clean_unified_managers(df: pd.DataFrame) -> pd.DataFrame:

    list_columns = df.columns
    df_cleaned = pd.DataFrame()     # Dataframe vacío - ahora vamos a añadir columnas

    df_cleaned['Name'] = df['ManagerName'] if 'manager_name_sw' not in list_columns else df['ManagerName'].combine_first(df['manager_name_sw'])                 # Vamos añadiendo columnas si existen
    df_cleaned['ShortName'] = df['short_name_ss'] if 'short_name_ss' in list_columns else df['match_name_sw'] if 'match_name_sw' in list_columns else np.nan
    df_cleaned['FirstName'] = df['first_name_sw'] if 'first_name_sw' in list_columns else np.nan
    df_cleaned['SecondName'] = df['last_name_sw'] if 'last_name_sw' in list_columns else np.nan
    df_cleaned['ShortFirstName'] = df['short_first_name_sw'] if 'short_first_name_sw' in list_columns else np.nan
    df_cleaned['ShortSecondName'] = df['short_last_name_sw'] if 'short_last_name_sw' in list_columns else np.nan
    df_cleaned['Country'] = df['country_ss'] if 'country_ss' in list_columns else df['nationality_sw'] if 'nationality_sw' in list_columns else np.nan
    df_cleaned['DateBirth'] = df['date_birth_ss'] if 'date_birth_ss' in list_columns else np.nan
    df_cleaned['Position'] = df['type_sw'] if 'type_sw' in list_columns else np.nan
    df_cleaned['Matches'] = df['matches_ss'] if 'matches_ss' in list_columns else np.nan
    df_cleaned['Wins'] = df['wins_ss'] if 'wins_ss' in list_columns else np.nan
    df_cleaned['Draws'] = df['draws_ss'] if 'draws_ss' in list_columns else np.nan
    df_cleaned['Losses'] = df['losses_ss'] if 'losses_ss' in list_columns else np.nan
    df_cleaned['GoalsFor'] = df['goals_scored_ss'] if 'goals_scored_ss' in list_columns else np.nan
    df_cleaned['GoalsAgainst'] = df['goals_conceded_ss'] if 'goals_conceded_ss' in list_columns else np.nan
    df_cleaned['Points'] = df['points_ss'] if 'points_ss' in list_columns else np.nan

    df_cleaned['IdSS'] = df['id_ss'] if 'id_ss' in list_columns else np.nan      # IDs de sofascore y de scoresway
    df_cleaned['IdSW'] = df['id_sw'] if 'id_sw' in list_columns else np.nan

    df_cleaned['DateBirth'] = pd.to_datetime(df_cleaned['DateBirth'], unit='s', errors='coerce').dt.strftime('%d/%m/%Y')
    df_cleaned['Matches'] = pd.to_numeric(df_cleaned['Matches'], errors='coerce').astype('Int64')
    df_cleaned['Wins'] = pd.to_numeric(df_cleaned['Wins'], errors='coerce').astype('Int64')
    df_cleaned['Draws'] = pd.to_numeric(df_cleaned['Draws'], errors='coerce').astype('Int64')
    df_cleaned['Losses'] = pd.to_numeric(df_cleaned['Losses'], errors='coerce').astype('Int64')
    df_cleaned['GoalsFor'] = pd.to_numeric(df_cleaned['GoalsFor'], errors='coerce').astype('Int64')
    df_cleaned['GoalsAgainst'] = pd.to_numeric(df_cleaned['GoalsAgainst'], errors='coerce').astype('Int64')
    df_cleaned['Points'] = pd.to_numeric(df_cleaned['Points'], errors='coerce').astype('Int64')
    df_cleaned['IdSS'] = pd.to_numeric(df_cleaned['IdSS'], errors='coerce').astype('Int64')

    df_cleaned.insert(0, 'Slug', df_cleaned['Name'].apply(create_slug))         # Añadimos slug como indice

    return df_cleaned

# Dataframe con información de jugadores
def create_managers_info_df(teams_df: pd.DataFrame, sw_managers_df: pd.DataFrame, ss_managers_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:

    if sw_managers_df is not None:
        sw_managers_df['manager_name'] = sw_managers_df['short_first_name'] + ' ' + sw_managers_df['short_last_name']
        list_sw = sw_managers_df['manager_name'].tolist()
    else:
        list_sw = []

    if ss_managers_df is not None:
        list_ss = ss_managers_df['name'].tolist()
    else:
        list_ss = []

    matched_managers = match_players(sw_list=list_sw, ss_list=list_ss)            # Usamos la función de jugadores ya que se puede extrapolar
    managers_df = unify_managers_info(matched_managers=matched_managers, sw_df=sw_managers_df, ss_df=ss_managers_df)
    managers_df = clean_unified_managers(df=managers_df)

    managers_name_dict = managers_df.set_index('IdSS')['Slug'].dropna().to_dict()        # Mapeamos nombres de managers a teams dataframe
    teams_df['ManagerCodeSS'] = teams_df['ManagerCodeSS'].map(managers_name_dict)
    teams_df = teams_df.rename(columns={'ManagerCodeSS': 'Manager'})

    return teams_df, managers_df

# Información de estadios
def create_venues_info_df(teams_df: pd.DataFrame, ss_venues_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:

    if ss_venues_df is None:
        return teams_df, None
    
    venues_df = ss_venues_df.copy()
    venues_df.columns = ['IdSS', 'Name', 'Capacity', 'City', 'Latitude', 'Longitude']
    venues_df = venues_df[['Name', 'Capacity', 'City', 'Latitude', 'Longitude', 'IdSS']]
    venues_df.insert(0, 'Slug', venues_df['Name'].apply(create_slug))                       # Añadimos slug como indice

    venues_name_dict = venues_df.set_index('IdSS')['Slug'].dropna().to_dict()        # Mapeamos nombres de venues a teams dataframe
    teams_df['VenueCodeSS'] = teams_df['VenueCodeSS'].map(venues_name_dict)
    teams_df = teams_df.rename(columns={'VenueCodeSS': 'Venue'})

    return teams_df, venues_df

# Limpieza del dataframe unificado de partidos
def clean_unified_matches(df: pd.DataFrame) -> pd.DataFrame:

    list_columns = df.columns
    df_cleaned = pd.DataFrame()     # Dataframe vacío - ahora vamos a añadir columnas

    df_cleaned['Slug'] = df['Slug']                                                   # Vamos añadiendo columnas si existen
    df_cleaned['Round'] = df['round_ss'] if 'round_ss' in list_columns else df['week_sw'] if 'week_sw' in list_columns else np.nan
    df_cleaned['Date_'] = df['date_time_ss'] if 'date_time_ss' in list_columns else np.nan
    df_cleaned['HomeTeam'] = df['HomeTeam_ss']
    df_cleaned['AwayTeam'] = df['AwayTeam_ss']
    df_cleaned['Winner'] = df['winner_ss'] if 'winner_ss' in list_columns else np.nan
    df_cleaned['HomeScore'] = df['home_score_ss'] if 'home_score_ss' in list_columns else np.nan
    df_cleaned['AwayScore'] = df['away_score_ss'] if 'away_score_ss' in list_columns else np.nan
    df_cleaned['HomeScoreHT'] = df['ht_home_score_sw'] if 'ht_home_score_sw' in list_columns else np.nan
    df_cleaned['AwayScoreHT'] = df['ht_away_score_sw'] if 'ht_away_score_sw' in list_columns else np.nan
    df_cleaned['Attendance'] = df['attendance_ss'] if 'attendance_ss' in list_columns else df['attendance_sw'] if 'attendance_sw' in list_columns else np.nan

    df_cleaned['IdSS'] = df['match_id_ss'] if 'match_id_ss' in list_columns else np.nan         # IDs
    df_cleaned['IdSW'] = df['match_id_sw'] if 'match_id_sw' in list_columns else np.nan

    df_cleaned.insert(2, 'Date', pd.to_datetime(df_cleaned['Date_'], unit='s', errors='coerce').dt.strftime('%d/%m/%Y'))
    df_cleaned.insert(3, 'Time', pd.to_datetime(df_cleaned['Date_'], unit='s', errors='coerce').dt.strftime('%H:%M'))
    df_cleaned = df_cleaned.drop(columns=['Date_'])
    df_cleaned['IdSS'] = pd.to_numeric(df_cleaned['IdSS'], errors='coerce').astype('Int64')
    df_cleaned['Round'] = pd.to_numeric(df_cleaned['Round'], errors='coerce').astype('Int64')
    df_cleaned['HomeScore'] = pd.to_numeric(df_cleaned['HomeScore'], errors='coerce').astype('Int64')
    df_cleaned['AwayScore'] = pd.to_numeric(df_cleaned['AwayScore'], errors='coerce').astype('Int64')
    df_cleaned['HomeScoreHT'] = pd.to_numeric(df_cleaned['HomeScoreHT'], errors='coerce').astype('Int64')
    df_cleaned['AwayScoreHT'] = pd.to_numeric(df_cleaned['AwayScoreHT'], errors='coerce').astype('Int64')
    df_cleaned['Attendance'] = pd.to_numeric(df_cleaned['Attendance'], errors='coerce').astype('Int64')
    df_cleaned['Winner'] = np.select([df_cleaned['Winner'] == 1, df_cleaned['Winner'] == 2, df_cleaned['Winner'] == 3], [df_cleaned['HomeTeam'], df_cleaned['AwayTeam'], 'x'], default=np.nan)      # Nombre de equipo que ha ganado o "X" en caso de empate

    return df_cleaned

# Creación del dataframe con información de los partidos
def create_matches_info_df(teams_df: pd.DataFrame, ss_matches_info_df: pd.DataFrame, sw_matches_info_df: pd.DataFrame) -> pd.DataFrame:

    ss = ss_matches_info_df.copy()              # Copia de Sofascore - siempre lo tendremos
    teams_names_dict = teams_df.set_index('IdSS')['Slug'].dropna().to_dict()
    ss['HomeTeam'] = ss['home_team'].map(teams_names_dict)
    ss['AwayTeam'] = ss['away_team'].map(teams_names_dict)
    ss = ss.rename(columns={c: f"{c}_ss" for c in ss.columns})
    ss['Slug'] = ss['HomeTeam_ss'] + '_' + ss['AwayTeam_ss']

    if sw_matches_info_df is not None:
        sw = sw_matches_info_df.copy()
        teams_names_dict = teams_df.set_index('IdSW')['Slug'].dropna().to_dict()
        sw['HomeTeam'] = sw['home_team_id'].map(teams_names_dict)
        sw['AwayTeam'] = sw['away_team_id'].map(teams_names_dict)
        sw = sw.rename(columns={c: f"{c}_sw" for c in sw.columns})
        sw['Slug'] = sw['HomeTeam_sw'] + '_' + sw['AwayTeam_sw']

        unified_matches_df = ss.merge(sw, how='left', on='Slug')        # Merge en caso de que exista

    else:
        unified_matches_df = ss

    return clean_unified_matches(df = unified_matches_df)

# Limpieza del dataframe de clasificación
def clean_standing(df: pd.DataFrame, rank_status: bool = True) -> pd.DataFrame:

    df_cleaned = pd.DataFrame()
    df_cleaned['Team'] = df['Team']

    cols_map = {'Rank':['rank_sw', 'position_ss', 'idx_fm'],
                'Matches':['played_fm', 'matches_ss', 'matchesPlayed_sw'],
                'Wins':['wins_fm', 'wins_ss', 'matchesWon_sw'],
                'Losses':['losses_fm', 'losses_ss', 'matchesLost_sw'],
                'Draws':['draws_fm', 'draws_ss', 'matchesDrawn_sw'],
                'Points':['pts_fm', 'points_ss', 'points_sw'],
                'GoalsFor':['scores_for_ss', 'goalsFor_sw'],
                'GoalsAgainst':['scores_against_ss', 'goalsAgainst_sw']}

    for new_col, possible_cols in cols_map.items():
        existing_cols = [c for c in possible_cols if c in df.columns]

        if len(existing_cols) > 0:
            df_cleaned[new_col] = df[existing_cols].bfill(axis=1).iloc[:, 0]
        else:
            df_cleaned[new_col] = np.nan

    num_cols = ['Matches', 'Wins', 'Losses', 'Draws', 'GoalsFor', 'GoalsAgainst', 'Points']
    for col in num_cols:
        df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce').astype('Int64')

    df_cleaned['GoalDiff'] = df_cleaned['GoalsFor'] - df_cleaned['GoalsAgainst']
    df_cleaned['Team'] = df_cleaned['Team'].apply(create_slug)

    if rank_status:
        df_cleaned.insert(2, 'Status', df['promotion_ss'])

    return df_cleaned.sort_values(by='Rank')

# Unificación de standings tables
def unified_standings_tables(matched_teams: pd.DataFrame, fm: pd.DataFrame, ss: pd.DataFrame, sw: pd.DataFrame, rank_status: bool = True) -> pd.DataFrame:

    dfs = []

    if fm is not None:          # Datos Fotmob
        fm_dict = matched_teams.set_index('fotmob')['team'].dropna().to_dict()
        fm['Team'] = fm['name'].map(fm_dict)
        fm = fm.rename(columns={c:f'{c}_fm' for c in fm.columns if c != 'Team'})
        dfs.append(fm)

    if ss is not None:          # Datos Sofascore
        ss_dict = matched_teams.set_index('sofascore')['team'].dropna().to_dict()
        ss['Team'] = ss['team'].map(ss_dict)
        ss = ss.rename(columns={c:f'{c}_ss' for c in ss.columns if c != 'Team'})
        dfs.append(ss)

    if sw is not None:          # Datos Scoresway
        sw_dict = matched_teams.set_index('scoresway')['team'].dropna().to_dict()
        sw['Team'] = sw['contestantClubName'].map(sw_dict)
        sw = sw.rename(columns={c:f'{c}_sw' for c in sw.columns if c != 'Team'})
        dfs.append(sw)

    if len(dfs) == 0:           # Unificamos según lo que tenemos
        unified_standing = pd.DataFrame(columns=['Team'])
    elif len(dfs) == 1:
        unified_standing = dfs[0]
    else:
        unified_standing = dfs[0]
        for df_ in dfs[1:]:
            unified_standing = unified_standing.merge(df_, on='Team', how='outer')

    return clean_standing(unified_standing, rank_status=rank_status)

# Creación de la tabla de clasificación esperada
def create_expected_standing_table(matched_teams: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:

    df_clean = df.copy()        # Copia y sacamos columnas
    df_clean = df_clean.drop(columns=['shortName', 'id', 'pageUrl', 'ongoing', 'played', 'wins', 'draws', 'losses', 'scoresStr', 'goalConDiff', 'pts', 'qualColor', 'teamId', 'teamName', 'idx'])

    fm_dict = matched_teams.set_index('fotmob')['team'].dropna().to_dict()      # Diccionario y añadimos nombre
    df_clean['Team'] = df_clean['name'].map(fm_dict)

    df_clean.columns = ['name', 'ExpectedGoalsFor', 'ExpectedGoalsAgainst', 'ExpectedPoints', 'Rank', 'ExpectedGoalsForDiff', 'ExpectedGoalsAgainstDiff', 'ExpectedPointsDiff', 'ExpectedRank', 'ExpectedRankDiff', 'Team']
    df_clean = df_clean[['Team', 'Rank', 'ExpectedRank', 'ExpectedRankDiff', 'ExpectedPoints', 'ExpectedPointsDiff', 'ExpectedGoalsFor', 'ExpectedGoalsAgainst', 'ExpectedGoalsAgainstDiff', 'ExpectedPointsDiff']]
    df_clean['Team'] = df_clean['Team'].apply(create_slug)

    return df_clean.sort_values(by='ExpectedRank')

# Añadimos goles del equipo, y del rival a aquellos partidos que los goles no se han scrapeado
def add_goals(matches_df: pd.DataFrame, team_stats_df: pd.DataFrame) -> pd.DataFrame:

    goals_df = matches_df[['Slug', 'HomeScore', 'AwayScore']]               # Dataframe con los goles home y away
    team_stats_df = team_stats_df.merge(goals_df, left_on='Match', right_on='Slug', how='left')    # Merge por slug

    team_stats_df['Goals'] = np.where(team_stats_df['HomeAway'] == 'h', team_stats_df['HomeScore'], np.where(team_stats_df['HomeAway'] == 'a', team_stats_df['AwayScore'], np.nan))             # Goles a favor
    team_stats_df['GoalsConceded'] = np.where(team_stats_df['HomeAway'] == 'h', team_stats_df['AwayScore'], np.where(team_stats_df['HomeAway'] == 'a', team_stats_df['HomeScore'], np.nan))     # Goles en contra

    team_stats_df['Goals'] = team_stats_df['Goals'].astype('Int64')                     # Entero
    team_stats_df['GoalsConceded'] = team_stats_df['GoalsConceded'].astype('Int64')

    return team_stats_df.drop(columns=['HomeScore', 'AwayScore', 'Slug'])

# Procesado de estadísticas de equipos
def team_stats_proc(df: pd.DataFrame, managers_dict: dict, cols_map: dict, cols_order: list) -> pd.DataFrame:

    df_cleaned = pd.DataFrame()
    list_columns = df.columns

    df_cleaned['Match'] = df['MatchSlug']           # Información principal partido
    df_cleaned['Team'] = df['Team']
    df_cleaned['Opponent'] = df['Opponent']
    
    df_cleaned['HomeAway'] = df['ha_ss'] if 'ha_ss' in list_columns else df['ha_sw'] if 'ha_sw' in list_columns else np.nan
    df_cleaned['Kit'] = df['kit_sw'] if 'kit_sw' in list_columns else np.nan
    df_cleaned['Formation'] = df['formation_sw'] if 'formation_sw' in list_columns else np.nan
    df_cleaned['Manager'] = df['manager_sw'].map(managers_dict) if 'manager_sw' in list_columns else np.nan
    df_cleaned['AverageAge'] = df['average_age_sw'] if 'average_age_sw' in list_columns else np.nan

    for col, possible_cols in cols_map.items():                             # Para cada columna, añadimos el MÀXIMO entre las dos
        existing_cols = [c for c in possible_cols if c in list_columns]
        if len(existing_cols) > 0:
            df_cleaned[col] = df[existing_cols].bfill(axis=1).iloc[:,0]
        else:
            df_cleaned[col] = np.nan

    not_integer_cols = ["Match","Team","Opponent","HomeAway","Kit","Formation","Manager","AverageAge","ExpectedGoals","GoalsPrevented"]
    for c in df_cleaned.columns:
        if c not in not_integer_cols:
            df_cleaned[c] = pd.to_numeric(df_cleaned[c], errors='coerce').astype('Int64')

    not_fill_na_cols = ["Match","Team","Opponent","HomeAway","Kit","Formation","Manager","AverageAge"]
    fill_na_cols = [c for c in df_cleaned.columns if c not in not_fill_na_cols]
    df_cleaned[fill_na_cols] = df_cleaned[fill_na_cols].fillna(0)                   # Columnas que tienen valores NaN ponemos 0

    df_cleaned['Formation'] = df_cleaned['Formation'].apply(lambda x: '-'.join(re.sub(r'[^0-9]', '', str(x).replace('.0', ''))) if pd.notna(x) and re.sub(r'[^0-9]', '', str(x).replace('.0', '')) != '' else np.nan)     # Limpieza de la formación
    df_cleaned = df_cleaned[cols_order]     # Orden en las columnas

    return df_cleaned

# Procesado de estadísticas de jugadores
def player_stats_proc(df: pd.DataFrame, cols_map: dict, cols_order: list, positions_dict: dict) -> pd.DataFrame:

    df_cleaned = pd.DataFrame()
    list_columns = df.columns

    df_cleaned['Match'] = df['MatchSlug']           # Información principal partido
    df_cleaned['Team'] = df['Team']
    df_cleaned['Opponent'] = df['Opponent']
    df_cleaned['Player'] = df['Player']

    df_cleaned['HomeAway'] = df['ha_ss'] if 'ha_ss' in list_columns else df['ha_sw'] if 'ha_sw' in list_columns else np.nan
    df_cleaned['Starter'] = df['starter_ss'] if 'starter_ss' in list_columns else np.nan
    df_cleaned['ShirtNumber'] = df['shirtNumber_sw'] if 'shirtNumber_sw' in list_columns else np.nan
    df_cleaned['Position'] = df['position_sw'] if 'position_sw' in list_columns else ''
    df_cleaned['PositionSide'] = df['positionSide_sw'] if 'positionSide_sw' in list_columns else ''
    df_cleaned['SubPosition'] = df['subPosition_sw'] if 'subPosition_sw' in list_columns else ''

    df_cleaned['Position'] = np.where(df_cleaned['Position'] == "Substitute", df_cleaned['SubPosition'], df_cleaned['Position'] + " " + df_cleaned['PositionSide'])     # Tratado de posición
    df_cleaned['Position'] = df_cleaned['Position'].fillna("Undefined")
    df_cleaned['Position'] = df_cleaned['Position'].map(positions_dict)     # Mapeamos con las posiciones definidas

    for col, possible_cols in cols_map.items():                             # Para cada columna, añadimos el MÀXIMO entre las dos
        existing_cols = [c for c in possible_cols if c in list_columns]
        if len(existing_cols) > 0:
            df_cleaned[col] = df[existing_cols].bfill(axis=1).iloc[:,0]
        else:
            df_cleaned[col] = np.nan

    not_integer_cols = ["Match","Team","Opponent","Player","HomeAway","Starter","Position","PositionSide","SubPosition","Rating","ExpectedAssists","TotalBallCarriesDistance","TotalProgression","BestBallCarryProgression",
                        "TotalProgressiveBallCarriesDistance","PassValue","DribbleValue","DefensiveValue","ExpectedGoals","ShotValue","ExpectedGoalsOnTarget","KeeperSaveValue","GoalkeeperValue","GoalsPrevented"]
    for c in df_cleaned.columns:
        if c not in not_integer_cols:
            df_cleaned[c] = pd.to_numeric(df_cleaned[c], errors='coerce').astype('Int64')

    not_fill_na_cols = ["Match","Team","Opponent","Player","HomeAway","Starter","Position","PositionSide","SubPosition","ShirtNumber"]
    fill_na_cols = [c for c in df_cleaned.columns if c not in not_fill_na_cols]
    df_cleaned[fill_na_cols] = df_cleaned[fill_na_cols].fillna(0)                   # Columnas que tienen valores NaN ponemos 0

    df_cleaned = df_cleaned[cols_order]

    return df_cleaned

# Procesado de datos de los partidos
def matches_proc(matches_df: pd.DataFrame, players_df: pd.DataFrame, teams_df: pd.DataFrame, managers_df: pd.DataFrame, venues_df: pd.DataFrame, sw_team: pd.DataFrame, ss_team: pd.DataFrame, sw_player: pd.DataFrame, ss_player: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:

    ss_team_dict = teams_df.set_index('IdSS')['Slug'].dropna().to_dict()            # Diccionarios con slugs para identificar jugadores y equipos a partir del ID
    sw_team_dict = teams_df.set_index('IdSW')['Slug'].dropna().to_dict()
    ss_player_dict = players_df.set_index('IdSS')['Slug'].dropna().to_dict()
    sw_player_dict = players_df.set_index('IdSW')['Slug'].dropna().to_dict()

    sw_managers_dict = managers_df.set_index('IdSW')['Slug'].dropna().to_dict()      # Mapeo de managers para los partidos

    teams_stats_list = []
    players_stats_list = []

    with open(os.path.join(utils, 'team_stats_proc', 'cols_map.json'), "r", encoding="utf-8") as f:     # Mapeo de columnas en UTILS
        teams_cols_map = jsonlib.load(f)
    with open(os.path.join(utils, 'team_stats_proc', 'cols_order.json'), "r", encoding="utf-8") as f:     # Orden de las columnas en UTILS
        teams_cols_order = jsonlib.load(f)

    with open(os.path.join(utils, 'player_stats_proc', 'cols_map.json'), "r", encoding="utf-8") as f:     # Mapeo de columnas en UTILS
        players_cols_map = jsonlib.load(f)
    with open(os.path.join(utils, 'player_stats_proc', 'cols_order.json'), "r", encoding="utf-8") as f:     # Orden de las columnas en UTILS
        players_cols_order = jsonlib.load(f)

    with open(os.path.join(utils, 'player_stats_proc', 'positions_map.json'), "r", encoding="utf-8") as f:     # Diccionario para mapear las posiciones
        positions_dict = jsonlib.load(f)
    
    for _, row in matches_df.iterrows():           # Para cada partido ya tratado, eligiremos sus datos
        id_ss = row['IdSS']             # ID sofascore
        id_sw = row['IdSW']             # ID scoresway
        slug = row['Slug']              # Slug del partido a añadir

        dfs_teams = []                  # Lista para concatenar dataframes no nulos
        dfs_players = []    

        ss_part_team = ss_team[ss_team['match_id'] == id_ss] if ss_team is not None and pd.notna(id_ss) else None               # Estadísticas de equipos SS
        if ss_part_team is not None:                                                                                            # Añadimos información en caso de que no sea None
            ss_part_team = ss_part_team.rename(columns={c:f'{c}_ss' for c in ss_part_team.columns})
            ss_part_team.insert(0, 'MatchSlug', slug)
            ss_part_team.insert(1, 'Team', ss_part_team['team_id_ss'].map(ss_team_dict))
            ss_part_team.insert(2, 'Opponent', ss_part_team['opponent_team_id_ss'].map(ss_team_dict))
            dfs_teams.append(ss_part_team)
        
        sw_part_team = sw_team[sw_team['match_id'] == id_sw] if sw_team is not None and pd.notna(id_sw) else None               # Estadísticas de equipos SW
        if sw_part_team is not None:                                                                                            # Añadimos información en caso de que no sea None
            sw_part_team = sw_part_team.rename(columns={c:f'{c}_sw' for c in sw_part_team.columns})
            sw_part_team.insert(0, 'MatchSlug', slug)
            sw_part_team.insert(1, 'Team', sw_part_team['team_id_sw'].map(sw_team_dict))
            dfs_teams.append(sw_part_team)

        ss_part_player = ss_player[ss_player['match_id'] == id_ss] if ss_player is not None and pd.notna(id_ss) else None       # Estadísitcas de jugadores
        if ss_part_player is not None:                                                                                          # Añadimos información en caso de que no sea None
            ss_part_player = ss_part_player.rename(columns={c:f'{c}_ss' for c in ss_part_player.columns})
            ss_part_player.insert(0, 'MatchSlug', slug)
            ss_part_player.insert(1, 'Team', ss_part_player['team_id_ss'].map(ss_team_dict))
            ss_part_player.insert(2, 'Opponent', ss_part_player['opponent_team_id_ss'].map(ss_team_dict))
            ss_part_player.insert(3, 'Player', ss_part_player['player_id_ss'].map(ss_player_dict))
            dfs_players.append(ss_part_player)

        sw_part_player = sw_player[sw_player['match_id'] == id_sw] if sw_player is not None and pd.notna(id_sw) else None       # Estadísitcas de jugadores SW
        if sw_part_player is not None:                                                                                          # Añadimos información en caso de que no sea None
            sw_part_player = sw_part_player.rename(columns={c:f'{c}_sw' for c in sw_part_player.columns})
            sw_part_player.insert(0, 'MatchSlug', slug)
            sw_part_player.insert(1, 'Team', sw_part_player['team_id_sw'].map(sw_team_dict))
            sw_part_player.insert(2, 'Player', sw_part_player['playerId_sw'].map(sw_player_dict))
            dfs_players.append(sw_part_player)
       
        if len(dfs_teams) == 2:                                                         # Concatenamos dependiendo de los dataframes que hemos obtenido
            raw_team_stats_df = dfs_teams[0].merge(dfs_teams[1], how='inner', on='Team', suffixes=['', '_'])
        elif len(dfs_teams) == 1:
            raw_team_stats_df = dfs_teams[0]
        else:
            raw_team_stats_df = None
        
        if len(dfs_players) == 2:
            raw_player_stats_df = dfs_players[0].merge(dfs_players[1], how='inner', on='Player', suffixes=['', '_'])
        elif len(dfs_players) == 1:
            raw_player_stats_df = dfs_players[0]
        else:
            raw_player_stats_df = None

        clean_team_stats_df = team_stats_proc(df=raw_team_stats_df, managers_dict=sw_managers_dict, cols_map=teams_cols_map, cols_order=teams_cols_order)               # Aplicamos las funciones de limpieza
        clean_player_stats_df = player_stats_proc(df=raw_player_stats_df, cols_map=players_cols_map, cols_order=players_cols_order, positions_dict=positions_dict)      # Limpieza a jugadores

        teams_stats_list.append(clean_team_stats_df)                    # Añadimos los df a la lista para concatenar en un futuro
        players_stats_list.append(clean_player_stats_df)

    team_stats_df = pd.concat(teams_stats_list, ignore_index=True)
    player_stats_df = pd.concat(players_stats_list, ignore_index=True)

    return team_stats_df, player_stats_df

# Estadísticas de temporada por equipo
def season_stats_team(teams_df: pd.DataFrame, team_stats_df: pd.DataFrame) -> pd.DataFrame:

    teams_season_df_list = []                                # Lista donde ñadiremos la información
    for team in teams_df['Slug'].unique().tolist():     # Para cada equipo, procesado
        single_team_stats = team_stats_df[team_stats_df['Team'] == team]        # Filtramos por equipo
        matches = len(single_team_stats)

        df = pd.DataFrame()
        df['Team'] = [team]
        df['Matches'] = matches
        formations = single_team_stats['Formation'].mode()
        df['Formation'] = formations.iloc[0] if len(formations) > 0 else np.nan              # No hace falta mirar si estan en las columnas porque siempre van a estar

        cols_to_sum = ['Goals', 'GoalsConceded', 'ExpectedGoals', 'GoalsPrevented', 'OwnGoals', 'GoalAssist', 'CleanSheet', 'TotalShots', 'ShotsOnTarget', 'ShotsOffTarget', 'BlockedShots', 'ShotsInsideBox', 
                       'ShotsOutsideBox', 'HitWoodwork', 'BigChances', 'TouchesInPenaltyArea', 'ThroughBalls', 'Crosses', 'Dribbles', 'BallPossession', 'Passes', 'AccuratePasses', 'LongBalls', 'FinalThirdEntries',
                       'FinalThirdPhase', 'CornerKicks', 'LostCorners', 'ThrowIns', 'Offsides', 'Fouls', 'FoulsWon', 'FoulsLost', 'YellowCards', 'RedCards', 'SecondYellow', 'Tackles', 'TacklesWon', 'TotalTackles', 
                       'Interceptions', 'Recoveries', 'Clearances', 'Duels', 'GroundDuels', 'AerialDuels', 'Dispossessed', 'FouledFinalThird', 'ErrorsLeadToShot', 'ErrorsLeadToGoal', 'GoalkeeperSaves', 'TotalSaves', 
                       'BigSaves', 'Punches', 'HighClaims', 'GoalKicks', 'PenaltySaves', 'PenaltyWon', 'PenaltyConceded', 'PenaltyFaced', 'PenGoalsConceded', 'SubsMade', 'SubsGoals']
        
        for col in cols_to_sum:
            df[col] = single_team_stats[col].sum()
        
        # Creación de nuevas columnas
        df['GoalsPerMatch'] = safe_div(df['Goals'].sum(), matches)                                      # Goles por partido
        df['GoalsConcededsPerMatch'] = safe_div(df['GoalsConceded'].sum(), matches)
        df['ExpectedGoalsPerMatch'] = safe_div(df['ExpectedGoals'].sum(), matches)
        df['GoalsPreventedPerMatch'] = safe_div(df['GoalsPrevented'].sum(), matches)

        df['ShotAccuracy'] = safe_div(df['ShotsOnTarget'].sum(), df['TotalShots'].sum())                # Goles y tiros
        df['ShotOffTargetRate'] = safe_div(df['ShotsOffTarget'].sum(), df['TotalShots'].sum())
        df['BlockedShotRate'] = safe_div(df['BlockedShots'].sum(), df['TotalShots'].sum())
        df['GoalConversion'] = safe_div(df['Goals'].sum(), df['TotalShots'].sum())
        df['OnTargetConversion'] = safe_div(df['Goals'].sum(), df['ShotsOnTarget'].sum())
        df['BigChanceRate'] = safe_div(df['BigChances'].sum(), df['TotalShots'].sum())
        df['BigChanceConversion'] = safe_div(df['Goals'].sum(), df['BigChances'].sum())
        df['BoxShotRate'] = safe_div(df['ShotsInsideBox'].sum(), df['TotalShots'].sum())
        df['OutsideShotRate'] = safe_div(df['ShotsOutsideBox'].sum(), df['TotalShots'].sum())
        df['XGPerShot'] = safe_div(df['ExpectedGoals'].sum(), df['TotalShots'].sum())
        df['GoalsMinusXG'] = df['Goals'].sum() - df['ExpectedGoals'].sum()

        df['PassAccuracy'] = safe_div(df['AccuratePasses'].sum(), df['Passes'].sum())                   # Creación y pases
        df['LongBallsPerMatch'] = safe_div(df['LongBalls'].sum(), matches)
        df['CrossesPerMatch'] = safe_div(df['Crosses'].sum(), matches)
        df['FinalThirdEntriesPerMatch'] = safe_div(df['FinalThirdEntries'].sum(), matches)
        df['TouchesInPenaltyAreaPerMatch'] = safe_div(df['TouchesInPenaltyArea'].sum(), matches)
        df['ThroughBallsPerMatch'] = safe_div(df['ThroughBalls'].sum(), matches)
        df['DribblesPerMatch'] = safe_div(df['Dribbles'].sum(), matches)

        df['GoalsConcededPerMatch'] = safe_div(df['GoalsConceded'].sum(), matches)                # Defensa
        df['CleanSheetRate'] = safe_div(df['CleanSheet'].sum(), matches)
        df['TackleSuccess'] = safe_div(df['TacklesWon'].sum(), df['Tackles'].sum())
        df['InterceptionsPerMatch'] = safe_div(df['Interceptions'].sum(), matches)
        df['RecoveriesPerMatch'] = safe_div(df['Recoveries'].sum(), matches)
        df['ClearancesPerMatch'] = safe_div(df['Clearances'].sum(), matches)
        df['DuelsPerMatch'] = safe_div(df['Duels'].sum(), matches)
        df['GroundDuelsPerMatch'] = safe_div(df['GroundDuels'].sum(), matches)
        df['AerialDuelsPerMatch'] = safe_div(df['AerialDuels'].sum(), matches)
        df['ErrorsLeadToShotRate'] = safe_div(df['ErrorsLeadToShot'].sum(), matches)
        df['ErrorsLeadToGoalRate'] = safe_div(df['ErrorsLeadToGoal'].sum(), matches)

        df['PenaltySaveRate'] = safe_div(df['PenaltySaves'].sum(), df['PenaltyFaced'].sum())            # Portería
        df['GoalsPreventedPerMatch'] = safe_div(df['GoalsPrevented'].sum(), matches)
        df['GoalkeeperSavesPerMatch'] = safe_div(df['GoalkeeperSaves'].sum(), matches)
        df['TotalSavesPerMatch'] = safe_div(df['TotalSaves'].sum(), matches)

        df['FoulsPerMatch'] = safe_div(df['Fouls'].sum(), matches)                                # Disciplina
        df['FoulsWonPerMatch'] = safe_div(df['FoulsWon'].sum(), matches)
        df['FoulsLostPerMatch'] = safe_div(df['FoulsLost'].sum(), matches)
        df['YellowCardsPerMatch'] = safe_div(df['YellowCards'].sum(), matches)
        df['RedCardsPerMatch'] = safe_div(df['RedCards'].sum(), matches)
        df['OffsidesPerMatch'] = safe_div(df['Offsides'].sum(), matches)

        df['GoalsPerMatch'] = safe_div(df['Goals'].sum(), matches)                                # Balance
        df['ExpectedGoalsPerMatch'] = safe_div(df['ExpectedGoals'].sum(), matches)
        df['GoalDifference'] = df['Goals'].sum() - df['GoalsConceded'].sum()

        teams_season_df_list.append(df)        # Añadimos a la lista
    
    return pd.concat(teams_season_df_list, ignore_index=True)

# Estadísticas de temporada por jugador
def season_stats_player(players_df: pd.DataFrame, player_stats_df: pd.DataFrame) -> pd.DataFrame:

    players_season_df_list = []                                 # Lista donde ñadiremos la información
    for _, row in players_df.iterrows():         # Para cada equipo, procesado

        player = row['Slug']

        single_player_stats = player_stats_df[player_stats_df['Player'] == player]        # Filtramos por jugador
        matches = len(single_player_stats)
        minutes = single_player_stats['MinutesPlayed'].sum()
        avg_minutes = round(single_player_stats['MinutesPlayed'].mean(), 4) if pd.notna(single_player_stats['MinutesPlayed'].mean()) else np.nan
        per90_factor = safe_div(90, minutes)

        df = pd.DataFrame()                                     # Añadimos columnas con informaciñó
        df['Player'] = [player]
        df['Team'] = row['Team']
        df['ShirtNumber'] = row['ShirtNumber']
        positions = single_player_stats['Position'].mode()
        df['Position'] = positions[0] if len(positions) > 0 else np.nan
        df['Matches'] = matches
        df['MatchesStarter'] = single_player_stats['Starter'].sum()
        df['MatchesBench'] = df['Matches'] - df['MatchesStarter']
        df['StartsRate'] = safe_div(df['MatchesStarter'].sum(), df['Matches'].sum())
        df['MinutesPlayed'] = minutes
        df['MinutesPerMatch'] = avg_minutes
        df['AvgRating'] = round(single_player_stats['Rating'].mean(), 4)

        cols_to_sum = ['Touches', 'Rating', 'PossessionLost', 'Passes', 'AccuratePasses', 'LongBalls', 'AccurateLongBalls', 'AccurateOwnHalfPasses', 'TotalOwnHalfPasses',
                    'TotalOppositionHalfPasses', 'AccurateOppositionHalfPasses', 'Crosses', 'AccurateCrosses', 'KeyPasses', 'GoalAssist', 'ExpectedAssists',
                    'BallCarriesCount', 'TotalBallCarriesDistance', 'TotalProgression', 'BestBallCarryProgression', 'ProgressiveBallCarriesCount',
                    'TotalProgressiveBallCarriesDistance', 'TotalShots', 'ShotsOnTarget', 'ShotsOffTarget', 'BlockedShots', 'ExpectedGoals',
                    'ExpectedGoalsOnTarget', 'BigChanceCreated', 'BigChanceMissed', 'Goals', 'HitWoodwork', 'ShotValue', 'DuelsWon', 'DuelsLost', 'AerialWon',
                    'AerialLost', 'Tackles', 'TacklesWon', 'Interceptions', 'Recoveries', 'Clearances', 'LastManTackle', 'OutfielderBlocks', 'ErrorsLeadToShot',
                    'ErrorsLeadToGoal', 'Saves', 'SavedShotsFromInsideTheBox', 'GoalsPrevented', 'KeeperSaveValue', 'GoalkeeperValue', 'Punches',
                    'HighClaims', 'ClearanceOffLine', 'KeeperSweeperActions', 'AccurateKeeperSweeperActions', 'GoalKicks', 'GoalsConceded',
                    'CleanSheet', 'Fouls', 'WasFouled', 'Offsides', 'YellowCards', 'RedCards', 'SecondYellow', 'PenaltyWon', 'PenaltyConceded',
                    'PenaltyFaced', 'PenaltySave', 'PenaltyMiss', 'PenGoalsConceded', 'OwnGoals', 'CrossNotClaimed', 'ThrowIns', 'CornerKicks', 'LostCorners']

        for c in cols_to_sum:
            df[c] = single_player_stats[c].sum()        # Suma del valor - valor total
            df[f'{c}Per90'] = single_player_stats[c].sum() * per90_factor

        df['PassAccuracy'] = safe_div(df['AccuratePasses'].sum(), df['Passes'].sum())                   # Pases
        df['LongBallAccuracy'] = safe_div(df['AccurateLongBalls'].sum(), df['LongBalls'].sum())
        df['OwnHalfPassAccuracy'] = safe_div(df['AccurateOwnHalfPasses'].sum(), df['TotalOwnHalfPasses'].sum())
        df['OppositionHalfPassAccuracy'] = safe_div(df['AccurateOppositionHalfPasses'].sum(), df['TotalOppositionHalfPasses'].sum())
        df['CrossAccuracy'] = safe_div(df['AccurateCrosses'].sum(), df['Crosses'].sum())

        df['KeyPassesPerPass'] = safe_div(df['KeyPasses'].sum(), df['Passes'].sum())                    # Creación
        df['ExpectedAssistsPerKeyPass'] = safe_div(df['ExpectedAssists'].sum(), df['KeyPasses'].sum())
        df['ProgressiveCarriesShare'] = safe_div(df['ProgressiveBallCarriesCount'].sum(), df['BallCarriesCount'].sum())
        df['AvgCarryDistance'] = safe_div(df['TotalBallCarriesDistance'].sum(), df['BallCarriesCount'].sum())
        df['AvgProgressionPerCarry'] = safe_div(df['TotalProgression'].sum(), df['BallCarriesCount'].sum())
        df['AvgProgressiveCarryDistance'] = safe_div(df['TotalProgressiveBallCarriesDistance'].sum(), df['ProgressiveBallCarriesCount'].sum())

        df['ShotAccuracy'] = safe_div(df['ShotsOnTarget'].sum(), df['TotalShots'].sum())                # Tiro
        df['ShotOffTargetRate'] = safe_div(df['ShotsOffTarget'].sum(), df['TotalShots'].sum())
        df['BlockedShotRate'] = safe_div(df['BlockedShots'].sum(), df['TotalShots'].sum())
        df['GoalConversion'] = safe_div(df['Goals'].sum(), df['TotalShots'].sum())
        df['OnTargetConversion'] = safe_div(df['Goals'].sum(), df['ShotsOnTarget'].sum())
        df['XGPerShot'] = safe_div(df['ExpectedGoals'].sum(), df['TotalShots'].sum())

        df['GoalsMinusXG'] = (round(df['Goals'].sum() - df['ExpectedGoals'].sum(), 4) if pd.notna(df['Goals'].sum()) and pd.notna(df['ExpectedGoals'].sum()) else np.nan)
        df['BigChanceMissRate'] = safe_div(df['BigChanceMissed'].sum(), df['BigChanceMissed'].sum() + df['Goals'].sum())
        df['BigChanceCreateToAssist'] = safe_div(df['GoalAssist'].sum(), df['BigChanceCreated'].sum())
        df['DuelWinRate'] = safe_div(df['DuelsWon'].sum(), df['DuelsWon'].sum() + df['DuelsLost'].sum())
        df['AerialWinRate'] = safe_div(df['AerialWon'].sum(), df['AerialWon'].sum() + df['AerialLost'].sum())

        df['TackleSuccess'] = safe_div(df['TacklesWon'].sum(), df['Tackles'].sum())                     # Defensa
        df['RecoveriesPerTouch'] = safe_div(df['Recoveries'].sum(), df['Touches'].sum())
        df['InterceptionsPlusRecoveries'] = (df['Interceptions'].sum() + df['Recoveries'].sum())
        df['DefensiveActions'] = (df['Tackles'].sum() + df['Interceptions'].sum() + df['Recoveries'].sum() + df['Clearances'].sum() + df['OutfielderBlocks'].sum())

        df['FoulsPerWasFouled'] = safe_div(df['Fouls'].sum(), df['WasFouled'].sum())                    # Disciplina
        df['PossessionLostPerTouch'] = safe_div(df['PossessionLost'].sum(), df['Touches'].sum())

        df['SaveRate'] = safe_div(df['Saves'].sum(), df['Saves'].sum() + df['GoalsConceded'].sum())     # Portero
        df['PenaltySaveRate'] = safe_div(df['PenaltySave'].sum(), df['PenaltyFaced'].sum())
        df['GoalsConcededPerSave'] = safe_div(df['GoalsConceded'].sum(), df['Saves'].sum())

        df['GoalContributions'] = df['Goals'].sum() + df['GoalAssist'].sum()                            #
        df['GoalContributionsPerMatch'] = safe_div(df['GoalContributions'].sum(), df['Matches'].sum())
        df['GoalContributionsPerStart'] = safe_div(df['GoalContributions'].sum(), df['MatchesStarter'].sum())
        df['StartsRate'] = safe_div(df['MatchesStarter'].sum(), df['Matches'].sum())

        players_season_df_list.append(df)

    return pd.concat(players_season_df_list, ignore_index=True)

# Tratado de imagenes - movemos las imagenes de los jugadores, escudos... a la carpeta final
def images_proc(players_df: pd.DataFrame, teams_df: pd.DataFrame, managers_df: pd.DataFrame, venues_df: pd.DataFrame, images_path: str, processed_data_path: str) -> None:

    default_images_path = os.path.join(utils, 'default_images')             # Imagenes por defecto - por si no hay imagen de jugador, equipo, entrenador...
    default_player = os.path.join(default_images_path, 'player.png')        # Jugador
    default_team = os.path.join(default_images_path, 'team.png')            # Escudo del equipo
    default_manager = os.path.join(default_images_path, 'manager.png')      # Entrenador
    default_venue = os.path.join(default_images_path, 'venue.png')          # Estadio

    players_images_path = os.path.join(images_path, 'player')               # Paths de las carpetas con todas las imagenes
    teams_images_path = os.path.join(images_path, 'team')
    managers_images_path = os.path.join(images_path, 'manager')
    venues_images_path = os.path.join(images_path, 'venue')

    out_images_path = os.path.join(processed_data_path, 'images')               # Dataframe con salida de imagenes - para cada tipo también y los creamos
    os.makedirs(out_images_path, exist_ok=True)
    out_players_images = os.path.join(out_images_path, 'player') 
    os.makedirs(out_players_images, exist_ok=True)
    out_teams_images = os.path.join(out_images_path, 'team') 
    os.makedirs(out_teams_images, exist_ok=True)
    out_managers_images = os.path.join(out_images_path, 'manager') 
    os.makedirs(out_managers_images, exist_ok=True)
    out_venues_images = os.path.join(out_images_path, 'venue') 
    os.makedirs(out_venues_images, exist_ok=True)

    for _, row in players_df.iterrows():
        input_image_path = os.path.join(players_images_path, f'{row['IdSS']}.png')
        output_image_path = os.path.join(out_players_images, f'{row['Slug']}.png')

        valid_image = False                         # Comprovación de que la imagen és buena
        if os.path.exists(input_image_path):
            try:
                with Image.open(input_image_path) as img:
                    img.verify()   # Comprueba que la imagen es válida
                valid_image = True
            except Exception:
                valid_image = False
        if valid_image:
            shutil.copy2(input_image_path, output_image_path)   # Copia sin borrar
        else:
            shutil.copy2(default_player, output_image_path)

    for _, row in managers_df.iterrows():
        input_image_path = os.path.join(managers_images_path, f'{row['IdSS']}.png')
        output_image_path = os.path.join(out_managers_images, f'{row['Slug']}.png')

        valid_image = False                         # Comprovación de que la imagen és buena
        if os.path.exists(input_image_path):
            try:
                with Image.open(input_image_path) as img:
                    img.verify()   # Comprueba que la imagen es válida
                valid_image = True
            except Exception:
                valid_image = False
        if valid_image:
            shutil.copy2(input_image_path, output_image_path)   # Copia sin borrar
        else:
            shutil.copy2(default_manager, output_image_path)

    for _, row in teams_df.iterrows():
        input_image_path = os.path.join(teams_images_path, f'{row['IdSS']}.png')
        output_image_path = os.path.join(out_teams_images, f'{row['Slug']}.png')

        valid_image = False                         # Comprovación de que la imagen és buena
        if os.path.exists(input_image_path):
            try:
                with Image.open(input_image_path) as img:
                    img.verify()   # Comprueba que la imagen es válida
                valid_image = True
            except Exception:
                valid_image = False
        if valid_image:
            shutil.copy2(input_image_path, output_image_path)   # Copia sin borrar
        else:
            shutil.copy2(default_team, output_image_path)

    for _, row in venues_df.iterrows():
        input_image_path = os.path.join(venues_images_path, f'{row['IdSS']}.png')
        output_image_path = os.path.join(out_venues_images, f'{row['Slug']}.png')

        valid_image = False                         # Comprovación de que la imagen és buena
        if os.path.exists(input_image_path):
            try:
                with Image.open(input_image_path) as img:
                    img.verify()   # Comprueba que la imagen es válida
                valid_image = True
            except Exception:
                valid_image = False
        if valid_image:
            shutil.copy2(input_image_path, output_image_path)   # Copia sin borrar
        else:
            shutil.copy2(default_venue, output_image_path)

# Obtiene los datos de una simple temporada de una liga
def season_data_unification(fotmob_clean_path: str, scoresway_clean_path: str, sofascore_clean_path: str, print_info: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    fm_info_df, fm_matches_df, fm_all_st_df, fm_home_st_df, fm_away_st_df, fm_form_st_df, fm_xg_st_df = read_fotmob_data(fotmob_clean_path=fotmob_clean_path)                                       # Lectura de los datos de Fotmob
    fotmob_teams = obtain_fotmob_teams(matches_df=fm_matches_df, all_st_df=fm_all_st_df, home_st_df=fm_home_st_df, away_st_df=fm_away_st_df, form_st_df=fm_form_st_df, xg_st_df=fm_xg_st_df)        # Obtención de los equipos

    sw_managers_df, sw_matches_df, sw_players_df, sw_teams_df, sw_total_st_df, sw_home_st_df, sw_away_st_df, sw_httotal_st_df, sw_hthome_st_df, sw_htaway_st_df, sw_formhome_st_df, sw_formaway_st_df, sw_overunder_st_df, sw_attendance_st_df, sw_matches_info_df, sw_matches_player_stats_df, sw_matches_team_stats_df, sw_matches_referees_df = read_scoresway_data(scoresway_clean_path=scoresway_clean_path)     # Lectura de datos de Scoresway
    scoresway_teams = sorted(sw_teams_df['club_name'].unique().tolist()) if sw_teams_df is not None else []           # Equipos en scoresway

    ss_managers_df, ss_players_df, ss_teams_df, ss_venues_df, ss_total_st_df, ss_home_st_df, ss_away_st_df, ss_matches_info_df, ss_matches_lineups_df, ss_matches_statistics_df = read_sofascore_data(sofascore_clean_path=sofascore_clean_path)        # Lectura de los datos de Sofascore
    sofascore_teams = sorted(ss_teams_df['name'].unique().tolist()) if ss_teams_df is not None else []                 # Equipos en sofascore

    matched_teams = match_teams(fm_list=fotmob_teams, sw_list=scoresway_teams, ss_list=sofascore_teams)     # Mapeamos equipos
    if sw_teams_df is not None:
        sw_long_name_dict = sw_teams_df.set_index('club_name')['name'].dropna().to_dict()                       # Diccionario con los nombres largos en scoresway
        matched_teams['longname_scoresway'] = matched_teams['scoresway'].map(sw_long_name_dict)                 # Aplicamos el diccionario
    else:
        matched_teams['longname_scoresway'] = np.nan

    # A partir de aquí, con toda la información que tenemos, vamos a crear dataframes unificados a partir de, también, el dataframe de mapeo de equipos
    teams_df = create_teams_info_df(matched_teams=matched_teams, sw_teams_df=sw_teams_df, ss_teams_df=ss_teams_df)                          # Información de equipos
    players_df = create_players_info_df(matched_teams=matched_teams, sw_players_df=sw_players_df, ss_players_df=ss_players_df)              # Información de jugadores
    teams_df, managers_df = create_managers_info_df(teams_df=teams_df, sw_managers_df=sw_managers_df, ss_managers_df=ss_managers_df)        # Información de managers - añadimos manager al dataframe de equipo
    teams_df, venues_df = create_venues_info_df(teams_df=teams_df, ss_venues_df=ss_venues_df)                                               # Información de estadios - añadimos estadio al dataframe de equipo
    matches_df = create_matches_info_df(teams_df=teams_df, ss_matches_info_df=ss_matches_info_df, sw_matches_info_df=sw_matches_info_df)    # Información de partidos
    if print_info:
        print(f"        - League information unified")

    # Tablas de clasificación
    all_standings = unified_standings_tables(matched_teams=matched_teams, fm=fm_all_st_df, ss=ss_total_st_df, sw=sw_total_st_df)
    home_standings = unified_standings_tables(matched_teams=matched_teams, fm=fm_home_st_df, ss=ss_home_st_df, sw=sw_home_st_df, rank_status=False)
    away_standings = unified_standings_tables(matched_teams=matched_teams, fm=fm_away_st_df, ss=ss_away_st_df, sw=sw_away_st_df, rank_status=False)
    half_time_standings = unified_standings_tables(matched_teams=matched_teams, fm=None, ss=None, sw=sw_httotal_st_df, rank_status=False)
    expected_standings = create_expected_standing_table(matched_teams=matched_teams, df=fm_xg_st_df)
    if print_info:
        print(f"        - Standings tables unified")

    # Estadísticas de jugadores y equipos durante los partidos
    team_stats_df, player_stats_df = matches_proc(matches_df=matches_df, players_df=players_df, teams_df=teams_df, managers_df=managers_df, venues_df=venues_df, sw_team=sw_matches_team_stats_df, ss_team=ss_matches_statistics_df, sw_player=sw_matches_player_stats_df, ss_player=ss_matches_lineups_df)
    if max(team_stats_df['Goals'] == 0):
        team_stats_df = add_goals(matches_df=matches_df, team_stats_df=team_stats_df)           # Añadimos goles al rendimiento del equipo para aquellos partidos que no se haya entrado
    if print_info:
        print(f"        - Teams and players stats in the matches")

    # Estadísticas por temporada de equipos y jugadores
    team_stats_season_df = season_stats_team(teams_df=teams_df, team_stats_df=team_stats_df)
    player_stats_season_df = season_stats_player(players_df=players_df, player_stats_df=player_stats_df)
    if print_info:
        print(f"        - Full season teams and players stats")

    return teams_df, players_df, managers_df, venues_df, all_standings, home_standings, away_standings, half_time_standings, expected_standings, team_stats_df, player_stats_df, team_stats_season_df, player_stats_season_df

# Unificador de datos de una liga
def league_data_unification(league_id: int, raw_data_path: str, clean_data_path: str, processed_data_path: str, print_info: bool = True) -> None:
    league_name = comps[comps['id'] == league_id]['tournament'].iloc[0]     # Nombre de la liga
    league_slug = create_slug(text=league_name)                             # Slug de la liga

    if print_info:
        print("================================================================================")
        print(f"Starting data unification ({league_name})")

    images_path = os.path.join(raw_data_path, 'images')                                          # Imagenes
    clean_league_path = os.path.join(processed_data_path, league_slug)
    os.makedirs(clean_league_path, exist_ok=True)

    clean_all_path = os.path.join(clean_league_path, 'All')         # Contendrà la información de todas las temporadas
    os.makedirs(clean_all_path, exist_ok=True)

    teams_list, players_list, managers_list, venues_list = [], [], [], []                                                                       # Lista con información
    all_standings_list, home_standings_list, away_standings_list, half_time_standings_list, expected_standings_list = [], [], [], [], []        # Tablas de clasificación
    team_stats_df_list, player_stats_df_list, team_stats_season_df_list, player_stats_season_df_list = [], [], [], []                           # Estadísticas

    for season_key in desired_seasons:
        if print_info:
            print(f"     - Data unification of season {season_key}")

        clean_season_path = os.path.join(clean_league_path, season_key)         # Contendrà la información de la temporada
        os.makedirs(clean_season_path, exist_ok=True)

        fotmob_clean_path = os.path.join(clean_data_path, 'fotmob', league_slug, season_key)         # Fotmob
        scoresway_clean_path = os.path.join(clean_data_path, 'scoresway', league_slug, season_key)   # Scoresway
        sofascore_clean_path = os.path.join(clean_data_path, 'sofascore', league_slug, season_key)   # Sofascore

        # Unificamos los datos y tratado de imagenes
        teams_df, players_df, managers_df, venues_df, all_standings, home_standings, away_standings, half_time_standings, expected_standings, team_stats_df, player_stats_df, team_stats_season_df, player_stats_season_df = season_data_unification(fotmob_clean_path=fotmob_clean_path, scoresway_clean_path=scoresway_clean_path, sofascore_clean_path=sofascore_clean_path, print_info=print_info)
        images_proc(players_df=players_df, managers_df=managers_df, teams_df=teams_df, venues_df=venues_df, images_path=images_path, processed_data_path=processed_data_path)
        if print_info:
            print(f"        - Images processed")
        
        # Tratado de cada dataframe - borramos IDs y añadimos liga y season
        dfs = [teams_df, players_df, managers_df, venues_df, all_standings, home_standings, away_standings, half_time_standings, expected_standings, team_stats_df, player_stats_df, team_stats_season_df, player_stats_season_df]
        for df in dfs:
            df.drop(columns=['IdSS', 'IdFM', 'IdSW'], errors='ignore', inplace=True)
            if 'League' not in df.columns:
                df.insert(0, 'League', league_slug)
            if 'Season' not in df.columns:
                df.insert(1, 'Season', season_key)

        info_path = os.path.join(clean_season_path, 'info')                 # Creación de carpetas para ir guardando información
        os.makedirs(info_path, exist_ok=True)       
        standings_path = os.path.join(clean_season_path, 'standings')
        os.makedirs(standings_path, exist_ok=True)
        stats_paths = os.path.join(clean_season_path, 'statistics')
        os.makedirs(stats_paths, exist_ok=True)

        # Para cada dataframe, lo vamos a concatenar a la lista general y lo vamos a guardar - Información
        teams_list.append(teams_df)
        teams_df.to_csv(os.path.join(info_path, 'team.csv'), index=False, sep=';')
        players_list.append(players_df)
        players_df.to_csv(os.path.join(info_path, 'player.csv'), index=False, sep=';')
        managers_list.append(managers_df)
        managers_df.to_csv(os.path.join(info_path, 'manager.csv'), index=False, sep=';')
        venues_list.append(venues_df)
        venues_df.to_csv(os.path.join(info_path, 'venue.csv'), index=False, sep=';')

        # Tablas de clasificación
        all_standings_list.append(all_standings)
        all_standings.to_csv(os.path.join(standings_path, 'all.csv'), index=False, sep=';')
        home_standings_list.append(home_standings)
        home_standings.to_csv(os.path.join(standings_path, 'home.csv'), index=False, sep=';')
        away_standings_list.append(away_standings)
        away_standings.to_csv(os.path.join(standings_path, 'away.csv'), index=False, sep=';')
        half_time_standings_list.append(half_time_standings)
        half_time_standings.to_csv(os.path.join(standings_path, 'half_time.csv'), index=False, sep=';')
        expected_standings_list.append(expected_standings)
        expected_standings.to_csv(os.path.join(standings_path, 'expected.csv'), index=False, sep=';')

        # Estadísticas
        team_stats_df_list.append(team_stats_df)
        team_stats_df.to_csv(os.path.join(stats_paths, 'team_match.csv'), index=False, sep=';')
        player_stats_df_list.append(player_stats_df)
        player_stats_df.to_csv(os.path.join(stats_paths, 'player_match.csv'), index=False, sep=';')
        team_stats_season_df_list.append(team_stats_season_df)
        team_stats_season_df.to_csv(os.path.join(stats_paths, 'team_season.csv'), index=False, sep=';')
        player_stats_season_df_list.append(player_stats_season_df)
        player_stats_season_df.to_csv(os.path.join(stats_paths, 'player_season.csv'), index=False, sep=';')

    info_path = os.path.join(clean_all_path, 'info')                 # Creación de carpetas para ir guardando información dentro de 'ALL'
    os.makedirs(info_path, exist_ok=True)       
    standings_path = os.path.join(clean_all_path, 'standings')
    os.makedirs(standings_path, exist_ok=True)
    stats_paths = os.path.join(clean_all_path, 'statistics')
    os.makedirs(stats_paths, exist_ok=True)

    # Guardado de todos los dataframes unificados en las carpetas unificadas
    teams_df = pd.concat(teams_list, ignore_index=True)                     # Info
    teams_df.to_csv(os.path.join(info_path, 'team.csv'))                    
    players_df = pd.concat(players_list, ignore_index=True)
    players_df.to_csv(os.path.join(info_path, 'player.csv'))
    managers_df = pd.concat(managers_list, ignore_index=True)
    managers_df.to_csv(os.path.join(info_path, 'manager.csv'))
    venues_df = pd.concat(venues_list, ignore_index=True)
    venues_df.to_csv(os.path.join(info_path, 'venue.csv'))

    all_standings = pd.concat(all_standings_list, ignore_index=True)        # Tablas de clasificación
    all_standings.to_csv(os.path.join(standings_path, 'all.csv'))
    home_standings = pd.concat(home_standings_list, ignore_index=True)
    home_standings.to_csv(os.path.join(standings_path, 'home.csv'))
    away_standings = pd.concat(away_standings_list, ignore_index=True)
    away_standings.to_csv(os.path.join(standings_path, 'away.csv'))
    half_time_standings = pd.concat(half_time_standings_list, ignore_index=True)
    half_time_standings.to_csv(os.path.join(standings_path, 'half_time.csv'))
    expected_standings = pd.concat(expected_standings_list, ignore_index=True)
    expected_standings.to_csv(os.path.join(standings_path, 'expected.csv'))

    team_stats_df = pd.concat(team_stats_df_list, ignore_index=True)        # Statistics
    team_stats_df.to_csv(os.path.join(stats_paths, 'team_match.csv'))
    player_stats_df = pd.concat(player_stats_df_list, ignore_index=True)
    player_stats_df.to_csv(os.path.join(stats_paths, 'player_match.csv'))
    team_stats_season_df = pd.concat(team_stats_season_df_list, ignore_index=True)
    team_stats_season_df.to_csv(os.path.join(stats_paths, 'team_season.csv'))
    player_stats_season_df = pd.concat(player_stats_season_df_list, ignore_index=True)
    player_stats_season_df.to_csv(os.path.join(stats_paths, 'team_season.csv'))

    elapsed_time = time.time() - start_time                 # Suele tardar más en Sofascore por eso añadimos la posibilidad de mostrarlo en minutos
    if elapsed_time >= 60:
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)
        time_str = f"{minutes} minutes {seconds} seconds"
    else:
        time_str = f"{elapsed_time:.2f} seconds"
    if print_info:
        print(f'Finished data unification ({league_name}) in {elapsed_time:.2f} seconds')
        print('================================================================================')