import os
import pandas as pd
import time
import numpy as np
from typing import Tuple

from use.config import comps
from use.functions import json_to_dict, create_slug

# Procesado de las tablas de clasificación
def standings_tables_proc(standings_path: str, standings_output_path: str) -> list:

    if not os.path.exists(standings_path):
        return []
    
    os.makedirs(standings_output_path, exist_ok=True)           # Creación del directorio de output
    standings_data = json_to_dict(json_path=standings_path)     # Lectura archivo

    list_return = []

    for type in ['total', 'home', 'away']:              # Para cada tipo de clasificación
        standings_part = standings_data.get(type, {}).get('standings', [])[0].get('rows')
        list_info = []
        if standings_part:
            for team in standings_part:
                list_info.append({'team_id': team.get('team', {}).get('id', np.nan),
                                'team': team.get('team', {}).get('name', np.nan),
                                'position': team.get('position', np.nan),
                                'promotion': team.get('promotion', {}).get('text', np.nan),
                                'matches': team.get('matches', np.nan),
                                'wins': team.get('wins', np.nan),
                                'losses': team.get('losses', np.nan),
                                'draws': team.get('draws', np.nan),
                                'scores_for': team.get('scoresFor', np.nan),
                                'scores_against': team.get('scoresAgainst', np.nan),
                                'points': team.get('points', np.nan),})
            standings_df = pd.DataFrame(list_info)
            list_return.append(standings_df)        # Append a la lista a devolver
            standings_df.to_csv(os.path.join(standings_output_path, f'{type}.csv'), index=False, sep=';')

    return list_return

# Procesado de información de los jugadores
def players_proc(players_json_path: str, players_dir_path: str, df_output_path: str) -> pd.DataFrame:

    if not os.path.exists(players_json_path):
        return pd.DataFrame()

    all_players_data = json_to_dict(json_path=players_json_path).get('players')         # Lectura archivo

    if all_players_data:                                # Dataframe con jugadores
        players_df = pd.DataFrame(all_players_data)

        players_info = []
        for player in os.listdir(players_dir_path):
            single_player_data = json_to_dict(json_path=os.path.join(players_dir_path, player)).get('player')   # Info del jugador
            if single_player_data:

                positions_detailed = single_player_data.get('positionsDetailed', [])                    # Posiciones
                first_position = positions_detailed[0] if len(positions_detailed) > 0 else np.nan
                second_position = positions_detailed[1] if len(positions_detailed) > 1 else np.nan
                third_position = positions_detailed[2] if len(positions_detailed) > 2 else np.nan

                players_info.append({'playerId': single_player_data.get('id', np.nan),      # Añadimos información por jugador
                                    'shortName': single_player_data.get('shortName', np.nan),
                                    'first_position': first_position,
                                    'second_position': second_position,
                                    'third_position': third_position,
                                    'shirt_num': single_player_data.get('shirtNumber', np.nan),
                                    'height': single_player_data.get('height', np.nan),
                                    'pref_foot': single_player_data.get('preferredFoot', np.nan),
                                    'date_birth': single_player_data.get('dateOfBirthTimestamp', np.nan),
                                    'country': single_player_data.get('country', {}).get('name', np.nan),
                                    'contract_until': single_player_data.get('contractUntilTimestamp', np.nan),
                                    'market_value': single_player_data.get('proposedMarketValue', np.nan)})

        players_more_info_df = pd.DataFrame(players_info)       # Dataframe con información extra
        players_df = players_df.merge(players_more_info_df, how='left', on='playerId')
        players_df.to_csv(df_output_path, index=False, sep=';') # Guardado
        return players_df

# Procesado de equipos
def teams_proc(teams_dir_path: str, df_output_path: str) -> pd.DataFrame:

    if not os.path.exists(teams_dir_path):
        return pd.DataFrame()
    
    teams_info = []                             # Lista para ir añadiendo información

    for team in os.listdir(teams_dir_path):     # Para cada equipo, concatenar información
        single_team_data = json_to_dict(os.path.join(teams_dir_path, team)).get('team')
        if single_team_data:
            teams_info.append({'team_id': single_team_data.get('id', np.nan),
                            'name': single_team_data.get('name', np.nan),
                            'short_name': single_team_data.get('shortName', np.nan),
                            'full_name': single_team_data.get('fullName', np.nan),
                            'manager': single_team_data.get('manager', {}).get('id', np.nan),
                            'venue': single_team_data.get('venue', {}).get('id', np.nan),
                            'country': single_team_data.get('country', {}).get('name', np.nan),
                            'foundation_date': single_team_data.get('foundationDateTimestamp', np.nan),
                            'primary_colour': single_team_data.get('teamColors', {}).get('primary', np.nan),
                            'secondary_colour': single_team_data.get('teamColors', {}).get('secondary', np.nan),
                            'text_colour': single_team_data.get('teamColors', {}).get('text', np.nan)})

    teams_df = pd.DataFrame(teams_info)     # Dataframe
    teams_df.to_csv(df_output_path, index=False, sep=';')
    return teams_df

# Procesado de estadios
def venues_proc(venues_json_path: str, df_output_path: str) -> pd.DataFrame:

    if not os.path.exists(venues_json_path):
        return pd.DataFrame()
    
    venues_data = json_to_dict(json_path=venues_json_path).get('venues')
    if venues_data:

        venues_info = []
        for venue in venues_data:
            venues_info.append({'venue_id': venue.get('id', np.nan),
                                'name': venue.get('name', np.nan),
                                'capacity': venue.get('capacity', np.nan),
                                'city': venue.get('city', {}).get('name', np.nan),
                                'latitude': venue.get('venueCoordinates', {}).get('latitude', np.nan),
                                'longitude': venue.get('venueCoordinates', {}).get('longitude', np.nan),})
        
        venues_df = pd.DataFrame(venues_info)       # Conversion a dataframe
        venues_df.to_csv(df_output_path, index=False, sep=';')
        return venues_df

# Procesado de entrenadores
def managers_proc(managers_dir_path: str, df_output_path: str) -> pd.DataFrame:

    if not os.path.exists(managers_dir_path):
        return pd.DataFrame()
    
    managers_info = []
    for manager in os.listdir(managers_dir_path):
        manager_data = json_to_dict(json_path=os.path.join(managers_dir_path, manager)).get('manager')

        if manager_data:
            managers_info.append({'id': manager_data.get('id', np.nan),
                                'name': manager_data.get('name', np.nan),
                                'short_name': manager_data.get('shortName', np.nan),
                                'country': manager_data.get('country', {}).get('name', np.nan),
                                'date_birth': manager_data.get('dateOfBirthTimestamp', np.nan),
                                'matches': manager_data.get('performance', {}).get('total', np.nan),
                                'wins': manager_data.get('performance', {}).get('wins', np.nan),
                                'draws': manager_data.get('performance', {}).get('draws', np.nan),
                                'losses': manager_data.get('performance', {}).get('losses', np.nan),
                                'goals_scored': manager_data.get('performance', {}).get('goalsScored', np.nan),
                                'goals_conceded': manager_data.get('performance', {}).get('goalsConceded', np.nan),
                                'points': manager_data.get('performance', {}).get('totalPoints', np.nan),})

    managers_df = pd.DataFrame(managers_info)
    managers_df.to_csv(df_output_path, index=False, sep=';')
    return managers_df

# Procesado de la información del partido
def match_info_proc(match_data: dict) -> pd.DataFrame:
    match_info = match_data.get('match', {}).get('event')
    if not match_info:
        return None

    return pd.DataFrame([{'match_id': match_info.get('id', np.nan),
                          'round': match_info.get('roundInfo', {}).get('round', np.nan),
                          'winner': match_info.get('winnerCode', np.nan),
                          'attendance': match_info.get('attendance', np.nan),
                          'venue': match_info.get('venue', {}).get('id', np.nan),
                          'referee': match_info.get('referee', {}).get('name', np.nan),
                          'home_team': match_info.get('homeTeam', {}).get('id', np.nan),
                          'away_team': match_info.get('awayTeam', {}).get('id', np.nan),
                          'home_score': match_info.get('homeScore', {}).get('display', np.nan),
                          'away_score': match_info.get('awayScore', {}).get('display', np.nan),
                          'date_time': match_info.get('startTimestamp', np.nan)}])

# Alineación única de un equipo
def single_team_lineups(team_lineups: dict) -> Tuple[str, pd.DataFrame]:

    if not team_lineups:
        return np.nan, None
    
    formation = team_lineups.get('formation', np.nan)       # Formación a devolver
    players = team_lineups.get('players')
    players_list = []

    if players:
        for player in players:
            player_id = player.get('player', {}).get('id')
            starter = not player.get('substitute', True)
            if player_id:
                player_statistics = player.get('statistics', {})
                player_statistics.pop('ratingVersions', None)           # Sacamos rating
                player_statistics.pop('statisticsType', None)           # Tipo de estadísticas  
                player_statistics = {'starter': starter, **player_statistics}           # Si el jugador es titular
                player_statistics = {'player_id': player_id, **player_statistics}       # Añadimos Id del jugador

                players_list.append(player_statistics)      # Añadimos estadísticas a la lista

        lineups_df = pd.DataFrame(players_list)
        lineups_df = lineups_df[lineups_df['minutesPlayed'].notna()]
        return formation, lineups_df

# Procesado de las estadísticas del partido
def match_stats_proc(match_data: dict) -> pd.DataFrame:
    teams_stats = match_data.get('statistics', {}).get('statistics')
    if not teams_stats:
        return None

    statistics_df = pd.DataFrame()      # Creamos dataframe
    statistics_df['ha'] = ['h','a']     # Añadimos si el equipo es local o visitante

    statistics_groups = teams_stats[0].get('groups', [])
    for group in statistics_groups:
        group_stats = group.get('statisticsItems', [])       # Lista con las estadísticas

        for stat in group_stats:
            statistics_df[f'{stat.get('name')}'] = [stat.get('homeValue'), stat.get('awayValue')]
    return statistics_df

# Procesado de todos los partidos de la liga
def all_matches_proc(league_raw_matches_path: str, league_clean_matches_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    
    list_matches_dfs = []       # Listas para ir añadiendo información
    list_lineups_dfs = []
    list_stats_dfs = []

    for match in os.listdir(league_raw_matches_path):
        match_data = json_to_dict(json_path=os.path.join(league_raw_matches_path, match))       # JSON con toda la información
        match_info_df = match_info_proc(match_data=match_data)                                  # Información básica del partido
        list_matches_dfs.append(match_info_df)

        match_id = match_info_df['match_id'].iloc[0]            # ID del partido
        home_team = match_info_df['home_team'].iloc[0]          # Equipo local
        away_team = match_info_df['away_team'].iloc[0]          # Equipo visitante
        
        match_lineups = match_data.get('lineups')                                       # Alineaciones
        home_formation, home_df = single_team_lineups(match_lineups.get('home'))        # Alineación local
        away_formation, away_df = single_team_lineups(match_lineups.get('away'))        # Alineación visitante

        home_df.insert(0, 'match_id', match_id)         # ID del partido
        away_df.insert(0, 'match_id', match_id)
        home_df.insert(1, 'team_id', home_team)         # Añadimos equipo local al df de equipo local
        home_df.insert(2, 'opponent_team_id', away_team)
        home_df.insert(3, 'ha', 'h')
        away_df.insert(1, 'team_id', away_team)         # Añadimos equipo visitante al df de equipo visitante
        away_df.insert(2, 'opponent_team_id', home_team)
        away_df.insert(3, 'ha', 'a')

        lineups_df = pd.concat([home_df, away_df], ignore_index=True)      # Concatenamos los dataframes de alineaciones
        list_lineups_dfs.append(lineups_df)

        match_stats_df = match_stats_proc(match_data=match_data)            # Estadísticas de los equipos
        match_stats_df.insert(0, 'match_id', match_id)                      # ID del partido
        match_stats_df.insert(1, 'team_id', [home_team, away_team])                     # Equipos
        match_stats_df.insert(2, 'opponent_team_id', [away_team, home_team])            # Contrincantes
        list_stats_dfs.append(match_stats_df)        

    all_matches_df = pd.concat(list_matches_dfs, ignore_index=True)         # Concatenamos las listas para obtener dfs
    all_lineups_df = pd.concat(list_lineups_dfs, ignore_index=True)
    all_stats_df = pd.concat(list_stats_dfs, ignore_index=True)

    all_matches_df.to_csv(os.path.join(league_clean_matches_path, 'matches.csv'), index=False, sep=';')     # Guardado
    all_lineups_df.to_csv(os.path.join(league_clean_matches_path, 'lineups.csv'), index=False, sep=';')
    all_stats_df.to_csv(os.path.join(league_clean_matches_path, 'statistics.csv'), index=False, sep=';')

    return all_matches_df, all_lineups_df, all_stats_df

# Función principal para la limpieza de datos de Sofascore de una liga
def main_sofascore_league_cleaning(league_id: int, out_path: str, print_info: bool = True) -> None:

    start_time = time.time()   # Inicio del contador

    league_name = comps[comps['id'] == league_id]['tournament'].iloc[0]     # Nombre de la liga
    league_slug = create_slug(text=league_name)                             # Slug de la liga

    league_raw_path = os.path.join(out_path, 'sofascore', league_slug)      # Path de datos raw
    league_clean_path = league_raw_path.replace('raw', 'clean')             # Obtención de la nueva carpeta
    os.makedirs(league_clean_path, exist_ok=True)                                                                                # Creación de la carpeta con datos limpios en caso de que no se haya hecho

    if print_info:
        print('================================================================================')
        print(f'Starting Sofascore cleaning ({league_name})')

    for season in (d for d in os.listdir(league_raw_path) if os.path.isdir(os.path.join(league_raw_path, d))):

        league_raw_info_path = os.path.join(league_raw_path, str(season), 'info')                # Información scrapeada
        league_raw_matches_path = os.path.join(league_raw_path, str(season), 'matches')          # Partidos scrapeados

        league_clean_info_path = os.path.join(league_clean_path, season, 'info')            # Carpeta para añadir información 
        os.makedirs(league_clean_info_path, exist_ok=True)
        league_clean_matches_path = os.path.join(league_clean_path, season, 'matches')      # Carpeta para partidos
        os.makedirs(league_clean_matches_path, exist_ok=True)

        # A añadir a información
        list_standings_tables = standings_tables_proc(standings_path=os.path.join(league_raw_info_path, 'standings.json'), standings_output_path=os.path.join(league_clean_info_path, 'standings'))
        players_df = players_proc(players_json_path=os.path.join(league_raw_info_path, 'player.json'), players_dir_path=os.path.join(league_raw_info_path, 'player'), df_output_path=os.path.join(league_clean_info_path, 'players.csv'))
        teams_df = teams_proc(teams_dir_path=os.path.join(league_raw_info_path, 'team'), df_output_path=os.path.join(league_clean_info_path, 'teams.csv'))
        venues_df = venues_proc(venues_json_path=os.path.join(league_raw_info_path, 'venue.json'), df_output_path=os.path.join(league_clean_info_path, 'venues.csv'))
        managers_df = managers_proc(managers_dir_path=os.path.join(league_raw_info_path, 'manager'), df_output_path=os.path.join(league_clean_info_path, 'managers.csv'))

        # Procesado de todos los partidos
        matches_df, lineups_df, stats_df = all_matches_proc(league_raw_matches_path=league_raw_matches_path, league_clean_matches_path=league_clean_matches_path)

        if print_info:
            print(f'     - Information cleaned for season {season}')

    elapsed_time = time.time() - start_time         # Tiempo transcurrido
    if print_info:
        print(f'Finished Scoresway cleaning ({league_name}) in {elapsed_time:.2f} seconds')
        print('================================================================================')