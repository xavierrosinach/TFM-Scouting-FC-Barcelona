import os
import pandas as pd
import time
import numpy as np
from typing import Tuple

from use.config import comps
from use.functions import json_to_dict, create_slug, elapsed_time_str

# Procesado de datos de partidos a partir de su path
def proc_matches(json_path: str, out_path: str) -> None:

    if os.path.exists(json_path):
        matches = json_to_dict(json_path=json_path).get('match')           # Obtenemos los partidos

        if not matches:     # Si no hay info, return
            return

        match_info_list = []
        for match in matches:
            single_match_info = match.get('matchInfo')              # Información del partido
            single_match_live_data = match.get('liveData')

            if single_match_info and single_match_live_data:
                try:
                    match_ref = f'{single_match_live_data.get('matchDetailsExtra', {}).get('matchOfficial')[0].get('firstName', '')} {single_match_live_data.get('matchDetailsExtra', {}).get('matchOfficial')[0].get('lastName', '')}'
                except:
                    match_ref = np.nan          # Árbitro del partido

                # Añadimos la info al diccionario
                match_info_list.append({'id': single_match_info.get('id', np.nan),
                                        'date': single_match_info.get('date', np.nan),
                                        'time': single_match_info.get('time', np.nan),
                                        'home_team': single_match_info.get('contestant')[0].get('officialName', np.nan),
                                        'away_team': single_match_info.get('contestant')[1].get('officialName', np.nan),
                                        'venue': single_match_info.get('venue', {}).get('longName', np.nan),
                                        'attendance': single_match_live_data.get('matchDetailsExtra', {}).get('attendance', np.nan),
                                        'match_min': single_match_live_data.get('matchDetails', {}).get('matchLengthMin', np.nan),
                                        'home_score_ht': single_match_live_data.get('matchDetails', {}).get('scores', {}).get('ht', {}).get('home', np.nan),
                                        'away_score_ht': single_match_live_data.get('matchDetails', {}).get('scores', {}).get('ht', {}).get('away', np.nan),
                                        'home_score_ft': single_match_live_data.get('matchDetails', {}).get('scores', {}).get('ft', {}).get('home', np.nan),
                                        'away_score_ft': single_match_live_data.get('matchDetails', {}).get('scores', {}).get('ft', {}).get('away', np.nan),
                                        'referee': match_ref})
        
        matches_df = pd.DataFrame(match_info_list)
        matches_df.to_csv(os.path.join(out_path, 'matches.csv'), index=False, sep=';')

# Procesado de datos de plantillas a partir de su path
def proc_squads(json_path: str, out_path: str) -> None:

    if os.path.exists(json_path):
        squads = json_to_dict(json_path=json_path).get('squad')           # Obtenemos las plantillas

        if not squads:     # Si no hay info, return
            return

        info_teams = []         # Información que iremos concatenando - de jugadores, equipos y entrenadores
        info_players = []
        info_managers = []

        for squad in squads:
            squad_name = squad.get('contestantName', np.nan)
            info_teams.append({'sw_id':squad.get('contestantId', np.nan),                       # Información del equipo
                               'code': squad.get('contestantCode', np.nan),
                               'name': squad_name,
                               'club_name': squad.get('contestantClubName', np.nan),
                               'short_name': squad.get('contestantShortName', np.nan),
                               'venue': squad.get('venueName', np.nan)})
            
            for person in squad.get('person', []):                                              # Person - incluye jugadores y entrenadores
                if person.get('type', '') == 'player' and person.get('shirtNumber'):            # Jugadores
                    info_players.append({'id': person.get('id', np.nan),
                                        'first_name': person.get('firstName', np.nan),
                                        'last_name': person.get('lastName', np.nan),
                                        'short_first_name': person.get('shortFirstName', np.nan),
                                        'short_last_name': person.get('shortLastName', np.nan),
                                        'match_name': person.get('matchName', np.nan),
                                        'team': squad_name,
                                        'nationality': person.get('nationality', np.nan),
                                        'position': person.get('position', np.nan),
                                        'shirt_number': person.get('shirtNumber', np.nan), 
                                        'active': person.get('active', np.nan)})
                elif person.get('type', '') != 'player':                                        # Entrenador
                    info_managers.append({'id': person.get('id', np.nan),
                                          'first_name': person.get('firstName', np.nan),
                                          'last_name': person.get('lastName', np.nan),
                                          'short_first_name': person.get('shortFirstName', np.nan),
                                          'short_last_name': person.get('shortLastName', np.nan),
                                          'match_name': person.get('matchName', np.nan),
                                          'team': squad_name,
                                          'nationality': person.get('nationality', np.nan),
                                          'type': person.get('type', np.nan)})
                    
            teams_df = pd.DataFrame(info_teams)                                                 # Dataframe entero de equipos
            teams_df.to_csv(os.path.join(out_path, 'teams.csv'), index=False, sep=';')
            players_df = pd.DataFrame(info_players)                                             # Dataframe entero de jugadores
            players_df.to_csv(os.path.join(out_path, 'players.csv'), index=False, sep=';')
            managers_df = pd.DataFrame(info_managers)                                           # Dataframe entero de entrenadores y staff
            managers_df.to_csv(os.path.join(out_path, 'managers.csv'), index=False, sep=';')

# Procesado de las tablas de clasificación
def proc_standings(json_path: str, out_path_standings: str) -> None:

    os.makedirs(out_path_standings, exist_ok=True)      # Creamos la carpeta con standings si no existe
    dict_output = {}                                    # Diccionario de output con todos los dataframes

    if os.path.exists(json_path):
        stages = json_to_dict(json_path=json_path).get('stage', [])[0].get('division')           # Obtenemos las tablas de clasificación

        if stages:
            for st in stages:
                st_type = st.get('type')        # Tipo de tabla
                st_table = st.get('ranking')    # Tabla
                if st_type and st_table:
                    table_df = pd.DataFrame([t for t in st_table])                                                  # Convertimos a dataframe
                    table_df.to_csv(os.path.join(out_path_standings, f'{st_type}.csv'), index=False, sep=';')       # Guardado
                    dict_output[st_type] = table_df                                                                 # Añadimos dataframe al diccionario

# Obtención del df de estadísticas de jugadores en un partido - a partir de las alineaciones locales y visitantes
def match_player_stats(home_lineup: dict, away_lineup: dict) -> pd.DataFrame:

    players_info = []

    if home_lineup:                         # Alineación local
        for player in home_lineup:
            player = player.copy()
            stats = player.pop('stat', [])
            
            stats_dict = {s['type']: s['value'] for s in stats}     # Añadimos estadísticas
            stats_dict['ha'] = 'h'

            players_info.append({**player, **stats_dict})

    if away_lineup:                         # Alineación visitante
        for player in away_lineup:
            player = player.copy()
            stats = player.pop('stat', [])
            
            stats_dict = {s['type']: s['value'] for s in stats}     # Añadimos estadísticas
            stats_dict['ha'] = 'a'  

            players_info.append({**player, **stats_dict})

    return pd.DataFrame(players_info)

# Obtención del df de estadísticas de equipo en un partido
def match_team_stats(home_stats: dict, away_stats: dict) -> pd.DataFrame:

    stats = []

    if home_stats:                          # Estadísticas local
        st_dict = {}
        st_dict['ha'] = 'h'
        for s in home_stats:
            stype = s.get('type')
            svalue = s.get('value')
            if stype and svalue:            # Solo si cumple
                st_dict[stype] = svalue
        stats.append(st_dict)

    if away_stats:                          # Estadísticas visitante
        st_dict = {}
        st_dict['ha'] = 'a'
        for s in away_stats:
            stype = s.get('type')
            svalue = s.get('value')
            if stype and svalue:            # Solo si cumple
                st_dict[stype] = svalue
        stats.append(st_dict)

    return pd.DataFrame(stats)

# Obtención de los datos de un partido
def single_match_stats(match_id: str, match_raw_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    match_data = json_to_dict(json_path=match_raw_path)         # Todos los datos del partido
    match_info = match_data.get('matchInfo')                    # Información - tratamos después
    match_live_data = match_data.get('liveData')                # Datos estadísticos - tratamos antes

    details = match_live_data.get('matchDetails')               # Detalles del partido
    details_extra = match_live_data.get('matchDetailsExtra')    # Detalles extra
    if details:
        details_df = pd.DataFrame([{'match_status': details.get('matchStatus', np.nan),
                                    'winner': details.get('winner', np.nan),
                                    'lenght_min': details.get('matchLengthMin', np.nan),
                                    'fh_lenght_min': details.get('period', [])[0].get('lengthMin', np.nan),
                                    'sh_lenght_min': details.get('period', [])[1].get('lengthMin', np.nan),
                                    'ht_home_score': details.get('scores', {}).get('ht', {}).get('home', np.nan),
                                    'ht_away_score': details.get('scores', {}).get('ht', {}).get('away', np.nan),
                                    'ft_home_score': details.get('scores', {}).get('ft', {}).get('home', np.nan),
                                    'ft_away_score': details.get('scores', {}).get('ft', {}).get('away', np.nan),
                                    'attendance': details_extra.get('attendance', np.nan) if details_extra else np.nan}])
    else:
        details_df = pd.DataFrame()
            
    referees = details_extra.get('matchOfficial')               # Árbitros del partido
    if referees:
        refs_df = pd.DataFrame(referees)
        refs_df.insert(0, 'match_id', match_id)

    lineup = match_live_data.get('lineUp')                      # Alineaciones
    if lineup:
        lineup_home = lineup[0]         # Local
        lineup_away = lineup[1]         # Visitante

        info_lineups = []                                       # Información alterna a los jugadores y equipos
        for l in [lineup_home, lineup_away]:
            info_lineups.append({'team_id': l.get('contestantId', np.nan),
                                'formation': l.get('formationUsed', np.nan),
                                'average_age': l.get('averageAge', np.nan),
                                'manager': l.get('teamOfficial', [])[0].get('id', np.nan),
                                'kit': l.get('kit', {}).get('type', np.nan)})
        info_lineups_df = pd.DataFrame(info_lineups)

        teams_stats_df = match_team_stats(home_stats=lineup_home.get('stat'), away_stats=lineup_away.get('stat'))                 # Estadísticas de los equipos
        teams_stats_df = pd.concat([info_lineups_df, teams_stats_df], axis=1)
        teams_stats_df.insert(0, 'match_id', match_id)

        players_stats_df = match_player_stats(home_lineup=lineup_home.get('player'), away_lineup=lineup_away.get('player'))      # Estadísticas de los jugadores
        players_stats_df.insert(0, 'match_id', match_id)
        ha_team_dict = dict(zip(teams_stats_df['ha'], teams_stats_df['team_id']))       # Diccionario con ID de equipos
        players_stats_df['team_id'] = players_stats_df['ha'].map(ha_team_dict)
        players_stats_df.insert(1, 'team_id', players_stats_df.pop('team_id'))
        players_stats_df = players_stats_df.dropna(subset=['minsPlayed'])               # Sacamos aquellos jugadores con 0 minutos jugador
        
    match_info_df = pd.DataFrame([{'match_id': match_info.get('id', np.nan),                    # Dataframe con información del partido
                                'date': match_info.get('date', np.nan),
                                'time': match_info.get('time', np.nan),
                                'week': match_info.get('week', np.nan),
                                'description': match_info.get('description', np.nan),
                                'home_team': match_info.get('contestant', [])[0].get('name', np.nan),
                                'home_team_id': match_info.get('contestant', [])[0].get('id', np.nan),
                                'away_team': match_info.get('contestant', [])[1].get('name', np.nan),
                                'away_team_id': match_info.get('contestant', [])[1].get('id', np.nan)}])
    match_info_df = pd.concat([match_info_df, details_df], axis=1) if len(details_df) > 0 else match_info_df

    return match_info_df, teams_stats_df, players_stats_df, refs_df

# Procesado de todos los partidos scrapeados de la liga
def proc_all_league_matches(matches_raw_path: str, matches_clean_path: str) -> None:

    list_match_info = []        # Listas para inr concatenando la información extraída
    list_team_stats = []    
    list_player_stats = []
    list_refs = []

    matches_to_proc = [m for m in os.listdir(matches_raw_path)]        # Partidos a procesar
    for match in matches_to_proc:                                      # Para cada partido
        match_raw_path = os.path.join(matches_raw_path, match)
        match_id = match.replace('.json', '')                                   # ID del partido - sacamos extensión

        try:            # Evitamos error
            match_info_df, teams_stats_df, players_stats_df, refs_df = single_match_stats(match_id=match_id, match_raw_path=match_raw_path)     # Obtención de datos del partido
            list_match_info.append(match_info_df)       # Concatenamos dentro de la lista
            list_team_stats.append(teams_stats_df)
            list_player_stats.append(players_stats_df)
            list_refs.append(refs_df)
        except:
            continue

    all_match_info_df = pd.concat(list_match_info, ignore_index=True)                               # Concatenamos y guardamos
    all_match_info_df.to_csv(os.path.join(matches_clean_path, 'info.csv'), index=False, sep=';')
    all_team_stats_df = pd.concat(list_team_stats, ignore_index=True)                               # Concatenamos y guardamos
    all_team_stats_df.to_csv(os.path.join(matches_clean_path, 'team_stats.csv'), index=False, sep=';')
    all_player_stats_df = pd.concat(list_player_stats, ignore_index=True)                           # Concatenamos y guardamos
    all_player_stats_df.to_csv(os.path.join(matches_clean_path, 'player_stats.csv'), index=False, sep=';')
    all_refs_df = pd.concat(list_refs, ignore_index=True)                                           # Concatenamos y guardamos
    all_refs_df.to_csv(os.path.join(matches_clean_path, 'referees.csv'), index=False, sep=';')

# Función principal para la limpieza de datos de Scoresway de una liga
def main_scoresway_league_cleaning(league_id: int, out_path: str, print_info: bool = True) -> None:

    start_time = time.time()                                                # Inicio del contador

    league_name = comps[comps['id'] == league_id]['tournament'].iloc[0]     # Nombre de la liga
    league_slug = create_slug(text=league_name)                             # Slug de la liga

    league_raw_path = os.path.join(out_path, 'scoresway', league_slug)      # Path de los datos raw
    league_clean_path = league_raw_path.replace('raw', 'clean')             # Path de los datos clean                           
    os.makedirs(league_clean_path, exist_ok=True)                           

    if print_info:
        print(f'Starting Scoresway cleaning ({league_name})')

    seasons_to_proc = [f for f in os.listdir(league_raw_path) if os.path.isdir(os.path.join(league_raw_path, f))]        # Lista con temporadas a procesar
    for s in seasons_to_proc:
        season_info_path = os.path.join(league_raw_path, s, 'info')                 # Path con la información de la liga
        season_matches_path = os.path.join(league_raw_path, s, 'matches')           # Path con los partidos

        league_clean_info_path = os.path.join(league_clean_path, s, 'info')         # Creación carpeta de output de información
        os.makedirs(league_clean_info_path, exist_ok=True)
        league_clean_matches_path = os.path.join(league_clean_path, s, 'matches')   # Creación carpeta de output de partidos
        os.makedirs(league_clean_matches_path, exist_ok=True)

        proc_matches(json_path=os.path.join(season_info_path, 'matches.json'), out_path=league_clean_info_path)                                                 # Procesado de partidos
        proc_squads(json_path=os.path.join(season_info_path, 'squads.json'), out_path=league_clean_info_path)                                                   # Procesado de plantillas
        proc_standings(json_path=os.path.join(season_info_path, 'standings.json'), out_path_standings=os.path.join(league_clean_info_path, 'standings'))        # Procesado de tablas de clasificación
        proc_all_league_matches(matches_raw_path=season_matches_path, matches_clean_path=league_clean_matches_path)                                             # Procesado de todos los partidos de la liga

        if print_info:
            print(f'     - Information cleaned for season {s}')

    if print_info:
        print(f'Finished Scoresway cleaning ({league_name}) in {elapsed_time_str(start_time=start_time)}')