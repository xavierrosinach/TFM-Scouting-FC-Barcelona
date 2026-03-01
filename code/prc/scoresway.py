import pandas as pd
import os
import json as jsonlib
from typing import Tuple

# Obtenemos el CSV con competiciones
cdir = os.getcwd()
utils = os.path.join(os.path.abspath(os.path.join(cdir, '..', '..')), 'utils')
comps = pd.read_csv(os.path.join(utils, 'comps.csv'), sep=';')

# JSON con temporadas deseadas
with open(os.path.join(utils, 'des_seasons.json'), 'r', encoding='utf-8') as f:
    desired_seasons = jsonlib.load(f)

# Lector de JSON
def json_to_dict(json_path: str) -> dict:
    with open(json_path, "r", encoding="utf-8") as f:
        dict = jsonlib.load(f)
    return dict

# Procesado del json de información sobre los partidos
def matches_processing(matches_path: str) -> pd.DataFrame:

    # JSON
    matches_json = json_to_dict(json_path=matches_path)

    # Lista para concatenar información
    match_info_list = []

    for match in matches_json.get('match', []):

        # Información del partido
        single_match_info = match.get('matchInfo', {})
        single_match_live_data = match.get('liveData', {})

        try:
            match_ref = f'{single_match_live_data.get('matchDetailsExtra', {}).get('matchOfficial')[0].get('firstName', '')} {single_match_live_data.get('matchDetailsExtra', {}).get('matchOfficial')[0].get('lastName', '')}'
        except:
            match_ref = ''

        # Añadimos la info al diccionario
        match_info_list.append({'id': single_match_info.get('id', ''),
                                'slug': f'{single_match_info.get('contestant')[0].get('code', '').lower()}-{single_match_info.get('contestant')[1].get('code', '').lower()}',
                                'date': single_match_info.get('date', ''),
                                'time': single_match_info.get('time', ''),
                                'home_team': single_match_info.get('contestant')[0].get('officialName', ''),
                                'away_team': single_match_info.get('contestant')[1].get('officialName', ''),
                                'venue': single_match_info.get('venue', {}).get('longName', ''),
                                'attendance': single_match_live_data.get('matchDetailsExtra', {}).get('attendance', 0),
                                'match_min': single_match_live_data.get('matchDetails', {}).get('matchLengthMin', 90),
                                'home_score_ht': single_match_live_data.get('matchDetails', {}).get('scores', {}).get('ht', {}).get('home', 0),
                                'away_score_ht': single_match_live_data.get('matchDetails', {}).get('scores', {}).get('ht', {}).get('away', 0),
                                'home_score_ft': single_match_live_data.get('matchDetails', {}).get('scores', {}).get('ft', {}).get('home', 0),
                                'away_score_ft': single_match_live_data.get('matchDetails', {}).get('scores', {}).get('ft', {}).get('away', 0),
                                'referee': match_ref})
        
    return pd.DataFrame(match_info_list)
    
# Función para el procesado de las plantillas de una liga
def squads_processing(squads_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    # JSON
    squads_json = json_to_dict(json_path=squads_path)
    squads_info = squads_json.get('squad')

    # Lista con la info de los equipos y jugadores
    info_teams = []
    info_players = []
    info_managers = []

    # Para cada equipo
    for squad in squads_info:
        # Concatenamos información
        info_teams.append({'id':squad.get('contestantId', ''),
                        'code': squad.get('contestantCode', ''),
                        'slug': squad.get('contestantCode', '').lower(),
                        'name': squad.get('contestantName', ''),
                        'club_name': squad.get('contestantClubName', ''),
                        'short_name': squad.get('contestantShortName', ''),
                        'venue': squad.get('venueName', '')})

        squad_name = squad.get('contestantName', '')

        # Concatenamos jugadores y entrenadores
        for player in squad.get('person', []):
            first_name = player.get('firstName', '')
            last_name = player.get('lastName', '')
            
            # Appendamos la info de jugadores solo si estan en activo
            if player.get('type', '') == 'player' and player.get('shirtNumber'):
                info_players.append({'id': player.get('id', ''),
                                    'name': f'{first_name} {last_name}',
                                    'short_name': f'{player.get('shortFirstName', '')} {player.get('shortLastName', '')}',
                                    'match_name': player.get('matchName', ''),
                                    'first_name': first_name,
                                    'last_name': last_name,
                                    'team': squad_name,
                                    'nationality': player.get('nationality', ''),
                                    'position': player.get('position', ''),
                                    'shirt_number': player.get('shirtNumber', '')})
            elif player.get('type', '') != 'player':       # Entrenador
                info_managers.append({'id': player.get('id', ''),
                                    'name': f'{first_name} {last_name}',
                                    'short_name': f'{player.get('shortFirstName', '')} {player.get('shortLastName', '')}',
                                    'match_name': player.get('matchName', ''),
                                    'first_name': first_name,
                                    'last_name': last_name,
                                    'team': squad_name,
                                    'nationality': player.get('nationality', ''),
                                    'type': player.get('type', '')})

    return pd.DataFrame(info_teams), pd.DataFrame(info_players), pd.DataFrame(info_managers)

# Función para obtener las tablas de clasificación de distintos tipos
def standings_processing(standings_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]: 

    # Función parcial
    def part_standings_table(standings_dict: dict, type: str) -> pd.DataFrame:

        # Ir añadiendo información
        list_info = []

        for team in standings_dict:
            if type == 'total':
                list_info.append({'rank': team.get('rank', 0),
                                'status': team.get('rankStatus', ''),
                                'team': team.get('contestantName', ''),
                                'points': team.get('points', 0),
                                'matches_played': team.get('matchesPlayed', 0),
                                'wins': team.get('matchesWon', 0),
                                'draws': team.get('matchesLost', 0),
                                'losses': team.get('matchesDrawn', 0),
                                'goals_for': team.get('goalsFor', 0),
                                'goals_against': team.get('goalsAgainst', 0)})
            elif type in ['home', 'away', 'half']:
                list_info.append({'rank': team.get('rank', 0),
                                'team': team.get('contestantName', ''),
                                'points': team.get('points', 0),
                                'matches_played': team.get('matchesPlayed', 0),
                                'wins': team.get('matchesWon', 0),
                                'draws': team.get('matchesLost', 0),
                                'losses': team.get('matchesDrawn', 0),
                                'goals_for': team.get('goalsFor', 0),
                                'goals_against': team.get('goalsAgainst', 0)})
            elif type == 'attendance':
                list_info.append({'rank': team.get('rank', 0),
                                'team': team.get('contestantName', ''),
                                'venue_name': team.get('venueName', ''),
                                'min_attendance': team.get('minimumAttendance', 0),
                                'max_attendance': team.get('maximumAttendance', 0),
                                'total_attendance': team.get('totalAttendance', 0),
                                'avg_attendance': team.get('averageAttendance', 0),
                                'capacity': team.get('capacity', 0),
                                'percent_sold': team.get('percentSold', 0)})
        
        return pd.DataFrame(list_info)

    # JSON
    standings_json = json_to_dict(json_path=standings_path)

    # Obtener información
    all_tables = standings_json.get('stage', [])[0].get('division', [])

    # Obtener las cinco tablas diferentes
    total_df = part_standings_table(standings_dict=all_tables[0].get('ranking', []), type='total')
    home_df = part_standings_table(standings_dict=all_tables[1].get('ranking', []), type='home')
    away_df = part_standings_table(standings_dict=all_tables[2].get('ranking', []), type='away')
    half_df = part_standings_table(standings_dict=all_tables[6].get('ranking', []), type='half')
    attendance_df = part_standings_table(standings_dict=all_tables[9].get('ranking', []), type='attendance')

    return total_df, home_df, away_df, half_df, attendance_df

# Función para el procesado de una temporada de una liga
def process_season_info_dfs(raw_data_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    # Paths de info y de partidos
    info_path = os.path.join(raw_data_path, 'info')

    # Dentro de info encontramos matches, squads y standings
    matches_df = matches_processing(os.path.join(info_path, 'matches.json'))
    teams_df, players_df, managers_df = squads_processing(os.path.join(info_path, 'squads.json'))
    st_total_df, st_home_df, st_away_df, st_half_df, st_attendance_df = standings_processing(os.path.join(info_path, 'standings.json'))

    return matches_df, teams_df, players_df, managers_df, st_total_df, st_home_df, st_away_df, st_half_df, st_attendance_df

# A partir de la alineación de un partido, obtenemos datos de los jguadores (de un solo equipo)
def obtain_team_lineup_df(team_lineup: dict, team_id: str) -> pd.DataFrame:

    # Lista para añadir jugadores
    list_player = []

    for player in team_lineup:
        
        # Información
        player_dict = {'player_id': player.get('playerId', ''), 
                       'name': f'{player.get('firstName', '')} {player.get('lastName', '')}',
                       'short_name': f'{player.get('shortFirstName', '')} {player.get('shortLastName', '')}',
                       'match_name': player.get('matchName', ''),
                       'shirt_number': player.get('shirtNumber', 0),
                       'position': player.get('position', ''),
                       'pos_side': player.get('positionSide', '') if player.get('positionSide') else player.get('subPosition', ''),
                       'formation_place': player.get('formationPlace', 0)}
        
        # Estadísticas
        for stat in player.get('stat', []):
            player_dict[stat.get('type', '')] = stat.get('value', 0)

        # Append
        list_player.append(player_dict)

    # Dataframe
    df = pd.DataFrame(list_player)
    df.insert(0, 'team', team_id)
    df.dropna(subset=['minsPlayed'], inplace=True)

    return df

# Función para obtener los datos de un partido
def obtain_match_data(match_json_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    # JSON
    event_json = json_to_dict(json_path=match_json_path)

    # Información básica que necesitaremos
    match_id = event_json.get('matchInfo', {}).get('id', '')
    match_slug = f'{event_json.get('matchInfo', {}).get('contestant', [])[0].get('code', '').lower()}-{event_json.get('matchInfo', {}).get('contestant', [])[1].get('code', '').lower()}'

    # Obtenemos live data, que es donde hay la información
    match_live_data = event_json.get('liveData', {})

    # Dataframe con los goles del partido
    goals_list = []
    for goal in match_live_data.get('goal', {}):
        goals_list.append({'match': match_id,
                           'match_slug': match_slug,
                           'type': goal.get('type', ''), 
                           'team_id': goal.get('contestantId', ''),
                           'minute': goal.get('timeMinSec', ''),
                           'scorer_id': goal.get('scorerId', ''),
                           'scorer': goal.get('scorerName', ''),
                           'assister_id': goal.get('assistPlayerId', ''),
                           'assister': goal.get('assistPlayerName', '')})
    goals_df = pd.DataFrame(goals_list)

    # Obtenemos datos a partir de las alineaciones de los equipos
    home_lineup = match_live_data.get('lineUp', [])[0]
    away_lineup = match_live_data.get('lineUp', [])[1]

    # Obtenemos diccionarios y concatenamos
    teams_dicts = []
    players_stats = []
    for team in [home_lineup, away_lineup]:
        single_team_dict = {'match': match_id,
                            'match_slug': match_slug,
                            'team_id': team.get('contestantId', ''),
                            'formation': team.get('formationUsed', ''),
                            'manager_id': team.get('teamOfficial', [])[0].get('id', ''), 
                            'manager': f'{team.get('teamOfficial', [])[0].get('firstName', '')} {team.get('teamOfficial', [])[0].get('lastName', '')}',
                            'kit': team.get('kit', {}).get('type'),
                            'kit_col1': team.get('kit', {}).get('colour1'),
                            'kit_col2': team.get('kit', {}).get('colour2')}
        
        # Para cada stat:
        for stat in team.get('stat', []):
            single_team_dict[stat.get('type', '')] = stat.get('value', 0)
        
        # Concatenamos
        teams_dicts.append(single_team_dict)

        # Estadísticas de jugadores
        players_stats.append(obtain_team_lineup_df(team.get('player', []), team_id=team.get('contestantId', '')))

    # Stats de equipos y de los jugadores
    teams_stats_df = pd.DataFrame(teams_dicts).fillna(0)
    players_stats_df = pd.concat(players_stats, ignore_index=True).fillna(0)
    players_stats_df.insert(0, 'match', match_id)
    players_stats_df.insert(1, 'match_slug', match_slug)

    return goals_df, teams_stats_df, players_stats_df

# Procesado de datos de una liga
def league_processing(league_id: int, raw_out_path: str, clean_out_path: str) -> None:

    # Obtenemos el nombre de la liga y el path -> creación de la carpeta de output (clean)
    league_name = comps[comps['id'] == league_id]['tournament'].iloc[0]
    league_slug = league_name.lower().replace(' ', '-')
    out_league_path = os.path.join(clean_out_path, 'sw', league_slug)
    os.makedirs(out_league_path, exist_ok=True)

    # Capeta de la liga y carpetas (seasons) dentro
    raw_data_path = os.path.join(raw_out_path, 'sw', league_slug)
    seasons_raw = [f for f in os.listdir(raw_data_path) if os.path.isdir(os.path.join(raw_data_path, f))]

    # Para cada temporada, procesamos
    for season in seasons_raw:

        # Creación de la carpeta de output
        out_season_path = os.path.join(out_league_path, season, 'info')
        os.makedirs(out_season_path, exist_ok=True)

        # Para los partidos
        out_season_matches_path = os.path.join(out_league_path, season, 'matches')
        os.makedirs(out_season_matches_path, exist_ok=True)

        # Goles
        out_goals_path = os.path.join(out_season_matches_path, 'goals')
        os.makedirs(out_goals_path, exist_ok=True)

        # Teams stats
        out_team_stats_path = os.path.join(out_season_matches_path, 'team_stats')
        os.makedirs(out_team_stats_path, exist_ok=True)

        # Players stats
        out_player_stats_path = os.path.join(out_season_matches_path, 'player_stats')
        os.makedirs(out_player_stats_path, exist_ok=True)

        # Obtenemos todos los dataframes
        matches_df, teams_df, players_df, managers_df, st_total_df, st_home_df, st_away_df, st_half_df, st_attendance_df = process_season_info_dfs(raw_data_path=os.path.join(raw_data_path, season))

        # Añadimos liga y temporada
        for df in [matches_df, teams_df, players_df, managers_df, st_total_df, st_home_df, st_away_df, st_half_df, st_attendance_df]:
            df.insert(0, 'league', league_id)
            df.insert(1, 'season', season)

        # Guardado de todos los dataframes en formato CSV
        matches_df.to_csv(os.path.join(out_season_path, 'matches.csv'), sep=';', index=False)
        teams_df.to_csv(os.path.join(out_season_path, 'teams.csv'), sep=';', index=False)
        players_df.to_csv(os.path.join(out_season_path, 'players.csv'), sep=';', index=False)
        managers_df.to_csv(os.path.join(out_season_path, 'managers.csv'), sep=';', index=False)
        st_total_df.to_csv(os.path.join(out_season_path, 'standings_total.csv'), sep=';', index=False)
        st_home_df.to_csv(os.path.join(out_season_path, 'standings_home.csv'), sep=';', index=False)
        st_away_df.to_csv(os.path.join(out_season_path, 'standings_away.csv'), sep=';', index=False)
        st_half_df.to_csv(os.path.join(out_season_path, 'standings_halftime.csv'), sep=';', index=False)
        st_attendance_df.to_csv(os.path.join(out_season_path, 'standings_attendance.csv'), sep=';', index=False)    

        # Path y partidos a procesar
        season_raw_path = os.path.join(raw_data_path, season, 'matches')
        matches_to_proc = [f for f in os.listdir(season_raw_path) if f.endswith('.json') and os.path.isfile(os.path.join(season_raw_path, f))]

        # For match en los partidos a procesar
        for match in matches_to_proc:
            
            # Obtenemos los datos
            data_path = os.path.join(season_raw_path, match)
            goals_df, teams_stats_df, players_stats_df = obtain_match_data(match_json_path=data_path)

            # Añadimos liga y temporada
            for df in [goals_df, teams_stats_df, players_stats_df]:
                df.insert(0, 'league', league_id)
                df.insert(1, 'season', season)

            # Guardado
            goals_df.to_csv(os.path.join(out_goals_path, match.replace('json', 'csv')), sep=';', index=False)
            teams_stats_df.to_csv(os.path.join(out_team_stats_path, match.replace('json', 'csv')), sep=';', index=False)
            players_stats_df.to_csv(os.path.join(out_player_stats_path, match.replace('json', 'csv')), sep=';', index=False)