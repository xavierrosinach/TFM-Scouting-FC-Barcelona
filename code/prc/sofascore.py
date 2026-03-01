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

# Generamos el dataframe de temporadas de una liga
def get_seasons_df(league_id: int, available_seasons_json: dict) -> pd.DataFrame:

    # Info que necesitaremos
    all_seasons = available_seasons_json.get('seasons', {})

    # Obtenemos información
    if not all_seasons:
        return pd.DataFrame()
    
    # Lista para concatenar info
    rows = []

    for season in all_seasons:

        season_key = season.get('year', '').replace('/','')

        if season_key in desired_seasons:
            rows.append({'league': league_id,
                         'year': season_key,
                         'season_name': season.get('name', '')})

    return pd.DataFrame(rows)

# Función para obtener las tablas de clasificación
def obtain_standings_tables(league_id: int, season_key: str, standings_json: dict) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    # Función para obtener la tabla con standings a partir de casa, away, etc
    def partial_standing(st_table: dict) -> pd.DataFrame:
        list_info = []
        for team in st_table:
            list_info.append({'position': team.get('position', 0),
                            'team': team.get('team', {}).get('name', ''),
                            'team_slug': team.get('team', {}).get('slug', ''),
                            'promotion': team.get('promotion', {}).get('text', ''),
                            'points': team.get('points', 0),
                            'matches': team.get('matches', 0),
                            'wins': team.get('wins', 0),
                            'losses': team.get('losses', 0),
                            'draws': team.get('draws', 0),
                            'scores_for': team.get('scoresFor', 0),
                            'scores_against': team.get('scoresAgainst', 0)})
        return pd.DataFrame(list_info)

    # Diferenciamos las tres tablas
    total_st = standings_json.get('total', {}).get('standings', [])[0].get('rows', [])
    home_st = standings_json.get('home', {}).get('standings', [])[0].get('rows', [])
    away_st = standings_json.get('away', {}).get('standings', [])[0].get('rows', [])

    # Obtenemos tablas
    total_st_df = partial_standing(st_table=total_st)
    home_st_df = partial_standing(st_table=home_st)
    away_st_df = partial_standing(st_table=away_st)

    # Añadimos liga y season
    for df in [total_st_df, home_st_df, away_st_df]:
        df.insert(0, 'league', league_id)
        df.insert(1, 'season', season_key)
    
    return total_st_df, home_st_df, away_st_df

# Procesamos el JSON de jugadores y la carpeta con más información
def players_processing(player_json: dict, players_dict_path: str) -> pd.DataFrame:

    players_list = []

    # Para cada jugador
    for player in player_json.get('players', []):

        # ID
        player_id = player.get('playerId')
        if player_id:
            player_dict = {'id': player_id, 'name': player.get('playerName', '')}

            # Si existe el json
            player_json_path = os.path.join(players_dict_path, f'{player_id}.json')
            if os.path.exists(player_json_path):
                player_json = json_to_dict(player_json_path)

                # Positions
                positions = player_json.get('player', {}).get('positionsDetailed', [])

                # Añadimos valores al diccionario
                player_dict['slug'] = player_json.get('player', {}).get('slug', '')
                player_dict['short_name'] = player_json.get('player', {}).get('shortName', '')
                player_dict['team'] = player_json.get('player', {}).get('team', {}).get('name', '')
                player_dict['country'] = player_json.get('player', {}).get('country', {}).get('name', '')
                player_dict['position'] = positions[0] if len(positions) == 1 else ''
                player_dict['second_position'] = positions[1] if len(positions) > 1 else ''
                player_dict['third_position'] = positions[2] if len(positions) > 2 else ''
                player_dict['weight'] = player_json.get('player', {}).get('weight', 0)
                player_dict['height'] = player_json.get('player', {}).get('height', 0)
                player_dict['shirt_number'] = player_json.get('player', {}).get('shirtNumber', 0)
                player_dict['pref_foot'] = player_json.get('player', {}).get('preferredFoot', '')
                player_dict['date_birth'] = player_json.get('player', {}).get('dateOfBirthTimestamp', 0)
                player_dict['contract_until'] = player_json.get('player', {}).get('contractUntilTimestamp', 0)
                player_dict['market_value'] = player_json.get('player', {}).get('proposedMarketValue', 0)

            players_list.append(player_dict)
    
    return pd.DataFrame(players_list)

# Procesamos el JSON de teams y la carpeta con más información
def teams_processing(team_json: dict, teams_dict_path: str) -> pd.DataFrame:

    teams_list = []

    # Para cada equipo
    for team in team_json.get('teams', []):

        # ID
        team_id = team.get('id')
        if team_id:
            team_dict = {'id': team_id, 'name': team.get('name', '')}

            # Si existe el json
            team_json_path = os.path.join(teams_dict_path, f'{team_id}.json')
            if os.path.exists(team_json_path):
                team_json = json_to_dict(team_json_path)

                # Añadimos información al diccionario
                team_dict['slug'] = team_json.get('team', {}).get('slug', '')
                team_dict['short_name'] = team_json.get('team', {}).get('shortName', '')
                team_dict['full_name'] = team_json.get('team', {}).get('fullName', '')
                team_dict['code'] = team_json.get('team', {}).get('nameCode', '')
                team_dict['manager'] = team_json.get('team', {}).get('manager', {}).get('name', '')
                team_dict['venue'] = team_json.get('team', {}).get('venue', {}).get('name', '')
                team_dict['country'] = team_json.get('team', {}).get('country', {}).get('name', '')
                team_dict['primary_colour'] = team_json.get('team', {}).get('teamColors', {}).get('primary', '')
                team_dict['secondary_colour'] = team_json.get('team', {}).get('teamColors', {}).get('secondary', '')
                team_dict['text_colour'] = team_json.get('team', {}).get('teamColors', {}).get('text', '')
                team_dict['foundation'] = team_json.get('team', {}).get('foundationDateTimestamp', 0)

            teams_list.append(team_dict)
    
    return pd.DataFrame(teams_list)

# Procesamos el JSON de teams y la carpeta con más información
def venues_processing(venue_json: dict, venues_dict_path: str) -> pd.DataFrame:

    venues_list = []

    # Para cada equipo
    for venue in venue_json.get('venues', []):

        # ID
        venue_id = venue.get('id')
        if venue_id:
            venue_dict = {'id': venue_id, 'name': venue.get('name', '')}

            # Si existe el json
            venue_json_path = os.path.join(venues_dict_path, f'{venue_id}.json')
            if os.path.exists(venue_json_path):
                venue_json = json_to_dict(venue_json_path)

                # Añadimos información al diccionario
                venue_dict['slug'] = venue_json.get('venue', {}).get('slug', '')
                venue_dict['capacity'] = venue_json.get('venue', {}).get('capacity', 0)
                venue_dict['city'] = venue_json.get('venue', {}).get('city', {}).get('name', '')
                venue_dict['country'] = venue_json.get('venue', {}).get('country', {}).get('name', '')
                venue_dict['latitude'] = venue_json.get('venue', {}).get('venueCoordinates', {}).get('latitude', 0.0)
                venue_dict['longitude'] = venue_json.get('venue', {}).get('venueCoordinates', {}).get('longitude', 0.0)
                venue_dict['matches'] = venue_json.get('statistics', {}).get('matches', 0)
                venue_dict['home_goals'] = venue_json.get('statistics', {}).get('homeTeamGoalsScored', 0)
                venue_dict['away_goals'] = venue_json.get('statistics', {}).get('awayTeamGoalsScored', 0)
                venue_dict['avg_red_cards_game'] = venue_json.get('statistics', {}).get('avgRedCardsPerGame', 0)
                venue_dict['avg_ck_game'] = venue_json.get('statistics', {}).get('avgCornerKicksPerGame', 0)
                venue_dict['home_wins_perc'] = venue_json.get('statistics', {}).get('homeTeamWinsPercentage', 0)
                venue_dict['away_wins_perc'] = venue_json.get('statistics', {}).get('awayTeamWinsPercentage', 0)
                venue_dict['draws_perc'] = venue_json.get('statistics', {}).get('drawsPercentage', 0)
                
            venues_list.append(venue_dict)
    
    return pd.DataFrame(venues_list)

# Dataframe con la información de los entrenadores
def managers_processing(managers_dict_path: str) -> pd.DataFrame:

    list_managers = []

    # Para cada manager que tenemos
    for manager_path in os.listdir(managers_dict_path):
        
        # Full path y json
        full_path = os.path.join(managers_dict_path, manager_path)
        manager_json = json_to_dict(json_path=full_path).get('manager')

        if manager_json:
            list_managers.append({'id': manager_json.get('id', 0),
                                  'name': manager_json.get('name', ''),
                                  'slug': manager_json.get('slug', ''),
                                  'short_name': manager_json.get('shortName', ''),
                                  'team': manager_json.get('team', {}).get('name', ''),
                                  'pref_formation': manager_json.get('preferredFormation', ''),
                                  'country': manager_json.get('country', {}).get('name', ''),
                                  'date_birth': manager_json.get('dateOfBirthTimestamp', 0),
                                  'matches': manager_json.get('performance', {}).get('total', 0),
                                  'wins': manager_json.get('performance', {}).get('wins', 0),
                                  'draws': manager_json.get('performance', {}).get('draws', 0),
                                  'losses': manager_json.get('performance', {}).get('losses', 0),
                                  'goals_for': manager_json.get('performance', {}).get('goalsScored', 0),
                                  'goals_against': manager_json.get('performance', {}).get('goalsConceded', 0),
                                  'points': manager_json.get('performance', {}).get('totalPoints', 0)})
            
    return pd.DataFrame(list_managers)

# Obtención de las alineaciones y estadísticas de los jugadores
def get_single_team_lineups(lineups_dict: dict, home_away: bool, home_team: str, away_team: str) -> Tuple[str, pd.DataFrame]:

    # Formación a devolver
    formation = lineups_dict.get('formation', '')

    lineup = []
    for player in lineups_dict.get('players', []):
        
        # Diccionario con información del jugador
        player_dict = {'player_id': player.get('player', {}).get('id', 0), 
                       'team': home_team if home_away else away_team,
                       'opponent': away_team if home_away else home_team,
                       'player': player.get('player', {}).get('name', ''),
                       'position': player.get('position', ''),
                       'shirt_number': player.get('shirtNumber', 0)}
        
        # Para cada estadística, la concatenamos
        statistics = player.get('statistics', {})
        for st in statistics.keys():
            if st not in ['ratingVersions', 'statisticsType']:
                player_dict[st] = statistics[st]

        lineup.append(player_dict)
    
    # DF y quitamos minutos cero
    lineup_df = pd.DataFrame(lineup).fillna(0)
    lineup_df = lineup_df[lineup_df['minutesPlayed'] > 0]

    return formation, lineup_df

# Función para processar un simple partido
def get_matches_info(raw_matches_m_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]: 

    # Listas para concatenar info
    match_info_dict = []
    lineups_list = []
    all_team_stats_list = []
    all_shots_info = []
    all_momentum = []

    # Para cada partido, obtenemos los datos
    for match in os.listdir(raw_matches_m_path):
        
        # Full path i JSON
        full_path = os.path.join(raw_matches_m_path, match)
        match_json = json_to_dict(full_path)

        # Equipos y ID
        match_id = match_json.get('match', {}).get('event', {}).get('id', 0)
        home_team = match_json.get('match', {}).get('event', {}).get('homeTeam', {}).get('name', '')
        away_team = match_json.get('match', {}).get('event', {}).get('awayTeam', {}).get('name', '')

        # Información del partido
        match_dict = {'id': match_id,
                      'slug': match_json.get('match', {}).get('event', {}).get('slug', ''),
                      'round': match_json.get('match', {}).get('event', {}).get('roundInfo', {}).get('round', 0),
                      'venue': match_json.get('match', {}).get('event', {}).get('venue', {}).get('name', ''),
                      'attendance': match_json.get('match', {}).get('event', {}).get('attendance'),
                      'referee': match_json.get('match', {}).get('event', {}).get('referee', {}).get('name', ''),
                      'home_team': home_team,
                      'away_team': away_team,
                      'home_score': match_json.get('match', {}).get('event', {}).get('homeScore', {}).get('display', 0),
                      'away_score': match_json.get('match', {}).get('event', {}).get('awayScore', {}).get('display', 0)}

        # Alineaciones locales y vistantes
        home_formation, home_lineup = get_single_team_lineups(lineups_dict=match_json.get('lineups', {}).get('home', {}), home_away=1, home_team=home_team, away_team=away_team)
        away_formation, away_lineup = get_single_team_lineups(lineups_dict=match_json.get('lineups', {}).get('away', {}), home_away=0, home_team=home_team, away_team=away_team)

        # Creamos el dataframe con las dos alineaciones y añadimos el ID del partido
        lineups = pd.concat([home_lineup, away_lineup], ignore_index=True)
        lineups.insert(0, 'match_id', match_id)

        # Añadimos formaciones al diccionario
        match_dict['home_formation'] = home_formation
        match_dict['away_formation'] = away_formation

        # Concatenamos informaciones
        match_info_dict.append(match_dict)
        lineups_list.append(lineups)

        # Estadísticas de los equipos
        teams_stats_df = pd.DataFrame({'match_id': [match_id, match_id], 'team': [home_team, away_team], 'opponent': [away_team, home_team]})
        for stats_group in match_json.get('statistics', {}).get('statistics', [])[0].get('groups', []):
            for stat_item in stats_group.get('statisticsItems', []):
                teams_stats_df[f'{stat_item.get('name', '')}'] = [stat_item.get('homeValue', 0), stat_item.get('awayValue', 0)]
        all_team_stats_list.append(teams_stats_df)

        # Tiros
        shots_info = []
        for shot in match_json.get('shotmap', {}).get('shotmap', []):
            shots_info.append({'match_id': match_id,
                            'team': home_team if shot.get('isHome', False) else away_team,
                            'opponent': away_team if shot.get('isHome', False) else home_team,
                            'player': shot.get('player', {}).get('name', ''),
                            'type': shot.get('shotType', ''),
                            'situation': shot.get('situation', ''),
                            'body_part': shot.get('situation', ''),
                            'xg': shot.get('xg', 0),
                            'xgot': shot.get('xgot', 0),
                            'time': shot.get('time', 0),
                            'goalkeeper': shot.get('goalkeeper', {}).get('name', ''),
                            'player_x': shot.get('playerCoordinates', {}).get('x', 0), 
                            'player_y': shot.get('playerCoordinates', {}).get('y', 0),
                            'block_x': shot.get('blockCoordinates', {}).get('x', 0),
                            'block_y': shot.get('blockCoordinates', {}).get('y', 0),
                            'goal_x': shot.get('goalMouthCoordinates', {}).get('x', 0),
                            'goal_y': shot.get('goalMouthCoordinates', {}).get('y', 0),
                            'goal_z': shot.get('goalMouthCoordinates', {}).get('z', 0)})
        all_shots_info.append(pd.DataFrame(shots_info))

        # Momentum
        momentum_list = []
        for point in match_json.get('graph', {}).get('graphPoints', []):
            momentum_list.append({'match_id': match_id,
                                'team': home_team if point.get('value', 0) >= 0 else away_team,
                                'minute': point.get('minute', 0),
                                'value': point.get('value', 0)})
        all_momentum.append(pd.DataFrame(momentum_list))
                
    # Dataframes
    match_info_df = pd.DataFrame(match_info_dict).fillna(0)
    lineups_df = pd.concat(lineups_list, ignore_index=True).fillna(0)
    stats_df = pd.concat(all_team_stats_list, ignore_index=True).fillna(0)
    shots_df = pd.concat(all_shots_info, ignore_index=True).fillna(0)
    momentum_df = pd.concat(all_momentum, ignore_index=True).fillna(0)
    
    return match_info_df, lineups_df, stats_df, shots_df, momentum_df

# Limpieza de los datos de una liga
def league_processing(league_id: int, raw_out_path: str, clean_out_path: str) -> None:

    # Obtenemos el nombre de la liga y el path -> creación de la carpeta de output (clean)
    league_name = comps[comps['id'] == league_id]['tournament'].iloc[0]
    league_slug = league_name.lower().replace(' ', '-')
    out_league_path = os.path.join(clean_out_path, 'ss', league_slug)
    os.makedirs(out_league_path, exist_ok=True)

    # Capeta de la liga y carpetas (seasons) dentro
    raw_data_path = os.path.join(raw_out_path, 'ss', league_slug)
    av_seasons_path = os.path.join(raw_data_path, 'available_seasons.json')
    av_seasons = json_to_dict(json_path=av_seasons_path)

    # Obtención del dataframe y guardado
    av_seasons_df = get_seasons_df(league_id=league_id, available_seasons_json=av_seasons)
    av_seasons_df.to_csv(os.path.join(out_league_path, 'available_seasons.csv'), sep=';', index=False)

    # Para cada temporada
    for season in av_seasons_df['year'].tolist():

        if not os.path.exists(os.path.join(raw_data_path, season)):
            continue

        # Carpeta de output de la temporada y otras carpetas que necesitaremos
        out_season_path = os.path.join(out_league_path, season)
        os.makedirs(out_season_path, exist_ok=True)

        # Info
        info_path = os.path.join(out_season_path, 'info')
        os.makedirs(info_path, exist_ok=True)

        # Standings
        standings_path = os.path.join(info_path, 'standings')
        os.makedirs(standings_path, exist_ok=True)

        # Match
        match_path = os.path.join(out_season_path, 'match')
        os.makedirs(match_path, exist_ok=True)

        # Carpeta con la información, lectura de los ficheros json
        raw_info_path = os.path.join(raw_data_path, season, 'info')
        standings_json = json_to_dict(json_path=os.path.join(raw_info_path, 'standings.json'))
        player_json = json_to_dict(json_path=os.path.join(raw_info_path, 'player.json'))
        team_json = json_to_dict(json_path=os.path.join(raw_info_path, 'team.json'))
        venue_json = json_to_dict(json_path=os.path.join(raw_info_path, 'venue.json'))

        # Tablas de clasificación y guardado
        total_st_df, home_st_df, away_st_df = obtain_standings_tables(league_id=league_id, season_key=season, standings_json=standings_json)
        total_st_df.to_csv(os.path.join(standings_path, 'total.csv'), sep=';', index=False)
        home_st_df.to_csv(os.path.join(standings_path, 'home.csv'), sep=';', index=False)
        away_st_df.to_csv(os.path.join(standings_path, 'away.csv'), sep=';', index=False)

        # Procesado de jugadores
        players_df = players_processing(player_json=player_json, players_dict_path=os.path.join(raw_info_path, 'player'))
        players_df.insert(0, 'league', league_id)
        players_df.insert(1, 'season', season)
        players_df.to_csv(os.path.join(info_path, 'player.csv'), sep=';', index=False)

        # Procesado de equipos
        teams_df = teams_processing(team_json=team_json, teams_dict_path=os.path.join(raw_info_path, 'team'))
        teams_df.insert(0, 'league', league_id)
        teams_df.insert(1, 'season', season)
        teams_df.to_csv(os.path.join(info_path, 'team.csv'), sep=';', index=False)

        # Procesado de estadios
        venues_df = venues_processing(venue_json=venue_json, venues_dict_path=os.path.join(raw_info_path, 'venue'))
        venues_df.insert(0, 'league', league_id)
        venues_df.insert(1, 'season', season)
        venues_df.to_csv(os.path.join(info_path, 'venue.csv'), sep=';', index=False)

        # Procesado de entrenadores
        managers_df = managers_processing(managers_dict_path=os.path.join(raw_info_path, 'manager'))
        managers_df.insert(0, 'league', league_id)
        managers_df.insert(1, 'season', season)
        managers_df.to_csv(os.path.join(info_path, 'manager.csv'), sep=';', index=False)

        # Paths con información
        raw_matches_path = os.path.join(raw_data_path, season, 'matches')
        raw_matches_m_path = os.path.join(raw_matches_path, 'match')
        raw_matches_p_path = os.path.join(raw_matches_path, 'player')

        # Información de los partidos y guardado en la carpeta match_path
        match_info_df, lineups_df, stats_df, shots_df, momentum_df = get_matches_info(raw_matches_m_path=raw_matches_m_path)
        match_info_df.to_csv(os.path.join(match_path, 'info.csv'), sep=';', index=False)
        lineups_df.to_csv(os.path.join(match_path, 'lineups.csv'), sep=';', index=False)
        stats_df.to_csv(os.path.join(match_path, 'stats.csv'), sep=';', index=False)
        shots_df.to_csv(os.path.join(match_path, 'shots.csv'), sep=';', index=False)
        momentum_df.to_csv(os.path.join(match_path, 'momentum.csv'), sep=';', index=False)