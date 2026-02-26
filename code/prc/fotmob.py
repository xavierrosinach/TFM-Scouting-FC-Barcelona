import pandas as pd
import os
import json as jsonlib

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
    all_seasons = available_seasons_json.get('allAvailableSeasons')

    # Obtenemos información
    if not all_seasons:
        return pd.DataFrame()
    
    # Lista para concatenar info
    rows = []

    for season, data in all_seasons.items():
        if data.get('key') in desired_seasons:
            rows.append({'league': league_id,
                         'season': data.get('key'),
                         'link': data.get('link')})

    return pd.DataFrame(rows)

# Obtenemos la tabla de clasificación
def get_standings_table(league_id: int, season_key: str, available_seasons_json: dict) -> pd.DataFrame:

    tables = available_seasons_json['table'][0]['data']['table']
    if not tables:
        return {}

    # Tabla de home o away
    def part_table(part_qual: dict, suffix: str) -> pd.DataFrame:

        if not part_qual:
            return pd.DataFrame()

        rows = []
        for team in part_qual:
            rows.append({'team': team.get('name', ''),
                        f'{suffix}pos': team.get('idx', 0),
                        f'{suffix}pts': team.get('pts', 0),
                        f'{suffix}played': team.get('played', 0),
                        f'{suffix}wins': team.get('wins', 0),
                        f'{suffix}draws': team.get('draws', 0),
                        f'{suffix}losses': team.get('losses', 0),
                        f'{suffix}goalsScored': team.get('goalsScored', 0),
                        f'{suffix}goalsConceded': int(team.get('scoresStr', '').split('-')[1]),
                        f'{suffix}goalDiff': team.get('goalConDiff', 0)})
        return pd.DataFrame(rows)

    # Tabla de todo, local y visitante
    all_table = part_table(part_qual=tables.get('all'), suffix='')
    home_table = part_table(part_qual=tables.get('home'), suffix='home_')
    away_table = part_table(part_qual=tables.get('away'), suffix='away_')

    # Para la tabla de xg, de forma diferente
    rows_xg = []
    for team in tables.get('xg', {}):
        rows_xg.append({'team': team.get('name', ''),
                        'xG': team.get('xg', 0.0),
                        'xGA': team.get('xgConceded', 0.0),
                        'xGDiff': team.get('xgDiff', 0.0),
                        'xGADiff': team.get('xgConcededDiff', 0.0),
                        'xPos': team.get('xPosition', 0),
                        'xPosDiff': team.get('xPositionDiff', 0),
                        'xPts': team.get('xPoints', 0.0),
                        'xPtsDiff': team.get('xPointsDiff', 0.0)})
    xg_df = pd.DataFrame(rows_xg)

    # Merge de todos los dfs por team
    standings_df = pd.merge(all_table, home_table, how='inner', on='team')
    standings_df = pd.merge(standings_df, away_table, how='inner', on='team')
    standings_df = pd.merge(standings_df, xg_df, how='inner', on='team')

    # Añadimos temporada y liga
    standings_df.insert(0, 'league', league_id)
    standings_df.insert(1, 'season', season_key)
    
    return standings_df.sort_values(by='pos')

# Procesado de datos de una liga entera en fotmob
def league_processing(league_id: int, out_path: str, clean_out_path: str) -> None:

    # Obtenemos el nombre de la liga y el path
    league_name = comps[comps['id'] == league_id]['tournament'].iloc[0]
    league_slug = league_name.lower().replace(' ', '-')
    out_league_path = os.path.join(clean_out_path, 'fm', league_slug)
    os.makedirs(out_league_path, exist_ok=True)

    # JSON de temporadas disponibles
    available_seasons = json_to_dict(os.path.join(out_path, 'fm', league_slug, 'available_seasons.json'))
    seasons_df = get_seasons_df(league_id=league_id, available_seasons_json=available_seasons)
    seasons_df.to_csv(os.path.join(out_league_path, 'available_seasons.csv'), sep=';', index=False)

    # Para todas las temporadas
    out_seasons_path = os.path.join(out_league_path, 'standings')
    os.makedirs(out_seasons_path, exist_ok=True)

    for season_key in seasons_df['season'].tolist():
        season_json = json_to_dict(os.path.join(out_path, 'fm', league_slug, 'seasons', f'{season_key}.json'))
        standings_df = get_standings_table(league_id=league_id, season_key=str(season_key), available_seasons_json=season_json)
        standings_df.to_csv(os.path.join(out_seasons_path, f'{season_key}.csv'), sep=';', index=False)