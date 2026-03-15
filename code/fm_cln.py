import os
import pandas as pd
import time
import json as jsonlib
import unicodedata
import re
import numpy as np

from config import comps, desired_seasons, act_season

# Lector de JSON
def json_to_dict(json_path: str) -> dict:
    with open(json_path, "r", encoding="utf-8") as f:
        dict = jsonlib.load(f)
    return dict

# Creación de slug a partir de un string.
def create_slug(text: str) -> str:

    text = text.lower()                                                                                     # Letra minúscula
    text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')        # Eliminación de acentos
    text = re.sub(r"\s+", "_", text)                                                                        # Substitución de espacios por '_'
    text = re.sub(r"[^a-z0-9_]", "", text)                                                                  # Eliminación de carácteres no alfanuméricos
    text = re.sub(r"_+", "_", text).strip("_")
    
    return text

# Obtenemos dataframes de información a partir del JSON de información de una temporada
def clean_season_information(season_info: dict, season_key: str, league_slug: str, league_name: str, season_out_path: str) -> None:

    os.makedirs(season_out_path, exist_ok=True)         # Creación del path si no existe

    details = season_info.get('details')                                        # Detalles - información de la liga
    if details:
        info_df = pd.DataFrame([{'fm_id': details.get('id', np.nan),            # Información a formato dataframe
                                 'type':details.get('type', np.nan),
                                 'season':season_key,
                                 'slug':league_slug,
                                 'name':league_name,
                                 'short_name':details.get('shortName', np.nan),
                                 'country':details.get('country', np.nan),
                                 'gender':details.get('gender', np.nan),
                                 'league_color':details.get('leagueColor', np.nan)}])
        info_df.to_csv(os.path.join(season_out_path, 'info.csv'), index=False, sep=';')               # Guardado
        
    table = season_info.get('table', [1])[0].get('data', {}).get('table')       # Standings table
    if table:
        filters = ['all', 'home', 'away', 'form', 'xg']                         # Distintas tablas que podemos encontrar
        os.makedirs(os.path.join(season_out_path, 'standings'), exist_ok=True)  # Creación de carpeta con tablas de clasificación si no existe
        for f in filters:
            part_table = table.get(f)                                           # Obtención de la tabla parcial
            if part_table:
                list_info = [t for t in part_table]
                table_df = pd.DataFrame(list_info)                              # Concatenamos información
                table_df.to_csv(os.path.join(season_out_path, 'standings', f'{f}.csv'), index=False, sep=';')

    matches = season_info.get('fixtures', {}).get('allMatches')                 # Partidos
    if matches:
        matches_list = []
        for match in matches:                                                   # Concatenamos información para cada partido
            matches_list.append({'round':match.get('round', np.nan),
                                 'round_name':match.get('roundName', np.nan),
                                 'match_id':match.get('id', np.nan),
                                 'home_team':match.get('home', {}).get('name', np.nan),
                                 'home_team_id':match.get('home', {}).get('id', np.nan),
                                 'away_team':match.get('away', {}).get('name', np.nan),
                                 'away_team_id':match.get('away', {}).get('id', np.nan),
                                 'time':match.get('status',{}).get('utcTime', np.nan),
                                 'score_str':match.get('status',{}).get('scoreStr', np.nan)})
        matches_df = pd.DataFrame(matches_list)
        matches_df.to_csv(os.path.join(season_out_path, 'matches.csv'), index=False, sep=';')         # Guardado
    
# Función principal para la limpieza de datos de Fotmob de una liga
def main_fotmob_league_cleaning(league_id: int, out_path: str, print_info: bool = True) -> None:

    start_time = time.time()   # Inicio del contador

    league_name = comps[comps['id'] == league_id]['tournament'].iloc[0]     # Nombre de la liga
    league_slug = create_slug(text=league_name)                             # Slug de la liga

    league_raw_path = os.path.join(out_path, 'fotmob', league_slug)         # Path de datos raw
    league_clean_path = league_raw_path.replace('raw', 'clean')             # Obtención de la nueva carpeta
    os.makedirs(league_clean_path, exist_ok=True)                           # Creación de la carpeta con datos limpios en caso de que no se haya hecho

    if print_info:
        print('================================================================================')
        print(f'Starting Fotmob cleaning ({league_name})')

    seasons_to_proc = [f for f in os.listdir(league_raw_path) if os.path.isdir(os.path.join(league_raw_path, f))]        # Lista con temporadas a procesar
    for s in seasons_to_proc:
        season_info_path = os.path.join(league_raw_path, s, 'info.json')        # Path con el JSON de información
        if os.path.exists(season_info_path):
            season_info = json_to_dict(json_path=season_info_path)              # Leemos información si existe
            clean_season_information(season_info=season_info, season_key=s, league_slug=league_slug, league_name=league_name, season_out_path=os.path.join(league_clean_path, s))
            if print_info:
                print(f'     - Information cleaned for season {s}')
        else:
            continue

    elapsed_time = time.time() - start_time         # Tiempo transcurrido
    if print_info:
        print(f'Finished Fotmob cleaning ({league_name}) in {elapsed_time:.2f} seconds')
        print('================================================================================')