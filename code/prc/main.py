import os
import pandas as pd
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

# Obtenemos el CSV con competiciones
cdir = os.getcwd()
utils = os.path.join(os.path.abspath(os.path.join(cdir, '..', '..')), 'utils')
comps = pd.read_csv(os.path.join(utils, 'comps.csv'), sep=';')

# Main scraper de una liga
def main_processing_league_data(league_id: int, raw_out_path: str, clean_out_path: str, do_ss: bool = False, do_sw: bool = False, do_fm: bool = False) -> None:

    if do_ss:
        import sofascore as ss
        ss.league_processing(league_id=league_id, raw_out_path=raw_out_path, clean_out_path=clean_out_path)
    elif do_sw:
        import scoresway as sw
        sw.league_processing(league_id=league_id, raw_out_path=raw_out_path, clean_out_path=clean_out_path)
    elif do_fm:
        import fotmob as fm
        fm.league_processing(league_id=league_id, out_path=raw_out_path, clean_out_path=clean_out_path)

raw_out_path = r'G:\Xavier Rosinach Capell\03 Máster en Big Data Deportivo\04 Trabajo Final de Master\data\raw'
clean_out_path = r'G:\Xavier Rosinach Capell\03 Máster en Big Data Deportivo\04 Trabajo Final de Master\data\clean'
main_processing_league_data(league_id=73, raw_out_path=raw_out_path, clean_out_path=clean_out_path, do_fm=True)
main_processing_league_data(league_id=73, raw_out_path=raw_out_path, clean_out_path=clean_out_path, do_ss=True)
main_processing_league_data(league_id=73, raw_out_path=raw_out_path, clean_out_path=clean_out_path, do_sw=True)