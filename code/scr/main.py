import os
import pandas as pd

# Obtenemos el CSV con competiciones
cdir = os.getcwd()
utils = os.path.join(os.path.abspath(os.path.join(cdir, '..', '..')), 'utils')
comps = pd.read_csv(os.path.join(utils, 'comps.csv'), sep=';')

# Main scraper de una liga
def main_scrape_league_data(league_id: int, out_path: str, do_ss: bool = False, do_sw: bool = False, do_fm: bool = False) -> None:

    if do_ss:
        import sofascore as ss
        ss.scrape_league_data(league_id=league_id, out_path=os.path.join(out_path, 'ss'))
    elif do_sw:
        import scoresway as sw
        sw.scrape_league_data(league_id=league_id, out_path=os.path.join(out_path, 'sw'))
    elif do_fm:
        import fotmob as fm
        fm.scrape_league_data(league_id=league_id, out_path=os.path.join(out_path, 'fm'))

out_path = r'G:\Xavier Rosinach Capell\03 Máster en Big Data Deportivo\04 Trabajo Final de Master\data\raw'
# out_path = r'C:\Users\xrosinach\Desktop\TFM-Scouting-FC-Barcelona\data\raw'
main_scrape_league_data(league_id=73, out_path=out_path, do_ss=True)
