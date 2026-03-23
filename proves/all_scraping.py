import pandas as pd
from ss_scr import ss_main_league_scraping
from sw_scr import sw_main_league_scraping
from use.config import COMPS, DATA_PATH

INVERSE_COMPS = COMPS.iloc[::-1]

for league_id in INVERSE_COMPS['id']:
    sw_main_league_scraping(league_id=league_id)
    ss_main_league_scraping(league_id=league_id)