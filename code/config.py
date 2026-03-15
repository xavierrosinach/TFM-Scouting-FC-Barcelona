import os
import pandas as pd
import json

cdir = os.getcwd()
utils = os.path.join(os.path.abspath(os.path.join(cdir, '..')), 'utils')

comps = pd.read_csv(os.path.join(utils, 'comps.csv'), sep=';', encoding='latin1')

with open(os.path.join(utils, 'des_seasons.json'), 'r', encoding='utf-8') as f:
    desired_seasons = json.load(f)

act_season = desired_seasons[0]