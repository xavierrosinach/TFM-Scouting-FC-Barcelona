import os
import pandas as pd
import time

from use.config import comps, comps_path
from use.functions import elapsed_time_str

# Función principal que procesa todos los datos de una liga
def main_league_data(league_id: int, data_path: str, act_time_scr: float, act_time_cln: float, act_time_uni:float=None, max_age_days:int=7, print_info: bool = True, matches_to_proc: int = None, scrape_images: bool = True, do_scr: bool = True, do_cln: bool = True, do_uni: bool = True, do_fm: bool = True, do_sw: bool = True, do_ss: bool = True) -> None:

    raw_data_path = os.path.join(data_path, 'raw')           # Datos de scraping
    clean_data_path = os.path.join(data_path, 'clean')       # Datos del cleaning
    processed_data_path = os.path.join(data_path, 'proc')    # Datos de la unificación

    now = time.time()                                        # Tiempo actual
    max_age = max_age_days * 24 * 60 * 60                    # En segundos

    # Da true si la diferencia es mayor a cinco días
    def run_proc(original_time:float) -> bool:
        if pd.isna(original_time):
            return True
        return (now - original_time) >= max_age

    # Parte de Scraping
    if do_scr and run_proc(act_time_scr):
        if do_fm:       # Scraping de Fotmob
            import scr.fm_scr as fm_scr
            fm_scr.main_fotmob_league_scraping(league_id=league_id, out_path=raw_data_path, print_info=print_info)
        if do_sw:       # Scraping de Scoresway
            import scr.sw_scr as sw_scr
            sw_scr.main_scoresway_league_scraping(league_id=league_id, out_path=raw_data_path, matches_to_proc=matches_to_proc, print_info=print_info)
        if do_ss:       # Scraping de Sofascore
            import scr.ss_scr as ss_scr
            ss_scr.main_sofascore_league_scraping(league_id=league_id, out_path=raw_data_path, scrape_images=scrape_images, matches_to_proc=matches_to_proc, print_info=print_info)
        time_scr = time.time()
    else:
        time_scr = None

    # Parte de Cleaning
    if do_cln and run_proc(act_time_cln):
        if do_fm:       # Cleaning de Fotmob
            import cln.fm_cln as fm_cln
            fm_cln.main_fotmob_league_cleaning(league_id=league_id, out_path=raw_data_path, print_info=print_info)
        if do_sw:       # Cleaning de Scoresway
            import cln.sw_cln as sw_cln
            sw_cln.main_scoresway_league_cleaning(league_id=league_id, out_path=raw_data_path, print_info=print_info)
        if do_ss:       # Cleaning de Sofascore
            import cln.ss_cln as ss_cln 
            ss_cln.main_sofascore_league_cleaning(league_id=league_id, out_path=raw_data_path, print_info=print_info)
        time_cln = time.time()
    else:
        time_cln = None

    # Parte de Unificación de datos
    if do_uni and run_proc(act_time_uni):
        import uni.unifier as unif
        unif.league_data_unification(league_id=league_id, raw_data_path=raw_data_path, clean_data_path=clean_data_path, processed_data_path=processed_data_path, print_info=print_info)
        time_uni = time.time()
    else:
        time_uni = None

    return time_scr, time_cln, time_uni

# Función principal - aplicado para todas las ligas
def main():

    data = r"C:\Users\xrosinach\Desktop\TFM\data"       # Data path
    for idx, row in comps.iterrows():

        if row['id'] != 82:
            continue

        print('================================================================================')
        print(f'Starting the full data pipeline ({row['tournament']})')

        start_time = time.time()

        # --------------------------------------------------------------------------------------------------------------
        #                                                  FUNCIÓN PRINCIPAL
        # --------------------------------------------------------------------------------------------------------------
        time_scr, time_cln, time_uni = main_league_data(league_id=row['id'], data_path=data, print_info=False, do_scr=False, do_uni=False,
                                                        act_time_scr=row['time_scr'], act_time_cln=row['time_cln'], 
                                                        act_time_uni=row['time_uni'])
        # --------------------------------------------------------------------------------------------------------------

        if time_scr is not None:
            comps.loc[idx, 'time_scr'] = time_scr       # Guardar los resultados en el dataframe
        if time_cln is not None:
            comps.loc[idx, 'time_cln'] = time_cln
        if time_uni is not None:
            comps.loc[idx, 'time_uni'] = time_uni

        comps.to_csv(comps_path, index=False, sep=';', encoding='latin1')       # Guardado una vez actualizado
  
        print(f'Finished the full data pipeline ({row['tournament']}) in {elapsed_time_str(start_time=start_time)}')
        print('================================================================================')

# Ejecución
if __name__ == "__main__":
    main()
