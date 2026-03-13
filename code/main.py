import fm_cln as fm
import sw_cln as sw
import ss_cln as ss

league_id = 82
out_path = r"c:\Users\xrosinach\Desktop\TFM\data\raw"

# Procesado (incluyendo scraping)
fm_clean_path = fm.main_fotmob_league_cleaning(league_id=league_id, out_path=out_path, do_scraping=True, print_info=True)
sw_clean_path = sw.main_scoresway_league_cleaning(league_id=league_id, out_path=out_path, do_scraping=True, print_info=True)
ss_clean_path = ss.main_sofascore_league_cleaning(league_id=league_id, out_path=out_path, do_scraping=True, scrape_images=False, print_info=True)
