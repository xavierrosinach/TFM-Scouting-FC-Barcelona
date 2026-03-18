import os
import time
import pandas as pd

from use.config import comps, comps_path


# --------------------------------------------------------------------------------------
# PIPELINE DE UNA LIGA
# --------------------------------------------------------------------------------------

def main_league_data(
    league_id: int,
    data_path: str,
    act_time_scr: float,
    act_time_cln: float,
    act_time_uni: float = None,
    max_age_days: int = 7,
    print_info: bool = True,
    matches_to_proc: int = None,
    scrape_images: bool = True,
    do_scr: bool = True,
    do_cln: bool = True,
    do_uni: bool = True,
    do_fm: bool = True,
    do_sw: bool = True,
    do_ss: bool = True
) -> tuple[float | None, float | None, float | None]:
    """
    Ejecuta el pipeline completo de una liga.

    Parameters
    ----------
    league_id : int
        ID interno de la liga.
    data_path : str
        Ruta base de datos.
    act_time_scr : float
        Último timestamp de scraping.
    act_time_cln : float
        Último timestamp de cleaning.
    act_time_uni : float, optional
        Último timestamp de unificación.
    max_age_days : int, default=7
        Días máximos antes de volver a ejecutar un proceso.
    print_info : bool, default=True
        Mostrar logs.
    matches_to_proc : int, optional
        Número máximo de partidos a procesar.
    scrape_images : bool, default=True
        Descargar imágenes.
    do_scr, do_cln, do_uni : bool
        Activar scraping, cleaning y unificación.
    do_fm, do_sw, do_ss : bool
        Activar Fotmob, Scoresway y Sofascore.

    Returns
    -------
    tuple[float | None, float | None, float | None]
        Nuevos timestamps de scraping, cleaning y unificación.
    """

    raw_data_path = os.path.join(data_path, "raw")
    clean_data_path = os.path.join(data_path, "clean")
    processed_data_path = os.path.join(data_path, "proc")

    now = time.time()
    max_age_seconds = max_age_days * 24 * 60 * 60

    def should_run(original_time: float | None) -> bool:
        if original_time is None or pd.isna(original_time):
            return True
        return (now - original_time) >= max_age_seconds

    time_scr = None
    time_cln = None
    time_uni = None

    # ----------------------------------------------------------------------------------
    # SCRAPING
    # ----------------------------------------------------------------------------------

    if do_scr and should_run(act_time_scr):
        if do_fm:
            import scr.fm_scr as fm_scr
            fm_scr.main_fotmob_league_scraping(
                league_id=league_id,
                out_path=raw_data_path,
                print_info=print_info
            )

        if do_sw:
            import scr.sw_scr as sw_scr
            sw_scr.main_scoresway_league_scraping(
                league_id=league_id,
                out_path=raw_data_path,
                matches_to_proc=matches_to_proc,
                print_info=print_info
            )

        if do_ss:
            import scr.ss_scr as ss_scr
            ss_scr.main_sofascore_league_scraping(
                league_id=league_id,
                out_path=raw_data_path,
                scrape_images=scrape_images,
                matches_to_proc=matches_to_proc,
                print_info=print_info
            )

        time_scr = time.time()

    # ----------------------------------------------------------------------------------
    # CLEANING
    # ----------------------------------------------------------------------------------

    if do_cln and should_run(act_time_cln):
        if do_fm:
            import cln.fm_cln as fm_cln
            fm_cln.main_fotmob_league_cleaning(
                league_id=league_id,
                out_path=raw_data_path,
                print_info=print_info
            )

        if do_sw:
            import cln.sw_cln as sw_cln
            sw_cln.main_scoresway_league_cleaning(
                league_id=league_id,
                out_path=raw_data_path,
                print_info=print_info
            )

        if do_ss:
            import cln.ss_cln as ss_cln
            ss_cln.main_sofascore_league_cleaning(
                league_id=league_id,
                out_path=raw_data_path,
                print_info=print_info
            )

        time_cln = time.time()

    # ----------------------------------------------------------------------------------
    # UNIFICACIÓN
    # ----------------------------------------------------------------------------------

    if do_uni and should_run(act_time_uni):
        import uni.unifier as unif
        unif.league_data_unification(
            league_id=league_id,
            raw_data_path=raw_data_path,
            clean_data_path=clean_data_path,
            processed_data_path=processed_data_path,
            print_info=print_info
        )

        time_uni = time.time()

    return time_scr, time_cln, time_uni


# --------------------------------------------------------------------------------------
# FUNCIÓN PRINCIPAL
# --------------------------------------------------------------------------------------

def main() -> None:
    """
    Ejecuta el pipeline para las ligas definidas en comps.csv.
    """
    data_path = r"C:\Users\ASUS\Desktop\TFM\data"

    for idx, row in comps.iterrows():

        league_name = row["tournament"]

        print("================================================================================")
        print(f"Starting the full data pipeline ({league_name})")

        start_time = time.time()

        time_scr, time_cln, time_uni = main_league_data(
            league_id=row["id"],
            data_path=data_path,
            print_info=True,
            act_time_scr=row["time_scr"],
            act_time_cln=row["time_cln"],
            act_time_uni=row["time_uni"])

        if time_scr is not None:
            comps.loc[idx, "time_scr"] = time_scr
        if time_cln is not None:
            comps.loc[idx, "time_cln"] = time_cln
        if time_uni is not None:
            comps.loc[idx, "time_uni"] = time_uni

        comps.to_csv(comps_path, index=False, sep=";", encoding="latin1")

        elapsed_time = time.time() - start_time
        if elapsed_time >= 60:
            minutes = int(elapsed_time // 60)
            seconds = int(elapsed_time % 60)
            time_str = f"{minutes} minutes {seconds} seconds"
        else:
            time_str = f"{elapsed_time:.2f} seconds"

        print(f"Finished the full data pipeline ({league_name}) in {time_str}")
        print("================================================================================")


# --------------------------------------------------------------------------------------
# EJECUCIÓN
# --------------------------------------------------------------------------------------

if __name__ == "__main__":
    main()