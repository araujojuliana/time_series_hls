#%%
from os import mkdir
import yaml
from pathlib import Path
import geopandas as gpd
import logging
from tqdm import tqdm
import time
from helper import *
log_dir = Path("log")
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_dir / "processing.log"),
        logging.StreamHandler() 
    ]
)

def main():

    with open(r"C:\Users\jd2725\Documents\Field_boundaries_v2\time_series_hls\conf_batch.yaml", "r") as f:
            config = yaml.safe_load(f)
    init_gee(config["project_id"])
    states = config.get("statefp", [])
    
    for state in states:
        
        gdf = gpd.read_file(Path(config["input_path"], f"fields_state_{state}.gpkg"))
        gdf = gdf[gdf["STATEFP"] == state]
        logging.info(f"Total features in state {state}: {len(gdf)}")

        counties = gdf["COUNTYFP"].unique()
        logging.info(f"Counties in state {state}: {counties}")

        
        output_processed = Path(config["output_postprocess"])
        output_processed.mkdir(parents=True, exist_ok=True)

        for county in counties:
            county_subset = gdf[gdf["COUNTYFP"] == county]
            total = len(county_subset)

            logging.info(f"Processing county: {county}. Total features: {total}")

            year = str(config.get("year"))
            state = str(state)
            county = str(county)

            base_path = Path(config["output_dir"]) / year / state / county
            base_path.mkdir(parents=True, exist_ok=True)
           
            batch_size = config["batch_size"]
            cc = config.get("cc")
            START_DATE = config.get("start_date")
            END_DATE = config.get("end_date")
            output_dir = config.get("output_dir")
            valid_pixels=config.get("valid_pixels")

            for start in tqdm(range(0, total, batch_size), desc=f"State {state} County {county}"):
                batch = county_subset.iloc[start:start+batch_size]
                
                all_ranges = get_all_ranges(base_path, state, county)

                batch_end = start + len(batch)

                ranges = get_relevant_ranges(all_ranges, start, batch_end)

                gaps = find_gaps(ranges, start, batch_end)

                if not gaps:
                    logging.info(f"[SKIP] Batch {start} fully processed")
                    continue

                for gap_start, gap_end in gaps:
                    logging.info(f"[GAP] Processing missing range {gap_start} -> {gap_end}")

                    gap_batch = county_subset.iloc[gap_start:gap_end]

                    try:
                        process_batch(
                            gap_batch, 
                            gap_start,  
                            output_dir, 
                            START_DATE, 
                            END_DATE, 
                            cc, 
                            state, 
                            county,
                            base_path, 
                            output_processed,
                            valid_pixels
                        )

                    except Exception as e:
                        logging.error(f"Failed to process batch {start} -> {start + len(batch)}: {e}")

if __name__ == "__main__":
    main()                  

# %%
