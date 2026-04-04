#%%
# =========================================================
# GEE Time Series Extraction (Polygon-based)
# ---------------------------------------------------------
# Extracts EVI time series from Sentinel-2 for input polygons
# using Google Earth Engine (GEE).
#
# Author: Juliana
# =========================================================
import logging
import time
from duckdb import df
import ee
import geemap
import time
from pathlib import Path
import re
from transform_dataframe import transform_dataframe

# =========================
# LOGGING SETUP
# =========================
def log_failure(state, county, start, batch_size, error_msg):
    FAILED_LOG = Path("log") / "failed_batches.csv"
    with open(FAILED_LOG, "a") as f:
        f.write(f"{state},{county},{start},{batch_size},{error_msg}\n")

# =========================
# GET PROCESSED RANGES OF A BATCH
# =========================
def get_processed_ranges(base_path, state, county, start):
    

    files = list(base_path.glob(f"{state}_{county}_batch_{start}_to_*.csv"))

    ranges = []

    for f in files:
        match = re.search(r"batch_(\d+)_to_(\d+)", f.name)
        if match:
            s = int(match.group(1))
            e = int(match.group(2))
            ranges.append((s, e))

    return sorted(ranges)

def get_all_ranges(base_path, state, county):

    files = list(base_path.glob(f"{state}_{county}_batch_*_to_*.csv"))

    ranges = []

    for f in files:
        match = re.search(r"batch_(\d+)_to_(\d+)", f.name)
        if match:
            s = int(match.group(1))
            e = int(match.group(2))
            ranges.append((s, e))

    return sorted(ranges)

def get_relevant_ranges(all_ranges, start, end):
    return [
        (s, e)
        for s, e in all_ranges
        if not (e <= start or s >= end)  # overlap
    ]

def find_gaps(ranges, start, end):
    gaps = []

    current = start

    for s, e in sorted(ranges):
        if s > current:
            gaps.append((current, s))
        current = max(current, e)

    if current < end:
        gaps.append((current, end))

    return gaps

def get_resume_point(ranges, start):
    if not ranges:
        return start

    current = start

    for s, e in ranges:
        if s > current:
            break  # gap
        
        current = max(current, e)

    return current

# =========================
# INIT GEE
# =========================
def init_gee(project_id):
    ee.Authenticate()
    ee.Initialize(project=project_id)

# =========================
# CLEAN GEOMETRIES
# =========================
def clean_geometries(gdf):

    # força CRS correto
    if gdf.crs is None:
        raise ValueError("CRS não definido")

    # reprojeta
    gdf = gdf.to_crs(epsg=4326)

    # remove inválidos
    gdf = gdf[gdf.geometry.notnull()]
    gdf = gdf[~gdf.geometry.is_empty]
    gdf = gdf[gdf.is_valid]

    # remove coordenadas absurdas (ESSENCIAL)
    bounds = gdf.bounds
    gdf = gdf[
        (bounds.minx >= -180) &
        (bounds.maxx <= 180) &
        (bounds.miny >= -90) &
        (bounds.maxy <= 90)
    ]

    return gdf
# =========================
# CONVERT TO GEE
# =========================
def to_gee(gdf):
    logging.info("[INFO] Converting to GEE FeatureCollection...")
    return geemap.geopandas_to_ee(gdf)

def get_utm_from_gdf(gdf):
    centroid = gdf.geometry.unary_union.centroid
    lon = centroid.x

    utm_zone = int((lon + 180) / 6) + 1
    epsg = 32600 + utm_zone

    return f"EPSG:{epsg}"

# =========================
# BUILD EVI COLLECTION
# =========================
def mask_clouds_and_shadows(image: 'ee.Image') -> 'ee.Image':
    """
    Mask clouds, shadows, snow, cirrus and adjacent pixels using HLS Fmask (bitmask).

    Bits:
    0: cirrus
    1: cloud
    2: adjacent cloud/shadow
    3: cloud shadow
    4: snow/ice
    5: water
    6-7: aerosol level
    """

    fmask = image.select('Fmask')

    cirrus   = fmask.bitwiseAnd(1 << 0).neq(0)
    cloud    = fmask.bitwiseAnd(1 << 1).neq(0)
    adjacent = fmask.bitwiseAnd(1 << 2).neq(0)
    shadow   = fmask.bitwiseAnd(1 << 3).neq(0)
    snow     = fmask.bitwiseAnd(1 << 4).neq(0)

    mask = cirrus.Not() \
        .And(cloud.Not()) \
        .And(adjacent.Not()) \
        .And(shadow.Not()) \
        .And(snow.Not())

    return image.updateMask(mask)

def get_collection_hlss30(fc, start_date, end_date, cloud_limit) -> 'ee.ImageCollection':

    def rename_bands(image):
        return image.rename(['NIR', 'RED', 'GREEN', 'BLUE'])
    
    def add_satellite_property(image):
        return image.set('collection', 'HLSS30')

    collection_hlss30= ee.ImageCollection('NASA/HLS/HLSS30/v002') \
        .filterBounds(fc)\
        .filterDate(start_date, end_date) \
        .filter(ee.Filter.lte('CLOUD_COVERAGE', cloud_limit)) \
        .map(mask_clouds_and_shadows) \
        .select(['B8', 'B4', 'B3', 'B2'])  \
    
    collection_hlss30 = collection_hlss30.map(rename_bands).map(add_satellite_property)

    return collection_hlss30

def get_collection_hlsl30(fc, start_date, end_date, cloud_limit) -> 'ee.ImageCollection':

    def rename_bands(image):
        return image.rename(['NIR', 'RED', 'GREEN', 'BLUE'])
    
    def add_satellite_property(image):
        return image.set('collection', 'HLSL30')
    
    collection_hlsl30 = ee.ImageCollection('NASA/HLS/HLSL30/v002') \
        .filterBounds(fc)\
        .filterDate(start_date, end_date) \
        .filter(ee.Filter.lte('CLOUD_COVERAGE', cloud_limit)) \
        .map(mask_clouds_and_shadows) \
        .select(['B5', 'B4', 'B3', 'B2'])\
    
    collection_hlsl30 = collection_hlsl30.map(rename_bands).map(add_satellite_property)
    
    return collection_hlsl30 

def add_ndvi(image: 'ee.Image') -> 'ee.Image':
    """
    Add NDVI band to the image.

    Parameters:
    image (ee.Image): The image to add NDVI.

    Returns:
    ee.Image: The image with the NDVI band.
    """
    ndvi = image.normalizedDifference(['NIR', 'RED']).rename('NDVI') 

    return image.addBands(ndvi)

def add_evi(image: 'ee.Image') -> 'ee.Image': 
    """
    Add EVI band to the image.
    '''https://www.usgs.gov/landsat-missions/landsat-enhanced-vegetation-index'''
    Parameters:
    image (ee.Image): The image to add EVI.

    Returns:
    ee.Image: The image with the EVI band.
    """
    evi = image.expression(
            '2.5 * ((NIR - RED) / (NIR + 6 * RED - 7.5 * BLUE + 1))',
            {
                'NIR': image.select('NIR'), 
                'RED': image.select('RED'), 
                'BLUE': image.select('BLUE') 
            }
        ).rename('EVI')
    
    return image.addBands(evi)   

def get_image_collection(fc, start_date, end_date, cloud_limit) -> 'ee.ImageCollection':
    """
    Get the image collection based on the specified product.

    Returns:
    ee.ImageCollection: The filtered and processed image collection.
    """
    hlss30_collection = get_collection_hlss30(fc, start_date, end_date, cloud_limit)
    hlsl30_collection = get_collection_hlsl30(fc, start_date, end_date, cloud_limit)
    image_collection = hlss30_collection.merge(hlsl30_collection)
    image_collection = image_collection.sort('system:time_start')

    #image_collection = image_collection.map(add_ndvi).map(add_evi)
    image_collection = image_collection.map(add_evi)

    return image_collection


def add_band_name(image):
    date = ee.Date(image.get('system:time_start')).format('YYYYMMdd')
    collection = ee.String(image.get('collection'))
    cloud = ee.Number(image.get('CLOUD_COVERAGE')).format('%.0f')

    band_name = ee.String('EVI_') \
        .cat(date) \
        .cat('_') \
        .cat(collection) \
        .cat('_CC') \
        .cat(cloud)

    return image.select('EVI').unmask(-9999).rename(band_name)

def process_batch(batch_gdf, start, output_dir, START_DATE, END_DATE, cc, state, county, base_path, output_root, valid_pixels):
    start_time = time.time()
    batch_size = len(batch_gdf)

    logging.info(f"[INFO] Processing batch {start} → {start + batch_size}")

    if batch_size == 0:
        return
    
    try:
        crs_local = get_utm_from_gdf(batch_gdf)
        logging.info(f"Using CRS: {crs_local}")
        
        fc = geemap.geopandas_to_ee(batch_gdf)
        subset = fc
        #subset = subset.map(lambda f: f.simplify(10))
        
        collection = get_image_collection(subset, START_DATE, END_DATE, cc)
        evi_named = collection.map(add_band_name)
        evi_images = ee.Image(evi_named.toBands())

        evi_ts = evi_images.reduceRegions(
            collection=subset,
            reducer = ee.Reducer.median(),
            scale=30, 
            crs=crs_local, 
            maxPixelsPerRegion=1e7
        )

        df_evi = ee.data.computeFeatures({
            'expression': evi_ts,
            'fileFormat': 'PANDAS_DATAFRAME'
        })

        cols_static = ['geo', 'COUNTYFP', 'Field_ID', 'Field_ID_TEXT',
               'STATEFP', 'area', 'id', 'perimeter', 'tile']
        
        df_evi = df_evi.rename(columns={
            col: f"{col}_median" for col in df_evi.columns if col not in cols_static
        })

        if valid_pixels:
            evi_count = evi_images.reduceRegions(
                collection=subset,
                reducer = ee.Reducer.count(),
                scale=30, 
                crs=crs_local, 
                maxPixelsPerRegion=1e7
            )

            df_count = ee.data.computeFeatures({
                'expression': evi_count,
                'fileFormat': 'PANDAS_DATAFRAME'
            })

            df_count = df_count.rename(columns={
                col: f"{col}_count" for col in df_count.columns if col not in cols_static
            })
            
            df_count = df_count.drop(columns=[col for col in cols_static if col != 'id'])
            df = df_evi.merge(df_count, on='id')
        else:
            df = df_evi

        output_path = Path(
            rf"{base_path}\{state}_{county}_batch_{start}_to_{start + batch_size}.csv"
        )

        df.to_csv(output_path, index=False)
        logging.info(f"[INFO] Saved: {output_path}")
        elapsed1 = time.time() - start_time
        logging.info(f"[TIME] Batch {start} → {start + batch_size} took {elapsed1:.2f} sec ({elapsed1/60:.2f} min)")
        time.sleep(5)
        #transform dataframe    
        start_transform = time.time()
        transform_dataframe(df, output_root)
        elapsed2 = time.time() - start_transform
        logging.info(f"[TIME] Batch {start} → {start + batch_size} transformation took {elapsed2:.2f} sec ({elapsed2/60:.2f} min)")

    except Exception as e:
        error_msg = str(e)

        logging.error(f"Failed batch {start} → {start + batch_size}: {error_msg}") 
               
        log_failure(state, county, start, batch_size, error_msg)

        if batch_size <= 1:
            logging.error(f"[FAILED] Cannot split further: {start}")
            log_failure(state, county, start, batch_size, error_msg)
            return
      
        logging.info(f"[FALLBACK] Smaller batch {start}")

        half = batch_size // 2
        
        batch1 = batch_gdf.iloc[:half]
        batch2 = batch_gdf.iloc[half:]
        
        process_batch(batch1, start, output_dir,
                      START_DATE, END_DATE, cc, state, county, base_path, output_root, valid_pixels)

        process_batch(batch2, start + half, output_dir,
                      START_DATE, END_DATE, cc, state, county, base_path, output_root, valid_pixels)



