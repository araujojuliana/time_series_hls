# 🌍 Time Series Extraction Pipeline (HLS - EVI/NDVI)

This repository contains a scalable pipeline to extract vegetation index time series (EVI/NDVI) from HLS imagery using Google Earth Engine (GEE).

The workflow processes field geometries per state → county → batch. 

---

# ⚙️ Environment Setup

## 1. Create Conda Environment

```bash
conda create -n gee_ts python=3.11
conda activate gee_ts
```

## 2. Install Dependencies

Using pip:

```bash
pip install geopandas pyogrio shapely fiona rasterio pandas numpy tqdm pyyaml earthengine-api geemap duckdb python-dotenv
```

Or using environment file:

```bash
conda env create -f environment.yml
conda activate gee_ts
```

---

# 🔐 Google Earth Engine Authentication

Authenticate your account:

```bash
earthengine authenticate
```

Then initialize inside Python (already handled in code):

```python
init_gee(project_id)
```

---

# 📁 Project Structure

```
time_series_hls/
│
├── main_batch.py              # Main pipeline script
├── helper.py                 # Processing functions (GEE, batching, retries)
├── transform_dataframe.py    # Post-processing of time series
├── conf_batch.yaml           # Configuration file
│
├── log/
│   └── processing.log
│
├── output/
│   └── {year}/{state}/{county}/
│
└── README.md
```

---

# ⚙️ Configuration

Edit the file:

```
conf_batch.yaml
```

Example:

```yaml
project_id: ee-your-project-id

input_path: C:\path\to\fields_states_all
output_dir: C:\path\to\output_raw
output_postprocess: C:\path\to\output_processed

statefp: ["19"]

year: 2024
batch_size: 50

start_date: "2024-01-01"
end_date: "2025-01-01"

cc: 50
valid_pixels: True
```

---

# ▶️ Running the Pipeline

```bash
python main_batch.py
```

---

# 🔁 Pipeline Logic

For each:

* State
* County
* Batch of fields

The pipeline:

1. Checks already processed ranges
2. Identifies missing intervals (gaps)
3. Processes only missing data
4. Saves outputs incrementally

---

# 📊 Output Structure

Raw outputs:

```
output/{year}/{state}/{county}/
```

Processed outputs:

```
output_processed/
```

Each field generates a time series CSV.

---

# 🚀 Performance Tips

### Batch Size

Recommended:

```yaml
batch_size: 200-500
```

Large batches may cause:

* GEE timeouts
* Memory errors

---

### Retry Strategy (Recommended)

The pipeline supports retry logic for:

* GEE failures
* Network issues

---

### Resume Capability

If interrupted, the pipeline:

✅ Detects processed batches
✅ Skips completed data
✅ Continues from missing gaps


---

# 👩‍💻 Author

Juliana Araujo
PhD Student – Biosystems Engineering
Remote Sensing & Geospatial Data Science

---
