#%%
import pandas as pd
import re
import os
import glob
import logging

#%%
import pandas as pd
import re
import os
import logging

def transform_dataframe(df, output_root):
    logging.info(f"[INFO] Transforming dataframe with {len(df)} rows and {len(df.columns)} columns")

    # =========================
    # CLEAN
    # =========================
    df = df.drop(columns=[col for col in df.columns if 'geo' in col.lower()], errors='ignore')

    df = df.loc[:, ~(df.astype(str).apply(lambda col: col.str.strip().eq('').all()))]

    # =========================
    # FIND COLUMNS
    # =========================
    evi_cols = [col for col in df.columns if "_median" in col]
    count_cols = [col for col in df.columns if "_count" in col]
    has_count = len(count_cols) > 0

    logging.info(f"[INFO] NDVI columns found: {len(evi_cols)}")
    logging.info(f"[INFO] Count columns found: {has_count}")

    # =========================
    # LOOP PER FIELD
    # =========================
    for idx, row in df.iterrows():

        field_id = row.get("Field_ID_TEXT", f"field_{idx}")
        state = row.get("STATEFP", "unknown")
        county = row.get("COUNTYFP", "unknown")

        records = []

        for col in evi_cols:
            match = re.search(r"NDVI_(\d{8})_(HLSS30|HLSL30)_CC(\d+)_median", col)
            col_count = col.replace("_median", "_count")

            if match:
                date_str, collection, cc = match.groups()
                value = row[col]

                record = {
                    "date": pd.to_datetime(date_str, format="%Y%m%d"),
                    "field_id": field_id,
                    "collection": collection,
                    #"CC_image": int(cc),
                    "NDVI": value
                }

                # valid pixels (optional)
                if has_count and col_count in df.columns:
                    record["valid_pixels"] = row.get(col_count, pd.NA)
                else:
                    record["valid_pixels"] = pd.NA

                records.append(record)

        # =========================
        # CREATE LONG DF
        # =========================
        df_long = pd.DataFrame(records)

        if df_long.empty:
            continue

        # clean NDVI
        df_long["NDVI"] = df_long["NDVI"].replace(-9999, pd.NA)
        df_long = df_long.dropna(subset=["NDVI"])

        if df_long.empty:
            continue

        # =========================
        # AGGREGATE SAME DAY
        # =========================
        df_long = (
            df_long
            .groupby("date", as_index=False)
            .agg({
                "NDVI": "mean",
                "valid_pixels": "mean"
            })
        )

        # =========================
        # FULL YEAR CALENDAR
        # =========================
        year = df_long["date"].dt.year.iloc[0]

        full_dates = pd.date_range(
            start=f"{year}-01-01",
            end=f"{year}-12-31",
            freq="D"
        )

        df_full = pd.DataFrame({"date": full_dates})

        df_long = df_full.merge(df_long, on="date", how="left")

        # =========================
        # ADD METADATA BACK
        # =========================
        df_long["field_id"] = field_id
        df_long["state"] = state
        df_long["county"] = county

        df_long = df_long.sort_values("date")

        # =========================
        # OUTPUT PATH
        # =========================
        output_folder = os.path.join(
            output_root,
            str(year),
            str(state),
            str(county)
        )

        os.makedirs(output_folder, exist_ok=True)

        output_path = os.path.join(
            output_folder,
            f"{field_id}.csv"
        )

        # =========================
        # SAVE
        # =========================
        df_long.to_csv(output_path, index=False)

    logging.info(f"[INFO] Saved {output_path}")

