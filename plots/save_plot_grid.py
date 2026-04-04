#%%
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

# =========================
# CONFIG
# =========================
fields_info = [
    ("001", "19001002004"),
    ("089", "19089002213"),
    ("099", "19099002773"),
    ("117", "19117000224"),
    ("161", "19161000418"),
    ("163", "19163000468"),
]

# =========================
# CREATE SUBPLOTS (3x2)
# =========================
fig = make_subplots(
    rows=2, cols=3,
    subplot_titles=[f"Field {f}" for _, f in fields_info]
)

EVI_COLOR = "#1f77b4"   # azul
NDVI_COLOR = "#ff7f0e"  # laranja

# =========================
# LOOP
# =========================
for i, (county, field) in enumerate(fields_info):

    row = i // 3 + 1
    col = i % 3 + 1

    # paths
    evi_path = rf"C:\evi_output_raw\2024\19\{county}\{field}.csv"
    ndvi_path = rf"C:\ndvi_output_raw\2024\19\{county}\{field}.csv"

    # load
    df_evi = pd.read_csv(evi_path)
    df_ndvi = pd.read_csv(ndvi_path)

    # format
    df_evi["date"] = pd.to_datetime(df_evi["date"], errors="coerce")
    df_ndvi["date"] = pd.to_datetime(df_ndvi["date"], errors="coerce")

    df_evi = df_evi.dropna(subset=["date"])[["date", "EVI"]]
    df_ndvi = df_ndvi.dropna(subset=["date"])[["date", "NDVI"]]

    df = pd.merge(df_evi, df_ndvi, on="date", how="inner")
    df = df.sort_values("date")

    # =========================
    # ADD TRACES
    # =========================
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["EVI"],
            mode="markers",
            name="EVI",
            marker=dict(size=5, color=EVI_COLOR),
            showlegend=(i == 0)
        ),
        row=row, col=col
    )

    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["NDVI"],
            mode="markers",
            name="NDVI",
            marker=dict(size=5, color=NDVI_COLOR),
            showlegend=(i == 0)
        ),
        row=row, col=col
    )

# =========================
# GLOBAL LAYOUT
# =========================
fig.update_layout(
    height=700,
    width=1200,
    title="EVI vs NDVI Time Series (2024)",
    template="simple_white",
    hovermode="closest"
)

# =========================
# FORMAT ALL AXES
# =========================
for i in range(1, 7):
    fig.update_xaxes(
        tickformat="%b %d",
        dtick=15 * 24 * 60 * 60 * 1000,
        range=["2024-01-01", "2024-12-31"],
        row=(i-1)//3 + 1,
        col=(i-1)%3 + 1
    )

# =========================
# SHOW
# =========================
fig.show()

# =========================
# SAVE
# =========================
output_path = r"C:\Users\jd2725\Documents\evi_ndvi_grid.html"
fig.write_html(output_path, auto_open=True)

print(f"Saved: {output_path}")
# %%
