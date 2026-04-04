#%%
import pandas as pd
import plotly.graph_objects as go

# =========================
# PATHS
# =========================
field = '19163000468'
county = '163'
evi_path = rf"C:\evi_output_raw\2024\19\{county}\{field}.csv"
ndvi_path = rf"C:\ndvi_output_raw\2024\19\{county}\{field}.csv"

# =========================
# LOAD
# =========================
df_evi = pd.read_csv(evi_path)
df_ndvi = pd.read_csv(ndvi_path)

# =========================
# FORMAT DATE
# =========================
df_evi["date"] = pd.to_datetime(df_evi["date"], errors="coerce")
df_ndvi["date"] = pd.to_datetime(df_ndvi["date"], errors="coerce")

# remove datas inválidas, se houver
df_evi = df_evi.dropna(subset=["date"])
df_ndvi = df_ndvi.dropna(subset=["date"])

# =========================
# KEEP ONLY NEEDED COLUMNS
# =========================
df_evi = df_evi[["date", "EVI"]].copy()
df_ndvi = df_ndvi[["date", "NDVI"]].copy()

# =========================
# MERGE + SORT
# =========================
df = pd.merge(df_evi, df_ndvi, on="date", how="inner")
df = df.sort_values("date").reset_index(drop=True)

# checagem opcional
print(df[["date", "EVI", "NDVI"]].head())
print(df[["date", "EVI", "NDVI"]].tail())

# =========================
# PLOT
# =========================
fig = go.Figure()

fig.add_trace(go.Scatter(
    x=df["date"],
    y=df["EVI"],
    mode="markers",
    name="EVI",
    marker=dict(size=6, opacity=0.8),
    hovertemplate="Date: %{x|%Y-%m-%d}<br>EVI: %{y:.4f}<extra></extra>"
))

fig.add_trace(go.Scatter(
    x=df["date"],
    y=df["NDVI"],
    mode="markers",
    name="NDVI",
    marker=dict(size=6, opacity=0.8),
    hovertemplate="Date: %{x|%Y-%m-%d}<br>NDVI: %{y:.4f}<extra></extra>"
))

# =========================
# DEFINE FIXED DATE RANGE
# =========================
start_date = pd.Timestamp("2024-01-01")
end_date = pd.Timestamp("2024-12-31")

# =========================
# LAYOUT
# =========================
fig.update_layout(
    title="EVI vs NDVI Time Series (2024)",
    xaxis_title="Date",
    yaxis_title="Vegetation Index",
    template="simple_white",
    hovermode="x unified",
    xaxis=dict(
        type="date",
        range=[start_date, end_date],
        tick0="2024-01-01",
        dtick=15 * 24 * 60 * 60 * 1000,  # 15 dias
        tickformat="%b %d"
    )
)

fig.show()

# =========================
# SAVE HTML
# =========================
output_path = rf"C:\evi_ndvi_plot\{field}.html"
fig.write_html(output_path, auto_open=True)

print(f"Plot saved at: {output_path}")
# %%
