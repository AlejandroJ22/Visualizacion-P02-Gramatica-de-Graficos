import pandas as pd
import geopandas as gpd

df = pd.read_csv("data/rentamedia-sc-3.csv")

gdf = gpd.read_file("data/cartografia-secciones/secciones_20220101_tenerife.json")

# filtrar 2021
df_2021 = df[df["año"] == 2021]

# VER TIPOS DE MÉTRICAS
print(df_2021["MEDIDAS_CODE"].unique())

# elegir una métrica concreta (ejemplo)
df_2021_renta = df_2021[df_2021["MEDIDAS_CODE"] == df_2021["MEDIDAS_CODE"].unique()[0]]

merged = gdf.merge(
    df_2021_renta, left_on="geocode", right_on="TERRITORIO_CODE", how="left"
)

print(merged[["geocode", "OBS_VALUE"]].head())
