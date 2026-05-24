import geopandas as gpd

gdf = gpd.read_file("data/cartografia-secciones/secciones_20220101_tenerife.json")

print(gdf.columns.tolist())
print(gdf.head(3))
