import geopandas as gpd

gdf = gpd.read_file(
    "data/cartografia-secciones/secciones_20220101_tenerife.json"
)  # o usar el asset materializado
g = gdf.copy()  # o renta_secciones_geo_2023 desde Dagster si ya lo materializaste

# 1) ¿Vacío?
print("empty:", g.empty)

# 2) geometrías nulas/vacías/invalidas
print("null geometries:", g.geometry.isna().sum())
print("empty geometries:", g.geometry.is_empty.sum())
print("invalid geometries:", (~g.geometry.is_valid).sum())

# 3) bounds y CRS
print("crs:", g.crs)
print("total_bounds:", g.total_bounds)  # [minx, miny, maxx, maxy]

# 4) ejemplos de centroids / valores Y problemáticos
centroids = g.geometry.centroid
print("centroid y nulls:", centroids.y.isna().sum())
print("min/max y:", centroids.y.min(), centroids.y.max())

# 5) revisar OBS_VALUE en el GeoDF que estás pintando
print(
    "OBS_VALUE nulls:",
    g["OBS_VALUE"].isna().sum() if "OBS_VALUE" in g.columns else "no column",
)
