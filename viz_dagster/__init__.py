"""
Definiciones de Dagster para el proyecto de visualización de rentas en Canarias
"""

from dagster import Definitions, load_assets_from_modules

from . import renta_assets, test_asset, template_ia, geo_assets, geo_checks

# Cargar todos los assets
all_assets = load_assets_from_modules(
    [test_asset, renta_assets, template_ia, geo_assets]
)

# Definir las definiciones del proyecto
defs = Definitions(
    assets=all_assets,
)
