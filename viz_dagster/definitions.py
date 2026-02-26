from dagster import (
    Definitions,
    load_assets_from_modules,
    load_asset_checks_from_modules,
)

from viz_dagster import renta_assets, renta_checks

defs = Definitions(
    assets=load_assets_from_modules([renta_assets]),
    asset_checks=load_asset_checks_from_modules([renta_checks]),
)
