from dagster import (
    Definitions,
    load_assets_from_modules,
    load_asset_checks_from_modules,
)

from viz_dagster import (
    renta_assets,
    renta_checks,
    template_ia,
    template_ia_checks,
    geo_assets,
    geo_checks,
)
from viz_dagster.sensors import sensor_nuevos_datos, data_sync_job

defs = Definitions(
    assets=load_assets_from_modules([renta_assets, template_ia, geo_assets]),
    asset_checks=load_asset_checks_from_modules(
        [renta_checks, template_ia_checks, geo_checks]
    ),
    jobs=[data_sync_job],
    sensors=[sensor_nuevos_datos],
)
