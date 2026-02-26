"""Checks de calidad para el pipeline de renta en Dagster."""

import pandas as pd
from dagster import AssetCheckResult, MetadataValue, asset_check

from viz_dagster.renta_assets import COLORES_RENTA


@asset_check(asset="carga_renta_raw")
def check_nulos_renta(carga_renta_raw: pd.DataFrame) -> AssetCheckResult:
    cols_clave = ["OBS_VALUE", "TIME_PERIOD_CODE", "TERRITORIO#es", "MEDIDAS#es"]
    nulos_por_columna = carga_renta_raw[cols_clave].isna().sum()
    total_nulos = int(nulos_por_columna.sum())
    columnas_afectadas = [col for col, n in nulos_por_columna.items() if n > 0]

    return AssetCheckResult(
        passed=total_nulos == 0,
        metadata={
            "porcentaje_nulos": MetadataValue.float(
                float((total_nulos / (len(carga_renta_raw) * len(cols_clave))) * 100)
                if len(carga_renta_raw) > 0
                else 0.0
            ),
            "columnas_afectadas": MetadataValue.json(columnas_afectadas),
            "nulos_totales": MetadataValue.int(total_nulos),
        },
    )


@asset_check(asset="carga_renta_raw")
def check_rango_porcentajes(carga_renta_raw: pd.DataFrame) -> AssetCheckResult:
    valores = pd.to_numeric(carga_renta_raw["OBS_VALUE"], errors="coerce")
    fuera_rango = ~(valores.between(0, 100) & valores.notna())

    min_value = float(valores.min()) if valores.notna().any() else float("nan")
    max_value = float(valores.max()) if valores.notna().any() else float("nan")

    return AssetCheckResult(
        passed=not bool(fuera_rango.any()),
        metadata={
            "min_value": MetadataValue.float(min_value),
            "max_value": MetadataValue.float(max_value),
            "filas_fuera_rango": MetadataValue.int(int(fuera_rango.sum())),
        },
    )


@asset_check(asset="limpieza_renta")
def check_cardinalidad_medidas(limpieza_renta: pd.DataFrame) -> AssetCheckResult:
    n_medidas = int(limpieza_renta["MEDIDAS#es"].nunique())

    return AssetCheckResult(
        passed=n_medidas == 5,
        metadata={
            "n_medidas_detectadas": MetadataValue.int(n_medidas),
            "n_medidas_esperadas": MetadataValue.int(5),
        },
    )


@asset_check(asset="renta_canarias_temporal")
def check_continuidad_temporal(
    renta_canarias_temporal: pd.DataFrame,
) -> AssetCheckResult:
    anios_detectados = sorted(
        renta_canarias_temporal["TIME_PERIOD_CODE"].unique().tolist()
    )
    anios_esperados = list(range(2015, 2024))
    anios_faltantes = sorted(list(set(anios_esperados) - set(anios_detectados)))

    return AssetCheckResult(
        passed=anios_detectados == anios_esperados,
        metadata={
            "anios_detectados": MetadataValue.json(anios_detectados),
            "anios_faltantes": MetadataValue.json(anios_faltantes),
        },
    )


@asset_check(asset="union_renta_codislas")
def check_union_codislas(union_renta_codislas: pd.DataFrame) -> AssetCheckResult:
    filas_sin_isla = int(union_renta_codislas["ISLA_CODISLAS"].isna().sum())

    return AssetCheckResult(
        passed=filas_sin_isla == 0,
        metadata={"filas_sin_isla": MetadataValue.int(filas_sin_isla)},
    )


@asset_check(asset="union_renta_codislas")
def check_eje_cero_barras(
    union_renta_codislas: pd.DataFrame,
) -> AssetCheckResult:
    limite_inferior_eje = 0.0
    min_obs = float(union_renta_codislas["OBS_VALUE"].min())

    return AssetCheckResult(
        passed=min_obs >= 0,
        metadata={
            "limite_inferior_eje": MetadataValue.float(limite_inferior_eje),
            "min_obs_value": MetadataValue.float(min_obs),
        },
    )


@asset_check(asset="renta_canarias_temporal")
def check_consistencia_color(
    renta_canarias_temporal: pd.DataFrame,
) -> AssetCheckResult:
    categorias_detectadas = sorted(
        renta_canarias_temporal["MEDIDAS#es"].unique().tolist()
    )
    categorias_palette = sorted(list(COLORES_RENTA.keys()))

    return AssetCheckResult(
        passed=set(categorias_detectadas) == set(categorias_palette),
        metadata={
            "categorias_detectadas": MetadataValue.json(categorias_detectadas),
            "palette_id": MetadataValue.text("COLORES_RENTA"),
        },
    )


@asset_check(asset="union_renta_codislas")
def check_orden_municipios(
    union_renta_codislas: pd.DataFrame,
) -> AssetCheckResult:
    sueldos = union_renta_codislas[
        union_renta_codislas["MEDIDAS#es"] == "Sueldos y salarios"
    ].copy()
    sueldos["municipio_nombre"] = sueldos["NOMBRE_CODISLAS"].fillna(
        sueldos["TERRITORIO#es"]
    )
    ordenados = sueldos.sort_values("OBS_VALUE", ascending=False)
    valores_ordenados = ordenados["OBS_VALUE"].tolist()
    is_sorted = valores_ordenados == sorted(valores_ordenados, reverse=True)
    n_municipios = int(ordenados["municipio_nombre"].nunique())

    return AssetCheckResult(
        passed=is_sorted,
        metadata={
            "is_sorted": MetadataValue.bool(is_sorted),
            "n_municipios": MetadataValue.int(n_municipios),
            "sugerencia": MetadataValue.text(
                "Ordenar municipios por OBS_VALUE descendente para 'Sueldos y salarios'."
            ),
        },
    )
