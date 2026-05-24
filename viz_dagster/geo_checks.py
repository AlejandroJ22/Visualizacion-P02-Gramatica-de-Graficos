"""Checks de calidad para datasets geoespaciales y de renta por secciones.

Contiene checks tanto de "datos" como de "visualización" (PNG/Mapas).
"""

import os
from pathlib import Path
from typing import Tuple

import pandas as pd
from dagster import AssetCheckResult, AssetIn, AssetKey, MetadataValue, asset_check


def _geom_duplicates_count(gdf) -> int:
    # usare WKB para comparar geometrías (estable y serializable)
    try:
        wkbs = gdf.geometry.apply(lambda g: g.wkb if g is not None else None)
    except Exception:
        # fallback a string representation
        wkbs = gdf.geometry.astype(str)
    return int(wkbs.duplicated().sum())


def _coverage_stats(gdf) -> Tuple[int, int, float]:
    total = int(len(gdf))
    matched = int(gdf["OBS_VALUE"].notna().sum())
    pct = float(matched / total * 100) if total > 0 else 0.0
    return total, matched, pct


def _strip_date_prefix(code: str) -> str:
    parts = str(code).split("_")
    return "_".join(parts[1:]) if len(parts) > 1 else str(code)


def _file_info(path_like) -> dict:
    p = Path(path_like) if path_like is not None else None
    if p is None:
        return {"exists": False, "size": 0}
    try:
        return {
            "exists": p.exists(),
            "size": int(p.stat().st_size) if p.exists() else 0,
        }
    except Exception:
        return {"exists": False, "size": 0}


def _mask_tenerife_actividad(df: pd.DataFrame) -> pd.Series:
    if "cod_provincia" in df.columns:
        return df["cod_provincia"].astype(str).str.startswith("38")
    if "provincia" in df.columns:
        return (
            df["provincia"]
            .astype(str)
            .str.contains("santa cruz|tenerife", case=False, na=False)
        )
    if "geocode" in df.columns:
        return df["geocode"].astype(str).str.contains("_38", na=False)
    return pd.Series([False] * len(df))


def _mask_tenerife_ocupacion(df: pd.DataFrame) -> pd.Series:
    if "code_municipio" in df.columns:
        return df["code_municipio"].astype(str).str.startswith("38")
    if "geocode" in df.columns:
        return df["geocode"].astype(str).str.contains("_38", na=False)
    return pd.Series([False] * len(df))


def _filter_by_geo_codes(df: pd.DataFrame, geo_codes: set) -> pd.DataFrame:
    if "geocode" not in df.columns or not geo_codes:
        return df.head(0)
    geo_norm = set(_strip_date_prefix(c) for c in geo_codes)
    df_codes = df["geocode"].dropna().astype(str)
    df_norm = df_codes.apply(_strip_date_prefix)
    mask = df_norm.isin(geo_norm)
    return df.loc[mask].copy()


def _sample_codes(values: set, limit: int = 5) -> list:
    if not values:
        return []
    return list(sorted(values))[:limit]


@asset_check(asset="renta_secciones_2021")
def check_renta_secciones_obs_value_no_nulos(
    renta_secciones_2021: pd.DataFrame,
) -> AssetCheckResult:
    nulos = int(renta_secciones_2021["OBS_VALUE"].isna().sum())
    return AssetCheckResult(
        passed=nulos == 0,
        metadata={"nulos_obs_value": MetadataValue.int(nulos)},
    )


@asset_check(asset="renta_secciones_2021")
def check_renta_secciones_territorio_code_no_vacio(
    renta_secciones_2021: pd.DataFrame,
) -> AssetCheckResult:
    codigos = renta_secciones_2021["TERRITORIO_CODE"].astype(str)
    vacios = int(codigos.str.strip().eq("").sum())
    return AssetCheckResult(
        passed=vacios == 0,
        metadata={"territorio_code_vacios": MetadataValue.int(vacios)},
    )


@asset_check(asset="renta_secciones_2021")
def check_renta_secciones_anio_2021(
    renta_secciones_2021: pd.DataFrame,
) -> AssetCheckResult:
    anios = sorted(renta_secciones_2021["año"].dropna().unique().tolist())
    passed = anios == [2021]
    return AssetCheckResult(
        passed=passed,
        metadata={"anios_detectados": MetadataValue.json(anios)},
    )


@asset_check(asset="renta_secciones_2021")
def check_renta_secciones_metrica_correcta(
    renta_secciones_2021: pd.DataFrame,
) -> AssetCheckResult:
    metricas = sorted(renta_secciones_2021["MEDIDAS_CODE"].dropna().unique().tolist())
    passed = metricas == ["RENTA_BRUTA_MEDIA_HOGAR"]
    return AssetCheckResult(
        passed=passed,
        metadata={"metricas_detectadas": MetadataValue.json(metricas)},
    )


@asset_check(asset="renta_secciones_geo_2021")
def check_geo_missing_obs_value(
    renta_secciones_geo_2021: pd.DataFrame,
) -> AssetCheckResult:
    """Porcentaje de geometrías sin OBS_VALUE tras el merge. Pasa si no hay nulos."""
    total = int(len(renta_secciones_geo_2021))
    n_nulos = int(renta_secciones_geo_2021["OBS_VALUE"].isna().sum())
    pct = float(n_nulos / total * 100) if total > 0 else 0.0
    passed = n_nulos == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "total_geometrias": MetadataValue.int(total),
            "nulos_obs_value": MetadataValue.int(n_nulos),
            "pct_nulos": MetadataValue.float(pct),
        },
    )


@asset_check(asset="renta_secciones_geo_2021")
def check_geo_geocode_correspondencia(
    renta_secciones_geo_2021: pd.DataFrame,
) -> AssetCheckResult:
    """Comprueba el grado de correspondencia entre `geocode` y datos de renta.

    Pasa si la cobertura >= 90%.
    """
    total, matched, pct = _coverage_stats(renta_secciones_geo_2021)
    passed = pct >= 90.0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "total_geometrias": MetadataValue.int(total),
            "geometrias_con_renta": MetadataValue.int(matched),
            "pct_cobertura": MetadataValue.float(pct),
        },
    )


@asset_check(asset="renta_secciones_geo_2021")
def check_geo_no_geometrias_duplicadas(
    renta_secciones_geo_2021: pd.DataFrame,
) -> AssetCheckResult:
    """Comprueba que no haya geometrías duplicadas tras el merge."""
    dup = _geom_duplicates_count(renta_secciones_geo_2021)
    passed = dup == 0
    return AssetCheckResult(
        passed=passed,
        metadata={"geometrias_duplicadas": MetadataValue.int(dup)},
    )


@asset_check(asset="renta_secciones_geo_2021")
def check_geo_obs_value_positivo(
    renta_secciones_geo_2021: pd.DataFrame,
) -> AssetCheckResult:
    """Comprueba que `OBS_VALUE` sea mayor que 0 en la mayoría de casos (pasa si no hay <=0)."""
    nonpos = int((renta_secciones_geo_2021["OBS_VALUE"] <= 0).sum())
    total = int(len(renta_secciones_geo_2021))
    passed = nonpos == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "total_geometrias": MetadataValue.int(total),
            "obs_value_nonpositive": MetadataValue.int(nonpos),
        },
    )


# Repetir mismos checks para 2023 (misma lógica)
@asset_check(asset="renta_secciones_geo_2023")
def check_geo_missing_obs_value_2023(
    renta_secciones_geo_2023: pd.DataFrame,
) -> AssetCheckResult:
    total = int(len(renta_secciones_geo_2023))
    n_nulos = int(renta_secciones_geo_2023["OBS_VALUE"].isna().sum())
    pct = float(n_nulos / total * 100) if total > 0 else 0.0
    passed = n_nulos == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "total_geometrias": MetadataValue.int(total),
            "nulos_obs_value": MetadataValue.int(n_nulos),
            "pct_nulos": MetadataValue.float(pct),
        },
    )


@asset_check(asset="renta_secciones_geo_2023")
def check_geo_geocode_correspondencia_2023(
    renta_secciones_geo_2023: pd.DataFrame,
) -> AssetCheckResult:
    total, matched, pct = _coverage_stats(renta_secciones_geo_2023)
    passed = pct >= 90.0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "total_geometrias": MetadataValue.int(total),
            "geometrias_con_renta": MetadataValue.int(matched),
            "pct_cobertura": MetadataValue.float(pct),
        },
    )


@asset_check(asset="renta_secciones_geo_2023")
def check_geo_no_geometrias_duplicadas_2023(
    renta_secciones_geo_2023: pd.DataFrame,
) -> AssetCheckResult:
    dup = _geom_duplicates_count(renta_secciones_geo_2023)
    passed = dup == 0
    return AssetCheckResult(
        passed=passed,
        metadata={"geometrias_duplicadas": MetadataValue.int(dup)},
    )


@asset_check(asset="renta_secciones_geo_2023")
def check_geo_obs_value_positivo_2023(
    renta_secciones_geo_2023: pd.DataFrame,
) -> AssetCheckResult:
    nonpos = int((renta_secciones_geo_2023["OBS_VALUE"] <= 0).sum())
    total = int(len(renta_secciones_geo_2023))
    passed = nonpos == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "total_geometrias": MetadataValue.int(total),
            "obs_value_nonpositive": MetadataValue.int(nonpos),
        },
    )


# -----------------------
# Visual checks (PNG existence + mapas no vacíos)
# -----------------------


@asset_check(
    asset="mapa_renta_secciones_2021",
    additional_ins={
        "renta_secciones_geo_2021": AssetIn(key=AssetKey("renta_secciones_geo_2021"))
    },
)
def check_mapa_renta_2021_exists_and_geo(
    mapa_renta_secciones_2021: str, renta_secciones_geo_2021: pd.DataFrame
) -> AssetCheckResult:
    info = _file_info(mapa_renta_secciones_2021)
    geo_rows = (
        int(len(renta_secciones_geo_2021))
        if renta_secciones_geo_2021 is not None
        else 0
    )
    geom_nulls = (
        int(renta_secciones_geo_2021.geometry.isna().sum())
        if (
            renta_secciones_geo_2021 is not None
            and "geometry" in renta_secciones_geo_2021.columns
        )
        else 0
    )
    passed = (
        bool(info.get("exists", False))
        and info.get("size", 0) > 0
        and geo_rows > 0
        and geom_nulls == 0
    )
    return AssetCheckResult(
        passed=passed,
        metadata={
            "path": MetadataValue.path(
                str(mapa_renta_secciones_2021)
                if mapa_renta_secciones_2021 is not None
                else ""
            ),
            "exists": MetadataValue.bool(info.get("exists", False)),
            "size_bytes": MetadataValue.int(info.get("size", 0)),
            "geo_rows": MetadataValue.int(geo_rows),
            "geo_null_geometries": MetadataValue.int(geom_nulls),
        },
    )


@asset_check(
    asset="mapa_renta_secciones_2023",
    additional_ins={
        "renta_secciones_geo_2023": AssetIn(key=AssetKey("renta_secciones_geo_2023"))
    },
)
def check_mapa_renta_2023_exists_and_geo(
    mapa_renta_secciones_2023: str, renta_secciones_geo_2023: pd.DataFrame
) -> AssetCheckResult:
    info = _file_info(mapa_renta_secciones_2023)
    geo_rows = (
        int(len(renta_secciones_geo_2023))
        if renta_secciones_geo_2023 is not None
        else 0
    )
    geom_nulls = (
        int(renta_secciones_geo_2023.geometry.isna().sum())
        if (
            renta_secciones_geo_2023 is not None
            and "geometry" in renta_secciones_geo_2023.columns
        )
        else 0
    )
    passed = (
        bool(info.get("exists", False))
        and info.get("size", 0) > 0
        and geo_rows > 0
        and geom_nulls == 0
    )
    return AssetCheckResult(
        passed=passed,
        metadata={
            "path": MetadataValue.path(
                str(mapa_renta_secciones_2023)
                if mapa_renta_secciones_2023 is not None
                else ""
            ),
            "exists": MetadataValue.bool(info.get("exists", False)),
            "size_bytes": MetadataValue.int(info.get("size", 0)),
            "geo_rows": MetadataValue.int(geo_rows),
            "geo_null_geometries": MetadataValue.int(geom_nulls),
        },
    )


@asset_check(
    asset="mapa_variacion_renta_2021_2023",
    additional_ins={
        "renta_variacion_geo_2021_2023": AssetIn(
            key=AssetKey("renta_variacion_geo_2021_2023")
        )
    },
)
def check_mapa_variacion_exists_and_geo(
    mapa_variacion_renta_2021_2023: str, renta_variacion_geo_2021_2023: pd.DataFrame
) -> AssetCheckResult:
    info = _file_info(mapa_variacion_renta_2021_2023)
    geo_rows = (
        int(len(renta_variacion_geo_2021_2023))
        if renta_variacion_geo_2021_2023 is not None
        else 0
    )
    geom_nulls = (
        int(renta_variacion_geo_2021_2023.geometry.isna().sum())
        if (
            renta_variacion_geo_2021_2023 is not None
            and "geometry" in renta_variacion_geo_2021_2023.columns
        )
        else 0
    )
    passed = (
        bool(info.get("exists", False))
        and info.get("size", 0) > 0
        and geo_rows > 0
        and geom_nulls == 0
    )
    return AssetCheckResult(
        passed=passed,
        metadata={
            "path": MetadataValue.path(
                str(mapa_variacion_renta_2021_2023)
                if mapa_variacion_renta_2021_2023 is not None
                else ""
            ),
            "exists": MetadataValue.bool(info.get("exists", False)),
            "size_bytes": MetadataValue.int(info.get("size", 0)),
            "geo_rows": MetadataValue.int(geo_rows),
            "geo_null_geometries": MetadataValue.int(geom_nulls),
        },
    )


@asset_check(
    asset="mapa_relacion_renta_servicios_2021_2023",
    additional_ins={
        "renta_secciones_geo_2021": AssetIn(key=AssetKey("renta_secciones_geo_2021")),
        "renta_secciones_geo_2023": AssetIn(key=AssetKey("renta_secciones_geo_2023")),
        "actividad_secciones": AssetIn(key=AssetKey("actividad_secciones")),
    },
)
def check_mapa_renta_servicios_exists_and_inputs(
    mapa_relacion_renta_servicios_2021_2023: str,
    renta_secciones_geo_2021: pd.DataFrame,
    renta_secciones_geo_2023: pd.DataFrame,
    actividad_secciones: pd.DataFrame,
) -> AssetCheckResult:
    info = _file_info(mapa_relacion_renta_servicios_2021_2023)
    r1 = (
        int(len(renta_secciones_geo_2021))
        if renta_secciones_geo_2021 is not None
        else 0
    )
    r2 = (
        int(len(renta_secciones_geo_2023))
        if renta_secciones_geo_2023 is not None
        else 0
    )
    a = int(len(actividad_secciones)) if actividad_secciones is not None else 0
    passed = (
        bool(info.get("exists", False))
        and info.get("size", 0) > 0
        and r1 > 0
        and r2 > 0
        and a > 0
    )
    return AssetCheckResult(
        passed=passed,
        metadata={
            "path": MetadataValue.path(
                str(mapa_relacion_renta_servicios_2021_2023)
                if mapa_relacion_renta_servicios_2021_2023 is not None
                else ""
            ),
            "exists": MetadataValue.bool(info.get("exists", False)),
            "size_bytes": MetadataValue.int(info.get("size", 0)),
            "renta_2021_rows": MetadataValue.int(r1),
            "renta_2023_rows": MetadataValue.int(r2),
            "actividad_rows": MetadataValue.int(a),
        },
    )


@asset_check(
    asset="mapa_relacion_renta_ocupacion_alta_2021_2023",
    additional_ins={
        "renta_secciones_geo_2021": AssetIn(key=AssetKey("renta_secciones_geo_2021")),
        "renta_secciones_geo_2023": AssetIn(key=AssetKey("renta_secciones_geo_2023")),
        "ocupacion_secciones": AssetIn(key=AssetKey("ocupacion_secciones")),
    },
)
def check_mapa_renta_ocupacion_exists_and_inputs(
    mapa_relacion_renta_ocupacion_alta_2021_2023: str,
    renta_secciones_geo_2021: pd.DataFrame,
    renta_secciones_geo_2023: pd.DataFrame,
    ocupacion_secciones: pd.DataFrame,
) -> AssetCheckResult:
    info = _file_info(mapa_relacion_renta_ocupacion_alta_2021_2023)
    r1 = (
        int(len(renta_secciones_geo_2021))
        if renta_secciones_geo_2021 is not None
        else 0
    )
    r2 = (
        int(len(renta_secciones_geo_2023))
        if renta_secciones_geo_2023 is not None
        else 0
    )
    o = int(len(ocupacion_secciones)) if ocupacion_secciones is not None else 0
    passed = (
        bool(info.get("exists", False))
        and info.get("size", 0) > 0
        and r1 > 0
        and r2 > 0
        and o > 0
    )
    return AssetCheckResult(
        passed=passed,
        metadata={
            "path": MetadataValue.path(
                str(mapa_relacion_renta_ocupacion_alta_2021_2023)
                if mapa_relacion_renta_ocupacion_alta_2021_2023 is not None
                else ""
            ),
            "exists": MetadataValue.bool(info.get("exists", False)),
            "size_bytes": MetadataValue.int(info.get("size", 0)),
            "renta_2021_rows": MetadataValue.int(r1),
            "renta_2023_rows": MetadataValue.int(r2),
            "ocupacion_rows": MetadataValue.int(o),
        },
    )


@asset_check(
    asset="grafico_actividad_santa_cruz",
    additional_ins={
        "actividad_secciones": AssetIn(key=AssetKey("actividad_secciones"))
    },
)
def check_grafico_actividad_exists_and_inputs(
    grafico_actividad_santa_cruz: str, actividad_secciones: pd.DataFrame
) -> AssetCheckResult:
    info = _file_info(grafico_actividad_santa_cruz)
    rows = int(len(actividad_secciones)) if actividad_secciones is not None else 0
    passed = bool(info.get("exists", False)) and info.get("size", 0) > 0 and rows > 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "path": MetadataValue.path(
                str(grafico_actividad_santa_cruz)
                if grafico_actividad_santa_cruz is not None
                else ""
            ),
            "exists": MetadataValue.bool(info.get("exists", False)),
            "size_bytes": MetadataValue.int(info.get("size", 0)),
            "actividad_rows": MetadataValue.int(rows),
        },
    )


@asset_check(
    asset="grafico_ocupacion_santa_cruz",
    additional_ins={
        "ocupacion_secciones": AssetIn(key=AssetKey("ocupacion_secciones"))
    },
)
def check_grafico_ocupacion_exists_and_inputs(
    grafico_ocupacion_santa_cruz: str, ocupacion_secciones: pd.DataFrame
) -> AssetCheckResult:
    info = _file_info(grafico_ocupacion_santa_cruz)
    rows = int(len(ocupacion_secciones)) if ocupacion_secciones is not None else 0
    passed = bool(info.get("exists", False)) and info.get("size", 0) > 0 and rows > 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "path": MetadataValue.path(
                str(grafico_ocupacion_santa_cruz)
                if grafico_ocupacion_santa_cruz is not None
                else ""
            ),
            "exists": MetadataValue.bool(info.get("exists", False)),
            "size_bytes": MetadataValue.int(info.get("size", 0)),
            "ocupacion_rows": MetadataValue.int(rows),
        },
    )


# -----------------------
# Data checks (nulls críticos y consistencia de claves geográficas)
# -----------------------


@asset_check(asset="actividad_secciones")
def check_actividad_no_nulos_criticos(
    actividad_secciones: pd.DataFrame,
) -> AssetCheckResult:
    if actividad_secciones is None:
        return AssetCheckResult(
            passed=False,
            metadata={"reason": MetadataValue.text("actividad_secciones es None")},
        )
    total = int(len(actividad_secciones))
    to_check = [
        c
        for c in ["geocode", "num_casos", "periodo"]
        if c in actividad_secciones.columns
    ]
    missing = {c: int(actividad_secciones[c].isna().sum()) for c in to_check}
    passed = all(v == 0 for v in missing.values())
    meta = {"rows": MetadataValue.int(total)}
    for k, v in missing.items():
        meta[f"missing_{k}"] = MetadataValue.int(v)
    return AssetCheckResult(passed=passed, metadata=meta)


@asset_check(asset="ocupacion_secciones")
def check_ocupacion_no_nulos_criticos(
    ocupacion_secciones: pd.DataFrame,
) -> AssetCheckResult:
    if ocupacion_secciones is None:
        return AssetCheckResult(
            passed=False,
            metadata={"reason": MetadataValue.text("ocupacion_secciones es None")},
        )
    total = int(len(ocupacion_secciones))
    year_col = (
        "ano"
        if "ano" in ocupacion_secciones.columns
        else ("año" if "año" in ocupacion_secciones.columns else None)
    )
    to_check = [
        c
        for c in ["geocode", "num_casos", year_col]
        if c is not None and c in ocupacion_secciones.columns
    ]
    missing = {c: int(ocupacion_secciones[c].isna().sum()) for c in to_check}
    passed = all(v == 0 for v in missing.values())
    meta = {"rows": MetadataValue.int(total)}
    for k, v in missing.items():
        meta[f"missing_{k}"] = MetadataValue.int(v)
    return AssetCheckResult(passed=passed, metadata=meta)


@asset_check(
    asset="actividad_secciones",
    additional_ins={
        "carga_secciones_tenerife_2022": AssetIn(
            key=AssetKey("carga_secciones_tenerife_2022")
        )
    },
)
def check_actividad_geokey_consistencia(
    actividad_secciones: pd.DataFrame, carga_secciones_tenerife_2022: pd.DataFrame
) -> AssetCheckResult:
    if actividad_secciones is None:
        return AssetCheckResult(
            passed=False,
            metadata={"reason": MetadataValue.text("actividad_secciones es None")},
        )
    if carga_secciones_tenerife_2022 is None:
        return AssetCheckResult(
            passed=False,
            metadata={
                "reason": MetadataValue.text("carga_secciones_tenerife_2022 es None")
            },
        )
    geo_codes = set(
        carga_secciones_tenerife_2022["geocode"].dropna().astype(str).unique()
    )
    mask_act = _mask_tenerife_actividad(actividad_secciones)
    df_act = actividad_secciones[mask_act].copy()
    if df_act.empty:
        df_act = _filter_by_geo_codes(actividad_secciones, geo_codes)
    if df_act.empty:
        return AssetCheckResult(
            passed=False,
            metadata={
                "reason": MetadataValue.text(
                    "Sin filas de Tenerife en actividad_secciones"
                ),
                "actividad_rows": MetadataValue.int(int(len(actividad_secciones))),
                "actividad_rows_mask": MetadataValue.int(int(mask_act.sum())),
                "geo_codes_total": MetadataValue.int(int(len(geo_codes))),
            },
        )
    act_codes_raw = set(df_act["geocode"].dropna().astype(str).unique())
    act_codes_norm = set(_strip_date_prefix(c) for c in act_codes_raw)
    geo_norm = set(_strip_date_prefix(c) for c in geo_codes)
    matched_raw = len(act_codes_raw & geo_codes)
    matched = len(act_codes_norm & geo_norm)
    total = max(1, len(act_codes_norm))
    pct = matched / total * 100.0
    passed = pct >= 80.0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "actividad_geocodes_total": MetadataValue.int(len(act_codes_norm)),
            "actividad_geocodes_total_raw": MetadataValue.int(len(act_codes_raw)),
            "matched": MetadataValue.int(matched),
            "matched_raw": MetadataValue.int(matched_raw),
            "pct_matched": MetadataValue.float(pct),
            "actividad_rows": MetadataValue.int(int(len(actividad_secciones))),
            "actividad_rows_mask": MetadataValue.int(int(mask_act.sum())),
            "actividad_rows_used": MetadataValue.int(int(len(df_act))),
            "geo_codes_total": MetadataValue.int(int(len(geo_codes))),
            "sample_act_geocodes": MetadataValue.json(_sample_codes(act_codes_raw)),
            "sample_act_geocodes_norm": MetadataValue.json(
                _sample_codes(act_codes_norm)
            ),
            "sample_geo_geocodes": MetadataValue.json(_sample_codes(geo_codes)),
            "sample_geo_geocodes_norm": MetadataValue.json(_sample_codes(geo_norm)),
        },
    )


@asset_check(
    asset="ocupacion_secciones",
    additional_ins={
        "carga_secciones_tenerife_2022": AssetIn(
            key=AssetKey("carga_secciones_tenerife_2022")
        )
    },
)
def check_ocupacion_geokey_consistencia(
    ocupacion_secciones: pd.DataFrame, carga_secciones_tenerife_2022: pd.DataFrame
) -> AssetCheckResult:
    if ocupacion_secciones is None:
        return AssetCheckResult(
            passed=False,
            metadata={"reason": MetadataValue.text("ocupacion_secciones es None")},
        )
    if carga_secciones_tenerife_2022 is None:
        return AssetCheckResult(
            passed=False,
            metadata={
                "reason": MetadataValue.text("carga_secciones_tenerife_2022 es None")
            },
        )
    geo_codes = set(
        carga_secciones_tenerife_2022["geocode"].dropna().astype(str).unique()
    )
    mask_occ = _mask_tenerife_ocupacion(ocupacion_secciones)
    df_occ = ocupacion_secciones[mask_occ].copy()
    if df_occ.empty:
        df_occ = _filter_by_geo_codes(ocupacion_secciones, geo_codes)
    if df_occ.empty:
        return AssetCheckResult(
            passed=False,
            metadata={
                "reason": MetadataValue.text(
                    "Sin filas de Tenerife en ocupacion_secciones"
                ),
                "ocupacion_rows": MetadataValue.int(int(len(ocupacion_secciones))),
                "ocupacion_rows_mask": MetadataValue.int(int(mask_occ.sum())),
                "geo_codes_total": MetadataValue.int(int(len(geo_codes))),
            },
        )
    occ_codes_raw = set(df_occ["geocode"].dropna().astype(str).unique())
    occ_codes_norm = set(_strip_date_prefix(c) for c in occ_codes_raw)
    geo_norm = set(_strip_date_prefix(c) for c in geo_codes)
    matched_raw = len(occ_codes_raw & geo_codes)
    matched = len(occ_codes_norm & geo_norm)
    total = max(1, len(occ_codes_norm))
    pct = matched / total * 100.0
    passed = pct >= 80.0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "ocupacion_geocodes_total": MetadataValue.int(len(occ_codes_norm)),
            "ocupacion_geocodes_total_raw": MetadataValue.int(len(occ_codes_raw)),
            "matched": MetadataValue.int(matched),
            "matched_raw": MetadataValue.int(matched_raw),
            "pct_matched": MetadataValue.float(pct),
            "ocupacion_rows": MetadataValue.int(int(len(ocupacion_secciones))),
            "ocupacion_rows_mask": MetadataValue.int(int(mask_occ.sum())),
            "ocupacion_rows_used": MetadataValue.int(int(len(df_occ))),
            "geo_codes_total": MetadataValue.int(int(len(geo_codes))),
            "sample_occ_geocodes": MetadataValue.json(_sample_codes(occ_codes_raw)),
            "sample_occ_geocodes_norm": MetadataValue.json(
                _sample_codes(occ_codes_norm)
            ),
            "sample_geo_geocodes": MetadataValue.json(_sample_codes(geo_codes)),
            "sample_geo_geocodes_norm": MetadataValue.json(_sample_codes(geo_norm)),
        },
    )
