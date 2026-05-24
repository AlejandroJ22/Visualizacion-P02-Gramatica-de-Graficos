"""Assets Dagster para cartografia y datos geoespaciales."""

import re
from pathlib import Path

import geopandas as gpd
import mapclassify as mc
import matplotlib.pyplot as plt
from matplotlib.colors import TwoSlopeNorm
import numpy as np
import pandas as pd
from dagster import asset, AssetExecutionContext

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
OUTPUTS_DIR = BASE_DIR / "outputs"
SECCIONES_TENERIFE_2022 = (
    DATA_DIR / "cartografia-secciones" / "secciones_20220101_tenerife.json"
)
RENTA_SECCIONES_CSV = DATA_DIR / "rentamedia-sc-3.csv"
ACTIVIDAD_SECCIONES_CSV = DATA_DIR / "actividad-sc-3.csv"
OCUPACION_SECCIONES_CSV = DATA_DIR / "ocupacion-sc-3.csv"


def _normalize_column_name(name: str) -> str:
    translations = str.maketrans("áéíóúüñÁÉÍÓÚÜÑ", "aeiouunAEIOUUN")
    return (
        name.strip()
        .translate(translations)
        .replace(" ", "_")
        .replace("/", "_")
        .replace("-", "_")
        .lower()
    )


def _round_to_hundreds(value: float) -> int:
    return int(round(value, -2))


def normalizar_geocode(x) -> str:
    """Normaliza geocodes para poder comparar y unir claves heterogeneas."""
    if pd.isna(x):
        return ""
    value = str(x).strip().upper()
    value = re.sub(r"\s+", "", value)
    value = re.sub(r"^\d{8}[_-]?", "", value)
    value = re.sub(r"[-\s]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value


def _sample_codes(values, limit: int = 5) -> list[str]:
    if not values:
        return []
    return list(sorted(values))[:limit]


def _log_geocode_profile(
    context: AssetExecutionContext,
    label: str,
    series: pd.Series,
    limit: int = 5,
) -> set[str]:
    raw = series.dropna().astype(str)
    norm = raw.map(normalizar_geocode)
    raw_unique = set(raw.unique())
    norm_unique = set(norm[norm != ""].unique())
    context.log.info(
        "%s | geocodes unicos raw=%s norm=%s",
        label,
        len(raw_unique),
        len(norm_unique),
    )
    context.log.info("%s | ejemplos raw: %s", label, _sample_codes(raw_unique, limit))
    context.log.info(
        "%s | ejemplos normalizados: %s", label, _sample_codes(norm_unique, limit)
    )
    return norm_unique


def _log_geocode_overlap(
    context: AssetExecutionContext,
    label: str,
    left_codes: set[str],
    right_codes: set[str],
) -> float:
    if not left_codes or not right_codes:
        context.log.warning(
            "%s | no hay codigos suficientes para calcular solapamiento",
            label,
        )
        return 0.0
    overlap = left_codes & right_codes
    pct_left = float(len(overlap) / len(left_codes) * 100)
    pct_right = float(len(overlap) / len(right_codes) * 100)
    context.log.info(
        "%s | coincidencias normalizadas=%s | cobertura izquierda=%.2f%% | cobertura derecha=%.2f%%",
        label,
        len(overlap),
        pct_left,
        pct_right,
    )
    context.log.info(
        "%s | ejemplos de coincidencia: %s",
        label,
        _sample_codes(overlap),
    )
    return pct_left


def _log_correlation_diagnostics(
    context: AssetExecutionContext,
    df: pd.DataFrame,
    columns: list[str],
    label: str,
) -> pd.DataFrame:
    """Loguea diagnosticos previos a una correlacion y devuelve datos limpios."""
    selected = df.loc[:, columns].apply(pd.to_numeric, errors="coerce")

    context.log.info("%s | filas dataframe final: %s", label, len(df))
    context.log.info("%s | NaN por columna: %s", label, selected.isna().sum().to_dict())
    context.log.info(
        "%s | valores unicos por variable: %s",
        label,
        selected.nunique(dropna=True).to_dict(),
    )

    stds = selected.std(numeric_only=True)
    context.log.info(
        "%s | std por variable: %s",
        label,
        {
            column: (None if pd.isna(std_value) else float(std_value))
            for column, std_value in stds.items()
        },
    )

    pct_zeros = {}
    for column in columns:
        series = selected[column].dropna()
        pct_zeros[column] = float((series == 0).mean() * 100) if len(series) else 0.0
    context.log.info("%s | porcentaje de ceros: %s", label, pct_zeros)

    head_preview = selected.head()
    context.log.info(
        "%s | head() columnas relevantes:\n%s",
        label,
        (
            head_preview.to_string(index=False)
            if not head_preview.empty
            else "<sin filas>"
        ),
    )

    clean = selected.dropna().copy()
    context.log.info("%s | filas tras dropna: %s", label, len(clean))
    context.log.info(
        "%s | head() limpio:\n%s",
        label,
        clean.head().to_string(index=False) if not clean.empty else "<sin filas>",
    )

    for column, std_value in stds.items():
        if pd.isna(std_value):
            context.log.warning(
                "%s | %s tiene std NaN; puede faltar informacion util para correlacion",
                label,
                column,
            )
        elif float(std_value) == 0.0:
            context.log.warning(
                "%s | %s tiene std == 0; la correlacion no esta definida si la columna es constante",
                label,
                column,
            )

    if len(clean) < 2:
        context.log.warning(
            "%s | no hay suficientes filas limpias para calcular la correlacion",
            label,
        )
    elif any(clean[column].nunique(dropna=True) < 2 for column in columns):
        context.log.warning(
            "%s | no hay suficientes valores distintos para calcular la correlacion",
            label,
        )

    return clean


@asset(group_name="geo")
def carga_secciones_tenerife_2022() -> gpd.GeoDataFrame:
    """Carga la cartografia de secciones censales de Tenerife (base 2022)."""
    gdf = gpd.read_file(SECCIONES_TENERIFE_2022)
    return gdf


@asset(group_name="geo")
def renta_secciones_2021() -> pd.DataFrame:
    """Carga y prepara la renta media por secciones censales (2021)."""
    df = pd.read_csv(RENTA_SECCIONES_CSV)
    df = df[df["año"] == 2021]
    df = df[df["MEDIDAS_CODE"] == "RENTA_BRUTA_MEDIA_HOGAR"]
    df = df.dropna(subset=["TERRITORIO_CODE", "OBS_VALUE"])
    df["TERRITORIO_CODE"] = df["TERRITORIO_CODE"].astype(str)
    df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    df = df.dropna(subset=["OBS_VALUE"])
    return df


@asset(group_name="geo")
def renta_secciones_2023() -> pd.DataFrame:
    """Carga y prepara la renta media por secciones censales (2023)."""
    df = pd.read_csv(RENTA_SECCIONES_CSV)
    df = df[df["año"] == 2023]
    df = df[df["MEDIDAS_CODE"] == "RENTA_BRUTA_MEDIA_HOGAR"]
    df = df.dropna(subset=["TERRITORIO_CODE", "OBS_VALUE"])
    df["TERRITORIO_CODE"] = df["TERRITORIO_CODE"].astype(str)
    df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    df = df.dropna(subset=["OBS_VALUE"])
    return df


@asset(group_name="geo")
def renta_secciones_bins_global(
    context: AssetExecutionContext,
    renta_secciones_2021: pd.DataFrame,
    renta_secciones_2023: pd.DataFrame,
) -> dict:
    """Calcula límites de clasificación (bins) globales usando datos 2021-2023 combinados.

    Devuelve un dict con keys: 'bins' (list de breakpoints length=k),
    'ud_bins' (breakpoints para UserDefined, length=k-1) y 'labels' (length=k).
    """
    # concatenar series disponibles sin modificar los DataFrames originales
    series_list = []
    for df in (renta_secciones_2021, renta_secciones_2023):
        if df is None:
            continue
        if "OBS_VALUE" in df.columns:
            series_list.append(pd.to_numeric(df["OBS_VALUE"], errors="coerce").dropna())

    if not series_list:
        raise ValueError(
            "No hay datos OBS_VALUE disponibles para calcular bins globales"
        )

    combined = pd.concat(series_list, axis=0)
    # usar mapclassify Quantiles sobre la serie combinada
    # k se fija aquí para evitar que Dagster lo interprete como asset input
    k = 5
    classifier = mc.Quantiles(combined, k=k)
    bins = classifier.bins.tolist()
    # construir labels (k labels) a partir del min y los breakpoints
    lower_bound = float(combined.min())
    labels = []
    for upper_bound in bins:
        labels.append(
            f"{_round_to_hundreds(lower_bound)} - {_round_to_hundreds(float(upper_bound))}"
        )
        lower_bound = float(upper_bound)

    ud_bins = bins[:-1]
    return {"bins": bins, "ud_bins": ud_bins, "labels": labels}


@asset(group_name="geo")
def renta_secciones_geo_2021(
    context: AssetExecutionContext,
    carga_secciones_tenerife_2022: gpd.GeoDataFrame,
    renta_secciones_2021: pd.DataFrame,
) -> gpd.GeoDataFrame:
    """Une cartografia de secciones con renta 2021 y valida nulos basicos.

    Añade logs diagnósticos y errores claros cuando la unión no produce
    coincidencias o cuando los valores/geometry no son válidos.
    """
    merged = carga_secciones_tenerife_2022.merge(
        renta_secciones_2021,
        left_on="geocode",
        right_on="TERRITORIO_CODE",
        how="left",
    )

    context.log.info(f"renta_secciones_geo_2021: filas tras merge={len(merged)}")

    geo_codes = set(carga_secciones_tenerife_2022["geocode"].astype(str).unique())
    renta_codes = set(renta_secciones_2021["TERRITORIO_CODE"].astype(str).unique())
    inter = geo_codes & renta_codes
    context.log.debug(
        f"geocodes total={len(geo_codes)}, renta_codes total={len(renta_codes)}, interseccion={len(inter)}"
    )
    if len(inter) == 0:
        # Intentar normalizar eliminando prefijo de fecha (p.ej. 20220101_... vs 20240101_...)
        def _strip_date_prefix(code: str) -> str:
            parts = str(code).split("_")
            return "_".join(parts[1:]) if len(parts) > 1 else str(code)

        geo_norm = set(_strip_date_prefix(c) for c in geo_codes)
        renta_norm = set(_strip_date_prefix(c) for c in renta_codes)
        inter_norm = geo_norm & renta_norm
        context.log.debug(
            f"interseccion tras normalizar prefijo fecha={len(inter_norm)}"
        )
        if len(inter_norm) > 0:
            context.log.info(
                "Se detectaron coincidencias tras eliminar prefijo de fecha; se usará clave normalizada para la unión."
            )
            gdf = carga_secciones_tenerife_2022.copy()
            gdf["geocode_norm"] = gdf["geocode"].astype(str).apply(_strip_date_prefix)
            renta_df = renta_secciones_2021.copy()
            renta_df["TERRITORIO_CODE_norm"] = (
                renta_df["TERRITORIO_CODE"].astype(str).apply(_strip_date_prefix)
            )
            merged = gdf.merge(
                renta_df,
                left_on="geocode_norm",
                right_on="TERRITORIO_CODE_norm",
                how="left",
            )
        else:
            sample_geo = list(sorted(geo_codes))[:5]
            sample_renta = list(sorted(renta_codes))[:5]
            msg = (
                "No hay coincidencias entre 'geocode' (geo) y 'TERRITORIO_CODE' (renta). "
                f"Ejemplos geo (5): {sample_geo}; ejemplos renta (5): {sample_renta}. "
                "Comprueba formato/prefijos/padding."
            )
            context.log.error(msg)
            raise ValueError(msg)

    if "OBS_VALUE" not in merged.columns:
        msg = "Columna 'OBS_VALUE' no encontrada tras merge."
        context.log.error(msg)
        raise ValueError(msg)

    total = len(merged)
    nulos_obs = int(merged["OBS_VALUE"].isna().sum())
    if nulos_obs == total:
        msg = "Tras el merge, todos los OBS_VALUE son nulos. Abortando."
        context.log.error(msg)
        raise ValueError(msg)
    if nulos_obs > 0:
        context.log.warning(
            f"{nulos_obs} filas con OBS_VALUE nulo; se eliminarán antes de devolver."
        )
        merged = merged.dropna(subset=["OBS_VALUE"]).copy()

    geom_nulls = int(merged.geometry.isna().sum())
    geom_empty = int(merged.geometry.is_empty.sum())
    geom_invalid = int((~merged.geometry.is_valid).sum())
    if geom_nulls or geom_empty or geom_invalid:
        context.log.warning(
            f"Geometrías: nulls={geom_nulls}, empty={geom_empty}, invalid={geom_invalid}."
        )

    tb = merged.total_bounds
    if not np.all(np.isfinite(tb)):
        msg = f"Bounds no finitos tras merge: {tb}. Abortando para evitar error de aspecto."
        context.log.error(msg)
        raise ValueError(msg)

    return merged


@asset(group_name="geo")
def renta_secciones_geo_2023(
    context: AssetExecutionContext,
    carga_secciones_tenerife_2022: gpd.GeoDataFrame,
    renta_secciones_2023: pd.DataFrame,
) -> gpd.GeoDataFrame:
    """Une cartografia de secciones con renta 2023 y valida nulos basicos.

    Añade logs diagnósticos y errores claros cuando la unión no produce
    coincidencias o cuando los valores/geometry no son válidos.
    """
    merged = carga_secciones_tenerife_2022.merge(
        renta_secciones_2023,
        left_on="geocode",
        right_on="TERRITORIO_CODE",
        how="left",
    )

    context.log.info(f"renta_secciones_geo_2023: filas tras merge={len(merged)}")

    geo_codes = set(carga_secciones_tenerife_2022["geocode"].astype(str).unique())
    renta_codes = set(renta_secciones_2023["TERRITORIO_CODE"].astype(str).unique())
    inter = geo_codes & renta_codes
    context.log.debug(
        f"geocodes total={len(geo_codes)}, renta_codes total={len(renta_codes)}, interseccion={len(inter)}"
    )
    if len(inter) == 0:
        # Intentar normalizar eliminando prefijo de fecha (p.ej. 20220101_... vs 20240101_...)
        def _strip_date_prefix(code: str) -> str:
            parts = str(code).split("_")
            return "_".join(parts[1:]) if len(parts) > 1 else str(code)

        geo_norm = set(_strip_date_prefix(c) for c in geo_codes)
        renta_norm = set(_strip_date_prefix(c) for c in renta_codes)
        inter_norm = geo_norm & renta_norm
        context.log.debug(
            f"interseccion tras normalizar prefijo fecha={len(inter_norm)}"
        )
        if len(inter_norm) > 0:
            context.log.info(
                "Se detectaron coincidencias tras eliminar prefijo de fecha; se usará clave normalizada para la unión."
            )
            gdf = carga_secciones_tenerife_2022.copy()
            gdf["geocode_norm"] = gdf["geocode"].astype(str).apply(_strip_date_prefix)
            renta_df = renta_secciones_2023.copy()
            renta_df["TERRITORIO_CODE_norm"] = (
                renta_df["TERRITORIO_CODE"].astype(str).apply(_strip_date_prefix)
            )
            merged = gdf.merge(
                renta_df,
                left_on="geocode_norm",
                right_on="TERRITORIO_CODE_norm",
                how="left",
            )
        else:
            sample_geo = list(sorted(geo_codes))[:5]
            sample_renta = list(sorted(renta_codes))[:5]
            msg = (
                "No hay coincidencias entre 'geocode' (geo) y 'TERRITORIO_CODE' (renta 2023). "
                f"Ejemplos geo (5): {sample_geo}; ejemplos renta (5): {sample_renta}. "
                "Comprueba formato/prefijos/padding."
            )
            context.log.error(msg)
            raise ValueError(msg)

    if "OBS_VALUE" not in merged.columns:
        msg = "Columna 'OBS_VALUE' no encontrada tras merge (2023)."
        context.log.error(msg)
        raise ValueError(msg)

    total = len(merged)
    nulos_obs = int(merged["OBS_VALUE"].isna().sum())
    if nulos_obs == total:
        msg = "Tras el merge 2023, todos los OBS_VALUE son nulos. Abortando."
        context.log.error(msg)
        raise ValueError(msg)
    if nulos_obs > 0:
        context.log.warning(
            f"{nulos_obs} filas con OBS_VALUE nulo; se eliminarán antes de devolver."
        )
        merged = merged.dropna(subset=["OBS_VALUE"]).copy()

    geom_nulls = int(merged.geometry.isna().sum())
    geom_empty = int(merged.geometry.is_empty.sum())
    geom_invalid = int((~merged.geometry.is_valid).sum())
    if geom_nulls or geom_empty or geom_invalid:
        context.log.warning(
            f"Geometrías: nulls={geom_nulls}, empty={geom_empty}, invalid={geom_invalid}."
        )

    tb = merged.total_bounds
    if not np.all(np.isfinite(tb)):
        msg = f"Bounds no finitos tras merge (2023): {tb}. Abortando para evitar error de aspecto."
        context.log.error(msg)
        raise ValueError(msg)

    return merged


@asset(group_name="geo")
def actividad_secciones() -> pd.DataFrame:
    """Carga y limpia el dataset de actividad economica por seccion censal."""
    df = pd.read_csv(ACTIVIDAD_SECCIONES_CSV)
    df.columns = [_normalize_column_name(col) for col in df.columns]

    if "num_casos" in df.columns:
        df["num_casos"] = pd.to_numeric(df["num_casos"], errors="coerce")

    if "periodo" in df.columns:
        df["periodo"] = pd.to_numeric(df["periodo"], errors="coerce")

    if "geocode" in df.columns:
        df["geocode"] = df["geocode"].astype(str)

    drop_cols = [col for col in ["geocode", "num_casos"] if col in df.columns]
    if drop_cols:
        df = df.dropna(subset=drop_cols)

    return df


@asset(group_name="geo")
def ocupacion_secciones() -> pd.DataFrame:
    """Carga y limpia el dataset de ocupación por seccion censal.

    No hace merges; deja el DataFrame listo para análisis/agrupaciones.
    """
    df = pd.read_csv(OCUPACION_SECCIONES_CSV)
    # normalizar nombres de columnas a estilo snake_case sin acentos
    df.columns = [_normalize_column_name(col) for col in df.columns]

    # convertir columnas esperadas
    if "num_casos" in df.columns:
        df["num_casos"] = pd.to_numeric(df["num_casos"], errors="coerce")

    if "ano" in df.columns:
        df["ano"] = pd.to_numeric(df["ano"], errors="coerce")

    if "geocode" in df.columns:
        df["geocode"] = df["geocode"].astype(str)

    # eliminar filas sin geocode o sin num_casos (no podemos agregarlas aquí)
    drop_cols = [c for c in ["geocode", "num_casos"] if c in df.columns]
    if drop_cols:
        df = df.dropna(subset=drop_cols).copy()

    return df


@asset(group_name="geo")
def grafico_actividad_santa_cruz(
    context: AssetExecutionContext, actividad_secciones: pd.DataFrame
) -> str:
    """Genera un gráfico de barras agrupadas con la evolución 2021 vs 2023
    de la actividad económica en Santa Cruz de Tenerife. Guarda PNG en outputs/.
    """
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUTS_DIR / "actividad_santa_cruz_evolucion.png"

    if actividad_secciones is None or actividad_secciones.empty:
        context.log.error("grafico_actividad_santa_cruz: DataFrame vacío")
        raise ValueError("actividad_secciones vacío")

    df = actividad_secciones.copy()

    # detectar provincia por código o nombre
    if "cod_provincia" in df.columns:
        mask = df["cod_provincia"].astype(str).str.startswith("38")
    elif "provincia" in df.columns:
        mask = (
            df["provincia"]
            .astype(str)
            .str.contains("santa cruz|tenerife", case=False, na=False)
        )
    else:
        # si no hay columna de provincia, intentar filtrar por geocode que contenga codigo provincial
        if "geocode" in df.columns:
            mask = df["geocode"].astype(str).str.contains("_38")
        else:
            mask = pd.Series([False] * len(df))

    df_sc = df[mask]
    if df_sc.empty:
        context.log.error(
            "grafico_actividad_santa_cruz: no se encontraron filas para Santa Cruz de Tenerife"
        )
        raise ValueError("Sin datos para Santa Cruz de Tenerife en actividad_secciones")

    # seleccionar 2021 y 2023
    periodo_col = "periodo" if "periodo" in df_sc.columns else None
    if periodo_col is None:
        context.log.error(
            "grafico_actividad_santa_cruz: columna de periodo no encontrada"
        )
        raise ValueError("Columna de periodo no encontrada en actividad_secciones")

    df_sc[periodo_col] = pd.to_numeric(df_sc[periodo_col], errors="coerce")
    df_sc = df_sc[df_sc[periodo_col].isin([2021, 2023])]
    if df_sc.empty:
        context.log.error("grafico_actividad_santa_cruz: sin datos para 2021 y 2023")
        raise ValueError("Sin datos de actividad para 2021 y 2023")

    # columna de categoría (actividad)
    cat_col = (
        "actividad_economica"
        if "actividad_economica" in df_sc.columns
        else df_sc.columns[0]
    )

    # agregar y ordenar (top global por suma 2021+2023)
    grouped = (
        df_sc.groupby([cat_col, periodo_col])["num_casos"].sum().unstack(fill_value=0)
    )
    totals = grouped.sum(axis=1).sort_values(ascending=False)
    topn = 12
    top_cats = totals.head(topn).index.tolist()
    plot_df = grouped.loc[top_cats][[2021, 2023]]

    context.log.info(
        f"grafico_actividad_santa_cruz: filas procesadas={len(df_sc)}, top={top_cats}"
    )

    # plot barras agrupadas
    fig, ax = plt.subplots(figsize=(12, 7))
    x = np.arange(len(plot_df))
    width = 0.35
    ax.bar(x - width / 2, plot_df[2021].values, width, label="2021", color="#1f78b4")
    ax.bar(x + width / 2, plot_df[2023].values, width, label="2023", color="#e31a1c")
    ax.set_xticks(x)
    ax.set_xticklabels(plot_df.index, rotation=45, ha="right")
    ax.set_ylabel("Número de casos")
    ax.set_title("Actividad económica — Santa Cruz de Tenerife (2021 vs 2023)")
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.legend(title="Año")

    fig.tight_layout()
    fig.savefig(output_path, dpi=300)
    plt.close(fig)
    return str(output_path)


@asset(group_name="geo")
def grafico_ocupacion_santa_cruz(
    context: AssetExecutionContext, ocupacion_secciones: pd.DataFrame
) -> str:
    """Genera un gráfico de barras agrupadas con la evolución 2021 vs 2023
    de ocupaciones en Santa Cruz de Tenerife (sin desagregar por sexo).
    Guarda PNG en outputs/.
    """
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUTS_DIR / "ocupacion_santa_cruz_evolucion.png"

    if ocupacion_secciones is None or ocupacion_secciones.empty:
        context.log.error("grafico_ocupacion_santa_cruz: DataFrame vacío")
        raise ValueError("ocupacion_secciones vacío")

    df = ocupacion_secciones.copy()

    # filtrar provincia por código municipal que empiece por 38 (Tenerife)
    if "code_municipio" in df.columns:
        mask = df["code_municipio"].astype(str).str.startswith("38")
    elif "geocode" in df.columns:
        mask = df["geocode"].astype(str).str.contains("_38")
    else:
        mask = pd.Series([False] * len(df))

    df_sc = df[mask]
    if df_sc.empty:
        context.log.error(
            "grafico_ocupacion_santa_cruz: no se encontraron filas para Santa Cruz de Tenerife"
        )
        raise ValueError("Sin datos para Santa Cruz de Tenerife en ocupacion_secciones")

    # elegir años 2021 y 2023
    year_col = (
        "ano" if "ano" in df_sc.columns else ("año" if "año" in df_sc.columns else None)
    )
    if year_col is None:
        context.log.error(
            "grafico_ocupacion_santa_cruz: no se encuentra columna de año"
        )
        raise ValueError("Columna de año no encontrada en ocupacion_secciones")
    df_sc[year_col] = pd.to_numeric(df_sc[year_col], errors="coerce")
    df_sc = df_sc[df_sc[year_col].isin([2021, 2023])]
    if df_sc.empty:
        context.log.error("grafico_ocupacion_santa_cruz: sin datos para 2021 y 2023")
        raise ValueError("Sin datos de ocupación para 2021 y 2023")

    # agrupar por ocupacion y año (sin sexo)
    cat_col = "ocupacion" if "ocupacion" in df_sc.columns else df_sc.columns[0]
    grouped = (
        df_sc.groupby([cat_col, year_col])["num_casos"].sum().unstack(fill_value=0)
    )
    totals = grouped.sum(axis=1).sort_values(ascending=False).head(10)
    top_cats = totals.index.tolist()
    plot_df = grouped.loc[top_cats].reindex(columns=[2021, 2023], fill_value=0)

    fig, ax = plt.subplots(figsize=(12, 7))
    x = np.arange(len(plot_df))
    width = 0.35
    ax.bar(x - width / 2, plot_df[2021].values, width, label="2021", color="#1f78b4")
    ax.bar(x + width / 2, plot_df[2023].values, width, label="2023", color="#e31a1c")
    ax.set_xticks(x)
    ax.set_xticklabels(plot_df.index, rotation=45, ha="right")
    ax.set_ylabel("Número de casos")
    ax.set_title("Top 10 ocupaciones — Santa Cruz de Tenerife (2021 vs 2023)")
    ax.legend(title="Año")
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    fig.tight_layout()
    fig.savefig(output_path, dpi=300)
    plt.close(fig)

    context.log.info(
        f"grafico_ocupacion_santa_cruz: filas procesadas={len(df_sc)}, top={top_cats}"
    )
    return str(output_path)


@asset(group_name="geo")
def mapa_relacion_renta_servicios_2021_2023(
    context: AssetExecutionContext,
    renta_secciones_geo_2021: gpd.GeoDataFrame,
    renta_secciones_geo_2023: gpd.GeoDataFrame,
    actividad_secciones: pd.DataFrame,
) -> str:
    """Mapa coropletico bivariado de crecimiento de renta ajustado y variacion de servicios.

    - Color: combinacion de cuantiles (renta ajustada x servicios)
    """
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUTS_DIR / "geo_relacion_renta_servicios_2021_2023.png"

    if renta_secciones_geo_2021 is None or renta_secciones_geo_2021.empty:
        context.log.error(
            "mapa_relacion_renta_servicios_2021_2023: GeoDataFrame 2021 vacio"
        )
        raise ValueError("GeoDataFrame 2021 vacio")
    if renta_secciones_geo_2023 is None or renta_secciones_geo_2023.empty:
        context.log.error(
            "mapa_relacion_renta_servicios_2021_2023: GeoDataFrame 2023 vacio"
        )
        raise ValueError("GeoDataFrame 2023 vacio")
    if actividad_secciones is None or actividad_secciones.empty:
        context.log.error(
            "mapa_relacion_renta_servicios_2021_2023: actividad_secciones vacio"
        )
        raise ValueError("actividad_secciones vacio")

    required_renta_cols = {"geocode", "OBS_VALUE", "geometry"}
    if not required_renta_cols.issubset(renta_secciones_geo_2021.columns):
        missing = required_renta_cols - set(renta_secciones_geo_2021.columns)
        raise ValueError(f"Faltan columnas en renta 2021: {sorted(missing)}")
    if not {"geocode", "OBS_VALUE"}.issubset(renta_secciones_geo_2023.columns):
        missing = {"geocode", "OBS_VALUE"} - set(renta_secciones_geo_2023.columns)
        raise ValueError(f"Faltan columnas en renta 2023: {sorted(missing)}")

    # calcular crecimiento de renta
    gdf_2021 = renta_secciones_geo_2021[["geocode", "geometry", "OBS_VALUE"]].copy()
    gdf_2021 = gdf_2021.rename(columns={"OBS_VALUE": "OBS_VALUE_2021"})
    df_2023 = renta_secciones_geo_2023[["geocode", "OBS_VALUE"]].copy()
    df_2023 = df_2023.rename(columns={"OBS_VALUE": "OBS_VALUE_2023"})
    renta = gdf_2021.merge(df_2023, on="geocode", how="inner")
    renta["crecimiento_renta"] = (
        (renta["OBS_VALUE_2023"] - renta["OBS_VALUE_2021"])
        / renta["OBS_VALUE_2021"]
        * 100
    )
    renta = renta.replace([np.inf, -np.inf], np.nan)
    renta = renta.dropna(subset=["crecimiento_renta"]).copy()
    if renta.empty:
        raise ValueError("Crecimiento de renta vacio tras limpiar NaN/Inf")
    renta_mean = float(renta["crecimiento_renta"].mean())
    renta["crecimiento_renta_ajustado"] = renta["crecimiento_renta"] - renta_mean

    # calcular variacion de servicios (2021 vs 2023) en actividad
    actividad = actividad_secciones.copy()
    if "actividad_economica" not in actividad.columns:
        raise ValueError(
            "actividad_secciones no contiene columna 'actividad_economica'"
        )
    if "periodo" not in actividad.columns:
        raise ValueError("actividad_secciones no contiene columna 'periodo'")
    if "num_casos" not in actividad.columns:
        raise ValueError("actividad_secciones no contiene columna 'num_casos'")
    if "geocode" not in actividad.columns:
        raise ValueError("actividad_secciones no contiene columna 'geocode'")

    actividad["periodo"] = pd.to_numeric(actividad["periodo"], errors="coerce")
    actividad = actividad[actividad["periodo"].isin([2021, 2023])]
    servicios = actividad[
        actividad["actividad_economica"]
        .astype(str)
        .str.contains("servicios", case=False, na=False)
    ]
    if servicios.empty:
        raise ValueError("No hay registros de 'Servicios' en actividad_secciones")

    renta["geocode_norm"] = renta["geocode"].map(normalizar_geocode)
    servicios = servicios.copy()
    servicios["geocode_norm"] = servicios["geocode"].map(normalizar_geocode)
    renta_norm = _log_geocode_profile(
        context,
        "renta servicios",
        renta["geocode"],
    )
    servicios_norm = _log_geocode_profile(
        context,
        "actividad servicios",
        servicios["geocode"],
    )
    _log_geocode_overlap(
        context,
        "servicios | overlap previo normalizacion",
        renta_norm,
        servicios_norm,
    )

    serv_grouped = (
        servicios.groupby(["geocode_norm", "periodo"])["num_casos"]
        .sum()
        .unstack(fill_value=0)
    )
    serv_grouped = serv_grouped.reindex(columns=[2021, 2023], fill_value=0)
    serv_grouped["delta_servicios"] = serv_grouped[2023] - serv_grouped[2021]
    serv_grouped = serv_grouped.replace([np.inf, -np.inf], np.nan)

    # unir por geocode
    merged = renta.merge(
        serv_grouped[["delta_servicios"]],
        left_on="geocode_norm",
        right_index=True,
        how="left",
    )
    corr_serv_df = _log_correlation_diagnostics(
        context,
        merged,
        ["crecimiento_renta_ajustado", "delta_servicios"],
        "diagnostico correlacion servicios",
    )
    if len(corr_serv_df) >= 2 and all(
        corr_serv_df[column].nunique(dropna=True) > 1
        for column in ["crecimiento_renta_ajustado", "delta_servicios"]
    ):
        corr_serv = (
            corr_serv_df[["crecimiento_renta_ajustado", "delta_servicios"]]
            .corr()
            .iloc[0, 1]
        )
        context.log.info(
            f"correlacion limpia renta ajustada vs delta_servicios: {corr_serv:.4f}"
            if np.isfinite(corr_serv)
            else "correlacion limpia renta ajustada vs delta_servicios: n/d"
        )
    else:
        corr_serv = np.nan
        context.log.warning(
            "diagnostico correlacion servicios | correlacion omitida por falta de filas o variabilidad"
        )

    matched_rows = int(merged["delta_servicios"].notna().sum())
    merge_coverage = float(matched_rows / len(merged) * 100) if len(merged) else 0.0
    context.log.info(
        "servicios | filas emparejadas=%s/%s | cobertura merge=%.2f%%",
        matched_rows,
        len(merged),
        merge_coverage,
    )
    if merge_coverage < 80.0:
        context.log.warning(
            "servicios | cobertura del merge por debajo del 80%%; revisar normalizacion de geocodes"
        )
    merged["delta_servicios"] = merged["delta_servicios"].fillna(0)

    # clasificar en cuantiles (3 bins por defecto)
    n_bins = 3
    renta_q = pd.qcut(
        merged["crecimiento_renta_ajustado"],
        q=n_bins,
        labels=False,
        duplicates="drop",
    )
    serv_q = pd.qcut(
        merged["delta_servicios"],
        q=n_bins,
        labels=False,
        duplicates="drop",
    )
    if renta_q.isna().any() or serv_q.isna().any():
        context.log.warning("Cuantiles con NaN; se rellenan con 0")
    merged["renta_q"] = renta_q.fillna(0).astype(int)
    merged["serv_q"] = serv_q.fillna(0).astype(int)

    # paleta bivariada 3x3 (filas: renta, columnas: servicios)
    # paleta bivariada de alto contraste (renta ajustada x servicios)
    bivar_colors = [
        ["#f1eef6", "#bdc9e1", "#6a51a3"],
        ["#c7e9c0", "#74c476", "#238b45"],
        ["#fdd0a2", "#f16913", "#8c2d04"],
    ]

    def _bivar_color(r, s):
        r = min(max(int(r), 0), n_bins - 1)
        s = min(max(int(s), 0), n_bins - 1)
        return bivar_colors[r][s]

    merged["bivar_color"] = [
        _bivar_color(r, s) for r, s in zip(merged["renta_q"], merged["serv_q"])
    ]

    # plot bivariado
    fig, ax = plt.subplots(figsize=(10, 10))
    merged.plot(color=merged["bivar_color"], ax=ax, linewidth=0.2, edgecolor="#333333")
    ax.set_title("Crecimiento renta ajustada vs delta sector servicios (2021-2023)")
    ax.set_axis_off()
    fig.text(
        0.02,
        0.02,
        (
            f"corr(renta ajustada, delta servicios) = {corr_serv:.3f}"
            if np.isfinite(corr_serv)
            else "corr(renta ajustada, delta servicios) = n/d"
        ),
        ha="left",
        va="bottom",
        fontsize=10,
        bbox={"facecolor": "white", "alpha": 0.75, "edgecolor": "none"},
    )

    # leyenda bivariada
    legend_ax = fig.add_axes([0.68, 0.08, 0.25, 0.25])
    legend_ax.set_xlim(0, n_bins)
    legend_ax.set_ylim(0, n_bins)
    for i in range(n_bins):
        for j in range(n_bins):
            legend_ax.add_patch(
                plt.Rectangle(
                    (j, i),
                    1,
                    1,
                    facecolor=bivar_colors[i][j],
                    edgecolor="white",
                )
            )
    legend_ax.set_xticks([0, n_bins])
    legend_ax.set_yticks([0, n_bins])
    legend_ax.set_xticklabels(["bajo", "alto"])
    legend_ax.set_yticklabels(["bajo", "alto"])
    legend_ax.set_xlabel("Crecimiento sector servicios")
    legend_ax.set_ylabel("Crecimiento renta ajustada")
    legend_ax.tick_params(length=0)
    legend_ax.set_frame_on(False)

    fig.tight_layout()
    fig.savefig(output_path, dpi=300)
    plt.close(fig)

    context.log.info(
        "mapa_relacion_renta_servicios_2021_2023: mapa generado sin inferir causalidad"
    )
    return str(output_path)


@asset(group_name="geo")
def mapa_relacion_renta_ocupacion_alta_2021_2023(
    context: AssetExecutionContext,
    renta_secciones_geo_2021: gpd.GeoDataFrame,
    renta_secciones_geo_2023: gpd.GeoDataFrame,
    ocupacion_secciones: pd.DataFrame,
) -> str:
    """Mapa bivariado entre crecimiento de renta ajustada y ocupación de nivel alta.

    - Color: combinacion de cuantiles (renta ajustada x ocupacion alta)
    """
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUTS_DIR / "geo_relacion_renta_ocupacion_alta_2021_2023.png"

    if renta_secciones_geo_2021 is None or renta_secciones_geo_2021.empty:
        context.log.error(
            "mapa_relacion_renta_ocupacion_alta_2021_2023: GeoDataFrame 2021 vacio"
        )
        raise ValueError("GeoDataFrame 2021 vacio")
    if renta_secciones_geo_2023 is None or renta_secciones_geo_2023.empty:
        context.log.error(
            "mapa_relacion_renta_ocupacion_alta_2021_2023: GeoDataFrame 2023 vacio"
        )
        raise ValueError("GeoDataFrame 2023 vacio")
    if ocupacion_secciones is None or ocupacion_secciones.empty:
        context.log.error(
            "mapa_relacion_renta_ocupacion_alta_2021_2023: ocupacion_secciones vacio"
        )
        raise ValueError("ocupacion_secciones vacio")

    required_renta_cols = {"geocode", "OBS_VALUE", "geometry"}
    if not required_renta_cols.issubset(renta_secciones_geo_2021.columns):
        missing = required_renta_cols - set(renta_secciones_geo_2021.columns)
        raise ValueError(f"Faltan columnas en renta 2021: {sorted(missing)}")
    if not {"geocode", "OBS_VALUE"}.issubset(renta_secciones_geo_2023.columns):
        missing = {"geocode", "OBS_VALUE"} - set(renta_secciones_geo_2023.columns)
        raise ValueError(f"Faltan columnas en renta 2023: {sorted(missing)}")

    # crecimiento de renta (ajustado por media)
    gdf_2021 = renta_secciones_geo_2021[["geocode", "geometry", "OBS_VALUE"]].copy()
    gdf_2021 = gdf_2021.rename(columns={"OBS_VALUE": "OBS_VALUE_2021"})
    df_2023 = renta_secciones_geo_2023[["geocode", "OBS_VALUE"]].copy()
    df_2023 = df_2023.rename(columns={"OBS_VALUE": "OBS_VALUE_2023"})
    renta = gdf_2021.merge(df_2023, on="geocode", how="inner")
    renta["crecimiento_renta"] = (
        (renta["OBS_VALUE_2023"] - renta["OBS_VALUE_2021"])
        / renta["OBS_VALUE_2021"]
        * 100
    )
    renta = renta.replace([np.inf, -np.inf], np.nan)
    renta = renta.dropna(subset=["crecimiento_renta"]).copy()
    if renta.empty:
        raise ValueError("Crecimiento de renta vacio tras limpiar NaN/Inf")
    renta_mean = float(renta["crecimiento_renta"].mean())
    renta["crecimiento_renta_ajustado"] = renta["crecimiento_renta"] - renta_mean

    # filtrar ocupacion alta (Directores/gerentes y profesionales/tecnicos nivel medio o alto)
    ocup = ocupacion_secciones.copy()
    if "ocupacion" not in ocup.columns:
        raise ValueError("ocupacion_secciones no contiene columna 'ocupacion'")
    if "num_casos" not in ocup.columns:
        raise ValueError("ocupacion_secciones no contiene columna 'num_casos'")
    if "geocode" not in ocup.columns:
        raise ValueError("ocupacion_secciones no contiene columna 'geocode'")

    year_col = (
        "ano" if "ano" in ocup.columns else ("año" if "año" in ocup.columns else None)
    )
    if year_col is None:
        raise ValueError("ocupacion_secciones no contiene columna de año")
    ocup[year_col] = pd.to_numeric(ocup[year_col], errors="coerce")
    ocup = ocup[ocup[year_col].isin([2021, 2023])]

    # filtrar Santa Cruz/Tenerife por codigo municipal o geocode
    if "code_municipio" in ocup.columns:
        mask = ocup["code_municipio"].astype(str).str.startswith("38")
    else:
        mask = ocup["geocode"].astype(str).str.contains("_38")
    ocup = ocup[mask]
    if ocup.empty:
        raise ValueError("Sin datos de ocupacion para Santa Cruz de Tenerife")

    # normalizar texto de ocupacion para matching robusto
    translations = str.maketrans("áéíóúüñÁÉÍÓÚÜÑ", "aeiouunAEIOUUN")
    target = "Directores/gerentes y profesionales/técnicos de nivel medio o alto"
    target_norm = (
        target.strip()
        .translate(translations)
        .replace(" ", "_")
        .replace("/", "_")
        .replace("-", "_")
        .lower()
    )
    ocup_norm = (
        ocup["ocupacion"]
        .astype(str)
        .str.strip()
        .str.translate(translations)
        .str.replace(" ", "_", regex=False)
        .str.replace("/", "_", regex=False)
        .str.replace("-", "_", regex=False)
        .str.lower()
    )
    ocup = ocup[ocup_norm == target_norm]
    if ocup.empty:
        sample = ocupacion_secciones["ocupacion"].dropna().astype(str).unique()[:5]
        raise ValueError(
            f"No se encontro la categoria de ocupacion alta. Ejemplos: {list(sample)}"
        )

    renta["geocode_norm"] = renta["geocode"].map(normalizar_geocode)
    ocup = ocup.copy()
    ocup["geocode_norm"] = ocup["geocode"].map(normalizar_geocode)
    renta_norm = _log_geocode_profile(
        context,
        "renta ocupacion",
        renta["geocode"],
    )
    ocup_norm_geocodes = _log_geocode_profile(
        context,
        "ocupacion alta",
        ocup["geocode"],
    )
    _log_geocode_overlap(
        context,
        "ocupacion alta | overlap previo normalizacion",
        renta_norm,
        ocup_norm_geocodes,
    )

    # crecimiento ocupacion alta
    occ_grouped = (
        ocup.groupby(["geocode_norm", year_col])["num_casos"]
        .sum()
        .unstack(fill_value=0)
    )
    occ_grouped = occ_grouped.reindex(columns=[2021, 2023], fill_value=0)
    occ_grouped["delta_ocupacion_alta"] = occ_grouped[2023] - occ_grouped[2021]
    occ_grouped = occ_grouped.replace([np.inf, -np.inf], np.nan)

    # unir por geocode
    merged = renta.merge(
        occ_grouped[["delta_ocupacion_alta"]],
        left_on="geocode_norm",
        right_index=True,
        how="left",
    )
    corr_occ_df = _log_correlation_diagnostics(
        context,
        merged,
        ["crecimiento_renta_ajustado", "delta_ocupacion_alta"],
        "diagnostico correlacion ocupacion_alta",
    )
    if len(corr_occ_df) >= 2 and all(
        corr_occ_df[column].nunique(dropna=True) > 1
        for column in ["crecimiento_renta_ajustado", "delta_ocupacion_alta"]
    ):
        corr_occ = (
            corr_occ_df[["crecimiento_renta_ajustado", "delta_ocupacion_alta"]]
            .corr()
            .iloc[0, 1]
        )
        context.log.info(
            f"correlacion limpia renta ajustada vs delta_ocupacion_alta: {corr_occ:.4f}"
            if np.isfinite(corr_occ)
            else "correlacion limpia renta ajustada vs delta_ocupacion_alta: n/d"
        )
    else:
        corr_occ = np.nan
        context.log.warning(
            "diagnostico correlacion ocupacion_alta | correlacion omitida por falta de filas o variabilidad"
        )

    matched_rows = int(merged["delta_ocupacion_alta"].notna().sum())
    merge_coverage = float(matched_rows / len(merged) * 100) if len(merged) else 0.0
    context.log.info(
        "ocupacion alta | filas emparejadas=%s/%s | cobertura merge=%.2f%%",
        matched_rows,
        len(merged),
        merge_coverage,
    )
    if merge_coverage < 80.0:
        context.log.warning(
            "ocupacion alta | cobertura del merge por debajo del 80%%; revisar normalizacion de geocodes"
        )
    merged["delta_ocupacion_alta"] = merged["delta_ocupacion_alta"].fillna(0)

    # clasificar en cuantiles (3 bins)
    n_bins = 3
    renta_q = pd.qcut(
        merged["crecimiento_renta_ajustado"],
        q=n_bins,
        labels=False,
        duplicates="drop",
    )
    ocup_q = pd.qcut(
        merged["delta_ocupacion_alta"],
        q=n_bins,
        labels=False,
        duplicates="drop",
    )
    if renta_q.isna().any() or ocup_q.isna().any():
        context.log.warning("Cuantiles con NaN; se rellenan con 0")
    merged["renta_q"] = renta_q.fillna(0).astype(int)
    merged["ocup_q"] = ocup_q.fillna(0).astype(int)

    bivar_colors = [
        ["#f1eef6", "#bdc9e1", "#6a51a3"],
        ["#c7e9c0", "#74c476", "#238b45"],
        ["#fdd0a2", "#f16913", "#8c2d04"],
    ]

    def _bivar_color(r, s):
        r = min(max(int(r), 0), n_bins - 1)
        s = min(max(int(s), 0), n_bins - 1)
        return bivar_colors[r][s]

    merged["bivar_color"] = [
        _bivar_color(r, s) for r, s in zip(merged["renta_q"], merged["ocup_q"])
    ]

    fig, ax = plt.subplots(figsize=(10, 10))
    merged.plot(color=merged["bivar_color"], ax=ax, linewidth=0.2, edgecolor="#333333")
    ax.set_title("Crecimiento renta ajustada vs delta alta cualificación (2021-2023)")
    ax.set_axis_off()
    fig.text(
        0.02,
        0.02,
        (
            f"corr(renta ajustada, delta ocupacion alta) = {corr_occ:.3f}"
            if np.isfinite(corr_occ)
            else "corr(renta ajustada, delta ocupacion alta) = n/d"
        ),
        ha="left",
        va="bottom",
        fontsize=10,
        bbox={"facecolor": "white", "alpha": 0.75, "edgecolor": "none"},
    )

    legend_ax = fig.add_axes([0.68, 0.08, 0.25, 0.25])
    legend_ax.set_xlim(0, n_bins)
    legend_ax.set_ylim(0, n_bins)
    for i in range(n_bins):
        for j in range(n_bins):
            legend_ax.add_patch(
                plt.Rectangle(
                    (j, i),
                    1,
                    1,
                    facecolor=bivar_colors[i][j],
                    edgecolor="white",
                )
            )
    legend_ax.set_xticks([0, n_bins])
    legend_ax.set_yticks([0, n_bins])
    legend_ax.set_xticklabels(["bajo", "alto"])
    legend_ax.set_yticklabels(["bajo", "alto"])
    legend_ax.set_xlabel("Crecimiento alta cualificación")
    legend_ax.set_ylabel("Crecimiento renta ajustada")
    legend_ax.tick_params(length=0)
    legend_ax.set_frame_on(False)

    fig.tight_layout()
    fig.savefig(output_path, dpi=300)
    plt.close(fig)

    context.log.info(
        "mapa_relacion_renta_ocupacion_alta_2021_2023: mapa generado sin inferir causalidad"
    )
    return str(output_path)


@asset(group_name="geo")
def renta_variacion_geo_2021_2023(
    context: AssetExecutionContext,
    renta_secciones_geo_2021: gpd.GeoDataFrame,
    renta_secciones_geo_2023: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Calcula variación porcentual de renta 2021-2023 por sección censal."""
    if renta_secciones_geo_2021 is None or renta_secciones_geo_2021.empty:
        context.log.error("renta_variacion_geo_2021_2023: GeoDataFrame 2021 vacío")
        raise ValueError("GeoDataFrame 2021 vacío para variación")

    if renta_secciones_geo_2023 is None or renta_secciones_geo_2023.empty:
        context.log.error("renta_variacion_geo_2021_2023: GeoDataFrame 2023 vacío")
        raise ValueError("GeoDataFrame 2023 vacío para variación")

    required_cols = {"geocode", "OBS_VALUE", "geometry"}
    if not required_cols.issubset(renta_secciones_geo_2021.columns):
        missing = required_cols - set(renta_secciones_geo_2021.columns)
        raise ValueError(f"Faltan columnas en 2021: {sorted(missing)}")

    if not {"geocode", "OBS_VALUE"}.issubset(renta_secciones_geo_2023.columns):
        missing = {"geocode", "OBS_VALUE"} - set(renta_secciones_geo_2023.columns)
        raise ValueError(f"Faltan columnas en 2023: {sorted(missing)}")

    gdf_2021 = renta_secciones_geo_2021[["geocode", "geometry", "OBS_VALUE"]].copy()
    gdf_2021 = gdf_2021.rename(columns={"OBS_VALUE": "OBS_VALUE_2021"})
    df_2023 = renta_secciones_geo_2023[["geocode", "OBS_VALUE"]].copy()
    df_2023 = df_2023.rename(columns={"OBS_VALUE": "OBS_VALUE_2023"})

    merged = gdf_2021.merge(df_2023, on="geocode", how="inner")
    if merged.empty:
        context.log.error(
            "renta_variacion_geo_2021_2023: merge vacío tras unir 2021 y 2023"
        )
        raise ValueError("Merge vacío para variación 2021-2023")

    merged["variacion_pct"] = (
        (merged["OBS_VALUE_2023"] - merged["OBS_VALUE_2021"])
        / merged["OBS_VALUE_2021"]
        * 100
    )
    merged = merged.replace([np.inf, -np.inf], np.nan)
    merged = merged.dropna(subset=["variacion_pct"]).copy()

    if merged.empty:
        context.log.error(
            "renta_variacion_geo_2021_2023: variación vacía tras limpiar NaN/Inf"
        )
        raise ValueError("Variación vacía tras limpiar NaN/Inf")

    return merged


@asset(group_name="geo")
def mapa_renta_secciones_2021(
    context: AssetExecutionContext,
    renta_secciones_geo_2021: gpd.GeoDataFrame,
    renta_secciones_bins_global: dict,
) -> str:
    """Genera un mapa coropletico de renta 2021 por seccion censal."""
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUTS_DIR / "geo_renta_secciones_2021.png"

    if renta_secciones_geo_2021 is None or renta_secciones_geo_2021.empty:
        context.log.error(
            "mapa_renta_secciones_2021: GeoDataFrame vacío; abortando mapado."
        )
        raise ValueError("GeoDataFrame vacío para mapa 2021")

    if "OBS_VALUE" not in renta_secciones_geo_2021.columns:
        context.log.error(
            "mapa_renta_secciones_2021: falta columna 'OBS_VALUE' en GeoDataFrame"
        )
        raise ValueError("Columna 'OBS_VALUE' no encontrada en GeoDataFrame 2021")

    # usar bins globales calculados por el asset renta_secciones_bins_global
    if not isinstance(renta_secciones_bins_global, dict):
        context.log.error(
            "renta_secciones_bins_global debe ser un dict con keys 'ud_bins' y 'labels'"
        )
        raise ValueError("renta_secciones_bins_global inválido")

    bins = renta_secciones_bins_global.get("bins")
    ud_bins = renta_secciones_bins_global.get("ud_bins")
    labels = renta_secciones_bins_global.get("labels")

    if bins is None or ud_bins is None or labels is None:
        context.log.error(
            "renta_secciones_bins_global no contiene 'bins'/'ud_bins'/'labels'"
        )
        raise ValueError("renta_secciones_bins_global incompleto")

    if len(labels) != (len(ud_bins) + 1):
        msg = f"Número de labels {len(labels)} no coincide con número de clases {len(ud_bins)+1}"
        context.log.error(msg)
        raise ValueError(msg)

    fig, ax = plt.subplots(figsize=(10, 10))
    renta_secciones_geo_2021.plot(
        column="OBS_VALUE",
        cmap="YlOrRd",
        legend=True,
        scheme="UserDefined",
        classification_kwds={"bins": ud_bins},
        legend_kwds={"labels": labels},
        ax=ax,
    )
    ax.set_title("Renta bruta media por seccion censal (Tenerife, 2021)")
    ax.set_axis_off()
    fig.tight_layout()
    fig.savefig(output_path, dpi=300)
    plt.close(fig)

    return str(output_path)


@asset(group_name="geo")
def mapa_renta_secciones_2023(
    context: AssetExecutionContext,
    renta_secciones_geo_2021: gpd.GeoDataFrame,
    renta_secciones_geo_2023: gpd.GeoDataFrame,
    renta_secciones_bins_global: dict,
) -> str:
    """Genera un mapa coropletico de renta 2023 con clases de 2021."""
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUTS_DIR / "geo_renta_secciones_2023.png"

    if renta_secciones_geo_2021 is None or renta_secciones_geo_2021.empty:
        context.log.error(
            "mapa_renta_secciones_2023: GeoDataFrame 2021 vacío; abortando."
        )
        raise ValueError("GeoDataFrame 2021 vacío para mapa 2023")

    if renta_secciones_geo_2023 is None or renta_secciones_geo_2023.empty:
        context.log.error(
            "mapa_renta_secciones_2023: GeoDataFrame 2023 vacío; abortando."
        )
        raise ValueError("GeoDataFrame 2023 vacío para mapa 2023")

    if "OBS_VALUE" not in renta_secciones_geo_2021.columns:
        context.log.error(
            "mapa_renta_secciones_2023: falta 'OBS_VALUE' en GeoDataFrame 2021"
        )
        raise ValueError("Columna 'OBS_VALUE' no encontrada en GeoDataFrame 2021")

    if "OBS_VALUE" not in renta_secciones_geo_2023.columns:
        context.log.error(
            "mapa_renta_secciones_2023: falta 'OBS_VALUE' en GeoDataFrame 2023"
        )
        raise ValueError("Columna 'OBS_VALUE' no encontrada en GeoDataFrame 2023")

    # usar bins globales calculados por el asset renta_secciones_bins_global
    if not isinstance(renta_secciones_bins_global, dict):
        context.log.error(
            "renta_secciones_bins_global debe ser un dict con keys 'ud_bins' y 'labels'"
        )
        raise ValueError("renta_secciones_bins_global inválido")

    bins = renta_secciones_bins_global.get("bins")
    ud_bins = renta_secciones_bins_global.get("ud_bins")
    labels = renta_secciones_bins_global.get("labels")

    if bins is None or ud_bins is None or labels is None:
        context.log.error(
            "renta_secciones_bins_global no contiene 'bins'/'ud_bins'/'labels'"
        )
        raise ValueError("renta_secciones_bins_global incompleto")

    context.log.debug(
        f"mapa_renta_secciones_2023: bins(len)={len(bins)}, ud_bins(len)={len(ud_bins)}, labels(len)={len(labels)}"
    )
    if len(labels) != (len(ud_bins) + 1):
        msg = f"Número de labels {len(labels)} no coincide con número de clases {len(ud_bins)+1}"
        context.log.error(msg)
        raise ValueError(msg)

    fig, ax = plt.subplots(figsize=(10, 10))
    renta_secciones_geo_2023.plot(
        column="OBS_VALUE",
        cmap="YlOrRd",
        legend=True,
        scheme="UserDefined",
        classification_kwds={"bins": ud_bins},
        legend_kwds={"labels": labels},
        ax=ax,
    )
    ax.set_title("Renta bruta media por seccion censal (Tenerife, 2023)")
    ax.set_axis_off()
    fig.tight_layout()
    fig.savefig(output_path, dpi=300)
    plt.close(fig)

    return str(output_path)


@asset(group_name="geo")
def mapa_variacion_renta_2021_2023(
    context: AssetExecutionContext,
    renta_variacion_geo_2021_2023: gpd.GeoDataFrame,
) -> str:
    """Genera mapa coroplético de variación porcentual 2021-2023."""
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUTS_DIR / "geo_renta_variacion_2021_2023.png"

    if renta_variacion_geo_2021_2023 is None or renta_variacion_geo_2021_2023.empty:
        context.log.error("mapa_variacion_renta_2021_2023: GeoDataFrame vacío")
        raise ValueError("GeoDataFrame vacío para mapa de variación")

    if "variacion_pct" not in renta_variacion_geo_2021_2023.columns:
        context.log.error(
            "mapa_variacion_renta_2021_2023: falta columna 'variacion_pct'"
        )
        raise ValueError("Columna 'variacion_pct' no encontrada")

    # leer percentiles de configuración si están presentes (opcional)
    cfg = getattr(context, "op_config", {}) or {}
    try:
        p_low = float(cfg.get("clip_p_low", cfg.get("p_low", 5)))
        p_high = float(cfg.get("clip_p_high", cfg.get("p_high", 95)))
    except Exception:
        p_low, p_high = 5.0, 95.0

    vals = renta_variacion_geo_2021_2023["variacion_pct"].dropna().values
    if vals.size == 0:
        context.log.error(
            "mapa_variacion_renta_2021_2023: no hay valores para calcular percentiles"
        )
        raise ValueError("No hay valores para calcular percentiles")

    lower = float(np.nanpercentile(vals, p_low))
    upper = float(np.nanpercentile(vals, p_high))

    # si percentiles dan un rango inválido, caer al comportamiento simétrico por defecto
    if not (np.isfinite(lower) and np.isfinite(upper)) or lower >= upper:
        vmin = float(renta_variacion_geo_2021_2023["variacion_pct"].min())
        vmax = float(renta_variacion_geo_2021_2023["variacion_pct"].max())
        max_abs = max(abs(vmin), abs(vmax))
        if not np.isfinite(max_abs) or max_abs == 0:
            context.log.error(
                "mapa_variacion_renta_2021_2023: rango de variación inválido"
            )
            raise ValueError("Rango de variación inválido")
        lower, upper = -max_abs, max_abs

    # garantizar que 0 quede entre lower y upper para TwoSlopeNorm
    if not (lower < 0.0 < upper):
        context.log.warning(
            f"Los límites calculados no encierran 0 (lower={lower:.3f}, upper={upper:.3f}); ajustando a simétrico alrededor de 0"
        )
        max_abs = max(abs(lower), abs(upper), 1e-6)
        lower, upper = -max_abs, max_abs

    # contar recortes
    n_below = int((renta_variacion_geo_2021_2023["variacion_pct"] < lower).sum())
    n_above = int((renta_variacion_geo_2021_2023["variacion_pct"] > upper).sum())
    context.log.info(
        f"Clipping variación_pct a percentiles (p_low={p_low}, p_high={p_high}): lower={lower:.2f}, upper={upper:.2f}; recortados_below={n_below}, recortados_above={n_above}"
    )

    renta_variacion_geo_2021_2023 = renta_variacion_geo_2021_2023.copy()
    renta_variacion_geo_2021_2023["variacion_pct_clip"] = renta_variacion_geo_2021_2023[
        "variacion_pct"
    ].clip(lower, upper)

    norm = TwoSlopeNorm(vmin=lower, vcenter=0, vmax=upper)

    fig, ax = plt.subplots(figsize=(10, 10))
    img = renta_variacion_geo_2021_2023.plot(
        column="variacion_pct_clip",
        cmap="RdYlGn",
        legend=True,
        norm=norm,
        ax=ax,
    )
    ax.set_title("Variación porcentual de renta (2021 vs 2023)")
    ax.set_axis_off()
    # ajustar ticks de la barra de color para mostrar porcentajes legibles
    try:
        cbar_ax = img.get_figure().axes[-1]
        ticks = [lower, (lower + 0) / 2.0, 0.0, (upper + 0) / 2.0, upper]
        # evitar ticks duplicados
        ticks = sorted(list(dict.fromkeys([float(t) for t in ticks])))
        cbar_ax.set_yticks(ticks)
        cbar_ax.set_yticklabels([f"{int(t)}%" for t in ticks])
    except Exception:
        # no fatal; seguir y guardar la figura
        context.log.debug("No se pudo ajustar ticks de la barra de color")

    fig.tight_layout()
    fig.savefig(output_path, dpi=300)
    plt.close(fig)

    return str(output_path)
