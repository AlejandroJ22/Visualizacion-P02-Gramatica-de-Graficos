"""
Assets Dagster para el pipeline de distribución de renta en Canarias.
Cadena: carga -> limpieza -> unión -> visualización.
"""

from pathlib import Path
from textwrap import fill

import pandas as pd
from dagster import asset
from plotnine import (
    aes,
    coord_flip,
    element_text,
    geom_area,
    geom_bar,
    geom_line,
    geom_point,
    ggplot,
    labs,
    scale_color_manual,
    scale_fill_manual,
    scale_x_continuous,
    theme,
    theme_minimal,
)

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
OUTPUTS_DIR = BASE_DIR / "outputs"

RENTA_CSV = DATA_DIR / "distribucion-renta-canarias.csv"
CODISLAS_CSV = DATA_DIR / "codislas.csv"
NIVELES_ESTUDIOS_XLSX = DATA_DIR / "nivelestudios.xlsx"

TERRITORIOS_EXCLUIR = [
    "Canarias",
    "Las Palmas",
    "Santa Cruz de Tenerife",
    "Lanzarote",
    "Fuerteventura",
    "Gran Canaria",
    "Tenerife",
    "La Gomera",
    "La Palma",
    "El Hierro",
]

COLORES_RENTA = {
    "Sueldos y salarios": "#1f77b4",
    "Pensiones": "#ff7f0e",
    "Otros ingresos": "#2ca02c",
    "Prestaciones por desempleo": "#d62728",
    "Otras prestaciones": "#9467bd",
}


def _guardar_grafico(grafico, nombre_archivo: str, dpi: int = 300) -> str:
    """Guarda un gráfico en outputs y devuelve la ruta del archivo generado."""
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUTS_DIR / nombre_archivo
    grafico.save(str(output_path), dpi=dpi)
    return str(output_path)


@asset(group_name="renta")
def carga_renta_raw() -> pd.DataFrame:
    df = pd.read_csv(RENTA_CSV, sep=",")
    return df


@asset(group_name="renta")
def carga_codislas_raw() -> pd.DataFrame:
    df = pd.read_csv(CODISLAS_CSV, sep=";", encoding="latin-1")
    return df


@asset(group_name="estudios")
def carga_niveles_estudios_raw() -> pd.DataFrame:
    """Carga el dataset de niveles de estudios desde Excel."""
    df = pd.read_excel(NIVELES_ESTUDIOS_XLSX)
    return df


@asset(group_name="renta")
def limpieza_renta(carga_renta_raw: pd.DataFrame) -> pd.DataFrame:
    df = carga_renta_raw.copy()
    df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    df["TIME_PERIOD_CODE"] = pd.to_numeric(df["TIME_PERIOD_CODE"], errors="coerce")
    df = df.dropna(
        subset=["OBS_VALUE", "TIME_PERIOD_CODE", "TERRITORIO#es", "MEDIDAS#es"]
    )
    df["TIME_PERIOD_CODE"] = df["TIME_PERIOD_CODE"].astype(int)
    return df


@asset(group_name="renta")
def limpieza_codislas(carga_codislas_raw: pd.DataFrame) -> pd.DataFrame:
    df = carga_codislas_raw.copy()
    df["CPRO"] = df["CPRO"].astype(str).str.zfill(2)
    df["CMUN"] = df["CMUN"].astype(str).str.zfill(3)
    df["TERRITORIO_CODE"] = df["CPRO"] + df["CMUN"]
    df = df.rename(columns={"NOMBRE": "NOMBRE_CODISLAS", "ISLA": "ISLA_CODISLAS"})
    return df[["TERRITORIO_CODE", "ISLA_CODISLAS", "NOMBRE_CODISLAS"]].drop_duplicates()


@asset(group_name="estudios")
def limpieza_niveles_estudios(carga_niveles_estudios_raw: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nombres de columnas y tipa campos clave para su análisis."""
    df = carga_niveles_estudios_raw.copy()
    df = df.rename(
        columns={
            "Municipios de 500 habitantes o más": "municipio",
            "Sexo": "sexo",
            "Nacionalidad": "nacionalidad",
            "Nivel de estudios en curso": "nivel_estudios",
            "Periodo": "periodo",
            "Total": "total",
        }
    )
    df["total"] = pd.to_numeric(df["total"], errors="coerce")
    df["periodo"] = pd.to_datetime(df["periodo"], errors="coerce")
    df = df.dropna(
        subset=[
            "municipio",
            "sexo",
            "nacionalidad",
            "nivel_estudios",
            "periodo",
            "total",
        ]
    )
    return df


@asset(group_name="estudios")
def niveles_estudios_ultimo_periodo(
    limpieza_niveles_estudios: pd.DataFrame,
) -> pd.DataFrame:
    """Agrega niveles de estudios del último período para población que cursa estudios."""
    df = limpieza_niveles_estudios.copy()
    ultimo_periodo = df["periodo"].max()

    df_filtrado = df[
        (df["periodo"] == ultimo_periodo)
        & (df["sexo"] == "Total")
        & (df["nivel_estudios"] != "Total")
        & (df["nivel_estudios"] != "No cursa estudios")
    ].copy()

    df_agg = (
        df_filtrado.groupby("nivel_estudios", as_index=False)["total"]
        .sum()
        .sort_values("total", ascending=False)
    )
    total_global = df_agg["total"].sum()
    if total_global <= 0:
        df_agg["porcentaje"] = 0.0
    else:
        df_agg["porcentaje"] = (df_agg["total"] / total_global) * 100
    return df_agg


@asset(group_name="renta")
def renta_municipal_2023(limpieza_renta: pd.DataFrame) -> pd.DataFrame:
    df = limpieza_renta.copy()
    df_2023 = df[
        (df["TIME_PERIOD_CODE"] == 2023)
        & (~df["TERRITORIO#es"].isin(TERRITORIOS_EXCLUIR))
    ].copy()
    return df_2023


@asset(group_name="renta")
def union_renta_codislas(
    renta_municipal_2023: pd.DataFrame,
    limpieza_codislas: pd.DataFrame,
) -> pd.DataFrame:
    df = renta_municipal_2023.merge(
        limpieza_codislas,
        on="TERRITORIO_CODE",
        how="left",
    )
    return df


@asset(group_name="renta")
def renta_canarias_temporal(limpieza_renta: pd.DataFrame) -> pd.DataFrame:
    return limpieza_renta[limpieza_renta["TERRITORIO#es"] == "Canarias"].copy()


@asset(group_name="visualizaciones")
def visualizacion_evolucion_renta(renta_canarias_temporal: pd.DataFrame) -> str:
    grafico = (
        ggplot(
            renta_canarias_temporal,
            aes(
                x="TIME_PERIOD_CODE",
                y="OBS_VALUE",
                color="MEDIDAS#es",
                group="MEDIDAS#es",
            ),
        )
        + geom_line(size=1)
        + geom_point(size=2)
        + labs(
            title="Evolución de la distribución de rentas en Canarias (2015–2023)",
            x="Año",
            y="Porcentaje de participación",
            color="Tipo de renta",
        )
        + scale_color_manual(values=COLORES_RENTA)
        + scale_x_continuous(
            breaks=sorted(renta_canarias_temporal["TIME_PERIOD_CODE"].unique())
        )
        + theme_minimal()
        + theme(
            figure_size=(11, 7),
            plot_title=element_text(weight="bold", size=14),
            legend_position="bottom",
        )
    )

    return _guardar_grafico(grafico, "dagster_v1_evolucion_temporal.png")


@asset(group_name="visualizaciones")
def visualizacion_distribucion_municipio(union_renta_codislas: pd.DataFrame) -> str:
    df_plot = union_renta_codislas.copy()
    df_plot["municipio_nombre"] = df_plot["NOMBRE_CODISLAS"].fillna(
        df_plot["TERRITORIO#es"]
    )

    orden_municipios = (
        df_plot[df_plot["MEDIDAS#es"] == "Sueldos y salarios"]
        .sort_values("OBS_VALUE", ascending=False)["municipio_nombre"]
        .tolist()
    )

    df_plot["municipio_ord"] = pd.Categorical(
        df_plot["municipio_nombre"], categories=orden_municipios, ordered=True
    )

    grafico = (
        ggplot(df_plot, aes(x="municipio_ord", y="OBS_VALUE", fill="MEDIDAS#es"))
        + geom_bar(stat="identity")
        + coord_flip()
        + labs(
            title="Distribución de renta por municipio (2023)",
            x="Municipio",
            y="Porcentaje",
            fill="Tipo de renta",
        )
        + scale_fill_manual(values=COLORES_RENTA)
        + theme_minimal()
        + theme(
            figure_size=(13, 16),
            plot_title=element_text(weight="bold", size=14),
            legend_position="bottom",
        )
    )

    return _guardar_grafico(grafico, "dagster_v2_distribucion_municipio_2023.png")


@asset(group_name="visualizaciones")
def visualizacion_composicion_estructural(renta_canarias_temporal: pd.DataFrame) -> str:
    grafico = (
        ggplot(
            renta_canarias_temporal,
            aes(x="TIME_PERIOD_CODE", y="OBS_VALUE", fill="MEDIDAS#es"),
        )
        + geom_area()
        + labs(
            title="Composición estructural de la renta en Canarias",
            x="Año",
            y="Porcentaje acumulado",
            fill="Tipo de renta",
        )
        + scale_x_continuous(
            breaks=sorted(renta_canarias_temporal["TIME_PERIOD_CODE"].unique())
        )
        + scale_fill_manual(values=COLORES_RENTA)
        + theme_minimal()
        + theme(
            figure_size=(11, 7),
            plot_title=element_text(weight="bold", size=14),
            legend_position="bottom",
        )
    )

    return _guardar_grafico(grafico, "dagster_v3_composicion_estructural.png")


@asset(group_name="visualizaciones")
def visualizacion_niveles_estudios(
    niveles_estudios_ultimo_periodo: pd.DataFrame,
) -> str:
    orden_niveles = niveles_estudios_ultimo_periodo["nivel_estudios"].tolist()
    df_plot = niveles_estudios_ultimo_periodo.copy()
    df_plot["nivel_estudios_label"] = df_plot["nivel_estudios"].apply(
        lambda txt: fill(str(txt), width=52)
    )
    orden_niveles_label = [fill(str(txt), width=52) for txt in orden_niveles]
    df_plot["nivel_estudios_ord"] = pd.Categorical(
        df_plot["nivel_estudios_label"], categories=orden_niveles_label, ordered=True
    )

    grafico = (
        ggplot(df_plot, aes(x="nivel_estudios_ord", y="porcentaje"))
        + geom_bar(stat="identity", fill="#1f77b4")
        + coord_flip()
        + labs(
            title="Niveles de estudios en curso (último periodo, sin 'No cursa estudios')",
            x="Nivel de estudios",
            y="Porcentaje sobre población que cursa estudios",
        )
        + theme_minimal()
        + theme(
            figure_size=(12, 8),
            plot_title=element_text(weight="bold", size=14),
            axis_text_y=element_text(size=10),
        )
    )

    return _guardar_grafico(grafico, "dagster_v4_niveles_estudios_ultimo_periodo.png")


@asset(group_name="renta")
def resumen_pipeline(
    carga_renta_raw: pd.DataFrame,
    renta_municipal_2023: pd.DataFrame,
    union_renta_codislas: pd.DataFrame,
    limpieza_niveles_estudios: pd.DataFrame,
    niveles_estudios_ultimo_periodo: pd.DataFrame,
    visualizacion_evolucion_renta: str,
    visualizacion_distribucion_municipio: str,
    visualizacion_composicion_estructural: str,
    visualizacion_niveles_estudios: str,
) -> dict:
    return {
        "filas_renta_raw": len(carga_renta_raw),
        "filas_municipios_2023": len(renta_municipal_2023),
        "filas_union": len(union_renta_codislas),
        "filas_niveles_estudios_raw_limpio": len(limpieza_niveles_estudios),
        "filas_niveles_estudios_ultimo_periodo": len(niveles_estudios_ultimo_periodo),
        "salidas": [
            visualizacion_evolucion_renta,
            visualizacion_distribucion_municipio,
            visualizacion_composicion_estructural,
            visualizacion_niveles_estudios,
        ],
    }
