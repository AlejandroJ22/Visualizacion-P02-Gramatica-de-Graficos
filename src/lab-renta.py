"""
lab-renta.py - Prototipado y experimentación con visualizaciones de renta
Diseño iterativo del gráfico antes de integrarlo en Dagster

NOTA: Los datos OBS_VALUE son PORCENTAJES de distribución de renta.
     Cada municipio tiene 5 medidas que suman alrededor del 100%:
     - Sueldos y salarios
     - Otros ingresos
     - Otras prestaciones
     - Pensiones
     - Prestaciones por desempleo
"""

import pandas as pd
from plotnine import *

# ============================================================================
# 1. CARGA DE DATOS
# ============================================================================

print("Cargando datos...")
df = pd.read_csv("data/distribucion-renta-canarias.csv", sep=",")
print(f"- {len(df):,} filas cargadas")

# ============================================================================
# 2. EXPLORACIÓN INICIAL
# ============================================================================

print("\nExplorando estructura de datos:")
print(f"- Columnas: {list(df.columns)}")
print(f"- Años disponibles: {sorted(df['TIME_PERIOD_CODE'].unique())}")
print(f"- Tipos de medidas: {df['MEDIDAS#es'].unique()}")
print(f"- Número de territorios únicos: {df['TERRITORIO#es'].nunique()}")

# ============================================================================
# 3. LIMPIEZA Y TRANSFORMACIÓN
# ============================================================================

print("\nLimpiando datos...")

# Convertir valores a numérico
df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")

# Definir territorios agregados a excluir (queremos solo municipios)
territorios_excluir = [
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

# Filtrar datos del año más reciente (2023)
df_2023 = df[
    (df["TIME_PERIOD_CODE"] == 2023) & (~df["TERRITORIO#es"].isin(territorios_excluir))
].copy()

print(f"- Filtrado a año 2023: {len(df_2023)} filas")
print(f"- Municipios encontrados: {df_2023['TERRITORIO#es'].nunique()}")

# ============================================================================
# 4. ANÁLISIS: Los valores son porcentajes de distribución de renta
# ============================================================================

print("\nIMPORTANTE: OBS_VALUE representa % de distribución de cada fuente de renta")
print("   La suma de todas las fuentes por municipio = 100%")
print("   Vamos a analizar la composición de la renta")

# Pivotear datos para tener cada tipo de renta como columna
df_pivot = df_2023.pivot_table(
    index="TERRITORIO#es", columns="MEDIDAS#es", values="OBS_VALUE", aggfunc="first"
).reset_index()

df_pivot.columns.name = None

# Renombrar columnas para facilitar su uso
df_pivot = df_pivot.rename(columns={"TERRITORIO#es": "municipio"})

print(f"\n- {len(df_pivot)} municipios procesados")

# Verificar que suma 100%
df_pivot["suma_verificacion"] = (
    df_pivot["Sueldos y salarios"]
    + df_pivot["Pensiones"]
    + df_pivot["Otros ingresos"]
    + df_pivot["Prestaciones por desempleo"]
    + df_pivot["Otras prestaciones"]
)
print(f"- Verificación: todas las sumas ≈ {df_pivot['suma_verificacion'].mean():.1f}%")

# Mostrar ejemplos
print(f"\n- Ejemplo de composición de renta:")
for i, row in df_pivot.head(3).iterrows():
    print(f"\n   {row['municipio']}:")
    print(f"      - Sueldos y salarios: {row['Sueldos y salarios']:.1f}%")
    print(f"      - Pensiones: {row['Pensiones']:.1f}%")
    print(f"      - Otros ingresos: {row['Otros ingresos']:.1f}%")
    print(f"      - Prestaciones desempleo: {row['Prestaciones por desempleo']:.1f}%")
    print(f"      - Otras prestaciones: {row['Otras prestaciones']:.1f}%")

# ============================================================================
# 5. PREPARACIÓN PARA VISUALIZACIÓN
# ============================================================================

print("\nPreparando datasets para las 3 visualizaciones de gramática de gráficos...")

# V1: Serie temporal de Canarias (2015-2023)
df_canarias = df[df["TERRITORIO#es"] == "Canarias"].copy()

# V2: Barras apiladas por municipio (año seleccionado: 2023)
df_municipios_2023 = df_2023.copy()

# Ordenar municipios por peso de sueldos y salarios para facilitar lectura
orden_municipios = (
    df_municipios_2023[df_municipios_2023["MEDIDAS#es"] == "Sueldos y salarios"]
    .sort_values("OBS_VALUE", ascending=False)["TERRITORIO#es"]
    .tolist()
)
df_municipios_2023["municipio_ord"] = pd.Categorical(
    df_municipios_2023["TERRITORIO#es"], categories=orden_municipios, ordered=True
)

# V3: Composición estructural agregada en Canarias
df_estructura = df_canarias.copy()

# Paleta fija para mantener colores consistentes entre gráficos
colores_renta = {
    "Sueldos y salarios": "#1f77b4",
    "Pensiones": "#ff7f0e",
    "Otros ingresos": "#2ca02c",
    "Prestaciones por desempleo": "#d62728",
    "Otras prestaciones": "#9467bd",
}

# ============================================================================
# 6. VISUALIZACIÓN V1: Evolución temporal de la distribución de renta
# ============================================================================

print("\nCreando visualización V1: Evolución temporal de la distribución de renta...")

grafico_v1 = (
    ggplot(
        df_canarias,
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
    + scale_color_manual(values=colores_renta)
    + scale_x_continuous(breaks=sorted(df_canarias["TIME_PERIOD_CODE"].unique()))
    + theme_minimal()
    + theme(
        figure_size=(11, 7),
        plot_title=element_text(weight="bold", size=14),
        legend_position="bottom",
    )
)

output_path_v1 = "outputs/lab_renta_v1_evolucion_temporal.png"
grafico_v1.save(output_path_v1, dpi=300)
print(f"- Guardado: {output_path_v1}")

# ============================================================================
# 7. VISUALIZACIÓN V2: Comparación territorial por municipio (año 2023)
# ============================================================================

print("\nCreando visualización V2: Distribución de renta por municipio (2023)...")

grafico_v2 = (
    ggplot(df_municipios_2023, aes(x="municipio_ord", y="OBS_VALUE", fill="MEDIDAS#es"))
    + geom_bar(stat="identity")
    + coord_flip()
    + labs(
        title="Distribución de renta por municipio (2023)",
        x="Municipio",
        y="Porcentaje",
        fill="Tipo de renta",
    )
    + scale_fill_manual(values=colores_renta)
    + theme_minimal()
    + theme(
        figure_size=(13, 16),
        plot_title=element_text(weight="bold", size=14),
        legend_position="bottom",
    )
)

output_path_v2 = "outputs/lab_renta_v2_distribucion_municipio_2023.png"
grafico_v2.save(output_path_v2, dpi=300)
print(f"- Guardado: {output_path_v2}")

# ============================================================================
# 8. VISUALIZACIÓN V3: Composición estructural de la renta (área apilada)
# ============================================================================

print("\nCreando visualización V3: Composición estructural de la renta en Canarias...")

grafico_v3 = (
    ggplot(df_estructura, aes(x="TIME_PERIOD_CODE", y="OBS_VALUE", fill="MEDIDAS#es"))
    + geom_area()
    + labs(
        title="Composición estructural de la renta en Canarias",
        x="Año",
        y="Porcentaje acumulado",
        fill="Tipo de renta",
    )
    + scale_x_continuous(breaks=sorted(df_estructura["TIME_PERIOD_CODE"].unique()))
    + scale_fill_manual(values=colores_renta)
    + theme_minimal()
    + theme(
        figure_size=(11, 7),
        plot_title=element_text(weight="bold", size=14),
        legend_position="bottom",
    )
)

output_path_v3 = "outputs/lab_renta_v3_composicion_estructural.png"
grafico_v3.save(output_path_v3, dpi=300)
print(f"- Guardado: {output_path_v3}")

# ============================================================================
# 9. RESUMEN ESTADÍSTICO
# ============================================================================

print("\nResumen estadístico por tipo de renta:")
print("\nSueldos y salarios:")
print(df_pivot["Sueldos y salarios"].describe())
print("\nPensiones:")
print(df_pivot["Pensiones"].describe())
print("\nPrestaciones por desempleo:")
print(df_pivot["Prestaciones por desempleo"].describe())

print("\nPrototipado completado. Revisa los archivos en outputs/:")
print("   - lab_renta_v1_evolucion_temporal.png: Evolución temporal por tipo de renta")
print(
    "   - lab_renta_v2_distribucion_municipio_2023.png: Distribución de renta por municipio"
)
print("   - lab_renta_v3_composicion_estructural.png: Composición estructural agregada")
