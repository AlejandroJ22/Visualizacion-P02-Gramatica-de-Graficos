# Análisis de Checks: Carga, Transformación y Visualización

A partir del análisis del pipeline `renta_assets.py`, se identifican puntos críticos donde conviene introducir validaciones automáticas.  
Los checks se agrupan en tres niveles: **Carga (Raw)**, **Transformación (Curated)** y **Visualización (Asset)**.

El objetivo es garantizar calidad de datos, coherencia estructural y veracidad visual antes de generar los gráficos finales.

---

## Tabla de Checks Propuestos e Implementados

| Etapa | Nombre del Check | Descripción Técnica | Cómo programarlo (Lógica) | Relación con el Diseño / Gestalt | Información para Metadata |
|-------|------------------|--------------------|---------------------------|----------------------------------|----------------------------|
| Carga (Raw) | check_nulos_renta | Verifica que no existan nulos en variables clave del CSV de renta. | `df[cols_clave].isna().sum().sum() == 0` | Figura-Fondo: Huecos inesperados distorsionan la forma del gráfico. | porcentaje_nulos, columnas_afectadas |
| Carga (Raw) | check_rango_porcentajes | Asegura que `OBS_VALUE` esté entre 0 y 100. | `(df["OBS_VALUE"].between(0,100)).all()` | Proporcionalidad: Valores fuera de rango distorsionan escalas. | min_value, max_value |
| Transformación (Curated) | check_cardinalidad_medidas | Valida que existan exactamente 5 tipos de renta. | `df["MEDIDAS#es"].nunique() == 5` | Similitud: Mantiene coherencia en la paleta de colores. | n_medidas_detectadas |
| Transformación (Curated) | check_continuidad_temporal | Verifica que la serie 2015–2023 esté completa. | `sorted(df["TIME_PERIOD_CODE"].unique()) == list(range(2015,2024))` | Continuidad: Evita pendientes falsas en líneas temporales. | años_detectados, años_faltantes |
| Transformación (Curated) | check_union_codislas | Detecta municipios sin correspondencia tras el merge. | `df["ISLA_CODISLAS"].isna().sum() == 0` | Similitud: Evita categorías sin identidad territorial. | filas_sin_isla |
| Visualización (Asset) | check_eje_cero_barras | Asegura que los gráficos de barras parten de 0. | Validar límites del eje Y antes de guardar. | Veracidad Visual: Evita exagerar diferencias. | limite_inferior_eje |
| Visualización (Asset) | check_consistencia_color | Verifica que los tipos de renta coincidan con `COLORES_RENTA`. | `set(df["MEDIDAS#es"]) == set(COLORES_RENTA.keys())` | Similitud: Un color estable por categoría. | categorias_detectadas, palette_id |
| Visualización (Asset) | check_orden_municipios | Comprueba que el orden sea descendente según valor. | `list(valores) == sorted(valores, reverse=True)` | Continuidad / Prägnanz: Facilita lectura visual fluida. | is_sorted (bool), sugerencia |

---

## Justificación General

### 1. Checks de Carga
Garantizan que los datos brutos no contengan errores estructurales (nulos, rangos inválidos).  
Si fallan aquí, el pipeline debe detenerse.

### 2. Checks de Transformación
Validan coherencia tras limpieza y agregaciones:
- Número correcto de categorías.
- Serie temporal completa.
- Integridad tras el merge con `codislas`.
- Normalización previa de `TERRITORIO_CODE` en limpieza para resolver códigos con sufijos históricos (p. ej. `38013_2007`) y evitar falsos nulos en la unión.

Estos checks evitan errores semánticos que afectarían directamente a escalas, colores y agrupaciones.

### 3. Checks de Visualización
Controlan aspectos perceptivos:
- Ejes correctamente definidos.
- Consistencia cromática.
- Orden lógico en barras.

Aquí se conecta explícitamente **DataOps con principios Gestalt y Gramática de Gráficos**.

---

## Conclusión

La incorporación sistemática de checks en cada etapa:

- Refuerza la robustez del pipeline.
- Reduce errores visuales.
- Alinea calidad de datos con calidad perceptiva.
- Integra principios de ingeniería de datos y diseño visual en un único flujo controlado.

El pipeline deja de ser solo un generador de gráficos y pasa a ser un sistema validado de producción visual.

---

## Nota de implementación en Dagster

En la implementación real (`viz_dagster/renta_checks.py`), los checks de visualización se evalúan sobre **assets tabulares previos** (por ejemplo, `union_renta_codislas` y `renta_canarias_temporal`) y no sobre los assets de imagen.

Esto permite validar reglas perceptivas (eje base, orden y consistencia cromática) antes de generar los PNG y evita errores de tipado al ejecutar checks sobre salidas que son rutas (`str`).

Además, `check_orden_municipios` valida el orden descendente sobre los datos ya ordenados para la construcción del gráfico (criterio visual efectivo), no sobre el orden físico original de las filas del DataFrame.