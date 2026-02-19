# Visualización de Renta y Estudios en Canarias

Proyecto de análisis y visualización con **Dagster** (orquestación), **pandas** (transformación) y **plotnine** (gramática de gráficos).

## Estructura real del proyecto

```
VIZ/
├── data/
│   ├── distribucion-renta-canarias.csv
│   ├── codislas.csv
│   ├── nivelestudios.xlsx
│   └── matriz_ocu_anonim.xlsx
├── docs/
│   └── gramatica-graficos-analisis.md
├── outputs/
│   ├── dagster_v1_evolucion_temporal.png
│   ├── dagster_v2_distribucion_municipio_2023.png
│   ├── dagster_v3_composicion_estructural.png
│   ├── dagster_v4_niveles_estudios_ultimo_periodo.png
│   ├── lab_renta_v1_evolucion_temporal.png
│   ├── lab_renta_v2_distribucion_municipio_2023.png
│   └── lab_renta_v3_composicion_estructural.png
├── src/
│   └── lab-renta.py
├── viz_dagster/
│   ├── __init__.py
│   ├── renta_assets.py
│   └── test_asset.py
├── pyproject.toml
├── requirements.txt
└── README.md
```

## Instalación

Desde la raíz del proyecto:

1. Activar entorno virtual
```bash
source venv/bin/activate
```

2. Instalar dependencias
```bash
pip install -r requirements.txt
```

Dependencias clave: `dagster`, `dagster-webserver`, `pandas`, `plotnine`, `openpyxl`.

## Ejecución con Dagster

Inicia Dagster:

```bash
dagster dev -m viz_dagster
```

Abre `http://localhost:3000` y materializa los assets desde la UI.

### Assets principales

- `carga_renta_raw`, `carga_codislas_raw`
- `limpieza_renta`, `limpieza_codislas`
- `renta_municipal_2023`, `union_renta_codislas`, `renta_canarias_temporal`
- `carga_niveles_estudios_raw`, `limpieza_niveles_estudios`, `niveles_estudios_ultimo_periodo`
- `visualizacion_evolucion_renta`
- `visualizacion_distribucion_municipio`
- `visualizacion_composicion_estructural`
- `visualizacion_niveles_estudios`
- `resumen_pipeline`

Las salidas se guardan en `outputs/`.

## Prototipado local

Script disponible:

```bash
python3 src/lab-renta.py
```

## Documentación

- Análisis de decisiones visuales y gramática de gráficos: `docs/gramatica-graficos-analisis.md`

## Estado actual de visualizaciones

- Serie temporal de rentas en Canarias.
- Distribución de renta por municipio (2023).
- Composición estructural de la renta.
- Niveles de estudios en curso (último período, excluyendo `No cursa estudios` para mejorar legibilidad).
