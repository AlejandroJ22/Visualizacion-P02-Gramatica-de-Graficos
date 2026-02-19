# Análisis de la Gramática de Gráficos

## Convención global de color (consistencia entre gráficos)

Para evitar confusiones, cada tipo de renta mantiene el mismo color en todas las visualizaciones.

- **Sueldos y salarios**: `#1f77b4`
- **Pensiones**: `#ff7f0e`
- **Otros ingresos**: `#2ca02c`
- **Prestaciones por desempleo**: `#d62728`
- **Otras prestaciones**: `#9467bd`

Implementación recomendada en plotnine:
- Gráficos con `color`: `scale_color_manual(values=colores_renta)`
- Gráficos con `fill`: `scale_fill_manual(values=colores_renta)`

## 1. Evolución temporal de la distribución de renta (2015–2023)

### Tabla Analítica de Componentes

| Componente | Descripción / Variable |
|-----------|------------------------|
| **Dataset** | `distribucion-renta-canarias.csv`<br>- Origen: ISTAC (Instituto Canario de Estadística)<br>- Filas: 4.411 registros<br>- Período: 2015–2023<br>- Granularidad: Canarias / islas / municipios |
| **Variables Clave** | - `TIME_PERIOD_CODE`: Año<br>- `MEDIDAS#es`: Tipo de renta<br>- `OBS_VALUE`: Porcentaje de renta<br>- `TERRITORIO#es`: Territorio seleccionado |
| **Geometría** | `geom_line()` + `geom_point()`<br>- Línea por cada tipo de renta<br>- Puntos para marcar cada año |
| **Estéticas (Aesthetics)** | - **x**: Año (cuantitativo discreto)<br>- **y**: OBS_VALUE (cuantitativo continuo)<br>- **color**: Tipo de renta<br>- **group**: Tipo de renta |
| **Escalas** | - **Escala X**: Temporal (2015–2023)<br>- **Escala Y**: Continua (0–100%)<br>- **Color**: Escala manual fija por tipo de renta (`scale_color_manual`) |
| **Etiquetas** | - **Título**: "Evolución de la distribución de rentas en Canarias (2015–2023)"<br>- **Eje X**: "Año"<br>- **Eje Y**: "Porcentaje de participación"<br>- **Leyenda**: "Tipo de renta" |

#### Justificación de Decisiones de Diseño

**¿Por qué gráfico de líneas?**
- Permite analizar tendencias temporales.
- Facilita comparar la evolución entre tipos de renta.
- Adecuado para series temporales.

**¿Por qué codificación por color?**
- Diferencia claramente cada tipo de ingreso.
- Permite lectura simultánea de múltiples variables.

**¿Por qué porcentaje como eje Y?**
- Representa la contribución relativa de cada renta.
- Permite comparar años sin sesgo por magnitud absoluta.

---

## 2. Comparación territorial de la renta por municipio (año seleccionado)

### Tabla Analítica de Componentes

| Componente | Descripción / Variable |
|-----------|------------------------|
| **Dataset** | `distribucion-renta-canarias.csv`<br>- Filtrado por año (ej. 2023)<br>- Granularidad: Municipios |
| **Variables Clave** | - `TERRITORIO#es`: Municipio<br>- `MEDIDAS#es`: Tipo de renta<br>- `OBS_VALUE`: Porcentaje |
| **Geometría** | `geom_bar(stat="identity")`<br>- Barras apiladas por municipio |
| **Estéticas (Aesthetics)** | - **x**: Municipio (categórico)<br>- **y**: OBS_VALUE<br>- **fill**: Tipo de renta |
| **Escalas** | - **Escala X**: Categórica (municipios)<br>- **Escala Y**: Continua (0–100%)<br>- **Color**: Escala manual fija por tipo de renta (`scale_fill_manual`) |
| **Etiquetas** | - **Título**: "Distribución de renta por municipio (2023)"<br>- **Eje X**: "Municipio"<br>- **Eje Y**: "Porcentaje"<br>- **Leyenda**: "Tipo de renta" |

#### Justificación de Decisiones de Diseño

**¿Por qué barras apiladas?**
- Permiten visualizar la composición de la renta.
- Comparación directa entre territorios.

**¿Por qué filtrar por un año?**
- Evita saturación visual.
- Facilita análisis espacial.

**¿Por qué municipios?**
- Mayor nivel de detalle territorial.
- Permite detectar desigualdades económicas locales.

---

## 3. Composición estructural de la renta (análisis socioeconómico)

### Tabla Analítica de Componentes

| Componente | Descripción / Variable |
|-----------|------------------------|
| **Dataset** | `distribucion-renta-canarias.csv`<br>- Agregado por Canarias o islas |
| **Variables Clave** | - `TIME_PERIOD_CODE`: Año<br>- `MEDIDAS#es`: Tipo de renta<br>- `OBS_VALUE`: Porcentaje |
| **Geometría** | `geom_area()`<br>- Área apilada por tipo de renta |
| **Estéticas (Aesthetics)** | - **x**: Año<br>- **y**: OBS_VALUE<br>- **fill**: Tipo de renta |
| **Escalas** | - **Escala X**: Temporal<br>- **Escala Y**: Continua acumulada<br>- **Color**: Escala manual fija por tipo de renta (`scale_fill_manual`) |
| **Etiquetas** | - **Título**: "Composición estructural de la renta en Canarias"<br>- **Eje X**: "Año"<br>- **Eje Y**: "Porcentaje acumulado"<br>- **Leyenda**: "Tipo de renta" |

#### Justificación de Decisiones de Diseño

**¿Por qué área apilada?**
- Muestra la estructura global de la renta.
- Permite observar cambios estructurales.

**¿Qué aporta frente al gráfico de líneas?**
- Enfatiza el peso relativo de cada ingreso.
- Representa el total como sistema económico.

**¿Por qué análisis agregado?**
- Facilita interpretación macroeconómica.
- Reduce ruido territorial.

---

## 4. Niveles de estudios en curso (último periodo disponible)

### Tabla Analítica de Componentes

| Componente | Descripción / Variable |
|-----------|------------------------|
| **Dataset** | `nivelestudios.xlsx` (`Hoja1`)<br>- Filtrado por último período disponible<br>- Filtro de `Sexo = Total`<br>- Exclusión de `Total` y `No cursa estudios` |
| **Variables Clave** | - `Periodo`: Fecha de referencia<br>- `Nivel de estudios en curso`: Categoría educativa<br>- `Total`: Conteo de personas<br>- `porcentaje`: Peso relativo por nivel de estudios |
| **Geometría** | `geom_bar(stat="identity")` + `coord_flip()`<br>- Barras horizontales por nivel de estudios |
| **Estéticas (Aesthetics)** | - **x**: Nivel de estudios (categórico)<br>- **y**: porcentaje<br>- **fill**: Color único |
| **Escalas** | - **Escala X**: Categórica (niveles educativos)<br>- **Escala Y**: Continua (0–100%)<br>- **Color**: Relleno único para foco en magnitud relativa |
| **Etiquetas** | - **Título**: "Niveles de estudios en curso (último periodo, sin 'No cursa estudios')"<br>- **Eje X**: "Nivel de estudios"<br>- **Eje Y**: "Porcentaje sobre población que cursa estudios" |

#### Justificación de Decisiones de Diseño

**¿Por qué excluir `No cursa estudios`?**
- Domina el volumen total y aplasta visualmente el resto de categorías.
- El objetivo de este gráfico es comparar la composición **de quienes sí cursan estudios**.

**¿Por qué usar porcentaje en vez de conteo absoluto?**
- Permite comparar categorías en la misma escala relativa.
- Reduce sesgo por tamaño poblacional agregado.

**¿Por qué barras horizontales y etiquetas envueltas?**
- Los nombres de categorías son largos y técnicos.
- Mejora legibilidad sin perder el texto completo de cada nivel educativo.