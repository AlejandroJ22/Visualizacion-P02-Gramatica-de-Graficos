from dagster import Output, asset
import pandas as pd
import subprocess

@asset
def codigo_generado_ia(template_ia):
    import requests

    response = requests.post(
        "http://gpu1.esit.ull.es:4000/v1/chat/completions",
        headers={
            "Authorization": "Bearer sk-1234",
            "Content-Type": "application/json"
        },
        json=template_ia
    )

    content = response.json()["choices"][0]["message"]["content"]

    codigo = content.strip()

    if "```" in codigo:
        codigo = codigo.split("```")[1]

    # Si el bloque viene como ```python, quitamos ese prefijo para que exec no falle.
    codigo = codigo.lstrip()
    if codigo.startswith("python\n"):
        codigo = codigo[len("python\n"):]

    # Debug: guarda el código generado para inspección
    with open("codigo_generado_debug.py", "w") as f:
        f.write(codigo)

    return codigo

@asset
def template_ia(carga_renta_raw):
    columnas = ", ".join(carga_renta_raw.columns)

    template_tecnico = """
def generar_plot(df):
    import pandas as pd
    from plotnine import ggplot, aes, geom_line, labs, theme_minimal, theme, scale_color_manual

    # 1) preparar datos
    # 2) construir plotnine en variable `plot`
    # 3) return plot
"""

    system_content = (
        "Eres un experto en la gramática de gráficos y Plotnine. "
        "Devuelve exclusivamente codigo Python ejecutable, sin markdown ni explicaciones. "
        "Tu salida debe definir una funcion `generar_plot(df)` que retorne un objeto plotnine en la variable `plot`. "
        "Prioriza legibilidad: punto focal claro, poco ruido visual y leyenda correcta. "
        f"Columnas disponibles: {columnas}. "
        f"Sigue esta estructura tecnica: {template_tecnico}."
    )

    descripcion_grafico = f"""
    - Dataset: carga_renta_raw
    - Objetivo:
        * Crear un grafico de lineas util y legible de la evolucion temporal de `OBS_VALUE`.

    - Preparacion de datos obligatoria:
        * Filtrar `MEDIDAS#es == 'Sueldos y salarios'`.
        * Convertir `TERRITORIO_CODE` a string.
        * Filtrar solo islas con codigo `ES7xx`: `(df['TERRITORIO_CODE'].str.startswith('ES7')) & (df['TERRITORIO_CODE'].str.len() == 5)`.
        * Excluir agregados: `Canarias`, `Las Palmas`, `Santa Cruz de Tenerife`.
        * Convertir `TIME_PERIOD_CODE` y `OBS_VALUE` a numerico con `errors='coerce'`.
        * Eliminar nulos en esas dos columnas y ordenar por `TIME_PERIOD_CODE`.

    - Construccion del grafico:
        * Usar un color distinto por isla mapeando `color='TERRITORIO#es'`.
        * Definir `plot = ggplot(df, aes(x='TIME_PERIOD_CODE', y='OBS_VALUE', group='TERRITORIO#es', color='TERRITORIO#es'))`.
        * Añadir `geom_line(size=0.9, alpha=0.8)`.
        * Usar `labs(title='Evolucion del gasto por isla', subtitle='Sueldos y salarios', x='Ano', y='Gasto en EUR', color='Territorio')`.
        * Usar `theme_minimal()` y `theme(legend_position='bottom')`.

    - Restricciones tecnicas:
        * No usar `guide`, `guides`, `factor`, ni `element_text`.
        * No pasar `size` dentro de `scale_color_manual`.
        * No incluir comentarios ni texto fuera de codigo.
        * El codigo debe terminar con `return plot`.
    """

    user_content = f"Basándote en esta descripción, completa el template:\n{descripcion_grafico}"

    return {
        "model": "ollama/llama3.1:8b",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content}
        ],
        "temperature": 0.1, # Muy baja para que no se invente nada
        "stream": False
    }

@asset
def visualizacion_png(context, codigo_generado_ia, carga_renta_raw):
    import plotnine
    # islas_procesadas es el DataFrame que viene del asset anterior
    df = carga_renta_raw
        # Entorno para ejecutar el código de la IA
    entorno_ejecucion = globals().copy()
    # Inyectamos todo el diccionario de plotnine para que ggplot sea global
    entorno_ejecucion['plotnine'] = plotnine
    entorno_ejecucion.update({
        k: v for k, v in plotnine.__dict__.items() if not k.startswith('_')
    })
    # Aseguramos que pandas también esté disponible como 'pd'
    entorno_ejecucion['pd'] = pd
    try:
        # Ejecutamos el string que devolvió la IA
        exec(codigo_generado_ia, entorno_ejecucion)
       
        # Invocamos la función que la IA creó dentro del template
        #Ejecuta la función generar_plot que está almacenada en el diccionario en el elemento con clave 'generar_plot'
        grafico = entorno_ejecucion['generar_plot'](carga_renta_raw)
       
        # Guardamos el archivo físicamente
        ruta_archivo = "visualizacion_ia.png"
        grafico.save(ruta_archivo, width=10, height=6, dpi=100)

        # Hacemos commit automático del archivo generado
        subprocess.run(["git", "add", ruta_archivo])
        subprocess.run(["git", "commit", "-m", "Auto update visualization"], check=False)
        subprocess.run(["git", "push"], check=False)

        #Creamos metadatos para auditar lor resultados
        return Output(
            value=ruta_archivo,
            metadata={"ruta": ruta_archivo, "mensaje": "Gráfico generado y guardado"}
        )
    except Exception as e:
        context.log.error(f"Error al renderizar el gráfico: {e}")
        raise e
