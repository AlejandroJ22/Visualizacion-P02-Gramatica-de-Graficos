from dagster import sensor, RunRequest, define_asset_job, job
from pathlib import Path

DATA_DIR = Path("data/")

# Definimos un job que incluye los assets que dependen de los datos
data_sync_job = define_asset_job(
    name="data_sync_job",
    selection=[
        "carga_renta_raw",
        "template_ia",
        "codigo_generado_ia",
        "visualizacion_png",
    ]
)

@sensor(job=data_sync_job)
def sensor_nuevos_datos(context):
    """Sensor que monitorea cambios en la carpeta data/"""
    # Guardamos el estado anterior
    last_mtime = context.cursor

    # Obtenemos el último cambio en la carpeta
    try:
        current_mtime = max(
            f.stat().st_mtime for f in DATA_DIR.glob("**/*") if f.is_file()
        )
    except ValueError:
        # Si no hay archivos en la carpeta
        context.update_cursor("0")
        return

    # Si no hay cursor, inicializamos
    if last_mtime is None:
        context.update_cursor(str(current_mtime))
        return

    # Si hay cambios → lanzamos el job
    if float(current_mtime) > float(last_mtime):
        context.log.info(f"Detectados cambios en data/. Lanzando job...")
        yield RunRequest(
            run_key=str(current_mtime),
            run_config={},
        )

        # Actualizamos cursor
        context.update_cursor(str(current_mtime))