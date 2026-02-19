"""
Test asset para verificar comunicación con Dagster
"""

from dagster import asset


@asset
def test_asset():
    """
    Asset de prueba para verificar que Dagster está correctamente instalado
    y funcionando.
    """
    mensaje = "¡Dagster está funcionando correctamente!"
    print(mensaje)
    return mensaje
