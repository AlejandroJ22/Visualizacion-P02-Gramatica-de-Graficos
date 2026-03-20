"""Checks de calidad para assets de generacion IA de visualizaciones."""

from pathlib import Path

from dagster import AssetCheckResult, MetadataValue, asset_check


@asset_check(asset="template_ia")
def check_template_ia_payload(template_ia: dict) -> AssetCheckResult:
    required_keys = {"model", "messages", "temperature", "stream"}
    payload_keys = set(template_ia.keys()) if isinstance(template_ia, dict) else set()
    missing_keys = sorted(list(required_keys - payload_keys))

    messages = template_ia.get("messages", []) if isinstance(template_ia, dict) else []
    roles = [m.get("role") for m in messages if isinstance(m, dict)]
    has_system = "system" in roles
    has_user = "user" in roles

    passed = isinstance(template_ia, dict) and not missing_keys and has_system and has_user

    return AssetCheckResult(
        passed=passed,
        metadata={
            "missing_keys": MetadataValue.json(missing_keys),
            "message_count": MetadataValue.int(len(messages)),
            "has_system_message": MetadataValue.bool(has_system),
            "has_user_message": MetadataValue.bool(has_user),
        },
    )


@asset_check(asset="codigo_generado_ia")
def check_codigo_generado_ia_basico(codigo_generado_ia: str) -> AssetCheckResult:
    code = codigo_generado_ia if isinstance(codigo_generado_ia, str) else ""
    has_function = "def generar_plot(" in code
    has_return_plot = "return plot" in code
    has_markdown_fence = "```" in code

    passed = has_function and has_return_plot and not has_markdown_fence

    return AssetCheckResult(
        passed=passed,
        metadata={
            "code_length": MetadataValue.int(len(code)),
            "has_generar_plot": MetadataValue.bool(has_function),
            "has_return_plot": MetadataValue.bool(has_return_plot),
            "has_markdown_fence": MetadataValue.bool(has_markdown_fence),
        },
    )


@asset_check(asset="visualizacion_png")
def check_visualizacion_png_archivo(visualizacion_png: str) -> AssetCheckResult:
    output_path = Path(visualizacion_png)
    exists = output_path.exists()
    is_png = output_path.suffix.lower() == ".png"
    size_bytes = output_path.stat().st_size if exists else 0

    passed = exists and is_png and size_bytes > 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "ruta": MetadataValue.text(str(output_path)),
            "exists": MetadataValue.bool(exists),
            "is_png": MetadataValue.bool(is_png),
            "size_bytes": MetadataValue.int(int(size_bytes)),
        },
    )
