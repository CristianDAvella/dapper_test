"""
Módulo de validación de datos extraídos.

- Intercala una etapa de validación entre extracción y escritura.
- Usa reglas configurables en YAML:
  - tipo de dato
  - regex por campo
  - obligatoriedad

Reglas:
- Si un campo no cumple su regla:
  - Si es obligatorio -> descartar toda la fila.
  - Si es opcional -> campo a NULL (None en pandas).
"""

import os
import re
from datetime import datetime
from typing import Any, Dict, Tuple

import pandas as pd
import yaml


def _get_rules_path() -> str:
    """
    Construye dinámicamente la ruta a configs/validation_rules.yaml
    asumiendo estructura:

    <BASE_DIR>/
      ├─ dags/
      ├─ modules/
      ├─ configs/
          └─ validation_rules.yaml

    donde este archivo vive en <BASE_DIR>/modules/validation.py
    """
    module_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.abspath(os.path.join(module_dir, os.pardir))
    rules_path = os.path.join(base_dir, "configs", "validation_rules.yaml")

    if not os.path.exists(rules_path):
        raise FileNotFoundError(
            f"No se encontró validation_rules.yaml en: {rules_path}"
        )

    print(f"[VALIDATION] Usando archivo de reglas: {rules_path}")
    return rules_path


def _load_rules() -> Dict[str, Any]:
    """
    Carga las reglas de validación desde configs/validation_rules.yaml.
    """
    rules_path = _get_rules_path()
    with open(rules_path, "r", encoding="utf-8") as f:
        rules = yaml.safe_load(f)
    return rules or {}


def _cast_value(value: Any, expected_type: str) -> Any:
    """Intenta castear el valor al tipo indicado. Si no puede, lanza ValueError."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    if expected_type == "string":
        return str(value).strip()

    if expected_type == "int":
        if isinstance(value, bool):
            raise ValueError("bool is not allowed as int")
        return int(value)

    if expected_type == "bool":
        if isinstance(value, bool):
            return value
        str_v = str(value).strip().lower()
        if str_v in {"true", "1", "t", "yes", "y"}:
            return True
        if str_v in {"false", "0", "f", "no", "n"}:
            return False
        raise ValueError(f"Cannot cast '{value}' to bool")

    if expected_type == "date":
        # Esperamos YYYY-MM-DD
        if isinstance(value, datetime):
            return value.date()
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()

    if expected_type == "datetime":
        # Esperamos YYYY-MM-DD HH:MM:SS
        if isinstance(value, datetime):
            return value.replace(tzinfo=None)
        return datetime.strptime(str(value)[:19], "%Y-%m-%d %H:%M:%S")

    # Si no se reconoce el tipo, devolver tal cual
    return value


def _validate_field(value: Any, field_rule: Dict[str, Any]) -> Tuple[bool, Any]:
    """
    Valida un campo según su regla.
    Devuelve (is_valid, normalized_value).
    """
    required = bool(field_rule.get("required", False))
    expected_type = field_rule.get("type")
    pattern = (field_rule.get("regex") or "").strip()

    # Tratar NaN como None
    if isinstance(value, float) and pd.isna(value):
        value = None

    # Si no hay valor
    if value is None or value == "":
        # Si es obligatorio, inválido
        if required:
            return False, None
        # Opcional sin valor: válido, se queda como None
        return True, None

    # 1) Cast de tipo si está configurado
    try:
        if expected_type:
            value = _cast_value(value, expected_type)
    except Exception:
        # Falla de tipo
        if required:
            return False, None
        return True, None

    # 2) Validación regex si existe
    if pattern:
        # Para fechas y datetimes, validamos sobre string formateado
        value_to_check = value
        if isinstance(value, datetime):
            value_to_check = value.strftime("%Y-%m-%d %H:%M:%S")
        elif hasattr(value, "isoformat"):
            # date u otros tipos con isoformat
            value_to_check = value.isoformat()
        else:
            value_to_check = str(value)

        if not re.match(pattern, value_to_check):
            if required:
                return False, None
            return True, None

    return True, value


def run_validation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Valida los datos extraídos según reglas configurables en YAML.

    - Campos obligatorios que fallen -> descarta fila completa.
    - Campos opcionales que fallen -> setea campo a NULL.
    """
    print(f"=== INICIANDO VALIDACIÓN: {len(df)} registros ===")

    if df.empty:
        print("DataFrame vacío, nada que validar.")
        return df.copy()

    rules = _load_rules()
    field_rules: Dict[str, Dict[str, Any]] = rules.get("fields", {})

    validated_rows = []
    discarded_rows = 0

    # Iterar fila a fila para aplicar reglas
    for idx, row in df.iterrows():
        row_dict = row.to_dict()
        row_valid = True
        new_row = dict(row_dict)

        for field_name, frule in field_rules.items():
            # Si el campo no existe en el DF, lo tratamos como None
            value = row_dict.get(field_name, None)
            is_ok, normalized = _validate_field(value, frule)

            if not is_ok and frule.get("required", False):
                # Campo obligatorio inválido -> descartar fila
                row_valid = False
                break

            # Para opcionales o válidos, normalizamos el valor (puede ser None)
            new_row[field_name] = normalized

        if row_valid:
            validated_rows.append(new_row)
        else:
            discarded_rows += 1

    valid_df = (
        pd.DataFrame(validated_rows)
        if validated_rows
        else pd.DataFrame(columns=df.columns)
    )

    print(
        f"=== VALIDACIÓN COMPLETADA: "
        f"{len(valid_df)} válidos, {discarded_rows} descartados por campos obligatorios ==="
    )

    return valid_df
