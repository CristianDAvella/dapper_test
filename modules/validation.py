"""
Módulo de validación de datos extraídos.
En la fase inicial mantiene la lógica existente.
Posteriormente se implementará validación configurable vía YAML.
"""

import pandas as pd


def run_validation(df):
    """
    Valida los datos extraídos según reglas configurables.
    
    Por ahora retorna el DataFrame sin cambios, manteniendo la validación
    que ya existe en el proceso de extracción (títulos largos, fechas inválidas, etc.).
    
    En la siguiente fase implementará:
    - Validación de tipos de datos
    - Validación con regex
    - Campos obligatorios vs opcionales
    - Descartar filas o dejar campos NULL según reglas
    
    Args:
        df (pd.DataFrame): DataFrame con datos extraídos
    
    Returns:
        pd.DataFrame: DataFrame validado
    """
    print(f"=== INICIANDO VALIDACIÓN: {len(df)} registros ===")
    
    # Versión inicial: retornar el DataFrame sin modificaciones
    # La validación actual está embebida en el scraping
    valid_df = df.copy()
    
    # Aquí se implementará la validación con YAML
    # Por ahora solo reportamos
    discarded_count = 0
    
    print(f"=== VALIDACIÓN COMPLETADA: {len(valid_df)} válidos, {discarded_count} descartados ===")
    
    return valid_df
