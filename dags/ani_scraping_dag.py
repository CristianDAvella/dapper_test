"""
DAG de Airflow para scraping de normativas de ANI.
Orquesta el proceso de Extracción -> Validación -> Escritura.
Usa archivos CSV intermedios para pasar datos entre tareas (buena práctica).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import sys
import os

# Agregar el path de modules al sys.path para que Airflow pueda importar
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from modules.extraction import run_extraction
from modules.validation import run_validation
from modules.persistence import run_persistence


# ==================== CONFIGURACIÓN DE RUTAS ====================

# Directorio base del proyecto (un nivel arriba de dags/)
BASE_DIR = os.path.join(os.path.dirname(__file__), '..')

# Directorios de datos intermedios
RAW_DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, 'data', 'processed')

# Crear directorios si no existen
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)


# ==================== CONFIGURACIÓN DEL DAG ====================

default_args = {
    'owner': 'dapper_test',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='ani_scraping_pipeline',
    default_args=default_args,
    description='Pipeline ETL para scraping de normativas ANI con validación',
    schedule_interval=None,  # Ejecución manual por ahora
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ani', 'scraping', 'etl'],
)


# ==================== FUNCIONES DE LAS TAREAS ====================

def extraction_task(**context):
    """
    Tarea de extracción: Scrapea las páginas de ANI y guarda en CSV.
    Pasa por XCom solo la ruta del archivo generado.
    """
    print("=" * 60)
    print("TAREA 1: EXTRACCIÓN")
    print("=" * 60)
    
    # Obtener parámetros del DAG run config (si se pasaron)
    dag_run_conf = context.get('dag_run').conf if context.get('dag_run') else {}
    num_pages = dag_run_conf.get('num_pages', 9)
    verbose = dag_run_conf.get('verbose', True)
    
    print(f"Parámetros: num_pages={num_pages}, verbose={verbose}")
    
    # Ejecutar extracción
    df = run_extraction(num_pages=num_pages, verbose=verbose)
    
    # Generar nombre de archivo con timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"ani_extracted_{timestamp}.csv"
    filepath = os.path.join(RAW_DATA_DIR, filename)
    
    # Guardar DataFrame como CSV
    df.to_csv(filepath, index=False, encoding='utf-8')
    print(f"\nDatos extraídos guardados en: {filepath}")
    print(f"Tamaño del archivo: {os.path.getsize(filepath) / 1024:.2f} KB")
    
    # Guardar SOLO la ruta en XCom (metadatos pequeños)
    ti = context['ti']
    ti.xcom_push(key='raw_data_path', value=filepath)
    
    # Logs de resumen
    print(f"\n{'=' * 60}")
    print(f"EXTRACCIÓN COMPLETADA:")
    print(f"  - Registros extraídos: {len(df)}")
    print(f"  - Páginas procesadas: {num_pages}")
    print(f"  - Archivo: {filename}")
    print(f"{'=' * 60}\n")
    
    return len(df)


def validation_task(**context):
    """
    Tarea de validación: Lee CSV de extracción, valida y guarda CSV validado.
    Pasa por XCom solo la ruta del archivo validado.
    """
    print("=" * 60)
    print("TAREA 2: VALIDACIÓN")
    print("=" * 60)
    
    # Obtener ruta del archivo raw desde XCom
    ti = context['ti']
    raw_filepath = ti.xcom_pull(key='raw_data_path', task_ids='extract')
    
    if not raw_filepath or not os.path.exists(raw_filepath):
        raise FileNotFoundError(f"No se encontró el archivo de extracción: {raw_filepath}")
    
    print(f"Leyendo datos de: {raw_filepath}")
    
    # Leer CSV
    df = pd.read_csv(raw_filepath, encoding='utf-8')
    print(f"Registros leídos: {len(df)}")
    
    # Ejecutar validación
    valid_df = run_validation(df)
    
    # Generar nombre de archivo validado
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"ani_validated_{timestamp}.csv"
    filepath = os.path.join(PROCESSED_DATA_DIR, filename)
    
    # Guardar DataFrame validado como CSV
    valid_df.to_csv(filepath, index=False, encoding='utf-8')
    print(f"\nDatos validados guardados en: {filepath}")
    print(f"Tamaño del archivo: {os.path.getsize(filepath) / 1024:.2f} KB")
    
    # Guardar SOLO la ruta en XCom
    ti.xcom_push(key='validated_data_path', value=filepath)
    
    # Logs de resumen
    discarded = len(df) - len(valid_df)
    print(f"\n{'=' * 60}")
    print(f"VALIDACIÓN COMPLETADA:")
    print(f"  - Registros válidos: {len(valid_df)}")
    print(f"  - Registros descartados: {discarded}")
    print(f"  - Tasa de éxito: {(len(valid_df)/len(df)*100):.2f}%")
    print(f"  - Archivo: {filename}")
    print(f"{'=' * 60}\n")
    
    return len(valid_df)


def persistence_task(**context):
    """
    Tarea de persistencia: Lee CSV validado y escribe en PostgreSQL.
    """
    print("=" * 60)
    print("TAREA 3: PERSISTENCIA")
    print("=" * 60)
    
    # Obtener ruta del archivo validado desde XCom
    ti = context['ti']
    validated_filepath = ti.xcom_pull(key='validated_data_path', task_ids='validate')
    
    if not validated_filepath or not os.path.exists(validated_filepath):
        raise FileNotFoundError(f"No se encontró el archivo validado: {validated_filepath}")
    
    print(f"Leyendo datos de: {validated_filepath}")
    
    # Leer CSV
    df = pd.read_csv(validated_filepath, encoding='utf-8')
    print(f"Registros a persistir: {len(df)}")
    
    # Ejecutar persistencia
    result = run_persistence(df)
    
    # Logs de resumen
    print(f"\n{'=' * 60}")
    print(f"PERSISTENCIA COMPLETADA:")
    print(f"  - Registros procesados: {result['total_processed']}")
    print(f"  - Registros insertados: {result['inserted']}")
    print(f"  - Duplicados evitados: {result['total_processed'] - result['inserted']}")
    print(f"  - Mensaje: {result['message']}")
    print(f"{'=' * 60}\n")
    
    return result['inserted']


# ==================== DEFINICIÓN DE TAREAS ====================

extract = PythonOperator(
    task_id='extract',
    python_callable=extraction_task,
    provide_context=True,
    dag=dag,
)

validate = PythonOperator(
    task_id='validate',
    python_callable=validation_task,
    provide_context=True,
    dag=dag,
)

load = PythonOperator(
    task_id='load',
    python_callable=persistence_task,
    provide_context=True,
    dag=dag,
)


# ==================== DEPENDENCIAS ====================

extract >> validate >> load
