# ETL Normatividad ANI con Airflow

Pipeline ETL que **extrae**, **valida** y **escribe** la normatividad de la ANI en una base **Postgres** orquestada con **Airflow**.

---

## 1. Qué hace el proyecto

- Scraping de la página de normatividad de la ANI.
- Validación configurable por campo (tipo + regex + obligatoriedad) mediante `configs/validation_rules.yaml`.
- Escritura en Postgres evitando duplicados (según lógica de la Lambda original).
- Orquestación en un DAG con tres tareas:

```text
extract → validate → load
```

---

## 2. Requisitos

- Docker y Docker Compose instalados.
- Puertos libres:
  - `8080` (Airflow web)
  - `5432` (Postgres, opcional para acceso desde host)

No se requiere instalar Python ni Airflow localmente.

---

## 3. Estructura mínima

```text
dapper_test/
├── configs/
│   └── validation_rules.yaml
├── dags/
│   └── ani_scraping_dag.py
├── data/
│   ├── raw/
│   └── processed/
├── modules/
│   ├── extraction.py
│   ├── validation.py
│   └── persistence.py
├── schema.sql
├── docker-compose.yml
└── Dockerfile
```
Para que el proyecto funcione correctamente, los directorios deben tener los permisos necesarios para ser manipulados por los contenedores. Como estamos en un ambiente de prueba, puedes ejecutar:

```bash
sudo chmod -R 777 dapper_test/
```

---

## 4. Levantar el entorno

Desde la raíz del proyecto:

```bash
docker-compose up --build
```

Esto levanta:

- Postgres (`airflow` / `airflow`, DB `airflow`)
- Airflow (`webserver`, `scheduler`, `airflow-init`)

---

## 5. Crear tablas de negocio (obligatorio)

Con los contenedores arriba, ejecutar en la raíz:

```bash
sudo docker exec -i dapper_test-postgres-1 \
    psql -U airflow -d airflow < schema.sql
```

Si el nombre del contenedor Postgres es distinto, ajusta `dapper_test-postgres-1` usando `docker ps`.

---

## 6. Ejecutar el DAG

1. Abrir Airflow:

   ```text
   http://localhost:8080
   ```

   Credenciales:

   - usuario: `admin`
   - password: `admin`

2. Buscar el DAG `ani_scraping_pipeline`.
3. Entrar al DAG y pulsar **Trigger DAG**.
4. (Opcional) Si tu UI permite configuración, usar una prueba pequeña:

   ```json
   {
     "num_pages": 1,
     "verbose": true
   }
   ```

Airflow ejecutará las tareas `extract`, `validate` y `load` en orden, usando CSV intermedios en `data/raw` y `data/processed`.

---

## 7. Ver logs y resultados

- En la vista **Graph**, haz clic en cada tarea → **View Log** para ver:
  - `extract`: páginas procesadas, registros extraídos, ruta del CSV.
  - `validate`: registros válidos vs descartados.
  - `load`: procesados, insertados y duplicados evitados.

Para comprobar los datos en Postgres:

```bash
docker exec -it dapper_test-postgres-1 bash
psql -U airflow -d airflow
```

Ejemplo de consultas:

```sql
SELECT id, title, created_at, entity
FROM regulations
ORDER BY id DESC
LIMIT 10;
```

Salir de `psql` con `\q`.

---

## 8. Apagar el entorno

```bash
docker-compose down
```

(Usa `docker-compose down -v` si también quieres borrar los datos de la base.)