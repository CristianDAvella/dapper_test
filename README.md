# ANI Regulations Scraper

Proyecto de scraping y carga de normatividad de la **Agencia Nacional de Infraestructura (ANI)** hacia una base de datos **PostgreSQL**, usando un pipeline Python (estilo Lambda) y un contenedor Docker con Postgres.

El objetivo es que el evaluador pueda levantar el entorno localmente, ejecutar el scraping y ver los datos persistidos sin fricción.

---

## 1. Requisitos

- Docker y Docker Compose instalados.
- Python 3.10+ instalado (solo si desea ejecutar el script fuera de Docker).
- Acceso a Internet (el scraper consulta la web de la ANI).

---

## 2. Estructura principal del proyecto

Los archivos relevantes son:

- `docker-compose.yml`  
  Define un contenedor con PostgreSQL, incluyendo usuario y contraseña

- `schema.sql`  
  Crea el esquema y tablas:
  - `public.regulations`
  - `public.components`
  - `public.regulations_component` 


- `persistence.py`  
  Lógica de acceso a base de datos (clase `DatabaseManager` y helpers de inserción).

- `validation.py`  
  Capa de validación intermedia, que aplica reglas configurables a un `DataFrame` antes de escribir en la base de datos.

- `validation_rules.yaml`  
  Archivo de configuración que define las reglas de validación por campo (tipo, regex, obligatoriedad).

---

## 3. Levantar la base de datos con Docker

1. Ir a la raíz del proyecto (donde está `docker-compose.yml`).
2. Levantar el contenedor de Postgres:

   ```bash
   docker compose up -d
   ```
3. Cargar el esquema de base de datos (ajusta el nombre del contenedor y credenciales según tu docker-compose.yml):

   ```bash
   docker exec -i NOMBRE_CONTENEDOR_POSTGRES \
    psql -U USUARIO -d BASE_DATOS < schema.sql
   ```

## 4. Ejecutar el ETL

Despues de esto puedes y a **http://localhost:8080/** y ejecutar el proceso.
