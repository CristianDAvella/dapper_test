-- ============================================
-- Esquema base de negocio para ANI Scraping
-- Tablas: regulations, components, regulations_component
-- ============================================

-- Por seguridad, crea el esquema público si no existe
CREATE SCHEMA IF NOT EXISTS public;

-- ============================================
-- 1. Tabla principal: regulations
-- ============================================
CREATE TABLE IF NOT EXISTS public.regulations (
    id                SERIAL PRIMARY KEY,
    title             VARCHAR(100) NOT NULL,
    created_at        DATE NOT NULL,
    entity            VARCHAR(150) NOT NULL,
    external_link     VARCHAR(500),
    summary           TEXT,
    rtype_id          INTEGER,
    classification_id INTEGER,
    gtype             VARCHAR(50),
    update_at         TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    is_active         BOOLEAN NOT NULL DEFAULT TRUE
);

-- Índice para acelerar la detección de duplicados
-- (título + fecha + enlace externo)
CREATE INDEX IF NOT EXISTS idx_regulations_title_created_link
    ON public.regulations (title, created_at, external_link);

-- Índice por entidad (filtras siempre por entity)
CREATE INDEX IF NOT EXISTS idx_regulations_entity
    ON public.regulations (entity);

-- ============================================
-- 2. Catálogo mínimo de componentes
-- ============================================
CREATE TABLE IF NOT EXISTS public.components (
    id    INTEGER PRIMARY KEY,
    name  VARCHAR(100) NOT NULL
);

-- Insertar el componente usado por el código (id = 7)
INSERT INTO public.components (id, name)
VALUES (7, '?')
ON CONFLICT (id) DO NOTHING;

-- ============================================
-- 3. Tabla relación: regulations_component
-- ============================================
CREATE TABLE IF NOT EXISTS public.regulations_component (
    regulations_id INTEGER NOT NULL,
    components_id  INTEGER NOT NULL,
    PRIMARY KEY (regulations_id, components_id),
    CONSTRAINT fk_regulations_component_regulations
        FOREIGN KEY (regulations_id)
        REFERENCES public.regulations (id)
        ON DELETE CASCADE,
    CONSTRAINT fk_regulations_component_components
        FOREIGN KEY (components_id)
        REFERENCES public.components (id)
        ON DELETE RESTRICT
);

-- Índices auxiliares para joins y consultas
CREATE INDEX IF NOT EXISTS idx_reg_comp_regulations_id
    ON public.regulations_component (regulations_id);

CREATE INDEX IF NOT EXISTS idx_reg_comp_components_id
    ON public.regulations_component (components_id);
