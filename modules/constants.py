"""
Constantes compartidas por los módulos del proyecto ANI scraping.
"""

# Constantes para el scraping
ENTITY_VALUE = 'Agencia Nacional de Infraestructura'
FIXED_CLASSIFICATION_ID = 13
URL_BASE = "https://www.ani.gov.co/informacion-de-la-ani/normatividad?field_tipos_de_normas__tid=12&title=&body_value=&field_fecha__value%5Bvalue%5D%5Byear%5D="

# Clasificaciones de documentos
CLASSIFICATION_KEYWORDS = {
    'resolución': 15,
    'resolucion': 15,
    'decreto': 14,
}

DEFAULT_RTYPE_ID = 14
