"""
Módulo de configuración global e infraestructura de ejecución.

Centraliza la gestión de rutas maestras, reglas de negocio estructurales
y la configuración del entorno de ejecución. Administra explícitamente la
infraestructura de memoria (Spill-to-Disk) y concurrencia para habilitar
el procesamiento Out-of-Core en Polars, previniendo desbordamientos de RAM.
"""

import os
from pathlib import Path
from typing import Any, Dict

import polars as pl
from dotenv import load_dotenv

# Inicialización del entorno
load_dotenv()

# =============================================================================
# 1. RUTAS E INFRAESTRUCTURA DE ALMACENAMIENTO
# =============================================================================
BASE_DIR: Path = Path(__file__).resolve().parent.parent
LOG_DIR: Path = BASE_DIR / "logs"
TEMP_DIR: Path = BASE_DIR / "temp_processing"

# Resolución de ruta de ingesta cruda con fallback de seguridad
ruta_sat_env = os.getenv("SAT_RAW_DIR", str(BASE_DIR / "data_fallback"))
SAT_RAW_DIR: Path = Path(ruta_sat_env)

# Aprovisionamiento automático de directorios operativos
for folder in (LOG_DIR, TEMP_DIR):
    folder.mkdir(parents=True, exist_ok=True)

# =============================================================================
# 2. GESTIÓN AVANZADA DE MEMORIA Y RENDIMIENTO (POLARS)
# =============================================================================
# Definición y aprovisionamiento del disco de trabajo para operaciones Out-of-Core
ruta_temp_env = os.getenv("ETL_TEMP_DIR", r"C:\Temp_Polars")
DISCO_TRABAJO: Path = Path(ruta_temp_env)
DISCO_TRABAJO.mkdir(parents=True, exist_ok=True)

# Redirección de los buffers de memoria virtual al disco de trabajo
os.environ["POLARS_TEMP_DIR"] = str(DISCO_TRABAJO)
os.environ["TMPDIR"] = str(DISCO_TRABAJO)
os.environ["TEMP"] = str(DISCO_TRABAJO)
os.environ["TMP"] = str(DISCO_TRABAJO)

# Restricción de concurrencia para evitar bloqueos por saturación de CPU
os.environ["POLARS_MAX_THREADS"] = "4"

# Configuración de visualización estándar para auditoría en consola
pl.Config.set_tbl_rows(20)
pl.Config.set_fmt_str_lengths(50)

# Límite de ingesta por lote transaccional
BATCH_SIZE: int = 100_000

# =============================================================================
# 3. DICCIONARIO MAESTRO DE ORQUESTACIÓN (TABLES_CONFIG)
# =============================================================================
TABLES_CONFIG: Dict[str, Dict[str, str]] = {
    "1A": {
        "file_name": "GERG_AECF_1891_Anexo1A-QA.txt",
        "table_name": "ANEXO_1A_2025_1S",
        "separator": "|"
    },
    "2B": {
        "file_name": "GERG_AECF_1891_Anexo2B.csv",
        "table_name": "ANEXO_2B_2025_1S",
        "separator": "|"
    },
    "1A_2024": {
        "file_name": "AECF_0101_Anexo1.csv",
        "table_name": "ANEXO_1A_2024",
        "separator": "|"
    },
    "2B_2024": {
        "file_name": "AECF_0101_Anexo2.csv",
        "table_name": "ANEXO_2B_2024",
        "separator": "|"
    },
    "1A_2025_2S": {
        "file_name": "AECF_0129_Anexo1_TablaA.csv.csv",
        "table_name": "ANEXO_1A_2025_2S",
        "separator": "|"
    },
    "2B_2025_2S": {
        "file_name": "AECF_0129_Anexo1_TablaB.csv.csv",
        "table_name": "ANEXO_2B_2025_2S",
        "separator": "|"
    },
    "1A_2023": {
        "file_name": "AECF_0488_Anexo1.csv",
        "table_name": "ANEXO_1A_2023", 
        "separator": "|" 
    },
    "2B_2023": {
        "file_name": "AECF_0488_Anexo2.csv",
        "table_name": "ANEXO_2B_2023", 
        "separator": "|" 
    }
}

# =============================================================================
# 4. REGLAS DE TIPADO DINÁMICO (SCHEMA ENFORCEMENT)
# =============================================================================
REGLAS_DINAMICAS: Dict[str, Any] = {
    "UUID": pl.Utf8,
    "EMISORRFC": pl.Utf8,
    "RECEPTORRFC": pl.Utf8,
    "FECHAEMISION": pl.Datetime,
    "FECHACERTIFICACION": pl.Datetime,
    "FECHACANCELACION": pl.Datetime,
    "DESCUENTO": pl.Float64,
    "SUBTOTAL": pl.Float64,
    "TOTAL": pl.Float64,
    "TRASLADOSIVA": pl.Float64,
    "TRASLADOSIEPS": pl.Float64,
    "TOTALIMPUESTOSTRASLADADOS": pl.Float64,
    "RETENIDOSIVA": pl.Float64,
    "RETENIDOSISR": pl.Float64,
    "TOTALIMPUESTOSRETENIDOS": pl.Float64,
    "TIPOCAMBIO": pl.Float64,
    "CONCEPTOCANTIDAD": pl.Float64,
    "CONCEPTOVALORUNITARIO": pl.Float64,
    "CONCEPTOIMPORTE": pl.Float64,
}