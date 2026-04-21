"""Módulo de Configuración e Infraestructura (Vertical Nómina).

Provee la configuración estática, rutas maestras y el contrato de datos
para la orquestación del ETL. Habilita el procesamiento Out-of-Core
gestionando el Swap de memoria del motor Polars para prevenir colapsos
de RAM durante la ingesta de archivos ultra-masivos.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional, TypedDict

import polars as pl
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# 1. INFRAESTRUCTURA DE ALMACENAMIENTO Y RUTAS
# =============================================================================
BASE_DIR: Path = Path(__file__).resolve().parent.parent
LOG_DIR: Path = BASE_DIR / "logs"
TEMP_DIR: Path = BASE_DIR / "temp_processing"

# REGLA DE ARQUITECTURA: El .env debe proveer rutas UNC (\\Servidor\Ruta) para entornos corporativos
ruta_sat_env = os.getenv("SAT_RAW_DIR", str(BASE_DIR / "data_fallback"))
SAT_RAW_DIR: Path = Path(ruta_sat_env)

# Aprovisionamiento de directorios de infraestructura local
for folder in (LOG_DIR, TEMP_DIR):
    folder.mkdir(parents=True, exist_ok=True)

# =============================================================================
# 2. GESTIÓN DE MEMORIA Y RENDIMIENTO (POLARS - SWAP)
# =============================================================================
# Redirección del directorio de Swap temporal para evadir saturación en disco de sistema (C:)
ruta_temp_env = os.getenv("ETL_TEMP_DIR", r"C:\Temp_Polars")
DISCO_TRABAJO: Path = Path(ruta_temp_env)
DISCO_TRABAJO.mkdir(parents=True, exist_ok=True)

# Inyección de variables de entorno para el motor subyacente de Rust
os.environ["POLARS_TEMP_DIR"] = str(DISCO_TRABAJO)
os.environ["TMPDIR"] = str(DISCO_TRABAJO)
os.environ["TEMP"] = str(DISCO_TRABAJO)
os.environ["TMP"] = str(DISCO_TRABAJO)
os.environ["POLARS_MAX_THREADS"] = "4"

# Parámetros de renderizado en consola para trazabilidad visual
pl.Config.set_tbl_rows(20)
pl.Config.set_fmt_str_lengths(50)

# Constante volumétrica para iteración de lectura (Chunking)
BATCH_SIZE: int = 100_000

# =============================================================================
# 3. CONTRATO DE ORQUESTACIÓN (TYPED DICT)
# =============================================================================
class TableConfig(TypedDict):
    """Esquema estricto para la configuración modular de cada Anexo.
    
    Attributes:
        file_name (str): Nombre del archivo físico a extraer.
        table_name (str): Nombre de la tabla lógica destino en SQL Server.
        separator (str): Carácter delimitador de campos.
        dedupe_enabled (bool): Activa la estrategia de deduplicación relacional.
        dedupe_keys (list[str]): Llaves lógicas para el agrupamiento de duplicados.
        order_by (list[str]): Criterio de ordenamiento para resolución de conflictos.
        log_table_name (Optional[str]): Tabla destino para auditoría forense (Logs).
        cast_warning_columns (list[str]): Columnas monitoreadas por conversión nula.
    """
    file_name: str
    table_name: str
    separator: str
    dedupe_enabled: bool
    dedupe_keys: list[str]
    order_by: list[str]
    log_table_name: Optional[str]
    cast_warning_columns: list[str]

# DICCIONARIO MAESTRO DE RUTEO LÓGICO
TABLES_CONFIG: dict[str, TableConfig] = {
    # ---------------- AÑO 2025 - SEMESTRE 1 ----------------
    "NOMINA_3C_2025_1S": {
        "file_name": "GERG_AECF_1891_Anexo3C.csv", "table_name": "ANEXO_3C_NOMINA_2025_1S", 
        "separator": "|", "dedupe_enabled": True, "dedupe_keys": ["UUID", "FechaPago", "TotalPercepciones", "TotalDeducciones"], 
        "order_by": ["FilaOrigen"], "log_table_name": "LOG_CONSOLIDACION_ANEXO_3C_NOMINA_2025_1S", "cast_warning_columns": ["FechaPago", "TotalPercepciones", "TotalDeducciones"]
    },
    "NOMINA_4D_2025_1S": {
        "file_name": "GERG_AECF_1891_Anexo4D.csv", "table_name": "ANEXO_4D_NOMINA_2025_1S", 
        "separator": "|", "dedupe_enabled": False, "dedupe_keys": [], 
        "order_by": ["FilaOrigen"], "log_table_name": None, "cast_warning_columns": []
    },
    "NOMINA_5E_2025_1S": {
        "file_name": "GERG_AECF_1891_Anexo5E.csv", "table_name": "ANEXO_5E_NOMINA_2025_1S", 
        "separator": "|", "dedupe_enabled": False, "dedupe_keys": [], 
        "order_by": ["FilaOrigen"], "log_table_name": None, "cast_warning_columns": []
    },
    "NOMINA_6F_2025_1S": {
        "file_name": "GERG_AECF_1891_Anexo6F.csv", "table_name": "ANEXO_6F_NOMINA_2025_1S", 
        "separator": "|", "dedupe_enabled": False, "dedupe_keys": [], 
        "order_by": ["FilaOrigen"], "log_table_name": None, "cast_warning_columns": []
    },
    "NOMINA_7G_2025_1S": {
        "file_name": "GERG_AECF_1891_Anexo7G.csv", "table_name": "ANEXO_7G_NOMINA_2025_1S", 
        "separator": "|", "dedupe_enabled": False, "dedupe_keys": [], 
        "order_by": ["FilaOrigen"], "log_table_name": None, "cast_warning_columns": []
    },

    # ---------------- AÑO 2025 - SEMESTRE 2 ----------------
    "NOMINA_3C_2025_2S": {
        "file_name": "AECF_0129_Anexo1_TablaC.csv", "table_name": "ANEXO_3C_NOMINA_2025_2S", 
        "separator": "|", "dedupe_enabled": True, "dedupe_keys": ["UUID", "FechaPago", "TotalPercepciones", "TotalDeducciones"], 
        "order_by": ["FilaOrigen"], "log_table_name": "LOG_CONSOLIDACION_ANEXO_3C_NOMINA_2025_2S", "cast_warning_columns": ["FechaPago", "TotalPercepciones", "TotalDeducciones"]
    },
    "NOMINA_4D_2025_2S": {
        "file_name": "AECF_0129_Anexo1_TablaD.csv", "table_name": "ANEXO_4D_NOMINA_2025_2S", 
        "separator": "|", "dedupe_enabled": False, "dedupe_keys": [], 
        "order_by": ["FilaOrigen"], "log_table_name": None, "cast_warning_columns": []
    },
    "NOMINA_5E_2025_2S": {
        "file_name": "AECF_0129_Anexo1_TablaE.csv", "table_name": "ANEXO_5E_NOMINA_2025_2S", 
        "separator": "|", "dedupe_enabled": False, "dedupe_keys": [], 
        "order_by": ["FilaOrigen"], "log_table_name": None, "cast_warning_columns": []
    },
    "NOMINA_6F_2025_2S": {
        "file_name": "AECF_0129_Anexo1_TablaF.csv", "table_name": "ANEXO_6F_NOMINA_2025_2S", 
        "separator": "|", "dedupe_enabled": False, "dedupe_keys": [], 
        "order_by": ["FilaOrigen"], "log_table_name": None, "cast_warning_columns": []
    },
    "NOMINA_7G_2025_2S": {
        "file_name": "AECF_0129_Anexo1_TablaG.csv", "table_name": "ANEXO_7G_NOMINA_2025_2S", 
        "separator": "|", "dedupe_enabled": False, "dedupe_keys": [], 
        "order_by": ["FilaOrigen"], "log_table_name": None, "cast_warning_columns": []
    },

    # ---------------- AÑO 2024 (COMPLETO) ----------------
    "NOMINA_ANEXO_3_2024": {
        "file_name": "AECF_0101_Anexo3.csv", "table_name": "ANEXO_3C_NOMINA_2024", 
        "separator": "|", "dedupe_enabled": True, "dedupe_keys": ["UUID", "FechaPago", "TotalPercepciones", "TotalDeducciones"], 
        "order_by": ["FilaOrigen"], "log_table_name": "LOG_CONSOLIDACION_ANEXO_3C_NOMINA_2024", "cast_warning_columns": ["FechaPago", "TotalPercepciones", "TotalDeducciones"]
    },
    "NOMINA_ANEXO_4_2024": {
        "file_name": "AECF_0101_Anexo4.csv", "table_name": "ANEXO_4D_NOMINA_2024", 
        "separator": "|", "dedupe_enabled": False, "dedupe_keys": [], 
        "order_by": ["FilaOrigen"], "log_table_name": None, "cast_warning_columns": []
    },
    "NOMINA_ANEXO_5_2024": {
        "file_name": "AECF_0101_Anexo5.csv", "table_name": "ANEXO_5E_NOMINA_2024", 
        "separator": "|", "dedupe_enabled": False, "dedupe_keys": [], 
        "order_by": ["FilaOrigen"], "log_table_name": None, "cast_warning_columns": []
    },
    "NOMINA_ANEXO_6_2024": {
        "file_name": "AECF_0101_Anexo6.csv", "table_name": "ANEXO_6F_NOMINA_2024", 
        "separator": "|", "dedupe_enabled": False, "dedupe_keys": [], 
        "order_by": ["FilaOrigen"], "log_table_name": None, "cast_warning_columns": []
    },
    "NOMINA_ANEXO_7_2024": {
        "file_name": "AECF_0101_Anexo7.csv", "table_name": "ANEXO_7G_NOMINA_2024", 
        "separator": "|", "dedupe_enabled": False, "dedupe_keys": [], 
        "order_by": ["FilaOrigen"], "log_table_name": None, "cast_warning_columns": []
    },

    # ---------------- AÑO 2023 (COMPLETO) ----------------
    "NOMINA_ANEXO_3_2023": {
        "file_name": "AECF_0488_Anexo3.csv", "table_name": "ANEXO_3C_NOMINA_2023", 
        "separator": "|", "dedupe_enabled": True, "dedupe_keys": ["UUID", "FechaPago", "TotalPercepciones", "TotalDeducciones"], 
        "order_by": ["FilaOrigen"], "log_table_name": "LOG_CONSOLIDACION_ANEXO_3C_NOMINA_2023", "cast_warning_columns": ["FechaPago", "TotalPercepciones", "TotalDeducciones"]
    },
    "NOMINA_ANEXO_4_2023": {
        "file_name": "AECF_0101_Anexo4.utf8_clean.csv", "table_name": "ANEXO_4D_NOMINA_2023", 
        "separator": "|", "dedupe_enabled": False, "dedupe_keys": [], 
        "order_by": ["FilaOrigen"], "log_table_name": None, "cast_warning_columns": []
    },
    "NOMINA_ANEXO_5_2023": {
        "file_name": "AECF_0488_Anexo5.csv", "table_name": "ANEXO_5E_NOMINA_2023", 
        "separator": "|", "dedupe_enabled": False, "dedupe_keys": [], 
        "order_by": ["FilaOrigen"], "log_table_name": None, "cast_warning_columns": []
    },
    "NOMINA_ANEXO_6_2023": {
        "file_name": "AECF_0488_Anexo6.csv", "table_name": "ANEXO_6F_NOMINA_2023", 
        "separator": "|", "dedupe_enabled": False, "dedupe_keys": [], 
        "order_by": ["FilaOrigen"], "log_table_name": None, "cast_warning_columns": []
    },
    "NOMINA_ANEXO_7_2023": {
        "file_name": "AECF_0488_Anexo7.csv", "table_name": "ANEXO_7G_NOMINA_2023", 
        "separator": "|", "dedupe_enabled": False, "dedupe_keys": [], 
        "order_by": ["FilaOrigen"], "log_table_name": None, "cast_warning_columns": []
    },
    
    # ---------------- ENTORNO DE PRUEBAS (TEST) ----------------
    "TEST_4D": {
        "file_name": "AECF_0101_Anexo4.csv",  
        "table_name": "ANEXO_4D_TEST",        
        "separator": "|", 
        "dedupe_enabled": False, 
        "dedupe_keys": [], 
        "order_by": ["FilaOrigen"], 
        "log_table_name": "LOG_CONSOLIDACION_TEST_4D", 
        "cast_warning_columns": []
    }
}

# =============================================================================
# 4. CONTRATO DE TIPADO DINÁMICO
# =============================================================================
# Definición de tipos de datos forzados en memoria RAM antes de inyección ADBC
REGLAS_DINAMICAS: dict[str, Any] = {
    "UUID": pl.Utf8,
    "EmisorRFC": pl.Utf8,
    "ReceptorRFC": pl.Utf8,
    "TipoNomina": pl.Utf8,
    "FechaPago": pl.Utf8,
    "FechaInicialPago": pl.Utf8,
    "FechaFinalPago": pl.Utf8,
    "NumDiasPagados": pl.Float64,
    "TotalPercepciones": pl.Float64,
    "TotalDeducciones": pl.Float64,
    "TotalOtrosPagos": pl.Float64,
    "PercepcionesTotalGravado": pl.Float64,
    "PercepcionesTotalExento": pl.Float64,
    "TotalOtrasDeducciones": pl.Float64,
    "NominaTotalImpuestosRetenidos": pl.Float64,
    "EmisorCurp": pl.Utf8,
    "EmisorEntidadSNCFOrigenRecurso": pl.Utf8,
    "EmisorEntidadSNCFMontoRecursoPropio": pl.Float64,
    "ReceptorNumSeguridadSocial": pl.Utf8,
    "ReceptorFechaInicioRelLaboral": pl.Utf8,
    "ReceptorTipoContrato": pl.Utf8,
    "ReceptorTipoRegimen": pl.Utf8,
    "ReceptorNumEmpleado": pl.Utf8,
    "ReceptorDepartamento": pl.Utf8,
    "ReceptorPuesto": pl.Utf8,
    "ReceptorPeriodicidadPago": pl.Utf8,
    "ReceptorCuentaBancaria": pl.Utf8,
    "ReceptorBanco": pl.Utf8,
    "FechaCancelacion": pl.Utf8,
    "FilaOrigen": pl.Int64,

    "TipoPercepcion": pl.Utf8,
    "PercepcionClave": pl.Utf8,
    "PercepcionConcepto": pl.Utf8,
    "PercepcionImporteGravado": pl.Float64,
    "PercepcionImporteExento": pl.Float64,

    "DeduccionTipoDeduccion": pl.Utf8,
    "DeduccionClave": pl.Utf8,
    "DeduccionConcepto": pl.Utf8,
    "DeduccionesImporte": pl.Float64,

    "PercepcionesTotalSueldos": pl.Float64,
    "PercepcionesTotalSeparacionIndemnizacion": pl.Float64,
    "PercepcionesTotalJubilacionPensionRetiro": pl.Float64,
    "JubilacionPensionRetiroTotalUnaExhibicion": pl.Float64,
    "JubilacionPensionRetiroTotalParcialidad": pl.Float64,
    "JubilacionPensionRetiroMontoDiario": pl.Float64,
    "JubilacionPensionRetiroIngresoAcumulable": pl.Float64,
    "JubilacionPensionRetiroIngresoNoAcumulable": pl.Float64,
    "SeparacionIndemnizacionTotalPagado": pl.Float64,
    "SeparacionIndemnizacionNumAniosServicio": pl.Float64,
    "SeparacionIndemnizacionUltimoSueldoMensOrd": pl.Float64,
    "SeparacionIndemnizacionIngresoAcumulable": pl.Float64,
    "SeparacionIndemnizacionIngresoNoAcumulable": pl.Float64,

    "SecuenciaOtrosPagos": pl.Utf8,
    "TipoOtroPago": pl.Utf8,
    "Clave": pl.Utf8,
    "Concepto": pl.Utf8,
    "Importe": pl.Float64,
    "SubsidioCausado": pl.Float64,
    "SaldoAFavor": pl.Float64,
    "Anio": pl.Utf8,
    "RemanenteSalFav": pl.Float64,
}