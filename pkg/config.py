"""Módulo de gestión centralizada de credenciales y conectividad (Capa de Persistencia).

Construye y reutiliza la instancia del motor relacional (SQLAlchemy) para garantizar
conexiones eficientes, seguras y resilientes a SQL Server mediante PyODBC. Implementa 
patrones de tolerancia a fallos de red y manejo de pool de conexiones optimizado 
para escenarios de alta volumetría transaccional.
"""

from __future__ import annotations

import os
import urllib.parse
from functools import lru_cache

from dotenv import load_dotenv
from sqlalchemy import Engine, create_engine

# Carga en memoria del archivo de configuración local (.env)
load_dotenv()

SERVER = os.getenv("DB_SERVER")
DATABASE = os.getenv("DB_NAME")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
DRIVER = os.getenv("DB_DRIVER", "{ODBC Driver 17 for SQL Server}")
TRUSTED = os.getenv("DB_TRUSTED", "NO")


def get_connection_string() -> str:
    """Construye la cadena de conexión blindada para el motor de SQL Server.

    Ensambla dinámicamente los parámetros de autenticación y configura los 
    parámetros ODBC para mitigar la interrupción de transacciones ante 
    inestabilidades transitorias en la capa de red.

    Returns:
        str: Cadena de conexión URI parametrizada y codificada de manera segura.

    Raises:
        ValueError: Si faltan parámetros críticos de infraestructura (Servidor/BD)
            o credenciales de acceso cuando no se utiliza autenticación integrada.
    """
    if not all([SERVER, DATABASE]):
        raise ValueError("Configuración incompleta: Faltan parámetros DB_SERVER o DB_NAME en las variables de entorno.")

    # Resolución del esquema de autenticación (Integrada vs. SQL Auth)
    if TRUSTED.upper() in {"YES", "TRUE", "1"}:
        auth = "Trusted_Connection=yes;"
    else:
        if not all([USER, PASSWORD]):
            raise ValueError("Autenticación denegada: Faltan credenciales DB_USER o DB_PASSWORD.")
        auth = f"UID={USER};PWD={PASSWORD};"

    # Configuración de tolerancia a fallos a nivel de socket de red
    odbc = (
        f"DRIVER={DRIVER};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"{auth}"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
        "ConnectRetryCount=3;"        # Límite de reintentos ante desconexiones abruptas
        "ConnectRetryInterval=10;"    # Período de enfriamiento (Backoff) de 10 segundos
    )

    # Codificación de la cadena ODBC para prevenir inyección o fallos por caracteres especiales
    params = urllib.parse.quote_plus(odbc)
    return f"mssql+pyodbc:///?odbc_connect={params}"


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """Retorna una instancia transaccional (Singleton) del motor SQLAlchemy.

    Utiliza una caché LRU de tamaño 1 para asegurar que todo el pipeline comparta 
    un único y optimizado pool de conexiones, previniendo la saturación de puertos 
    y sobrecarga de I/O en el servidor destino.

    Returns:
        Engine: Instancia del motor SQLAlchemy configurado para ejecución vectorizada
        (FastExecutemany) y validación de conectividad en tiempo real (Pool Pre-ping).
    """
    return create_engine(
        get_connection_string(),
        fast_executemany=True,  # Habilita el volcado masivo en memoria para conectores ODBC
        pool_pre_ping=True,     # Emite un query de validación ("ping") antes de asignar una conexión del pool
        future=True,
    )