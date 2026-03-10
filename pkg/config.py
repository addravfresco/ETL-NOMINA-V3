"""
Módulo de gestión centralizada de credenciales y parámetros de conexión.

Provee la lógica necesaria para construir cadenas de conexión seguras 
hacia SQL Server mediante SQLAlchemy y PyODBC. Depende estrictamente 
de las variables de entorno definidas en el archivo .env local.
"""

import os
import urllib.parse

from dotenv import load_dotenv

# Inicialización del entorno
load_dotenv()

SERVER = os.getenv("DB_SERVER")
DATABASE = os.getenv("DB_NAME")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
DRIVER = os.getenv("DB_DRIVER", "{ODBC Driver 17 for SQL Server}")
TRUSTED = os.getenv("DB_TRUSTED", "NO")


def get_connection_string() -> str:
    """
    Construye la cadena de conexión ODBC codificada para SQLAlchemy.

    Evalúa el tipo de autenticación configurada (Integrada de Windows vs. 
    SQL Server) y aplica URL-encoding a los parámetros para garantizar la 
    compatibilidad del motor si existen caracteres especiales en los secretos.

    Returns:
        str: Cadena de conexión formateada bajo el esquema mssql+pyodbc.

    Raises:
        ValueError: Si existen variables críticas de conexión ausentes en el entorno.
    """
    if not all([SERVER, DATABASE]):
        raise ValueError("Ausencia de variables críticas en el entorno: DB_SERVER o DB_NAME.")

    if TRUSTED.upper() in ["YES", "TRUE", "1"]:
        auth_str = "Trusted_Connection=yes;"
    else:
        if not all([USER, PASSWORD]):
            raise ValueError("Ausencia de DB_USER o DB_PASSWORD en el entorno para autenticación SQL.")
        auth_str = f"UID={USER};PWD={PASSWORD};"

    # Construcción de la cadena ODBC con bypass de validación SSL estricta (estándar On-Premise)
    cadena_odbc = (
        f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};"
        f"{auth_str}Encrypt=no;TrustServerCertificate=yes;"
    )
    
    # URL-encoding obligatorio para el motor de SQLAlchemy
    params = urllib.parse.quote_plus(cadena_odbc)
    
    return f"mssql+pyodbc:///?odbc_connect={params}"