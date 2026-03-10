"""
Módulo de persistencia masiva y carga idempotente (Upsert).

Implementa la arquitectura de Staging-to-Production para garantizar cargas
masivas de alto volumen hacia SQL Server. Ejecuta rutinas de de-duplicación 
intra-lote (Window Functions) y validación transaccional contra la tabla destino
para mantener la integridad referencial absoluta.
"""

import polars as pl
from sqlalchemy import create_engine, text

from pkg.config import get_connection_string

# Inicialización global del motor de conexión.
# El parámetro fast_executemany es crítico para habilitar la inserción
# masiva en bloque (Bulk Insert) a nivel del driver ODBC.
engine = create_engine(get_connection_string(), fast_executemany=True)


def upload_to_sql_blindado(df: pl.DataFrame, table_name: str, anexo_id: str) -> None:
    """
    Ejecuta una carga transaccional hacia SQL Server con resolución de conflictos.

    Implementa un patrón ELT (Extract, Load, Transform) volcando el bloque de
    memoria a una tabla temporal (Staging). Posteriormente, ejecuta sentencias
    SQL nativas para de-duplicar los registros físicamente leídos y descartar
    aquellos que ya existen en el Data Warehouse.

    Args:
        df (pl.DataFrame): Lote de datos normalizado y tipado.
        table_name (str): Nombre de la tabla destino en producción.
        anexo_id (str): Identificador del flujo para aplicar reglas específicas de llave primaria.

    Raises:
        ValueError: Si el anexo proporcionado no tiene una estrategia de Upsert definida.
    """
    if df.is_empty():
        return

    stg_table = f"STG_{table_name}"

    # Limpieza preventiva del área de Staging para evitar colisiones
    # entre hilos de ejecución o cargas previas abortadas.
    with engine.begin() as conn:
        conn.execute(
            text(f"IF OBJECT_ID('{stg_table}', 'U') IS NOT NULL TRUNCATE TABLE {stg_table};")
        )

    # Volcado masivo a Staging. La columna técnica 'FilaOrigen' se transfiere
    # temporalmente para permitir el ordenamiento determinista en base de datos.
    df.write_database(
        table_name=stg_table,
        connection=engine,
        if_table_exists="replace",
        engine="sqlalchemy"
    )

    # Resolución transaccional y mezcla (Merge)
    with engine.begin() as conn:
        # Aislamiento de columnas reales (excluyendo FilaOrigen) para la inserción final
        cols_destino = [col for col in df.columns if col != "FilaOrigen"]
        columnas_insert = ", ".join([f"[{col}]" for col in cols_destino])

        if anexo_id.startswith("1A"):
            # Estrategia Anexo 1A (Cabeceras): De-duplicación fundamentada en el UUID.
            upsert_query = f"""
                INSERT INTO {table_name} ({columnas_insert})
                SELECT {columnas_insert} FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY [UUID] ORDER BY [FilaOrigen] ASC) as rn
                    FROM {stg_table}
                ) AS Stg
                WHERE rn = 1
                AND NOT EXISTS (
                    SELECT 1 FROM {table_name} AS Dest
                    WHERE Dest.UUID = Stg.UUID
                );
            """

        elif anexo_id.startswith("2B"):
            # Estrategia Anexo 2B (Detalles): De-duplicación y generación de llave sintética.
            # Dado que el SAT no provee ID de detalle, se calcula un Hash SHA-256 compuesto.
            upsert_query = f"""
                WITH CalculoHash AS (
                    SELECT
                        CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', CONCAT([UUID], '|', [ConceptoClaveProdServ], '|', [ConceptoImporte], '|', [FilaOrigen])), 2) as NewHash,
                        *
                    FROM {stg_table}
                ),
                Unicos AS (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY NewHash ORDER BY [FilaOrigen] ASC) as rn
                    FROM CalculoHash
                )
                INSERT INTO {table_name} ([HashID], {columnas_insert})
                SELECT NewHash, {columnas_insert}
                FROM Unicos
                WHERE rn = 1
                AND NOT EXISTS (
                    SELECT 1 FROM {table_name} AS Dest
                    WHERE Dest.HashID = Unicos.NewHash
                );
            """
        else:
            raise ValueError(f"El prefijo del anexo '{anexo_id}' carece de una estrategia de Upsert configurada.")

        # Ejecución del motor transaccional
        conn.execute(text(upsert_query))

        # Destrucción del área de Staging post-carga
        conn.execute(
            text(f"IF OBJECT_ID('{stg_table}', 'U') IS NOT NULL DROP TABLE {stg_table};")
        )