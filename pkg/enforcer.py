"""
Módulo de Integridad Estructural y Tipado (Schema Enforcement).

Garantiza que el lote de datos cumpla estrictamente con el contrato DDL
de la base de datos destino (SQL Server). Aplica conversiones de tipo
seguras (Safe Casting) y truncamiento dinámico de cadenas para prevenir
desbordamientos y colapsos de integridad referencial.
"""

from typing import Any, Dict

import polars as pl

# Catálogo de longitudes máximas para mapeo DDL.
# Define la barrera de seguridad contra errores de truncamiento (String Truncation Errors).
LIMITES_LONGITUD: Dict[str, int] = {
    "UUID": 36,
    "EMISORRFC": 13,
    "RECEPTORRFC": 13,
    "MONEDA": 10,
    "TIPOCAMBIO": 10,
    "TIPODECOMPROBANTE": 10,
    "TIPOCONTRIBUYENTE": 10,
    "SERIE": 50,
    "FOLIO": 50,
    "FORMAPAGO": 5,
    "METODOPAGO": 5,
    "CONCEPTONOIDENTIFICACION": 100,
    "CONCEPTOCLAVEPRODSERV": 20,
    "CONCEPTOUNIDAD": 50,
    "CONCEPTODESCRIPCION": 1000,
    "NOCERTIFICADO": 500,
    "CONDICIONESDEPAGO": 500,
    "LUGAREXPEDICION": 500,
    "EMISORREGIMENFISCAL": 250,
    "RECEPTORNOMBRE": 500,
    "EMISORNOMBRE": 500
}


def estandarizar_nombres_columnas(df: pl.DataFrame) -> pl.DataFrame:
    """
    Sanitiza los identificadores de columna eliminando caracteres anómalos.

    Remueve saltos de línea y comillas para prevenir errores de sintaxis
    en las sentencias dinámicas de SQL, preservando la capitalización original.

    Args:
        df (pl.DataFrame): DataFrame con encabezados crudos.

    Returns:
        pl.DataFrame: DataFrame con la estructura de columnas sanitizada.
    """
    nuevas_columnas = [
        col.strip().replace("\n", "").replace('"', '').replace("'", "")
        for col in df.columns
    ]
    
    return df.rename(dict(zip(df.columns, nuevas_columnas)))


def aplicar_tipos_seguros(df: pl.DataFrame, reglas_tipos: Dict[str, Any]) -> pl.DataFrame:
    """
    Aplica coerciones de tipo tolerantes a fallos y limita la longitud de texto.

    Ejecuta un mapeo basado en metadatos para transformar el DataFrame al 
    tipado esperado por SQL Server. Utiliza el modo estricto desactivado 
    (strict=False) para convertir valores corruptos inmanejables en nulos 
    analíticos, evitando la detención del pipeline.

    Args:
        df (pl.DataFrame): Lote de datos normalizado.
        reglas_tipos (Dict[str, Any]): Diccionario de mapeo de tipos destino.

    Returns:
        pl.DataFrame: DataFrame tipado y truncado estructuralmente.
    """
    if df.is_empty():
        return df

    df = estandarizar_nombres_columnas(df)
    expresiones = []

    for col in df.columns:
        col_up = col.upper()

        # Bloque de Coerción Segura (Safe Casting)
        if col_up in reglas_tipos:
            dtype_destino = reglas_tipos[col_up]

            if dtype_destino == pl.Datetime:
                # Truncamiento previo para alinear con el formato ISO-8601 base
                expr = (
                    pl.col(col)
                    .str.slice(0, 19)
                    .str.to_datetime(strict=False)
                    .alias(col)
                )
            elif dtype_destino == pl.Float64:
                # Sanitización numérica pre-conversión
                expr = (
                    pl.col(col)
                    .str.replace_all(",", "")
                    .str.strip_chars()
                    .cast(pl.Float64, strict=False)
                    .alias(col)
                )
            else:
                expr = pl.col(col).cast(dtype_destino, strict=False).alias(col)

            expresiones.append(expr)

        # Bloque de Control de Longitud (Varchar Enforcement)
        elif df.schema[col] == pl.Utf8:
            limite = LIMITES_LONGITUD.get(col_up, 1000)
            expr = pl.col(col).str.slice(0, limite).alias(col)
            expresiones.append(expr)

    return df.with_columns(expresiones) if expresiones else df