"""
Módulo vectorizado de saneamiento, normalización y bifurcación lógica.

Constituye el núcleo de las reglas de negocio del pipeline. Ejecuta la limpieza 
de codificación (Mojibake), normalización de cadenas y validaciones estructurales.
Actúa como un enrutador (Router), segregando los registros con integridad 
referencial comprometida hacia una bandeja de cuarentena (Dead Letter Queue)
para blindar la inserción en SQL Server.
"""

from typing import Tuple

import polars as pl

from pkg.cleaning_rules import REEMPLAZOS_MOJIBAKE


def transform_sat_batch(df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Ejecuta el ciclo completo de limpieza y validación sobre un lote de datos.

    Args:
        df (pl.DataFrame): Lote crudo escaneado desde el archivo fuente.

    Returns:
        Tuple[pl.DataFrame, pl.DataFrame]: Una tupla que contiene:
            - DataFrame [0]: Registros validados y curados, listos para inserción.
            - DataFrame [1]: Registros anómalos (Cuarentena) con motivo de rechazo.
    """
    if df.is_empty():
        return df, df.with_columns(pl.lit("").alias("Motivo_Rechazo")).clear()

    # Inicialización del esquema para la bandeja de cuarentena (Dead Letter Queue)
    df_cuarentena = df.clear().with_columns(pl.lit("").alias("Motivo_Rechazo"))
    df_sano = df

    # =========================================================================
    # FASE 1: Normalización Base (Vectorizada)
    # =========================================================================
    df_sano = df_sano.with_columns(
        pl.col(pl.Utf8).str.strip_chars().str.to_uppercase().name.keep()
    )

    # =========================================================================
    # FASE 2 y 3: Identificación Sensible y Limpieza Quirúrgica (Mojibake)
    # =========================================================================
    palabras_clave = ["NOMBRE", "CONCEPTO", "DESCRIPCION", "CONDICIONES"]
    
    cols_a_limpiar = [
        col for col in df_sano.columns 
        if any(key in col.upper() for key in palabras_clave) and df_sano.schema[col] == pl.Utf8
    ]

    if cols_a_limpiar:
        patrones_sucios = list(REEMPLAZOS_MOJIBAKE.keys())
        valores_limpios = list(REEMPLAZOS_MOJIBAKE.values())
        regex_basura = r"[\ufffdÃÐðƑâÂ˜¨´™&]"

        # Aplicación condicional de diccionario para mitigar sobrecarga de CPU
        df_sano = df_sano.with_columns(
            [
                pl.when(pl.col(c).str.contains(regex_basura))
                .then(pl.col(c).str.replace_many(patrones_sucios, valores_limpios))
                .otherwise(pl.col(c))
                .alias(c)
                for c in cols_a_limpiar
            ]
        )

    # =========================================================================
    # FASE 4: Validación de Integridad Referencial (Cruce UUID)
    # =========================================================================
    if "UUID" in df_sano.columns:
        regex_uuid = r"^[A-Za-z0-9]{8}-[A-Za-z0-9]{4}-[A-Za-z0-9]{4}-[A-Za-z0-9]{4}-[A-Za-z0-9]{12}$"
        
        # Aislamiento de registros con UUIDs anómalos o saltos de línea mal formados
        df_uuid_malos = df_sano.filter(~pl.col("UUID").str.contains(regex_uuid)).with_columns(
            pl.lit("Violación de Integridad: UUID Inválido o Salto de Línea").alias("Motivo_Rechazo")
        )
        if not df_uuid_malos.is_empty():
            df_cuarentena = pl.concat([df_cuarentena, df_uuid_malos], how="diagonal_relaxed")
            
        # Preservación exclusiva del bloque con llaves foráneas válidas
        df_sano = df_sano.filter(pl.col("UUID").str.contains(regex_uuid))    

    # =========================================================================
    # FASE 5: Blindaje Anti-Desbordamiento Numérico (Out-of-Range Guard)
    # =========================================================================
    cols_monetarias = [
        "Descuento", "SubTotal", "Total", "TrasladosIVA", "TrasladosIEPS", 
        "TotalImpuestosTrasladados", "RetenidosIVA", "RetenidosISR", 
        "TotalImpuestosRetenidos", "TipoCambio"
    ]
    
    LIMITE_SEGURO = 999999999999.0 
    columnas_presentes = [c for c in cols_monetarias if c in df_sano.columns]

    if columnas_presentes:
        # Evaluación horizontal para detectar cualquier celda desbordada en la fila
        condicion_desbordamiento = pl.any_horizontal(
            [pl.col(c).cast(pl.Float64, strict=False).abs() > LIMITE_SEGURO for c in columnas_presentes]
        )
        
        # Aislamiento de facturas con montos irreales
        filas_culpables = df_sano.filter(condicion_desbordamiento).with_columns(
            pl.lit("Desbordamiento Numérico en BD: Anulado lógicamente").alias("Motivo_Rechazo")
        )
        
        if not filas_culpables.is_empty():
            df_cuarentena = pl.concat([df_cuarentena, filas_culpables], how="diagonal_relaxed")

        # Conversión del valor ofensivo a Nulo Analítico para permitir inserción del resto del registro
        df_sano = df_sano.with_columns(
            [
                pl.when(pl.col(c).cast(pl.Float64, strict=False).abs() > LIMITE_SEGURO)
                .then(None) 
                .otherwise(pl.col(c))
                .alias(c)
                for c in columnas_presentes
            ]
        )

    return df_sano, df_cuarentena