"""
Módulo vectorizado de saneamiento, normalización y Data Quality.

Constituye el núcleo de las reglas de negocio del pipeline. Ejecuta la limpieza 
de codificación (Mojibake), normalización de cadenas y validaciones estructurales.
Actúa como un enrutador (Router), segregando los registros en dos categorías:
1. Hard Rejects: Violaciones críticas que se destruyen del flujo principal.
2. Soft Rejects: Anomalías de negocio que se insertan en SQL pero se auditan en DLQ.
"""

from typing import Tuple

import polars as pl

from pkg.cleaning_rules import REEMPLAZOS_MOJIBAKE


def transform_sat_batch(df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Ejecuta el ciclo completo de Data Quality y limpieza sobre un lote de datos.

    Args:
        df (pl.DataFrame): Lote crudo escaneado desde el archivo fuente.

    Returns:
        Tuple[pl.DataFrame, pl.DataFrame]:
            - DataFrame [0]: Registros validados y curados, listos para SQL Server.
            - DataFrame [1]: Bandeja de Cuarentena (Hard y Soft Rejects) consolidada.
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
        c for c in df_sano.columns 
        if any(key in c.upper() for key in palabras_clave) and df_sano.schema[c] == pl.Utf8
    ]

    if cols_a_limpiar:
        patrones_sucios = list(REEMPLAZOS_MOJIBAKE.keys())
        valores_limpios = list(REEMPLAZOS_MOJIBAKE.values())
        # Actualizado con el signo de interrogación literal (\?)
        regex_basura = r"[\ufffdÃÐðƑâÂ˜¨´™&\?]"

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
    # FASE 4: HARD REJECTS (Violaciones Estructurales - Se excluyen de SQL)
    # =========================================================================
    
    # 4.1. Integridad de la Llave Primaria (UUID)
    if "UUID" in df_sano.columns:
        regex_uuid = r"^[A-Z0-9]{8}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{12}$"
        
        # Aislamiento de registros con UUIDs anómalos o saltos de línea mal formados
        df_uuid_malos = df_sano.filter(~pl.col("UUID").str.contains(regex_uuid)).with_columns(
            pl.lit("[HARD] Violación de Integridad: UUID Inválido o Roto").alias("Motivo_Rechazo")
        )
        if not df_uuid_malos.is_empty():
            df_cuarentena = pl.concat([df_cuarentena, df_uuid_malos], how="diagonal_relaxed")
            
        # Preservación exclusiva del bloque con llaves foráneas válidas
        df_sano = df_sano.filter(pl.col("UUID").str.contains(regex_uuid))    

    # 4.2. Nulos Críticos (EmisorRFC Vacío)
    if "EmisorRFC" in df_sano.columns:
        df_rfc_vacio = df_sano.filter(pl.col("EmisorRFC").is_null() | (pl.col("EmisorRFC") == "")).with_columns(
            pl.lit("[HARD] Dato Huérfano: EmisorRFC Vacío").alias("Motivo_Rechazo")
        )
        if not df_rfc_vacio.is_empty():
            df_cuarentena = pl.concat([df_cuarentena, df_rfc_vacio], how="diagonal_relaxed")
            
        df_sano = df_sano.filter(pl.col("EmisorRFC").is_not_null() & (pl.col("EmisorRFC") != ""))

    # =========================================================================
    # FASE 5: SOFT REJECTS (Advertencias de Negocio - SÍ entran a SQL, pero se auditan)
    # =========================================================================
    
    # 5.1. Validación Criptográfica de Formato RFC
    if "EmisorRFC" in df_sano.columns:
        # Regex: 3 o 4 letras (incluye Ñ y &), 6 números, 3 alfanuméricos
        regex_rfc = r"^[A-Z&Ñ]{3,4}\d{6}[A-Z0-9]{3}$"
        df_rfc_invalido = df_sano.filter(~pl.col("EmisorRFC").str.contains(regex_rfc)).with_columns(
            pl.lit("[SOFT] Formato de EmisorRFC Anómalo").alias("Motivo_Rechazo")
        )
        if not df_rfc_invalido.is_empty():
            df_cuarentena = pl.concat([df_cuarentena, df_rfc_invalido], how="diagonal_relaxed")

    # 5.2. Catálogos Cerrados Básicos
    if "TipoDeComprobante" in df_sano.columns:
        df_tc_invalido = df_sano.filter(~pl.col("TipoDeComprobante").is_in(["I", "E", "T", "P", "N"])).with_columns(
            pl.lit("[SOFT] Tipo de Comprobante Desconocido").alias("Motivo_Rechazo")
        )
        if not df_tc_invalido.is_empty():
            df_cuarentena = pl.concat([df_cuarentena, df_tc_invalido], how="diagonal_relaxed")

    if "Moneda" in df_sano.columns:
        df_moneda_rara = df_sano.filter(
            pl.col("Moneda").is_not_null() & (pl.col("Moneda").str.len_chars() > 3)
        ).with_columns(
            pl.lit("[SOFT] Moneda Anómala (No ISO 4217)").alias("Motivo_Rechazo")
        )
        if not df_moneda_rara.is_empty():
            df_cuarentena = pl.concat([df_cuarentena, df_moneda_rara], how="diagonal_relaxed")

    # 5.3. Coherencia Temporal (Viajes en el tiempo)
    if "FechaEmision" in df_sano.columns and "FechaCancelacion" in df_sano.columns:
        # Al estar en formato ISO YYYY-MM-DD, la comparación de strings es matemáticamente segura
        df_fechas_malas = df_sano.filter(
            pl.col("FechaCancelacion").is_not_null() & 
            (pl.col("FechaCancelacion") != "") & 
            (pl.col("FechaCancelacion") < pl.col("FechaEmision"))
        ).with_columns(
            pl.lit("[SOFT] Inconsistencia: Fecha Cancelación anterior a Emisión").alias("Motivo_Rechazo")
        )
        if not df_fechas_malas.is_empty():
            df_cuarentena = pl.concat([df_cuarentena, df_fechas_malas], how="diagonal_relaxed")

    # 5.4. Blindaje Anti-Desbordamiento Numérico (Out-of-Range Guard)
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
            pl.lit("[SOFT] Alerta de Desbordamiento Aritmético (Trillones)").alias("Motivo_Rechazo")
        )
        
        if not filas_culpables.is_empty():
            df_cuarentena = pl.concat([df_cuarentena, filas_culpables], how="diagonal_relaxed")

        # NOTA: Se eliminó el bloque que volvía NULL los montos gigantes.
        # Ahora el número pasa íntegro a SQL Server para su evaluación de negocio.

    return df_sano, df_cuarentena