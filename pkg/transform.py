"""Módulo vectorizado de saneamiento y Data Quality - VERTICAL NÓMINA.

Ejecuta rutinas de limpieza de codificación (Mojibake, mutilación de cadenas) 
y despliega un motor dinámico de Reglas de Negocio Forense (Data Quality) 
adaptable a la topología de los anexos de nómina (Tablas C, D, E, F, G).

Implementa el principio de "100% Volumetría": La totalidad de los registros 
se consolida en la capa relacional destino. Las anomalías estructurales y lógicas 
son detectadas en memoria y aisladas en una bitácora analítica paralela (DLQ) 
clasificada por severidad (HARD/SOFT Rejects).
"""

from typing import Tuple

import polars as pl

from pkg.cleaning_rules import REEMPLAZOS_MOJIBAKE, REEMPLAZOS_MUTILACION


def transform_sat_batch(df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Ejecuta el ciclo de saneamiento estructural y validación de reglas de negocio.

    Aplica normalización léxica vectorizada sobre el lote de datos y evalúa 
    condicionantes fiscales, aritméticos y de integridad referencial. Compila 
    las violaciones de reglas en un DataFrame secundario para su auditoría forense.

    Args:
        df (pl.DataFrame): Lote de datos crudos (Chunk) extraído del sistema de archivos.

    Returns:
        Tuple[pl.DataFrame, pl.DataFrame]: Una tupla que contiene:
            1. El DataFrame principal sanitizado y listo para consolidación.
            2. El DataFrame de bitácora (DQ Log) conteniendo la FilaOrigen, 
               la severidad del error y la descripción de la regla vulnerada.
    """
    if df.is_empty():
        schema_dq = {"FilaOrigen": pl.Int64, "Severidad": pl.Utf8, "Regla_Rota": pl.Utf8}
        return df, pl.DataFrame(schema=schema_dq)

    df_sano = df

    # =========================================================================
    # FASE 1: Sanitización Estructural y Resolución de Codificación (Mojibake)
    # =========================================================================
    # 1.1 Normalización léxica básica, remoción de comillas y caracteres de control
    acentos = ["Á", "É", "Í", "Ó", "Ú", "Ä", "Ë", "Ï", "Ö", "Ü", "Â", "Ê", "Î", "Ô", "Û"]
    limpias = ["A", "E", "I", "O", "U", "A", "E", "I", "O", "U", "A", "E", "I", "O", "U"]

    df_sano = df_sano.with_columns(
        pl.col(pl.Utf8).str.strip_chars().str.replace_all('"', '')
        .str.replace_all(r"ÃA", "IA").str.replace_all(r"ÃO", "IO") # Parches léxicos para SECRETARÍA, ENVÍO
        .str.replace_many(acentos, limpias)
        .name.keep()
    )

    # 1.2 Ejecución de diccionarios de recuperación forense (Mojibake y Mutilación)
    palabras_clave = ["NOMBRE", "CONCEPTO", "DESCRIPCION", "DEPARTAMENTO", "PUESTO"]
    cols_a_limpiar = [
        c for c in df_sano.columns 
        if any(key in c.upper() for key in palabras_clave) and df_sano.schema[c] == pl.Utf8
    ]

    if cols_a_limpiar:
        patrones_sucios = list(REEMPLAZOS_MOJIBAKE.keys())
        valores_limpios = list(REEMPLAZOS_MOJIBAKE.values())
        regex_basura = r"[\ufffdÃÐðƑâÂ˜¨´™&\?#]"

        # Aplicación condicional de reemplazo masivo para mitigación de sobrecarga en CPU
        df_sano = df_sano.with_columns(
            [
                pl.when(pl.col(c).str.contains(regex_basura))
                .then(pl.col(c).str.replace_many(patrones_sucios, valores_limpios))
                .otherwise(pl.col(c))
                .alias(c)
                for c in cols_a_limpiar
            ]
        )

        patrones_mut = list(REEMPLAZOS_MUTILACION.keys())
        valores_mut = list(REEMPLAZOS_MUTILACION.values())
        df_sano = df_sano.with_columns(
            [pl.col(c).str.replace_many(patrones_mut, valores_mut).alias(c) for c in cols_a_limpiar]
        )

    # Estandarización a mayúsculas y conversión de cadenas vacías a valores nulos (NULL)
    df_sano = df_sano.with_columns(pl.col(pl.Utf8).str.to_uppercase().name.keep())
    df_sano = df_sano.with_columns(pl.when(pl.col(pl.Utf8) == "").then(None).otherwise(pl.col(pl.Utf8)).name.keep())

    # =========================================================================
    # FASE 2: MOTOR DE DATA QUALITY (Validaciones Universales y por Anexo)
    # =========================================================================
    alertas = []
    
    # --- 2.1 Validación Estricta de Identificadores Primarios (HARD Rejects) ---
    if "UUID" in df_sano.columns:
        regex_uuid = r"^[A-Z0-9]{8}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{12}$"
        malos = df_sano.filter(~pl.col("UUID").str.contains(regex_uuid) & pl.col("UUID").is_not_null())
        if not malos.is_empty():
            alertas.append(malos.select([pl.col("FilaOrigen"), pl.lit("HARD").alias("Severidad"), pl.lit("UUID Inválido o Roto").alias("Regla_Rota")]))

    if "EmisorRFC" in df_sano.columns:
        vacios = df_sano.filter(pl.col("EmisorRFC").is_null())
        if not vacios.is_empty():
            alertas.append(vacios.select([pl.col("FilaOrigen"), pl.lit("HARD").alias("Severidad"), pl.lit("EmisorRFC Vacío (Dato Huérfano)").alias("Regla_Rota")]))

    # --- 2.2 Auditoría Fiscal (Formato RFC y NSS) ---
    regex_rfc = r"^[A-Z&Ñ]{3,4}\d{6}[A-Z0-9]{3}$"
    if "ReceptorRFC" in df_sano.columns:
        invalido = df_sano.filter(pl.col("ReceptorRFC").is_not_null() & ~pl.col("ReceptorRFC").str.contains(regex_rfc))
        if not invalido.is_empty():
            alertas.append(invalido.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("ReceptorRFC con formato anómalo").alias("Regla_Rota")]))

    if "ReceptorNumSeguridadSocial" in df_sano.columns: # Regla específica Anexo C
        nss_malo = df_sano.filter(pl.col("ReceptorNumSeguridadSocial").is_not_null() & (pl.col("ReceptorNumSeguridadSocial").str.len_chars() != 11))
        if not nss_malo.is_empty():
            alertas.append(nss_malo.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("NSS con longitud inválida").alias("Regla_Rota")]))

    # --- 2.3 Auditoría de Integridad Aritmética: Importes Negativos (HARD) ---
    cols_verificar_negativos = ["PercepcionImporteGravado", "PercepcionImporteExento", "DeduccionesImporte"]
    cols_neg_presentes = [c for c in cols_verificar_negativos if c in df_sano.columns]
    if cols_neg_presentes:
        mask_neg = pl.any_horizontal([(pl.col(c).cast(pl.Float64, strict=False) < 0) for c in cols_neg_presentes])
        negativos = df_sano.filter(mask_neg)
        if not negativos.is_empty():
            alertas.append(negativos.select([pl.col("FilaOrigen"), pl.lit("HARD").alias("Severidad"), pl.lit("Importe Negativo Detectado").alias("Regla_Rota")]))

    # --- 2.4 Cuadratura Contable de Nómina (Tabla C) ---
    if all(c in df_sano.columns for c in ["TotalPercepciones", "PercepcionesTotalGravado", "PercepcionesTotalExento"]):
        df_math = df_sano.select([
            pl.col("TotalPercepciones").cast(pl.Float64, strict=False).fill_null(0.0),
            pl.col("PercepcionesTotalGravado").cast(pl.Float64, strict=False).fill_null(0.0),
            pl.col("PercepcionesTotalExento").cast(pl.Float64, strict=False).fill_null(0.0),
            pl.col("FilaOrigen")
        ])
        mask_descuadre = (
            (df_math["PercepcionesTotalGravado"] + df_math["PercepcionesTotalExento"] - df_math["TotalPercepciones"]).abs() > 1.0
        )
        descuadrados = df_math.filter(mask_descuadre)
        if not descuadrados.is_empty():
            alertas.append(descuadrados.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("Descuadre Interno: Gravado+Exento != TotalPercepciones").alias("Regla_Rota")]))

    # --- 2.5 Validación de Integridad de Subsidio (Tabla G) ---
    if "TipoOtroPago" in df_sano.columns and "SubsidioCausado" in df_sano.columns:
        sub_error = df_sano.filter((pl.col("TipoOtroPago") == "002") & pl.col("SubsidioCausado").is_null())
        if not sub_error.is_empty():
            alertas.append(sub_error.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("Falta SubsidioCausado en TipoOtroPago 002").alias("Regla_Rota")]))

    # --- 2.6 Prevención de Desbordamiento Aritmético (Overflow Threshold) ---
    cols_monetarias = [
        "TotalPercepciones", "TotalDeducciones", "TotalOtrosPagos", 
        "PercepcionesTotalGravado", "PercepcionesTotalExento", 
        "TotalOtrasDeducciones", "NominaTotalImpuestosRetenidos",
        "PercepcionImporteGravado", "PercepcionImporteExento", "DeduccionesImporte", "Importe"
    ]
    columnas_presentes = [c for c in cols_monetarias if c in df_sano.columns]
    if columnas_presentes:
        LIMITE_SEGURO = 999999999999.0 
        condicion_desbordamiento = pl.any_horizontal(
            [pl.col(c).cast(pl.Float64, strict=False).abs() > LIMITE_SEGURO for c in columnas_presentes]
        )
        filas_culpables = df_sano.filter(condicion_desbordamiento)
        if not filas_culpables.is_empty():
            alertas.append(filas_culpables.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("Alerta de Desbordamiento Aritmético (> 1 Trillón)").alias("Regla_Rota")]))

    # --- 2.7 Validación Lógica de Fechas (Tabla C) ---
    if "FechaInicialPago" in df_sano.columns and "FechaFinalPago" in df_sano.columns:
        df_fechas_malas = df_sano.filter(
            pl.col("FechaFinalPago").is_not_null() & (pl.col("FechaFinalPago") < pl.col("FechaInicialPago"))
        )
        if not df_fechas_malas.is_empty():
            alertas.append(df_fechas_malas.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("Inconsistencia: Fecha Final anterior a Inicial").alias("Regla_Rota")]))

    # --- 2.8 Auditoría de Parámetros de Operación: Días Pagados Atípicos (Tabla C) ---
    if "NumDiasPagados" in df_sano.columns:
        if "TipoNomina" in df_sano.columns:
            mask_dias_atipicos = (
                ((pl.col("TipoNomina") == "O") & (pl.col("NumDiasPagados").cast(pl.Float64, strict=False) > 35.0)) | 
                ((pl.col("TipoNomina") == "E") & (pl.col("NumDiasPagados").cast(pl.Float64, strict=False) > 365.0))
            )
        else:
            mask_dias_atipicos = pl.col("NumDiasPagados").cast(pl.Float64, strict=False) > 365.0

        df_dias_atipicos = df_sano.filter(mask_dias_atipicos)
        if not df_dias_atipicos.is_empty():
            alertas.append(df_dias_atipicos.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("Días Pagados atípicos según Tipo de Nómina").alias("Regla_Rota")]))

    # =========================================================================
    # Consolidación de Bitácora Analítica (DQ Log)
    # =========================================================================
    if alertas:
        df_dq_log = pl.concat(alertas)
    else:
        df_dq_log = pl.DataFrame(schema={"FilaOrigen": pl.Int64, "Severidad": pl.Utf8, "Regla_Rota": pl.Utf8})

    return df_sano, df_dq_log


def inyectar_hash_nomina(df: pl.DataFrame, dedupe_enabled: bool) -> pl.DataFrame:
    """Calcula un identificador transaccional único (HashID) para estrategias de deduplicación.

    Implementa el motor de hash nativo de Polars (respaldado en Rust) para generar 
    firmas únicas de filas completas en memoria, garantizando tiempos de procesamiento 
    sub-milisegundo en lotes masivos.

    Args:
        df (pl.DataFrame): Lote de datos sobre el cual generar la firma.
        dedupe_enabled (bool): Bandera arquitectónica que habilita o suprime el cálculo.

    Returns:
        pl.DataFrame: DataFrame con la columna 'HashID' inyectada o con valores nulos 
        si la estrategia de deduplicación está inactiva.
    """
    if df.is_empty() or not dedupe_enabled:
        return df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("HashID"))

    # Exclusión estricta del identificador secuencial para evitar variabilidad del hash
    cols_llave = [c for c in df.columns if c != "FilaOrigen"]
    if not cols_llave:
        return df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("HashID"))

    # Ejecución vectorizada del hash lógico
    df = df.with_columns(
        pl.concat_str([pl.col(c).cast(pl.Utf8).fill_null("") for c in cols_llave], separator="|")
        .hash()
        .cast(pl.Utf8)
        .alias("HashID")
    )
    
    return df