"""Módulo Core de Transformación y Data Quality - VERTICAL NÓMINA.

Ejecuta el ciclo de validación y limpieza estructural (Espejo Milimétrico).
Aplica el estándar unificado de arquitectura: Inmunidad a Schema Drift (búsqueda
dinámica de columnas) y alta eficiencia de memoria mediante proyección temprana
de atributos para el registro de anomalías forenses (DLQ).
"""

from typing import List, Tuple

import polars as pl
import polars.selectors as cs

from pkg.cleaning_rules import REEMPLAZOS_MOJIBAKE, REEMPLAZOS_MUTILACION


def _get_col_name(df: pl.DataFrame, target_name: str) -> str | None:
    """Busca el identificador físico de una columna mediante coincidencia case-insensitive."""
    for col in df.columns:
        if col.upper() == target_name.upper():
            return col
    return None


def transform_sat_batch(df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Ejecuta el ciclo de saneamiento estructural y validación de nómina."""
    if df.is_empty():
        schema_dq = {"FilaOrigen": pl.Int64, "Severidad": pl.Utf8, "Regla_Rota": pl.Utf8}
        return df, pl.DataFrame(schema=schema_dq)

    df_sano = df
    alertas: List[pl.DataFrame] = []

    # =========================================================================
    # FASE 1: Sanitización Estructural y Resolución de Codificación (Mojibake)
    # =========================================================================
    acentos = ["Á", "É", "Í", "Ó", "Ú", "Ä", "Ë", "Ï", "Ö", "Ü", "Â", "Ê", "Î", "Ô", "Û"]
    limpias = ["A", "E", "I", "O", "U", "A", "E", "I", "O", "U", "A", "E", "I", "O", "U"]

    df_sano = df_sano.with_columns(
        cs.string()
        .str.replace_all('"', '')  
        .str.strip_chars()
        .str.to_uppercase()
        .str.replace_all(r"ÃA", "IA")
        .str.replace_all(r"ÃO", "IO")
        .str.replace_many(acentos, limpias)
    ).with_columns(
        pl.when(cs.string() == "").then(None).otherwise(cs.string()).name.keep()
    )

    palabras_clave = ["NOMBRE", "CONCEPTO", "DESCRIPCION", "DEPARTAMENTO", "PUESTO"]
    cols_a_limpiar = [c for c in df_sano.columns if any(key in c.upper() for key in palabras_clave) and df_sano.schema[c] == pl.Utf8]

    if cols_a_limpiar:
        patrones_sucios = list(REEMPLAZOS_MOJIBAKE.keys())
        valores_limpios = list(REEMPLAZOS_MOJIBAKE.values())
        regex_basura = r"[\ufffdÃÐðƑâÂ˜¨´™&\?#]"

        df_sano = df_sano.with_columns([
            pl.when(pl.col(c).str.contains(regex_basura))
            .then(pl.col(c).str.replace_many(patrones_sucios, valores_limpios))
            .otherwise(pl.col(c)).alias(c)
            for c in cols_a_limpiar
        ])

        patrones_mut = list(REEMPLAZOS_MUTILACION.keys())
        valores_mut = list(REEMPLAZOS_MUTILACION.values())
        df_sano = df_sano.with_columns([
            pl.col(c).str.replace_many(patrones_mut, valores_mut).alias(c) 
            for c in cols_a_limpiar
        ])

    # =========================================================================
    # FASE 2: HARD REJECTS (Violaciones Estructurales Críticas)
    # =========================================================================
    c_uuid = _get_col_name(df_sano, "UUID")
    if c_uuid:
        regex_uuid = r"^[A-Z0-9]{8}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{12}$"
        malos = df_sano.filter(~pl.col(c_uuid).str.contains(regex_uuid) & pl.col(c_uuid).is_not_null())
        if not malos.is_empty():
            alertas.append(malos.select([pl.col("FilaOrigen"), pl.lit("HARD").alias("Severidad"), pl.lit("UUID Inválido o Roto").alias("Regla_Rota")]))

    c_rfc = _get_col_name(df_sano, "EMISORRFC")
    if c_rfc:
        vacios = df_sano.filter(pl.col(c_rfc).is_null())
        if not vacios.is_empty():
            alertas.append(vacios.select([pl.col("FilaOrigen"), pl.lit("HARD").alias("Severidad"), pl.lit("EmisorRFC Vacío (Dato Huérfano)").alias("Regla_Rota")]))

    cols_verificar_negativos = ["PERCEPCIONIMPORTEGRAVADO", "PERCEPCIONIMPORTEEXENTO", "DEDUCCIONESIMPORTE"]
    cols_neg_presentes = [c for c in df_sano.columns if _get_col_name(df_sano, c) in cols_verificar_negativos]
    if cols_neg_presentes:
        mask_neg = pl.any_horizontal([(pl.col(c).cast(pl.Float64, strict=False) < 0) for c in cols_neg_presentes])
        negativos = df_sano.filter(mask_neg)
        if not negativos.is_empty():
            alertas.append(negativos.select([pl.col("FilaOrigen"), pl.lit("HARD").alias("Severidad"), pl.lit("Importe Negativo Detectado").alias("Regla_Rota")]))

    # =========================================================================
    # FASE 3: SOFT REJECTS (Advertencias Fiscales de Nómina)
    # =========================================================================
    regex_rfc = r"^[A-Z&Ñ]{3,4}\d{6}[A-Z0-9]{3}$"
    
    c_rec = _get_col_name(df_sano, "RECEPTORRFC")
    if c_rec:
        inv = df_sano.filter(pl.col(c_rec).is_not_null() & ~pl.col(c_rec).str.contains(regex_rfc))
        if not inv.is_empty():
            alertas.append(inv.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("ReceptorRFC con formato anómalo").alias("Regla_Rota")]))

    c_nss = _get_col_name(df_sano, "RECEPTORNUMSEGURIDADSOCIAL")
    if c_nss:
        nss_malo = df_sano.filter(pl.col(c_nss).is_not_null() & (pl.col(c_nss).str.len_chars() != 11))
        if not nss_malo.is_empty():
            alertas.append(nss_malo.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("NSS con longitud inválida").alias("Regla_Rota")]))

    c_tot_per = _get_col_name(df_sano, "TOTALPERCEPCIONES")
    c_grav = _get_col_name(df_sano, "PERCEPCIONESTOTALGRAVADO")
    c_exe = _get_col_name(df_sano, "PERCEPCIONESTOTALEXENTO")

    if all([c_tot_per, c_grav, c_exe]):
        calc_per = pl.col(c_grav).cast(pl.Float64, strict=False).fill_null(0.0) + pl.col(c_exe).cast(pl.Float64, strict=False).fill_null(0.0)
        desc = df_sano.filter(
            pl.col(c_tot_per).is_not_null() & ((calc_per - pl.col(c_tot_per).cast(pl.Float64, strict=False).fill_null(0.0)).abs() > 1.0)
        )
        if not desc.is_empty():
            alertas.append(desc.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("Descuadre Interno: Gravado+Exento != TotalPercepciones").alias("Regla_Rota")]))

    c_tipo_pago = _get_col_name(df_sano, "TIPOOTROPAGO")
    c_subsidio = _get_col_name(df_sano, "SUBSIDIOCAUSADO")
    if c_tipo_pago and c_subsidio:
        sub_error = df_sano.filter((pl.col(c_tipo_pago) == "002") & pl.col(c_subsidio).is_null())
        if not sub_error.is_empty():
            alertas.append(sub_error.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("Falta SubsidioCausado en TipoOtroPago 002").alias("Regla_Rota")]))

    cols_monetarias = ["TotalPercepciones", "TotalDeducciones", "TotalOtrosPagos", "PercepcionesTotalGravado", "PercepcionesTotalExento", "TotalOtrasDeducciones", "NominaTotalImpuestosRetenidos", "Importe"]
    cols_pres = [c for c in df_sano.columns if _get_col_name(df_sano, c) in [m.upper() for m in cols_monetarias] or c in cols_monetarias]

    if cols_pres:
        cond_desb = pl.any_horizontal([
            pl.col(c).cast(pl.Utf8).str.replace_all(r"[^\d.-]", "").cast(pl.Float64, strict=False).abs() > 999999999999.0 
            for c in cols_pres
        ])
        filas_culpables = df_sano.filter(cond_desb)
        if not filas_culpables.is_empty():
            alertas.append(filas_culpables.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("Alerta de Desbordamiento Aritmético (Trillones)").alias("Regla_Rota")]))

    c_f_ini = _get_col_name(df_sano, "FECHAINICIALPAGO")
    c_f_fin = _get_col_name(df_sano, "FECHAFINALPAGO")
    if c_f_ini and c_f_fin:
        df_fechas_malas = df_sano.filter(
            pl.col(c_f_fin).is_not_null() & (pl.col(c_f_fin) < pl.col(c_f_ini))
        )
        if not df_fechas_malas.is_empty():
            alertas.append(df_fechas_malas.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("Inconsistencia: Fecha Final anterior a Inicial").alias("Regla_Rota")]))

    c_dias = _get_col_name(df_sano, "NUMDIASPAGADOS")
    c_tipo_nom = _get_col_name(df_sano, "TIPONOMINA")
    if c_dias:
        if c_tipo_nom:
            mask_dias_atipicos = (
                ((pl.col(c_tipo_nom) == "O") & (pl.col(c_dias).cast(pl.Float64, strict=False) > 35.0)) | 
                ((pl.col(c_tipo_nom) == "E") & (pl.col(c_dias).cast(pl.Float64, strict=False) > 365.0))
            )
        else:
            mask_dias_atipicos = pl.col(c_dias).cast(pl.Float64, strict=False) > 365.0

        df_dias_atipicos = df_sano.filter(mask_dias_atipicos)
        if not df_dias_atipicos.is_empty():
            alertas.append(df_dias_atipicos.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("Días Pagados atípicos").alias("Regla_Rota")]))

    # =========================================================================
    # FASE 4: Consolidación de Bitácora Analítica (DQ Log)
    # =========================================================================
    if alertas:
        df_dq_log = pl.concat(alertas)
    else:
        df_dq_log = pl.DataFrame(schema={"FilaOrigen": pl.Int64, "Severidad": pl.Utf8, "Regla_Rota": pl.Utf8})

    return df_sano, df_dq_log


def inyectar_hash_nomina(df: pl.DataFrame, dedupe_enabled: bool) -> pl.DataFrame:
    """Calcula un identificador transaccional único (HashID) para deduplicación."""
    if df.is_empty() or not dedupe_enabled:
        return df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("HashID"))

    cols_llave = [c for c in df.columns if c != "FilaOrigen"]
    if not cols_llave:
        return df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("HashID"))

    return df.with_columns(
        pl.concat_str([pl.col(c).cast(pl.Utf8).fill_null("") for c in cols_llave], separator="|")
        .hash().cast(pl.Utf8).alias("HashID")
    )