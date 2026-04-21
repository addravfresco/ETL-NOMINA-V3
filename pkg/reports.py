"""Módulo de Telemetría y Auditoría Transaccional (UNIVERSAL).

Implementa el motor de observación (Observability) pasiva del pipeline.
Supervisa el rendimiento de ingesta y ejecuta muestreos forenses en memoria 
para generar un reporte de texto consolidado con estadísticas de degradación 
de datos (Mojibake, mutilaciones de texto y nulos).

Nota de Arquitectura: La persistencia de las métricas de negocio y cuarentena 
estricta (Hard/Soft Rejects) ha sido delegada a la capa relacional (SQL Server).
Este módulo provee exclusivamente observabilidad del proceso de ingesta masiva.
"""

from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from typing import Set

import polars as pl

from pkg.globals import LOG_DIR


class ETLReport:
    """Controlador de telemetría y auditoría dinámica para procesos ETL.
    
    Gestiona la inicialización de contadores de rendimiento, ejecuta rutinas
    de muestreo léxico sobre el lote de datos y consolida las métricas 
    finales en artefactos de texto plano (.txt) para trazabilidad forense.
    """

    def __init__(self, id_anexo: str, is_recovery: bool = False):
        """Inicializa los contadores y gestiona la preservación de logs históricos.
        
        Args:
            id_anexo (str): Identificador unívoco del flujo lógico (ej. 'NOMINA_3C_2024').
            is_recovery (bool, optional): Bandera que indica si el proceso actual es 
                una reanudación (Cold Resume) tras un fallo. Por defecto False.
        """
        self.start_time = time.time()
        self.total_rows = 0
        self.total_batches = 0
        
        # Alertas Pasivas (Profiling Léxico)
        self.alerts_mojibake = 0
        self.alerts_length = 0
        self.alerts_nulls = 0
        
        self.samples_mojibake: Set[str] = set()
        self.samples_length: Set[str] = set()

        self.archivo_auditoria = LOG_DIR / f"AUDIT_ANEXO_{id_anexo}.txt"

        # Purgado de artefacto previo en escenarios de inicio en frío (Cold Start)
        if not is_recovery:
            if self.archivo_auditoria.exists():
                self.archivo_auditoria.unlink()

        # Diccionario léxico para detección de anomalías de codificación (Windows-1252 / UTF-8)
        self.regex_mojibake = r"[?ÃÂƒ†‡‰‹›ŒŽ‘’“”•–—˜™š›œžŸ¡¢£¤¥¦§¨©ª«¬®¯°±²³´µ¶·¸¹º»¼½¾¿Ðð]"
        self.min_text_length = 3

    def audit_batch(self, df: pl.DataFrame) -> None:
        """Ejecuta un escaneo forense selectivo sobre columnas de texto descriptivo.
        
        Evalúa el lote en memoria para identificar indicios de corrupción léxica o
        mutilación estructural. Alimenta los contadores estáticos del reporte sin 
        alterar ni transformar el DataFrame original.

        Args:
            df (pl.DataFrame): Lote de datos vectorizado a inspeccionar.
        """
        keywords = ["NOMBRE", "CONCEPTO", "DESCRIPCION"]

        # Aislamiento topológico: Filtrado de columnas objetivo basadas en metadatos
        cols_audit = [
            c for c in df.columns 
            if any(k in c.upper() for k in keywords) and df.schema[c] == pl.Utf8
        ]

        if not cols_audit:
            return

        for col in cols_audit:
            n_nulls = df.filter(pl.col(col).is_null()).height
            if n_nulls > 0:
                self.alerts_nulls += n_nulls

            df_valid = df.filter(pl.col(col).is_not_null())
            if df_valid.height == 0:
                continue

            # Detección de Mojibake mediante expresiones regulares
            suspect_mojibake = df_valid.filter(pl.col(col).str.contains(self.regex_mojibake))
            if suspect_mojibake.height > 0:
                self.alerts_mojibake += suspect_mojibake.height
                samples = suspect_mojibake.select(col).unique().head(5).to_series().to_list()
                self.samples_mojibake.update([f"[{col}] {s}" for s in samples])

            # Detección de mutilación léxica (cadenas inusualmente cortas)
            suspect_len = df_valid.filter(pl.col(col).str.len_chars() < self.min_text_length)
            if suspect_len.height > 0:
                self.alerts_length += suspect_len.height
                samples = suspect_len.select(col).unique().head(3).to_series().to_list()
                self.samples_length.update([f"[{col}] {s}" for s in samples])

    def update_metrics(self, rows_count: int) -> None:
        """Acumula métricas de volumen para la síntesis de telemetría.
        
        Args:
            rows_count (int): Volumen de filas consolidadas en el lote actual.
        """
        self.total_rows += rows_count
        self.total_batches += 1

    def generate_final_report(self, id_anexo: str, file_name: str, status: str = "SUCCESS", error_details: str = "") -> Path:
        """Sintetiza las métricas y anexa el resultado al archivo de auditoría (.txt).
        
        Ensambla el documento final de telemetría, incluyendo estadísticas generales
        de sesión, volumetría procesada y el compendio de alertas forenses.

        Args:
            id_anexo (str): Identificador unívoco del flujo lógico.
            file_name (str): Nombre del artefacto crudo evaluado.
            status (str, optional): Estado de finalización de la orquestación. Por defecto "SUCCESS".
            error_details (str, optional): Volcado del stack trace en caso de fallo crítico.

        Returns:
            Path: Ruta absoluta al artefacto de texto plano generado.
        """
        total_duration = (time.time() - self.start_time) / 60
        timestamp_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        lines = [
            "\n" + "=" * 80,
            f"SESIÓN FINALIZADA: {timestamp_fin} | ESTADO: {status}",
            "=" * 80,
            f"DOCUMENTO AUDITADO: {file_name}",
            f"SAT ETL AUDIT REPORT - ANEXO {id_anexo}",
            "-" * 80,
            f"Total Duration (Session): {total_duration:.2f} minutes",
            f"Processed Rows (Session): {self.total_rows:,.0f}",
            "-" * 80,
            "QUALITY SUMMARY (Passive Profiling)",
            "-" * 80,
            f"Encoding Alerts (Mojibake): {self.alerts_mojibake:,.0f}",
            f"Integrity Alerts (Nulls):    {self.alerts_nulls:,.0f}",
            f"Mutilation Alerts (Short):   {self.alerts_length:,.0f}",
            "Nota: Las reglas de negocio (Data Quality) se persistieron de manera paralela en SQL Server.",
            "=" * 80,
        ]

        if self.samples_mojibake:
            lines.append("\n[EVIDENCE] MOJIBAKE SAMPLES DETECTED:")
            lines.extend([f"  > {s}" for s in sorted(list(self.samples_mojibake))[:50]])

        if error_details:
            lines.append(f"\n[CRITICAL ERROR DETAILS]\n{error_details}")

        try:
            with open(self.archivo_auditoria, "a", encoding="utf-8") as file:
                file.write("\n".join(lines) + "\n")
            return self.archivo_auditoria
        except Exception as e:
            print(f"\n[ERROR] Excepción estructural al persistir el reporte de auditoría: {e}")
            return Path("ERROR")