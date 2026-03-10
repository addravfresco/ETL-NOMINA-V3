"""
Módulo de Telemetría, Auditoría y Control de Calidad Transaccional.

Implementa el motor de observación (Observability) del pipeline.
Supervisa el rendimiento de ingesta en tiempo real, detecta degradación 
en la calidad de los datos (Nulos, Mutilación, Mojibake) y administra
la bandeja de cuarentena para preservar la integridad de la base de datos.
"""

import time
from datetime import datetime
from pathlib import Path
from typing import Set

import polars as pl

from pkg.globals import LOG_DIR


class ETLReport:
    """
    Controlador de telemetría y auditoría dinámica para procesos ETL.

    Mantiene el estado transaccional de las métricas de calidad y rendimiento
    durante el ciclo de vida de la ejecución, consolidando los hallazgos en
    un artefacto de auditoría al finalizar el procesamiento.
    """

    def __init__(self):
        """Inicializa los contadores de rendimiento y las estructuras de muestreo."""
        self.start_time = time.time()
        self.total_rows = 0
        self.total_batches = 0
        self.alerts_mojibake = 0
        self.alerts_length = 0
        self.alerts_nulls = 0
        self.total_cuarentena = 0
        self.samples_mojibake: Set[str] = set()
        self.samples_length: Set[str] = set()

        # Patrón heurístico para identificar fallos de codificación residuales
        self.regex_mojibake = r"[?ÃÂƒ†‡‰‹›ŒŽ‘’“”•–—˜™š›œžŸ¡¢£¤¥¦§¨©ª«¬®¯°±²³´µ¶·¸¹º»¼½¾¿Ðð]"
        self.min_text_length = 3

    def audit_batch(self, df: pl.DataFrame) -> None:
        """
        Ejecuta una auditoría forense selectiva sobre las columnas de texto descriptivo.

        Identifica y muestrea anomalías de codificación, valores nulos inesperados
        y truncamientos severos, acumulando las métricas en el estado de la clase.

        Args:
            df (pl.DataFrame): Lote de datos normalizado (Post-Transformación).
        """
        keywords = ["NOMBRE", "CONCEPTO", "DESCRIPCION"]

        # Aislamiento de columnas target basadas en heurística de nombres y tipado
        cols_audit = [
            c for c in df.columns 
            if any(k in c.upper() for k in keywords) and df.schema[c] == pl.Utf8
        ]

        if not cols_audit:
            return

        for col in cols_audit:
            # 1. Monitoreo de Integridad Estructural (Nulos)
            n_nulls = df.filter(pl.col(col).is_null()).height
            if n_nulls > 0:
                self.alerts_nulls += n_nulls

            df_valid = df.filter(pl.col(col).is_not_null())
            if df_valid.height == 0:
                continue

            # 2. Análisis de Degradación de Codificación (Mojibake)
            suspect_mojibake = df_valid.filter(pl.col(col).str.contains(self.regex_mojibake))
            if suspect_mojibake.height > 0:
                self.alerts_mojibake += suspect_mojibake.height
                samples = suspect_mojibake.select(col).unique().head(5).to_series().to_list()
                self.samples_mojibake.update([f"[{col}] {s}" for s in samples])

            # 3. Análisis de Mutilación (Cadenas anómalamente cortas)
            suspect_len = df_valid.filter(pl.col(col).str.len_chars() < self.min_text_length)
            if suspect_len.height > 0:
                self.alerts_length += suspect_len.height
                samples = suspect_len.select(col).unique().head(3).to_series().to_list()
                self.samples_length.update([f"[{col}] {s}" for s in samples])

    def update_metrics(self, rows_count: int) -> None:
        """
        Actualiza los contadores globales e imprime la telemetría en tiempo real.

        Args:
            rows_count (int): Número de registros consolidados en el lote actual.
        """
        self.total_rows += rows_count
        self.total_batches += 1
        elapsed = time.time() - self.start_time
        speed = self.total_rows / (elapsed + 1e-6)
        print(f"[INFO] Lote {self.total_batches:04d} | Filas Totales: {self.total_rows:,.0f} | Vel: {speed:,.0f} regs/seg")

    def log_quarantine(self, df_quarentena: pl.DataFrame, id_anexo: str) -> None:
        """
        Aísla los registros anómalos persistiendo la información en una Dead Letter Queue.

        Escribe el DataFrame de cuarentena en un archivo CSV local de forma acumulativa
        (Append mode), permitiendo el análisis forense posterior sin detener el pipeline.

        Args:
            df_quarentena (pl.DataFrame): Subconjunto de registros que violaron reglas de integridad.
            id_anexo (str): Identificador del flujo de datos para clasificar el log.
        """
        if df_quarentena.is_empty():
            return
            
        self.total_cuarentena += df_quarentena.height
        timestamp = datetime.now().strftime("%Y%m%d")
        archivo_salida = LOG_DIR / f"CUARENTENA_ANEXO_{id_anexo}_{timestamp}.csv"
        
        es_nuevo = not archivo_salida.exists()
        
        with open(archivo_salida, "a", encoding="utf-8") as file:
            df_quarentena.write_csv(file, include_header=es_nuevo)

    def generate_final_report(self, id_anexo: str, file_name: str, status: str = "SUCCESS", error_details: str = "") -> Path:
        """
        Sintetiza las métricas recolectadas y genera el artefacto final de auditoría.

        Args:
            id_anexo (str): Identificador del flujo de datos.
            file_name (str): Nombre del archivo físico procesado.
            status (str): Estado final de la ejecución transaccional.
            error_details (str): Trazabilidad de la excepción (Stacktrace) en caso de fallo.

        Returns:
            Path: Ruta absoluta donde se persistió el reporte de auditoría.
        """
        total_duration = (time.time() - self.start_time) / 60
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"AUDIT_{id_anexo}_{timestamp}.txt"
        report_path = LOG_DIR / log_filename

        lines = [
            "=" * 80,
            f"DOCUMENTO AUDITADO: {file_name}",
            "=" * 80,
            f"SAT ETL AUDIT REPORT - ANEXO {id_anexo}",
            "-" * 80,
            f"Execution Status: {status}",
            f"Total Duration:   {total_duration:.2f} minutes",
            f"Processed Rows:   {self.total_rows:,.0f}",
            "-" * 80,
            "QUALITY SUMMARY",
            "-" * 80,
            f"Encoding Alerts (Mojibake): {self.alerts_mojibake}",
            f"Integrity Alerts (Nulls):    {self.alerts_nulls}",
            f"Mutilation Alerts (Short):   {self.alerts_length}",
            f"Quarantined Rows (DLQ):      {self.total_cuarentena:,.0f}",
            "=" * 80,
        ]

        if self.samples_mojibake:
            lines.append("\n[EVIDENCE] MOJIBAKE SAMPLES DETECTED:")
            lines.extend([f"  > {s}" for s in sorted(list(self.samples_mojibake))[:50]])

        if error_details:
            lines.append(f"\n[CRITICAL ERROR DETAILS]\n{error_details}")

        try:
            with open(report_path, "w", encoding="utf-8") as file:
                file.write("\n".join(lines))
            return report_path
        except Exception as e:
            print(f"[ERROR] Fallo al persistir el reporte de auditoría: {e}")
            return Path("ERROR")