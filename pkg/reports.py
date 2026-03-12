"""
Módulo de Telemetría, Auditoría y Control de Calidad Transaccional.

Implementa el motor de observación (Observability) del pipeline.
Supervisa el rendimiento de ingesta en tiempo real, detecta degradación 
en la calidad de los datos (Nulos, Mutilación, Mojibake) y administra
la bandeja de cuarentena en modo Append Inteligente.
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
    durante el ciclo de vida de la ejecución. Emplea un patrón de Bitácora
    Estática para consolidar ejecuciones fragmentadas en archivos maestros únicos.
    """

    def __init__(self, id_anexo: str, is_recovery: bool = False):
        """
        Inicializa los contadores y gestiona la preservación de logs históricos.
        
        Args:
            id_anexo (str): Identificador del anexo (ej. '1A_2023').
            is_recovery (bool): Bandera que indica si el proceso se reanudó tras un fallo.
        """
        self.start_time = time.time()
        self.total_rows = 0
        self.total_batches = 0
        self.alerts_mojibake = 0
        self.alerts_length = 0
        self.alerts_nulls = 0
        
        # Desglose de Cuarentena
        self.total_cuarentena = 0
        self.hard_rejects = 0
        self.soft_rejects = 0
        
        self.samples_mojibake: Set[str] = set()
        self.samples_length: Set[str] = set()

        # Nombres estáticos y limpios (sin marcas de tiempo que generen desorden)
        self.archivo_cuarentena = LOG_DIR / f"CUARENTENA_ANEXO_{id_anexo}.csv"
        self.archivo_auditoria = LOG_DIR / f"AUDIT_ANEXO_{id_anexo}.txt"

        # Si es un inicio en frío (desde cero), purgamos los archivos viejos
        if not is_recovery:
            if self.archivo_cuarentena.exists():
                self.archivo_cuarentena.unlink()
            if self.archivo_auditoria.exists():
                self.archivo_auditoria.unlink()

        # Patrón heurístico para identificar fallos de codificación residuales
        self.regex_mojibake = r"[?ÃÂƒ†‡‰‹›ŒŽ‘’“”•–—˜™š›œžŸ¡¢£¤¥¦§¨©ª«¬®¯°±²³´µ¶·¸¹º»¼½¾¿Ðð]"
        self.min_text_length = 3

    def audit_batch(self, df: pl.DataFrame) -> None:
        """Ejecuta una auditoría forense selectiva sobre las columnas de texto descriptivo."""
        keywords = ["NOMBRE", "CONCEPTO", "DESCRIPCION"]

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
        """Actualiza los contadores globales e imprime la telemetría en tiempo real."""
        self.total_rows += rows_count
        self.total_batches += 1
        elapsed = time.time() - self.start_time
        speed = self.total_rows / (elapsed + 1e-6)
        print(f"[INFO] Lote {self.total_batches:04d} | Filas Totales: {self.total_rows:,.0f} | Vel: {speed:,.0f} regs/seg")

    def log_quarantine(self, df_quarentena: pl.DataFrame, id_anexo: str) -> None:
        """Aísla los registros anómalos persistiendo la información de forma acumulativa."""
        if df_quarentena.is_empty():
            return
            
        self.total_cuarentena += df_quarentena.height
        
        # Desglose estadístico por tipo de rechazo
        if "Motivo_Rechazo" in df_quarentena.columns:
            self.hard_rejects += df_quarentena.filter(pl.col("Motivo_Rechazo").str.contains(r"\[HARD\]")).height
            self.soft_rejects += df_quarentena.filter(pl.col("Motivo_Rechazo").str.contains(r"\[SOFT\]")).height
        
        # Verifica si el archivo existe para decidir si inyecta o no las cabeceras
        es_nuevo = not self.archivo_cuarentena.exists()
        
        # Modo 'a' (append) para inyectar filas sin sobrescribir
        with open(self.archivo_cuarentena, "a", encoding="utf-8") as file:
            df_quarentena.write_csv(file, include_header=es_nuevo)

    def generate_final_report(self, id_anexo: str, file_name: str, status: str = "SUCCESS", error_details: str = "") -> Path:
        """Sintetiza las métricas y anexa el resultado al archivo de auditoría maestro."""
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
            "QUALITY SUMMARY (Session)",
            "-" * 80,
            f"Encoding Alerts (Mojibake): {self.alerts_mojibake:,.0f}",
            f"Integrity Alerts (Nulls):    {self.alerts_nulls:,.0f}",
            f"Mutilation Alerts (Short):   {self.alerts_length:,.0f}",
            f"Quarantined Rows (DLQ):      {self.total_cuarentena:,.0f}",
            f"  ├─ [HARD] Rejects (Dropped): {self.hard_rejects:,.0f}",
            f"  └─ [SOFT] Rejects (Flagged): {self.soft_rejects:,.0f}",
            "=" * 80,
        ]

        if self.samples_mojibake:
            lines.append("\n[EVIDENCE] MOJIBAKE SAMPLES DETECTED:")
            lines.extend([f"  > {s}" for s in sorted(list(self.samples_mojibake))[:50]])

        if error_details:
            lines.append(f"\n[CRITICAL ERROR DETAILS]\n{error_details}")

        try:
            # Modo 'a' (append) en lugar de 'w' para conservar el historial de pausas/errores
            with open(self.archivo_auditoria, "a", encoding="utf-8") as file:
                file.write("\n".join(lines) + "\n")
            return self.archivo_auditoria
        except Exception as e:
            print(f"[ERROR] Fallo al persistir el reporte de auditoría: {e}")
            return Path("ERROR")