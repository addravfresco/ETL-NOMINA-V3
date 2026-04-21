"""
Módulo orquestador maestro de ejecución en cadena (Pipeline Secuencial).

Gestiona la ejecución coordinada de múltiples flujos de ingesta
asegurando un orden lógico estricto. Implementa un patrón fail-fast
para abortar la cadena en caso de error y prevenir inconsistencias
de integridad en la base de datos destino.
"""

from __future__ import annotations

import subprocess
import sys
import time
from datetime import datetime

ANEXOS_POR_DEFECTO: list[str] = [
    #"NOMINA_3C_2025_2S",
    "NOMINA_4D_2025_2S",
    "NOMINA_5E_2025_2S",
    "NOMINA_6F_2025_2S",
    "NOMINA_7G_2025_2S",
]


def ejecutar_cadena(anexos: list[str]) -> None:
    """
    Orquesta la ejecución secuencial de los procesos ELT definidos.

    Args:
        anexos (list[str]): Secuencia ordenada de identificadores a procesar.
    """
    start_time_total = time.time()

    print(f"\n{'=' * 80}")
    print("[INFO] ORQUESTADOR MAESTRO ELT SAT - INICIO DE SECUENCIA")
    print(f"[INFO] Marca temporal: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Flujos encolados para ejecución: {len(anexos)} -> {anexos}")
    print(f"{'=' * 80}\n")

    for indice, anexo in enumerate(anexos, start=1):
        print(f"\n[{indice}/{len(anexos)}] >>> INICIANDO PROCESAMIENTO: {anexo} <<<")

        proceso = subprocess.run([sys.executable, "main.py", anexo])

        if proceso.returncode != 0:
            print(f"\n[CRITICAL ERROR] Fallo sistémico detectado durante el procesamiento del anexo: {anexo}.")
            print("[INFO] Abortando la cadena de orquestación para preservar la integridad lógica.")
            sys.exit(1)

        print(f"\n[SUCCESS] Ejecución del anexo {anexo} completada exitosamente.")
        print("-" * 80)

    minutos_totales = (time.time() - start_time_total) / 60

    print("\n" + "=" * 80)
    print("[SUCCESS] PIPELINE SECUENCIAL COMPLETADO SIN ERRORES.")
    print(f"[INFO] Tiempo total de procesamiento de la cadena: {minutos_totales:.2f} minutos")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    anexos_a_procesar = sys.argv[1:] if len(sys.argv) > 1 else ANEXOS_POR_DEFECTO
    ejecutar_cadena(anexos_a_procesar)