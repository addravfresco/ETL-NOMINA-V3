"""Módulo orquestador maestro de ejecución en cadena (Pipeline Secuencial).

Gestiona la ejecución coordinada de múltiples flujos de ingesta (anexos) 
asegurando un orden lógico estricto. Implementa un patrón de fallo rápido 
(Fail-Fast) para abortar la cadena de orquestación en caso de error, previniendo 
inconsistencias de integridad referencial cruzada en la base de datos destino.
"""

import subprocess
import sys
import time
from datetime import datetime
from typing import List

# =============================================================================
# COLA DE EJECUCIÓN MAESTRA (Full Historical Load)
# =============================================================================
# Los identificadores deben coincidir exactamente con las llaves definidas en 
# el diccionario TABLES_CONFIG del archivo globals.py. Se ordenan cronológicamente
# para garantizar la consistencia temporal durante cargas masivas (Backfilling).
ANEXOS_POR_DEFECTO: List[str] = [
    # Carga Histórica 2023
    "NOMINA_ANEXO_3_2023",
    "NOMINA_ANEXO_4_2023",
    "NOMINA_ANEXO_5_2023",
    "NOMINA_ANEXO_6_2023",
    "NOMINA_ANEXO_7_2023",
    
    # Carga Histórica 2024
    "NOMINA_ANEXO_3_2024",
    "NOMINA_ANEXO_4_2024",
    "NOMINA_ANEXO_5_2024",
    "NOMINA_ANEXO_6_2024",
    "NOMINA_ANEXO_7_2024",
    
    # Carga Ejercicio 2025 (1er Semestre)
    "NOMINA_3C_2025_1S",
    "NOMINA_4D_2025_1S",
    "NOMINA_5E_2025_1S",
    "NOMINA_6F_2025_1S",
    "NOMINA_7G_2025_1S",
    
    # Carga Ejercicio 2025 (2do Semestre)
    "NOMINA_3C_2025_2S",
    "NOMINA_4D_2025_2S",
    "NOMINA_5E_2025_2S",
    "NOMINA_6F_2025_2S",
    "NOMINA_7G_2025_2S"
]


def ejecutar_cadena(anexos: List[str]) -> None:
    """Orquesta la ejecución secuencial de los procesos ETL definidos.

    Itera sobre la lista de anexos proporcionada, delegando la carga de trabajo 
    al motor unitario (main.py) mediante subprocesos aislados del sistema operativo. 
    Evalúa el código de salida de cada subproceso para garantizar la atomicidad 
    lógica de la cadena de despliegue.

    Args:
        anexos (List[str]): Secuencia ordenada de identificadores de anexos a procesar.

    Raises:
        SystemExit: Si cualquier subproceso retorna un código de error distinto a cero (Fail-Fast).
    """
    start_time_total = time.time()
    
    print(f"\n{'='*80}")
    print("[INFO] ORQUESTADOR MAESTRO ETL NÓMINA - INICIO DE SECUENCIA")
    print(f"[INFO] Marca temporal: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Flujos encolados para ejecución: {len(anexos)} -> {anexos}")
    print(f"{'='*80}\n")

    for i, anexo in enumerate(anexos, 1):
        print(f"\n[{i}/{len(anexos)}] >>> INICIANDO PROCESAMIENTO: {anexo} <<<")
        
        # Delegación de ejecución al intérprete de Python del entorno virtual activo
        proceso = subprocess.run([sys.executable, "main.py", anexo])
        
        # Evaluación de código de retorno transaccional (Patrón Fail-Fast)
        if proceso.returncode != 0:
            print(f"\n[CRITICAL ERROR] Fallo sistémico detectado durante el procesamiento del anexo: {anexo}.")
            print("[INFO] Abortando la cadena de orquestación para preservar la integridad referencial cruzada.")
            sys.exit(1)
            
        print(f"\n[SUCCESS] Ejecución del anexo {anexo} completada exitosamente.")
        print("-" * 80)

    end_time_total = time.time()
    minutos_totales = (end_time_total - start_time_total) / 60

    print("\n" + "="*80)
    print("[SUCCESS] PIPELINE SECUENCIAL COMPLETADO SIN ERRORES.")
    print(f"[INFO] Tiempo total de procesamiento de la cadena: {minutos_totales:.2f} minutos")
    print("="*80 + "\n")


if __name__ == "__main__":
    # Análisis de argumentos de la interfaz de línea de comandos (CLI)
    # Permite la sobreescritura dinámica de la cadena de ejecución para operaciones ad-hoc.
    if len(sys.argv) > 1:
        anexos_a_procesar = sys.argv[1:]
    else:
        anexos_a_procesar = ANEXOS_POR_DEFECTO
        
    ejecutar_cadena(anexos_a_procesar)