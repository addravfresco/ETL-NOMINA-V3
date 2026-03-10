"""
Módulo orquestador maestro de ejecución en cadena (Pipeline Secuencial).

Gestiona la ejecución coordinada de múltiples flujos de ingesta (anexos) 
asegurando un orden lógico estricto. Implementa un patrón de fallo rápido 
(Fail-Fast) para abortar la cadena en caso de error, previniendo 
inconsistencias de integridad referencial en la base de datos destino.
"""

import subprocess
import sys
import time
from datetime import datetime
from typing import List

# Configuración predeterminada de la cadena de ejecución semestral
ANEXOS_POR_DEFECTO: List[str] = [
    "1A_2025_2S", 
    "2B_2025_2S"
]


def ejecutar_cadena(anexos: List[str]) -> None:
    """
    Orquesta la ejecución secuencial de los procesos ETL definidos.

    Itera sobre la lista de anexos proporcionada, delegando la carga de trabajo 
    al motor unitario (main.py) mediante subprocesos aislados. Evalúa el 
    código de salida de cada subproceso para garantizar la atomicidad 
    lógica de la cadena de despliegue.

    Args:
        anexos (List[str]): Secuencia ordenada de identificadores de anexos a procesar.

    Raises:
        SystemExit: Si cualquiera de los subprocesos retorna un código de error distinto a cero.
    """
    start_time_total = time.time()
    
    print(f"\n{'='*80}")
    print("[INFO] ORQUESTADOR MAESTRO ETL SAT - INICIO DE SECUENCIA")
    print(f"[INFO] Marca temporal: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Flujos encolados para ejecución: {len(anexos)} -> {anexos}")
    print(f"{'='*80}\n")

    for i, anexo in enumerate(anexos, 1):
        print(f"\n[{i}/{len(anexos)}] >>> INICIANDO PROCESAMIENTO: {anexo} <<<")
        
        # Delegación de ejecución al intérprete de Python del entorno virtual activo
        proceso = subprocess.run([sys.executable, "main.py", anexo])
        
        # Evaluación de código de retorno (Patrón Fail-Fast)
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