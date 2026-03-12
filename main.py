"""
Módulo orquestador del Pipeline ETL SAT.

Coordina el flujo transaccional de datos: Extracción perezosa (Lazy), 
Transformación vectorizada, Tipado estricto (Enforcement) y Persistencia 
masiva (Upsert Bulk). Gestiona la tolerancia a fallos y la reanudación 
en frío mediante el control de estado (Checkpoints).
"""

import os
import sys
import traceback

from pkg.checkpoint import guardar_estado, leer_estado
from pkg.enforcer import aplicar_tipos_seguros
from pkg.extract import get_sat_reader
from pkg.globals import BATCH_SIZE, REGLAS_DINAMICAS, SAT_RAW_DIR, TABLES_CONFIG
from pkg.load import upload_to_sql_blindado
from pkg.reports import ETLReport
from pkg.transform import transform_sat_batch

# Configuración del búfer de salida estándar para visualización fluida de logs
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
except AttributeError:
    pass


def main() -> None:
    """
    Ejecuta el ciclo de vida principal del proceso ETL para un anexo específico.

    Resuelve los metadatos del anexo mediante argumentos de línea de comandos (CLI),
    verifica la disponibilidad del recurso físico, inicializa el estado transaccional
    y ejecuta el procesamiento por lotes asegurando la recolección de métricas.

    Raises:
        SystemExit: Si el anexo no está configurado o el archivo físico no existe.
    """
    # =========================================================================
    # 1. Resolución de Identidad y Metadatos
    # =========================================================================
    id_anexo = sys.argv[1].upper() if len(sys.argv) > 1 else "1A"

    if id_anexo not in TABLES_CONFIG:
        print(f"[ERROR] El identificador '{id_anexo}' carece de configuración en TABLES_CONFIG.")
        sys.exit(1)

    meta = TABLES_CONFIG[id_anexo]
    table_name = meta["table_name"]
    file_name = meta["file_name"]
    separator = meta["separator"]

    print(f"\n[INFO] INICIANDO ORQUESTACIÓN ETL SAT: ANEXO {id_anexo}")
    print(f"[INFO] TABLA DESTINO: {table_name}")

    # =========================================================================
    # 2. Validación de Disponibilidad de Recursos
    # =========================================================================
    ruta_archivo = SAT_RAW_DIR / file_name
    if not ruta_archivo.exists():
        print(f"[ERROR] Recurso físico no localizado: {ruta_archivo}")
        sys.exit(1)

    file_size_gb = os.path.getsize(ruta_archivo) / (1024**3)
    print(f"[INFO] Artefacto: {file_name} | Volumen: {file_size_gb:.2f} GB\n" + "-" * 60)

    # =========================================================================
    # 3. Inicialización de Estado Transaccional (Bookmarking)
    # =========================================================================
    filas_procesadas_historicas = leer_estado(id_anexo)
    if filas_procesadas_historicas > 0:
        print(f"[RECOVERY] Punto de control detectado. Reanudando en fila: {filas_procesadas_historicas:,.0f}")
    else:
        print("[INFO] Inicio de procesamiento desde el registro 0 (Cold Start).")

    es_reanudacion = filas_procesadas_historicas > 0
    report = ETLReport(id_anexo=id_anexo, is_recovery=es_reanudacion)

    try:
        # =========================================================================
        # 4. Configuración del Stream de Lectura y Ejecución del Ciclo de Vida
        # =========================================================================
        reader = get_sat_reader(
            file_path=ruta_archivo, 
            batch_size=BATCH_SIZE, 
            separator=separator, 
            skip_rows=filas_procesadas_historicas
        )
        print("[INFO] Plan de ejecución perezoso compilado. Iniciando ingesta en streaming...")

        for df_batch in reader:
            # Captura de la volumetría física real del lote actual
            filas_leidas_en_este_lote = len(df_batch)
            
            # Transformación, saneamiento y bifurcación lógica (Dead Letter Queue)
            df_processed, df_quarantine = transform_sat_batch(df_batch)
            report.log_quarantine(df_quarantine, id_anexo)
            
            # Coerción estructural y tipado seguro
            df_final = aplicar_tipos_seguros(df_processed, REGLAS_DINAMICAS)
            
            # Auditoría forense de calidad de datos
            report.audit_batch(df_final)
            
            # Persistencia transaccional en Data Warehouse
            upload_to_sql_blindado(df_final, table_name, id_anexo)
            
            # Actualización de telemetría y estado
            report.update_metrics(len(df_final))
            
            filas_procesadas_historicas += filas_leidas_en_este_lote
            guardar_estado(id_anexo, filas_procesadas_historicas)

        print("\n[INFO] Flujo finalizado exitosamente (EOF).")
        report_path = report.generate_final_report(id_anexo, file_name, status="SUCCESS")
        print(f"[INFO] Artefacto de auditoría generado: {report_path}")

    except KeyboardInterrupt:
        print("\n[WARN] Interrupción manual del proceso por el operador.")
        report.generate_final_report(id_anexo, file_name, status="CANCELLED")
    except Exception as e:
        print("\n[CRITICAL ERROR] Excepción no controlada en el flujo de ejecución.")
        print(f"Traza técnica: {e}")
        print("-" * 60)
        traceback.print_exc()
        report.generate_final_report(id_anexo, file_name, status="FAILED", error_details=str(e))


if __name__ == "__main__":
    main()