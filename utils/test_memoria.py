import os

os.environ["POLARS_VERBOSE"] = "1"

import polars as pl

archivo = "AECF_0101_Anexo4.utf8_clean.csv"
batch_size = 100_000

print("--- INICIANDO TEST DE MEMORIA POLARS ---")
print(f"Archivo: {archivo}")

try:
    lazy_plan = pl.scan_csv(
        archivo,
        separator="|",
        has_header=True,
        encoding="utf8-lossy",
        ignore_errors=True,
        truncate_ragged_lines=True,
        infer_schema_length=0,
        quote_char=None,
        rechunk=False
    )
    
    print("Plan compilado. Solicitando el primer lote al motor Rust...")
    
    # Aquí es donde va a estallar
    generador = lazy_plan.collect_batches(chunk_size=batch_size)
    primer_lote = next(generador)
    
    print(f"¡ÉXITO! Lote extraído. Filas: {len(primer_lote)}")
    
except Exception as e:
    print(f"\n[ERROR EXPLOSIVO] {e}")