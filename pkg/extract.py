"""
Módulo de ingesta de datos masivos mediante evaluación perezosa (Lazy Evaluation).

Utiliza el escáner nativo de Polars para crear un plan de ejecución optimizado,
delegando el manejo de codificación al motor subyacente en Rust. Previene
desbordamientos de memoria (Out-of-Memory) estructurando la lectura en lotes
e implementando lógica de reanudación en frío (Cold Resume).
"""

from pathlib import Path
from typing import Iterator

import polars as pl


def get_sat_reader(
    file_path: Path, 
    batch_size: int, 
    separator: str = "|", 
    skip_rows: int = 0
) -> Iterator[pl.DataFrame]:
    """
    Inicializa un escáner de Polars para la lectura fragmentada de archivos masivos.

    Implementa la lógica de preservación de esquema estructural cuando se realiza
    un salto de líneas (reanudación desde un Bookmark), asegurando que los metadatos
    de las columnas no se pierdan al omitir la cabecera física del archivo.

    Args:
        file_path (Path): Ruta absoluta al archivo fuente.
        batch_size (int): Cantidad máxima de filas a procesar por lote en memoria.
        separator (str): Carácter delimitador utilizado en el archivo fuente.
        skip_rows (int): Filas a omitir utilizando punteros de ejecución (Bookmark).

    Returns:
        Iterator[pl.DataFrame]: Generador que entrega fragmentos (DataFrames) del archivo.

    Raises:
        FileNotFoundError: Si el archivo especificado no existe en la ruta.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"El recurso fuente no fue localizado: {file_path}")

    # Gestión de cabeceras durante la reanudación transaccional
    if skip_rows > 0:
        # Extracción del esquema estructural leyendo estáticamente la cabecera (Fila 0)
        header_df = pl.read_csv(
            str(file_path),
            separator=separator,
            n_rows=0,
            infer_schema_length=0,
            quote_char=None,
            encoding="utf8-lossy",
            ignore_errors=True
        )
        nombres_columnas = header_df.columns

        # Configuración del escáner perezoso omitiendo los registros ya procesados
        # y la fila física del encabezado, inyectando el esquema previamente guardado.
        lazy_plan = pl.scan_csv(
            str(file_path),
            separator=separator,
            has_header=False,
            skip_rows=skip_rows + 1,
            new_columns=nombres_columnas,
            encoding="utf8-lossy",
            ignore_errors=True,
            truncate_ragged_lines=True,
            infer_schema_length=0,
            quote_char=None
        )
    else:
        # Configuración del escáner perezoso para el flujo inicial estándar
        lazy_plan = pl.scan_csv(
            str(file_path),
            separator=separator,
            has_header=True,
            encoding="utf8-lossy",
            ignore_errors=True,
            truncate_ragged_lines=True,
            infer_schema_length=0,
            quote_char=None
        )

    # Inyección de coordenada física (FilaOrigen) para trazabilidad y de-duplicación en BD
    lazy_plan = lazy_plan.with_row_index("FilaOrigen", offset=skip_rows)

    # Ejecución del plan de consulta mediante fragmentación dinámica
    return lazy_plan.collect_batches(chunk_size=batch_size)