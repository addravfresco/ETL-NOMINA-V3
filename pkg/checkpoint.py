"""Módulo de persistencia de estado transaccional (Bookmarking Avanzado).

Gestiona la lectura y escritura de punteros de ejecución para el pipeline de Nómina.
Implementa memoria espacial (Byte Offset) para lograr una reanudación (Cold Resume) 
en tiempo constante O(1), erradicando el costo de búsqueda secuencial en la 
recuperación de archivos masivos tras una interrupción.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, TypedDict

STATE_FILE = "estado_etl.json"

class CheckpointData(TypedDict):
    """Estructura estricta del estado transaccional guardado en disco.
    
    Attributes:
        filas_procesadas (int): Cantidad de registros lógicos consolidados.
        byte_offset (int): Coordenada física exacta en el archivo crudo.
    """
    filas_procesadas: int
    byte_offset: int


def leer_estado(anexo_id: str) -> tuple[int, int]:
    """Recupera el índice de fila y la coordenada en bytes para un flujo específico.

    Realiza una lectura segura del archivo de estado local. Implementa tolerancia 
    a fallos por corrupción del archivo JSON y asegura compatibilidad con 
    versiones anteriores del esquema de estado.

    Args:
        anexo_id (str): Identificador único del flujo de datos (Ej. 'NOMINA_2024').

    Returns:
        tuple[int, int]: Tupla que contiene (filas_procesadas, byte_offset). 
        Retorna (0, 0) si no existe estado previo o si el archivo está corrupto.
    """
    ruta_archivo = Path(STATE_FILE)

    if not ruta_archivo.exists():
        return 0, 0

    try:
        with ruta_archivo.open("r", encoding="utf-8") as file:
            datos: dict[str, dict[str, Any]] = json.load(file)
            estado_anexo = datos.get(anexo_id, {})
            
            # Manejo de retrocompatibilidad con esquemas de estado heredados (Solo int)
            if isinstance(estado_anexo, int):
                return estado_anexo, 0
                
            return int(estado_anexo.get("filas_procesadas", 0)), int(estado_anexo.get("byte_offset", 0))
            
    except (json.JSONDecodeError, ValueError, TypeError):
        return 0, 0


def guardar_estado(anexo_id: str, filas_procesadas: int, byte_offset: int) -> None:
    """Actualiza el puntero de ejecución y la coordenada física en el estado global.

    Serializa de manera segura el progreso del pipeline hacia el disco, permitiendo
    operaciones de I/O eficientes y minimizando el riesgo de pérdida de datos.

    Args:
        anexo_id (str): Identificador único del flujo de datos en procesamiento.
        filas_procesadas (int): Volumen de filas lógicas procesadas acumuladas.
        byte_offset (int): Posición física exacta (seek) en el archivo crudo original.
    """
    ruta_archivo = Path(STATE_FILE)
    datos: dict[str, Any] = {}

    if ruta_archivo.exists():
        try:
            with ruta_archivo.open("r", encoding="utf-8") as file:
                datos = json.load(file)
        except json.JSONDecodeError:
            # Recreación del estado ante corrupción crítica del artefacto JSON
            datos = {}

    datos[anexo_id] = {
        "filas_procesadas": filas_procesadas,
        "byte_offset": byte_offset
    }

    with ruta_archivo.open("w", encoding="utf-8") as file:
        json.dump(datos, file, indent=4, ensure_ascii=False)


def eliminar_estado(anexo_id: str) -> None:
    """Elimina el estado persistido de un anexo tras una consolidación exitosa.

    Ejecuta la purga del punto de control específico dentro del artefacto JSON.
    Si el documento queda vacío tras la eliminación, el archivo físico es 
    removido para mantener la higiene del entorno de despliegue.

    Args:
        anexo_id (str): Identificador único del flujo de datos concluido.
    """
    ruta_archivo = Path(STATE_FILE)

    if not ruta_archivo.exists():
        return

    try:
        with ruta_archivo.open("r", encoding="utf-8") as file:
            datos: dict[str, Any] = json.load(file)
    except json.JSONDecodeError:
        return

    if anexo_id in datos:
        del datos[anexo_id]

    if datos:
        with ruta_archivo.open("w", encoding="utf-8") as file:
            json.dump(datos, file, indent=4, ensure_ascii=False)
    else:
        ruta_archivo.unlink(missing_ok=True)