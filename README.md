# SAT ETL Data Pipeline - Out-of-Core Processing

Este repositorio contiene la arquitectura de Ingeniería de Datos para la ingesta, normalización masiva y persistencia (Upsert) de los anexos del SAT (e.g., Anexo 1A, Anexo 2B). Está diseñado bajo el estándar *Out-of-Core* utilizando **Polars**, lo que le permite procesar archivos planos superiores a 50 GB sin desbordar la memoria RAM del servidor.

##  Características Principales

* **Evaluación Perezosa (Lazy Evaluation):** Escaneo de archivos masivos mediante fragmentación dinámica (*Batched Processing*).
* **Sanitización Vectorizada:** Cacería y limpieza de anomalías de codificación (*Mojibake*) y caracteres irregulares a máxima velocidad.
* **Tolerancia a Fallos (Bookmarking):** Implementación de puntos de control (*Checkpoints*) para reanudar el procesamiento en frío ante interrupciones de I/O.
* **Carga Idempotente:** Operaciones lógicas de Staging y Upsert en SQL Server para garantizar una integridad referencial absoluta.
* **Dead Letter Queue (DLQ):** Bifurcación en tiempo de ejecución para aislar registros corruptos o desbordamientos numéricos en una cuarentena auditable.

##  Prerrequisitos de Infraestructura

* **Python:** 3.9+
* **Controlador ODBC:** ODBC Driver 17 for SQL Server.
* **Base de Datos:** SQL Server (On-Premise / Enterprise).

##  Instalación y Configuración

1. **Clonar el repositorio y crear el entorno virtual:**
   ```powershell
   git clone <url_del_repositorio>
   cd sat_etl_pipeline
   python -m venv env
   .\env\Scripts\activate

2. Instalar dependencias:

```powershell
    pip install -r requirements.txt
```
3. Configuración de Variables de Entorno (.env):
Cree un archivo .env en la raíz del proyecto basándose en la siguiente estructura:

```powershell
DB_SERVER=IP_SERVIDOR\INSTANCIA,PUERTO
DB_NAME=SAT_V2
DB_TRUSTED=NO
DB_USER=usuario_etl
DB_PASSWORD=secreto
DB_DRIVER={ODBC Driver 17 for SQL Server}

SAT_RAW_DIR=\\ruta\de\red\Bases de Datos\SAT
ETL_TEMP_DIR=D:\Temp_Polars
```
## Topología del Proyecto
Estructura generada mediante project_mapper.py:

```
sat_etl_pipeline/
├── env/                   # Entorno virtual aislado (Ignorado en Git)
├── logs/                  # Artefactos de auditoría y archivos de cuarentena (DLQ)
├── pkg/                   # Módulos Core del Pipeline
│   ├── checkpoint.py      # Gestor de reanudación y estado
│   ├── cleaning_rules.py  # Repositorio maestro de saneamiento léxico
│   ├── config.py          # Enlace seguro con variables .env
│   ├── enforcer.py        # Coerción estructural y Safe Casting
│   ├── extract.py         # Motor de escaneo binario
│   ├── globals.py         # Infraestructura de ejecución y mapeo DDL
│   ├── load.py            # Motor de persistencia (Upsert SQL)
│   ├── reports.py         # Sistema de telemetría y auditoría
│   └── transform.py       # Reglas de negocio y bifurcación lógica
├── scripts_operativos/    # Herramientas de SOP
│   ├── contador.py        # Escáner de volumetría física
│   ├── mojibake_hunter.py # Analizador forense de texto anómalo
│   ├── profile_data.py    # Detector de Schema Drift y layouts
│   └── project_mapper.py  # Utilitario de topología de directorios
├── .env                   # Credenciales e infraestructura local
├── estado_etl.json        # Archivo de control de progreso (Checkpoints)
├── main.py                # Orquestador Unitario
├── requirements.txt       # Dependencias estrictas
└── run_all.py             # Orquestador de Secuencia Maestra (Fail-Fast)
```
## Operación del Pipeline

**Ejecución Unitaria**

Para iniciar el procesamiento de un único anexo configurado en TABLES_CONFIG (pkg/globals.py):

```powershell
python main.py 1A_2025_2S
```
**Ejecución en Cadena (Semestral)**

Para orquestar múltiples anexos con protección de integridad referencial:
```powershell
python run_all.py 1A_2025_2S 2B_2025_2S
```
**Auditoría Forense Pre-Vuelo (SOP)**

Antes de ingresar nuevos layouts, valide el esquema y detecte Mojibake residual:
```powershell
python scripts_operativos/profile_data.py NombreArchivoSAT.csv
python scripts_operativos/mojibake_hunter.py NombreArchivoSAT.csv
```
