# SAT ETL Data Pipeline V3 - Nómina Out-of-Core Processing

Este repositorio contiene la arquitectura de Ingeniería de Datos para la extracción, saneamiento masivo y consolidación transaccional de los anexos de Nómina del SAT (Tablas C, D, E, F y G).

La solución está diseñada bajo el estándar *Out-of-Core* utilizando el motor vectorial Polars (Rust) y conectores ADBC. Esta infraestructura permite procesar y cargar archivos planos de alta volumetría (superiores a 150 GB) hacia SQL Server sin exceder los límites de memoria RAM del host, garantizando la integridad referencial y la estabilidad del motor relacional.

---

## Características Principales

* **Evaluación Vectorizada Out-of-Core:** Procesamiento de archivos masivos mediante fragmentación dinámica (Chunking) y lectura binaria de buffer para evadir cuellos de botella de I/O.
* **Sanitización de Datos y Data Quality:** Rutinas de limpieza para anomalías de codificación (Mojibake), validación de estructuras fiscales (RFC/NSS) y auditoría de cuadratura contable de nómina.
* **Espejo Milimétrico con DLQ:** Garantiza la ingesta del 100% de la volumetría original. Las anomalías se segregan en un Log Analítico (Dead Letter Queue) en SQL Server para su posterior auditoría forense.
* **Resiliencia Transaccional O(1):** Implementación de punteros físicos (Byte Offsets) para reanudación instantánea ante interrupciones de red o fallos de infraestructura.
* **Persistencia Blindada:** Motor de carga con fallback automático (ADBC -> SQLAlchemy) para asegurar la inyección de datos y el reporte detallado de errores de base de datos.

---

## Prerrequisitos de Infraestructura

* **Lenguaje:** Python 3.9 o superior.
* **Controlador de Base de Datos:** ODBC Driver 17 for SQL Server.
* **Motor Relacional:** SQL Server (On-Premise / Enterprise).
* **Almacenamiento:** Unidad de red (SMB) para archivos fuente y partición local de alto rendimiento (D: o V:) asignada al Swap transaccional de Polars.

---

## Instalación y Configuración

**1. Clonación del repositorio y aprovisionamiento del entorno virtual:**
```powershell
git clone <url_del_repositorio>
cd ETL-NOMINA-V3
python -m venv venv
.\venv\Scripts\activate

2. Instalación de dependencias (Sincronización Estricta):
Se requiere el uso de pip-tools para certificar la homogeneidad del entorno y la correcta instalación del conector ADBC.

python -m pip install pip-tools
python -m piptools sync requirements.txt

3. Configuración de Variables de Entorno (.env):
Cree un archivo .env en la raíz del proyecto respetando el siguiente contrato:

DB_SERVER=IP_SERVIDOR\INSTANCIA,PUERTO
DB_NAME=SAT_NOMINA_V3
DB_TRUSTED=NO
DB_USER=usuario_etl
DB_PASSWORD=secreto_industrial
DB_DRIVER={ODBC Driver 17 for SQL Server}

# Infraestructura de Almacenamiento
SAT_RAW_DIR=\\servidor_red\SAT_DATA\NOMINA
ETL_TEMP_DIR=D:\Temp_Polars

Topología del Proyecto
Estructura técnica generada mediante la arquitectura modular de la V3:

Operación del Pipeline
Ejecución Unitaria
Para procesar un anexo específico definido en el diccionario maestro:

python main.py NOMINA_3C_2025_1S

Ejecución de Carga Masiva (Pipeline Secuencial)
Para orquestar la cadena completa de anexos con protección de integridad:

ETL-NOMINA-V3/
├── Data/                   # Manuales técnicos, diccionarios y reportes locales
├── logs/                   # Artefactos de auditoría estática (.txt)
├── pkg/                    # Core Engine del Pipeline
│   ├── checkpoint.py       # Gestor de persistencia de estado y reanudación
│   ├── cleaning_rules.py   # Diccionarios de saneamiento léxico
│   ├── config.py           # Gestión de conectividad y Pool de SQL
│   ├── consolidation.py    # Rutinas Set-Based (Staging -> Target)
│   ├── enforcer.py         # Coerción de tipos y validación de esquemas
│   ├── extract.py          # Extractor binario por bloques
│   ├── globals.py          # Configuración maestra, rutas y DDL
│   ├── load.py             # Motor de carga masiva (ADBC/SQLAlchemy)
│   ├── reports.py          # Sistema de telemetría y monitoreo
│   └── transform.py        # Motor de Data Quality y reglas de negocio
├── scripts_operativos/     # Herramientas SOP (Standard Operating Procedures)
│   ├── contador.py         # Validación de volumetría física
│   ├── mojibake_hunter.py  # Analizador forense de texto anómalo
│   ├── profile_sat.py      # Detector de Schema Drift y layouts
│   └── project_mapper.py   # Renderizador de topología del repositorio
├── .env                    # Variables de entorno y credenciales
├── estado_etl.json         # Control de progreso transaccional
├── main.py                 # Orquestador Unitario
├── requirements.txt        # Manifiesto de dependencias certificado
└── run_all.py              # Orquestador Maestro Secuencial (Fail-Fast)

# Ejecuta la secuencia histórica predeterminada (2023 - 2025)
python run_all.py

Auditoría Forense (SOP)
Antes de procesar nuevos archivos, valide la integridad estructural:

# Evaluar densidad de datos y tipos
python scripts_operativos/profile_sat.py ArchivoNomina.csv

# Identificar patrones de corrupción de caracteres
python scripts_operativos/mojibake_hunter.py ArchivoNomina.csv

