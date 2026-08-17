# Pipeline de transacciones - Covalto

Pipeline batch que ingiere un archivo diario de transacciones financieras, lo limpia, lo valida y lo entrega en BigQuery listo para consumo de los equipos de Riesgo y Finanzas.

Prueba técnica · Fabián Gómez · Agosto 2026

Repo: https://github.com/perch1n/prueba-covalto

---

## Contexto

En Covalto llega diariamente un archivo CSV con las transacciones del día que necesita procesarse para dejarlo disponible en el warehouse para reportería y análisis de riesgo. El proceso hoy no está estructurado, no hay trazabilidad de lo que entra vs lo que sale, y los registros con errores se descartan sin dejar rastro.

Este pipeline atiende ese vacío: procesa el archivo, separa lo válido de lo inválido, guarda todo con capa por etapa, y deja los errores accesibles para revisión.

---

## Arquitectura

```
Archivo diario (CSV)
       ↓
Cloud Storage           gs://covalto-transacciones/YYYY/MM/DD/
       ↓
Dataflow (Apache Beam)  Leer → Parsear → Deduplicar → Validar
       ↓
BigQuery
  ├── raw.transacciones     ← tal como llegó
  ├── stg.transacciones     ← limpio y validado
  ├── stg.errores           ← rechazados con motivo
  └── ctd.transacciones     ← capa de consumo final
       ↓
Riesgo · Finanzas · Reportería
```

Piezas concretas:

- Ingesta desde Cloud Storage con particionado por fecha
- Procesamiento con Apache Beam en DataflowRunner (`us-central1`)
- Tres datasets en BigQuery para separar etapas del pipeline
- Tabla adicional `stg.errores` para registros rechazados con su motivo
- Cada fila lleva `id_ejecucion` para poder rastrearla hasta la corrida que la procesó

---

## Modelo por etapas

**`raw.transacciones`** — el dato como llegó, sin transformar. Sirve para auditar el origen y para reprocesar si algo sale mal en las etapas siguientes.

**`stg.transacciones`** — datos limpios, deduplicados y validados. Es la capa "confiable" pero todavía no expuesta al consumo.

**`stg.errores`** — registros que no pasaron la validación, con el motivo del rechazo (`monto_negativo`, `fecha_invalida`, `id_duplicado`, etc). No se descartan porque muchas veces son datos recuperables o revelan problemas en el origen.

**`ctd.transacciones`** — capa final para los equipos de Riesgo y Finanzas. Deriva de `stg` con las columnas y particionado que necesita el consumo.

Todas las tablas particionadas por fecha para consultas eficientes.

---

## Trazabilidad

Cada fila procesada lleva un `id_ejecucion` (UUID de la corrida de Dataflow). Con eso se puede reconstruir qué corrida procesó qué registro, cuánto entró vs cuánto salió, y dónde se cayeron las filas si algo no cuadra.

```sql
-- conteo por etapa de una ejecucion
SELECT 'raw' AS etapa, COUNT(*) FROM raw.transacciones WHERE id_ejecucion = 'xxx'
UNION ALL
SELECT 'stg', COUNT(*) FROM stg.transacciones WHERE id_ejecucion = 'xxx'
UNION ALL
SELECT 'errores', COUNT(*) FROM stg.errores WHERE id_ejecucion = 'xxx'
UNION ALL
SELECT 'ctd', COUNT(*) FROM ctd.transacciones WHERE id_ejecucion = 'xxx';
```

Si raw no cuadra con stg + errores, hay un problema en la lógica del pipeline y se detecta de inmediato.

---

## Resultados de la última corrida

| Etapa | Filas |
|---|---|
| raw | 5.000 |
| stg | 4.988 |
| stg.errores | 9 |
| ctd | 4.988 |

Motivos de rechazo detectados: montos negativos, fechas inválidas, IDs duplicados.

---

## Estructura del repo

```
prueba-covalto/
├── pipeline/
│   └── pipeline_transacciones.py    # apache beam
├── consultas/
│   ├── ctd.sql                       # crear la capa de consumo
│   ├── conteo_etapas.sql             # verificar traza por etapa
│   └── errores.sql                   # errores agrupados por motivo
├── docs/
│   └── arquitectura.md
├── capturas/                         # evidencia de la ejecucion
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Cómo correrlo

Requisitos: Python 3.11+, `gcloud` instalado, cuenta GCP con billing activo, Dataflow y BigQuery habilitados.

```bash
# clonar
git clone https://github.com/perch1n/prueba-covalto.git
cd prueba-covalto

# entorno virtual
python -m venv venv
.\venv\Scripts\Activate.ps1  

pip install -r requirements.txt

# autenticacion GCP
gcloud auth application-default login
gcloud config set project prueba-covalto-data

# habilitar apis
gcloud services enable dataflow.googleapis.com bigquery.googleapis.com storage.googleapis.com

# crear buckets
gsutil mb -l us-central1 gs://covalto-transacciones
gsutil mb -l us-central1 gs://covalto-temp-fabian

# subir archivo de entrada
gsutil cp transacciones.csv gs://covalto-transacciones/2026/08/17/

# correr el pipeline en dataflow
python pipeline/pipeline_transacciones.py \
  --runner=DataflowRunner \
  --project=prueba-covalto-data \
  --region=us-central1 \
  --temp_location=gs://covalto-temp-fabian/temp \
  --input=gs://covalto-transacciones/2026/08/17/transacciones.csv

# crear la capa de consumo
bq query --use_legacy_sql=false < consultas/ctd.sql

# verificar la traza
bq query --use_legacy_sql=false < consultas/conteo_etapas.sql
```

---

## Comentarios del diseño

**Tres etapas separadas (raw / stg / ctd)** Permite ver exactamente en qué punto se transforman o pierden filas. Si el pipeline falla, el debug es directo porque puedo comparar entrada vs salida por etapa.

**Errores en tabla** Los registros rechazados van a `stg.errores` con su motivo, no se descartan al vacío. Muchos son recuperables o revelan problemas en el origen que el equipo de Riesgo necesita ver.

**Trasabilidad con `id_ejecucion` en cada fila.**: Se puede rastrear cualquier registro hasta la corrida que lo procesó. Sirve para auditoría.

**Particionamiento por fecha en todas las tablas.** Con volumen bajo el impacto es mínimo, pero es la práctica correcta y escala sin cambios cuando el volumen crece.

**Idempotencia con WRITE_TRUNCATE por partición.** Si una corrida se vuelve a ejecutar sobre el mismo día, sobreescribe esa partición en vez de duplicar. 

---

## Stack

Python 3.14, Apache Beam sobre Dataflow, Google Cloud (Cloud Storage + BigQuery), Git.

Librerías: `apache-beam[gcp]`, `google-cloud-storage`, `google-cloud-bigquery`.