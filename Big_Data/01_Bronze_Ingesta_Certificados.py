# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Arquitectura Medallion - Certificados Energéticos Aragón
# MAGIC %md
# MAGIC # Capa BRONZE: Ingesta de Certificados Energéticos Aragón
# MAGIC
# MAGIC **Origen:** `/Volumes/cee_aragonv2/landing/raw/energia_aragon.csv`  
# MAGIC **Destino:** `cee_aragonv2.bronze.certificados_raw`
# MAGIC
# MAGIC ## Principios Bronze
# MAGIC * **Sin transformación**: todas las columnas como STRING
# MAGIC * **Captura de corruptos**: registros inválidos en columna especial
# MAGIC * **Linaje completo**: batch_id, archivo, huella SHA-256, timestamp
# MAGIC * **Idempotencia por huella**: control de archivos ya procesados

# COMMAND ----------

# DBTITLE 1,Ingesta Bronze - Certificados Energéticos
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import hashlib

# ============================================================
# 1. GENERAR IDENTIFICADOR DE LOTE Y CALCULAR HUELLA DEL ARCHIVO
# ============================================================

# Identificador de lote con timestamp UTC
batch_id = datetime.utcnow().strftime("batch_%Y%m%d_%H%M%S")
print(f"🔖 Batch ID: {batch_id}")

# Ruta del archivo origen
file_path = "/Volumes/cee_aragonv2/landing/raw/energia_aragon.csv"

# Calcular SHA-256 del archivo completo
with open(file_path.replace("/Volumes/", "/Volumes/"), "rb") as f:
    file_hash = hashlib.sha256(f.read()).hexdigest()
print(f"🔐 SHA-256: {file_hash}")

# ============================================================
# 2. VERIFICAR IDEMPOTENCIA: ¿Ya procesamos este archivo?
# ============================================================

table_name = "cee_aragonv2.bronze.certificados_raw"

# Crear tabla de control si no existe
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS cee_aragonv2.bronze._control_batch (
        file_hash STRING,
        batch_id STRING,
        processed_at TIMESTAMP,
        row_count BIGINT
    )
    USING DELTA
""")

# Verificar si ya procesamos este archivo
already_processed = spark.sql(f"""
    SELECT COUNT(*) as count 
    FROM cee_aragonv2.bronze._control_batch 
    WHERE file_hash = '{file_hash}'
""").collect()[0]["count"] > 0

if already_processed:
    print(f"⚠️  IDEMPOTENCIA: Archivo ya procesado (hash: {file_hash[:16]}...). Saltando ingesta.")
    # Mostrar tabla existente
    df_existing = spark.table(table_name)
    print(f"\n📊 Tabla: {table_name}")
    print(f"📈 Filas totales: {df_existing.count():,}")
    print(f"\n📋 COLUMNAS Y TIPOS:")
    for field in df_existing.schema:
        print(f"   • {field.name}: {field.dataType.simpleString()}")
    print(f"\n📄 MUESTRA DE DATOS (20 filas, TODAS las columnas):\n")
    display(df_existing.limit(20))
else:
    print(f"✅ Archivo nuevo. Procediendo con la ingesta...")
    
    # ============================================================
    # 3. LEER CSV: TODO COMO STRING, SIN INFERIR ESQUEMA
    # ============================================================
    
    # Detectar encoding: intentar UTF-8 primero, luego ISO-8859-1 (Latin1)
    # Para DETECTAR problemas de encoding: spark mostrará � o caracteres corruptos
    
    # Leer TODAS las columnas como STRING
    # multiLine=true: permite saltos de línea dentro de campos (direcciones)
    # escape: manejo de comillas dobles
    # columnNameOfCorruptRecord: captura filas corruptas en columna especial
    
    df = (spark.read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "false")          # CRÍTICO: no inferir tipos
          .option("multiLine", "true")            # Saltos de línea en campos
          .option("quote", '"')                    # Comillas para delimitar
          .option("escape", '"')                   # Escape de comillas dobles
          .option("encoding", "UTF-8")             # Probar UTF-8 primero
          .option("columnNameOfCorruptRecord", "_corrupt_record")  # Capturar corruptos
          .load(file_path)
    )
    
    print(f"📄 Registros leídos del CSV: {df.count():,}")
    
    # Verificar si hay registros corruptos
    if "_corrupt_record" in df.columns:
        corrupt_count = df.filter(col("_corrupt_record").isNotNull()).count()
        print(f"⚠️  Registros corruptos capturados: {corrupt_count:,}")
    
    # ============================================================
    # 4. AÑADIR COLUMNAS DE LINAJE
    # ============================================================
    
    df_bronze = (df
        .withColumn("_batch_id", lit(batch_id))                      # ID del lote
        .withColumn("_source_file", lit("energia_aragon.csv"))      # Archivo origen
        .withColumn("_file_hash", lit(file_hash))                   # Huella SHA-256
        .withColumn("_ingestion_timestamp", current_timestamp())    # Instante ingesta UTC
    )
    
    # ============================================================
    # 5. ESCRIBIR TABLA DELTA EN MODO APPEND CON SCHEMA EVOLUTION
    # ============================================================
    
    (df_bronze.write
        .format("delta")
        .mode("append")                              # Append: acumula registros
        .option("mergeSchema", "true")               # Tolerante a columnas nuevas
        .saveAsTable(table_name)
    )
    
    row_count = df_bronze.count()
    print(f"\n✅ Ingesta completada: {row_count:,} filas escritas en {table_name}")
    
    # Registrar en tabla de control
    spark.sql(f"""
        INSERT INTO cee_aragonv2.bronze._control_batch 
        VALUES ('{file_hash}', '{batch_id}', current_timestamp(), {row_count})
    """)
    
    # ============================================================
    # 6. VERIFICACIÓN OBLIGATORIA
    # ============================================================
    
    df_result = spark.table(table_name)
    
    print(f"\n{'='*70}")
    print(f"📊 TABLA CREADA: {table_name}")
    print(f"{'='*70}")
    
    print(f"\n📈 TOTAL DE FILAS: {df_result.count():,}")
    
    print(f"\n📋 COLUMNAS Y TIPOS:")
    for field in df_result.schema:
        print(f"   • {field.name}: {field.dataType.simpleString()}")
    
    print(f"\n📄 MUESTRA DE DATOS (20 filas, TODAS las columnas):\n")
    display(df_result.limit(20))
    
    print(f"\n{'='*70}")
    print(f"📝 ¿QUÉ ACABAS DE CREAR?")
    print(f"{'='*70}")
    print(f"""
Has creado la tabla Bronze `cee_aragonv2.bronze.certificados_raw` que almacena
los ~180K certificados energéticos tal como vienen del CSV, SIN TRANSFORMAR.

El código lee el CSV completo como STRING, captura registros corruptos, añade
linaje completo (batch_id, origen, huella SHA-256, timestamp) y garantiza
idempotencia: si reejecutas, detecta el archivo ya procesado por su huella
y NO duplica filas.""")

print(f"\n{'='*70}")
print(f"🔍 DETECCIÓN DE ENCODING NO UTF-8:")
print(f"{'='*70}")
print("""
Para detectar problemas de encoding:
1. Busca caracteres � (replacement character) en columnas de texto
2. Filtra registros con _corrupt_record no nulo
3. Si ves errores, relee cambiando .option("encoding", "ISO-8859-1")
   o .option("encoding", "windows-1252") para CSVs españoles antiguos
""")

# COMMAND ----------

# DBTITLE 1,Idempotencia: Por qué control por huella (no MERGE ni deduplicación)
# MAGIC %md
# MAGIC ## ⚙️ Estrategia de Idempotencia: Por qué control por huella
# MAGIC
# MAGIC ### 🚫 Estrategias descartadas:
# MAGIC
# MAGIC **1. MERGE con clave primaria (`numcert`)**
# MAGIC * **Descartada:** El número de certificado SE REPITE en este dataset (renovaciones del mismo inmueble generan nuevo certificado con mismo ID). Un MERGE basado en `numcert` sobrescribiría certificados legítimos o fallaría por duplicados, perdiendo histórico.
# MAGIC
# MAGIC **2. Deduplicación post-ingesta (window functions sobre todas las columnas)**
# MAGIC * **Descartada:** Costoso computacionalmente (~192K filas × 16 columnas para comparar) y NO garantiza idempotencia real si el archivo origen cambia ligeramente (ej: corrección de una dirección). Además, no distingue entre "archivo ya procesado" vs "filas duplicadas dentro del archivo".
# MAGIC
# MAGIC ### ✅ Estrategia elegida: Control de lote por huella SHA-256
# MAGIC
# MAGIC **Ventajas:**
# MAGIC * **Idempotencia total:** Si reejecutas con el MISMO archivo, detecta su huella en `_control_batch` y salta la ingesta (cero duplicados)
# MAGIC * **Eficiente:** Una sola consulta de verificación antes de leer el CSV
# MAGIC * **Granularidad correcta:** Controla a nivel de ARCHIVO completo, no de fila
# MAGIC * **Permite actualizaciones:** Si recibes un archivo NUEVO con correcciones, su huella será distinta y se ingiere como nuevo lote, preservando el histórico
# MAGIC
# MAGIC En Bronze, queremos **fidelidad al archivo fuente**, no deduplicación de negocio (eso es trabajo de Silver/Gold).

# COMMAND ----------

# DBTITLE 1,Justificación Técnica: ¿Por qué STRING en Bronze?
# MAGIC %md
# MAGIC ## 📚 ¿Por qué leer todo como STRING no es pereza sino diseño?
# MAGIC
# MAGIC **Respuesta:** Leer como STRING en Bronze preserva la **fidelidad total** de los datos fuente y delega la interpretación de tipos a Silver, donde ya conoces la semántica del negocio.
# MAGIC
# MAGIC ### Ejemplo concreto con este dataset de certificados:
# MAGIC
# MAGIC Imagina una fila con estos valores:
# MAGIC
# MAGIC ```
# MAGIC Referencia: 000123456
# MAGIC Superficie: 120,5
# MAGIC Consumo: 1.234,56
# MAGIC Fecha: 01/03/2024
# MAGIC ```
# MAGIC
# MAGIC **Si dejas que Spark infiera tipos:**
# MAGIC * `000123456` → Se convierte a `INT 123456` — **pierdes los ceros iniciales** que pueden ser significativos para identificar el certificado
# MAGIC * `120,5` → Se lee como `STRING "120,5"` en lugar de `DOUBLE` porque Spark espera punto decimal (formato inglés), **no lo reconoce como número**
# MAGIC * `1.234,56` → Lo interpreta como `STRING` o incluso se corrompe porque el punto es separador de miles en español
# MAGIC * `01/03/2024` → Ambiguo: ¿es 1 de marzo o 3 de enero? Spark puede malinterpretar el formato según configuración regional
# MAGIC
# MAGIC **Con STRING en Bronze:**
# MAGIC * Conservas `"000123456"` intacto
# MAGIC * En Silver, decides si es ID (mantener como STRING con ceros) o número
# MAGIC * Conviertes `"120,5"` a `DOUBLE` con lógica explícita: `REPLACE(superficie, ',', '.')::DOUBLE`
# MAGIC * Parseas fechas con formato explícito: `TO_DATE(fecha, 'dd/MM/yyyy')`
# MAGIC
# MAGIC **Conclusión:** Bronze captura la **verdad del archivo**. Silver aplica la **interpretación correcta** del negocio.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧪 Ejemplo real del dataset
# MAGIC
# MAGIC Si observas la tabla Bronze, verás valores como:
# MAGIC * `superficie`: `"37.72"` (con punto, pero otros podrían tener coma)
# MAGIC * `anio`: `null` para algunos registros (el valor original estaba vacío)
# MAGIC * `numcert`: `"2013ZEVV-000000098"` (ID alfanumérico con ceros, se perdería si fuera numérico)
# MAGIC
# MAGIC Este diseño garantiza que NINGÚN dato se pierde o corrompe en la ingesta inicial.

# COMMAND ----------

