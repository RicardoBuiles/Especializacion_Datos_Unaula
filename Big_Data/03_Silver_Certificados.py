# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Título y descripción
# MAGIC %md
# MAGIC # Silver - Certificados Energéticos Aragón
# MAGIC
# MAGIC **Capa Silver**: Limpieza, tipado, validación y separación calidad/cuarentena.
# MAGIC
# MAGIC **Entrada:** `cee_aragonv2.bronze.certificados_raw` (todo STRING, 192,145 filas)  
# MAGIC **Salidas:**
# MAGIC * `cee_aragonv2.silver.certificados` → Filas que pasan todas las reglas de calidad
# MAGIC * `cee_aragonv2.silver.cuarentena` → Filas que fallan, con motivo detallado
# MAGIC
# MAGIC **Garantía:** Silver + Cuarentena = Bronze (exacto)
# MAGIC
# MAGIC **Estrategia:**
# MAGIC * Reglas declaradas como datos (no dispersas en código)
# MAGIC * Un solo recorrido, dos salidas
# MAGIC * Manejo explícito de NULLs en condiciones booleanas (NULL ≠ False)
# MAGIC * Contradiciones clasificacion_consumo/emisiones → Se conservan con flag (son errores reales de certificación, útiles para auditoría)

# COMMAND ----------

# DBTITLE 1,1. Definición de reglas de calidad como datos (CORREGIDO)
# 1. REGLAS DE CALIDAD COMO DATOS
# Cada regla tiene: id, columna afectada, descripción legible, condición SQL
# Se derivan de los hallazgos del perfilado y los rangos de dominio

from pyspark.sql.functions import col, when, concat_ws, collect_list, expr, array, lit, coalesce

# CONTEXTO DE DOMINIO:
# - Consumo real: 20-600 kWh/m²/año
# - Año construcción válido: 1800-2024 (ampliado desde 1400 por contexto histórico)
# - Desajustes entre letras consumo/emisiones son errores de certificación reales

# Definir reglas de validación
reglas = [
    {
        'id': 'R01',
        'columna': 'anio_construccion',
        'descripcion': 'Año construcción fuera de rango válido [1800, 2024]',
        'condicion': '(anio_construccion IS NOT NULL AND (anio_construccion < 1800 OR anio_construccion > 2024))'
    },
    {
        'id': 'R02',
        'columna': 'fecha_emision',
        'descripcion': 'Fecha emisión con año fuera de rango [2009, 2023]',
        'condicion': '(fecha_emision IS NOT NULL AND (YEAR(fecha_emision) < 2009 OR YEAR(fecha_emision) > 2023))'
    },
    {
        'id': 'R03',
        'columna': 'superficie_m2',
        'descripcion': 'Superficie sospechosa: mayor a 50,000 m²',
        'condicion': '(superficie_m2 IS NOT NULL AND superficie_m2 > 50000)'
    },
    {
        'id': 'R04',
        'columna': 'emision_co2_kg_m2_anio',
        'descripcion': 'Emisión CO2 outlier extremo: >74,260 (P99*10)',
        'condicion': '(emision_co2_kg_m2_anio IS NOT NULL AND emision_co2_kg_m2_anio > 74260)'
    },
    {
        'id': 'R05',
        'columna': 'emision_co2_kg_m2_anio',
        'descripcion': 'Emisión CO2 no convertible a número (valor original no numérico)',
        'condicion': '(emision_co2_kg_m2_anio IS NULL AND emision_co_raw IS NOT NULL AND TRIM(emision_co_raw) != "")'
    },
    {
        'id': 'R06',
        'columna': 'consumo_kwh_m2_anio',
        'descripcion': 'Consumo fuera de rango físico razonable [20, 1000] kWh/m²/año',
        'condicion': '(consumo_kwh_m2_anio IS NOT NULL AND (consumo_kwh_m2_anio < 20 OR consumo_kwh_m2_anio > 1000))'
    },
    # R07 ELIMINADA: rango anterior demasiado estrecho [20, 600]
    # P99=797.28 → Nuevo límite conservador: 1000 kWh/m²/año (cubre 99%+ de datos legítimos)
    # R06 ahora implementa este rango corregido
    {
        'id': 'R08',
        'columna': 'consumo_kwh_m2_anio',
        'descripcion': 'Consumo no convertible a número (valor original no numérico)',
        'condicion': '(consumo_kwh_m2_anio IS NULL AND consumo_ener_raw IS NOT NULL AND TRIM(consumo_ener_raw) != "")'
    }
]

print(f"✅ Definidas {len(reglas)} reglas de calidad (R07 deshabilitada por ser demasiado restrictiva)")
for r in reglas:
    print(f"  {r['id']}: {r['descripcion']}")

# COMMAND ----------

# DBTITLE 1,2. Tipado, renombrado y limpieza (Bronze → Silver preparado)
# 2. TIPADO, RENOMBRADO Y LIMPIEZA
# Cuidados especiales:
# - columna consumo_ener tiene barras en el nombre → usar backticks
# - decimales con coma española → reemplazar , por .
# - espacios y puntos de millar → limpiar antes de convertir (si no, devuelve NULL silencioso)
# - superficie=0 → NULL (dato no capturado)
# - coordenadas: corregir extracción de coord_y (problema crítico detectado)

from pyspark.sql.functions import col, regexp_replace, regexp_extract, trim, when, expr, to_timestamp

df_bronze = spark.table("cee_aragonv2.bronze.certificados_raw")

print(f"📊 Filas en Bronze: {df_bronze.count():,}")

# TRANSFORMACIÓN Y TIPADO
df_typed = df_bronze.select(
    # Clave compuesta (numcert NO es único, se reutiliza por edificio)
    col('numcert').alias('numero_certificado'),
    col('refcatastral').alias('referencia_catastral'),
    
    # Fechas: convertir a TIMESTAMP
    to_timestamp(col('fec_emision')).alias('fecha_emision'),
    to_timestamp(col('fec_expira')).alias('fecha_expiracion'),
    
    # Emisión CO2: preservar raw, extraer número, limpiar, convertir
    col('emision_co').alias('emision_co_raw'),
    expr("""
        TRY_CAST(
            REGEXP_REPLACE(
                REGEXP_EXTRACT(emision_co, '^([0-9]+[,.]?[0-9]*)', 1), 
                ',', 
                '.'
            ) AS DOUBLE
        )
    """).alias('emision_co2_kg_m2_anio'),
    
    col('clasificacion_emisiones').alias('clasificacion_emisiones'),
    
    # Consumo energético: preservar raw, extraer número, limpiar, convertir
    col('consumo_ener').alias('consumo_ener_raw'),
    expr("""
        TRY_CAST(
            REGEXP_REPLACE(
                REGEXP_EXTRACT(consumo_ener, '^([0-9]+[,.]?[0-9]*)', 1), 
                ',', 
                '.'
            ) AS DOUBLE
        )
    """).alias('consumo_kwh_m2_anio'),
    
    col('clasificacion_consumo').alias('clasificacion_consumo'),
    
    # Tipo y estado de edificio
    col('tipoedi').alias('tipo_edificio'),
    col('estadoedi').alias('estado_edificio'),
    
    # Año construcción: convertir a INT
    expr("TRY_CAST(anio AS INT)").alias('anio_construccion'),
    
    # Superficie: reemplazar coma por punto, convertir, 0 → NULL
    when(
        expr("TRY_CAST(REGEXP_REPLACE(superficie, ',', '.') AS DOUBLE)") == 0,
        lit(None)
    ).otherwise(
        expr("TRY_CAST(REGEXP_REPLACE(superficie, ',', '.') AS DOUBLE)")
    ).alias('superficie_m2'),
    
    # Ubicación
    col('munic').alias('municipio'),
    col('prov').alias('provincia'),
    col('direccion').alias('direccion'),
    
    # Coordenadas: extracción robusta que maneja formatos variados
    # Formato esperado: "674903,68 , 4612931,37" → X , Y
    # Estrategia: regexp_extract para cada coordenada por separado
    # Patrón: primeros dígitos antes del separador central
    expr("""
        TRY_CAST(
            REGEXP_REPLACE(
                REGEXP_EXTRACT(coordenadas, '^([0-9]+,[0-9]+)', 1), 
                ',', 
                '.'
            ) AS DOUBLE
        )
    """).alias('coord_x'),
    
    # coord_y: últimos dígitos después del separador central
    # Buscar el patrón después de coma-espacio-coma o espacios
    expr("""
        TRY_CAST(
            REGEXP_REPLACE(
                REGEXP_EXTRACT(coordenadas, ',\\s*,?\\s*([0-9]+,[0-9]+)$', 1), 
                ',', 
                '.'
            ) AS DOUBLE
        )
    """).alias('coord_y'),
    
    # Linaje (preservar)
    col('_batch_id').alias('batch_id'),
    col('_source_file').alias('source_file'),
    col('_file_hash').alias('file_hash'),
    col('_ingestion_timestamp').alias('ingestion_timestamp')
)

print("✅ Tipado y renombrado completado")
print(f"   Columnas transformadas: {len(df_typed.columns)}")

# COMMAND ----------

# DBTITLE 1,3. Flags adicionales (desajustes clasificación)
# 3. FLAGS ADICIONALES
# Marcar desajustes clasificacion_consumo vs clasificacion_emisiones
# Decisión: NO van a cuarentena, se conservan con flag
# Razón: Son ~25% de datos (47,761 filas), errores de certificación reales,
#        útiles para auditoría de calidad de certificación

df_flagged = df_typed.withColumn(
    'flag_desajuste_clasificacion',
    when(
        col('clasificacion_consumo') != col('clasificacion_emisiones'),
        lit(True)
    ).otherwise(lit(False))
)

print("✅ Flag de desajuste de clasificación añadido")
print("   Justificación: Se conservan ~47K filas con desajuste (25%)")
print("   porque son errores reales de certificación, no de ingesta.")
print("   Útiles para análisis de calidad de certificación en Gold.")

# COMMAND ----------

# DBTITLE 1,4. Aplicar reglas y separar válidas/inválidas (UN SOLO RECORRIDO)
# 4. APLICAR TODAS LAS REGLAS Y SEPARAR VÁLIDAS/INVÁLIDAS
# UN SOLO RECORRIDO, DOS SALIDAS
# GARANTÍA DURA: silver + cuarentena = bronze (exacto)

from pyspark.sql.functions import array, when, lit, size, concat_ws, coalesce

# Crear columnas booleanas para cada regla
# CRÍTICO: Manejo explícito de NULLs con COALESCE
# Problema: condición booleana sobre NULL devuelve NULL (no False)
# → esa fila desaparece de ambas ramas y descuadra el recuento
# Solución: COALESCE(condicion, False) convierte NULL → False explícitamente

for regla in reglas:
    col_name = f"_falla_{regla['id']}"
    # LÍNEA CRÍTICA QUE RESUELVE EL PROBLEMA DE NULOS:
    # coalesce(expr(...), lit(False)) garantiza que NULL → False
    df_flagged = df_flagged.withColumn(
        col_name,
        coalesce(expr(regla['condicion']), lit(False))
    )

# Crear lista de reglas fallidas para cada fila
# Si la fila falla R01 y R03, motivo_rechazo = "R01: ...; R03: ..."
# IMPORTANTE: usar FILTER para eliminar NULLs del array antes de contar
reglas_fallidas_expr = array(*[
    when(
        col(f"_falla_{r['id']}"),
        lit(f"{r['id']}: {r['descripcion']}")
    )
    for r in reglas
])

df_con_validacion = df_flagged.withColumn(
    '_reglas_fallidas_array_raw',
    reglas_fallidas_expr
).withColumn(
    '_reglas_fallidas_array',
    expr('FILTER(_reglas_fallidas_array_raw, x -> x IS NOT NULL)')
).withColumn(
    'motivo_rechazo',
    concat_ws('; ', col('_reglas_fallidas_array'))
).withColumn(
    '_num_reglas_fallidas',
    size(col('_reglas_fallidas_array'))
)

# SEPARAR EN DOS SALIDAS
# Válidas: las que NO fallan ninguna regla
df_silver = df_con_validacion.filter(
    col('_num_reglas_fallidas') == 0
).drop(
    *[f"_falla_{r['id']}" for r in reglas],
    '_reglas_fallidas_array_raw',
    '_reglas_fallidas_array',
    '_num_reglas_fallidas',
    'motivo_rechazo',
    'emision_co_raw',
    'consumo_ener_raw'
)

# Inválidas: las que fallan al menos una regla
df_cuarentena = df_con_validacion.filter(
    col('_num_reglas_fallidas') > 0
).drop(
    *[f"_falla_{r['id']}" for r in reglas],
    '_reglas_fallidas_array_raw',
    '_reglas_fallidas_array',
    '_num_reglas_fallidas'
)

print("✅ Validación aplicada y filas separadas")
print(f"   Filas válidas (→ silver): {df_silver.count():,}")
print(f"   Filas inválidas (→ cuarentena): {df_cuarentena.count():,}")
print(f"   Total procesado: {df_silver.count() + df_cuarentena.count():,}")
print(f"   Bronze original: {df_bronze.count():,}")

# VERIFICACIÓN DE GARANTÍA
if df_silver.count() + df_cuarentena.count() == df_bronze.count():
    print("\n✅ GARANTÍA CUMPLIDA: Silver + Cuarentena = Bronze")
else:
    print("\n❌ ERROR: Descuadre detectado. Silver + Cuarentena ≠ Bronze")
    print(f"   Diferencia: {df_bronze.count() - (df_silver.count() + df_cuarentena.count())} filas")

# COMMAND ----------

# DBTITLE 1,5. Escribir tablas Silver y Cuarentena (OVERWRITE)
# 5. ESCRIBIR TABLAS SILVER Y CUARENTENA
# Modo: OVERWRITE (no APPEND)
#
# ¿Por qué Silver en OVERWRITE mientras Bronze va en APPEND?
#
# - BRONZE (append): Preserva la historia de ingestas incrementales.
#   Cada batch nuevo se añade, creando un registro temporal completo.
#   Nunca se sobrescribe porque la fuente externa puede cambiar o
#   desaparecer, y queremos auditoría de lo que llegó y cuándo.
#
# - SILVER (overwrite): Es el resultado de procesar TODO Bronze cada vez.
#   Es una transformación completa y determinista: misma entrada →
#   misma salida. No tiene sentido acumular versiones porque cada
#   ejecución reemplaza la anterior con el estado más reciente.
#   Overwrite garantiza que Silver siempre refleja el Bronze actual.

print("📝 Escribiendo tabla cee_aragonv2.silver.certificados...")
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("cee_aragonv2.silver.certificados")

print("✅ Tabla silver.certificados escrita")

print("\n📝 Escribiendo tabla cee_aragonv2.silver.cuarentena...")
df_cuarentena.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("cee_aragonv2.silver.cuarentena")

print("✅ Tabla silver.cuarentena escrita")
print("\n" + "="*80)

# COMMAND ----------

# DBTITLE 1,6. Verificación final y visualización (OBLIGATORIO)
# 6. VERIFICACIÓN FINAL Y VISUALIZACIÓN
# Obligatorio: nombres completos, conteos, display de ambas tablas

from pyspark.sql.functions import col

# Leer tablas escritas
df_silver_final = spark.table("cee_aragonv2.silver.certificados")
df_cuarentena_final = spark.table("cee_aragonv2.silver.cuarentena")

print("📊 TABLAS ESCRITAS:")
print("="*80)
print(f"\n1. Tabla válidas: cee_aragonv2.silver.certificados")
print(f"   Filas: {df_silver_final.count():,}")
print(f"   Columnas: {len(df_silver_final.columns)}")

print(f"\n2. Tabla rechazadas: cee_aragonv2.silver.cuarentena")
print(f"   Filas: {df_cuarentena_final.count():,}")
print(f"   Columnas: {len(df_cuarentena_final.columns)}")

print(f"\n✅ GARANTÍA: {df_silver_final.count():,} + {df_cuarentena_final.count():,} = {df_silver_final.count() + df_cuarentena_final.count():,} filas")
print(f"   Bronze original: {df_bronze.count():,} filas")

if df_silver_final.count() + df_cuarentena_final.count() == df_bronze.count():
    print("   ✅ Cuadra perfecto")
else:
    print("   ❌ Descuadre detectado")

print("\n" + "="*80)
print("📋 MUESTRA DE TABLA SILVER (válidas) - 20 filas, todas las columnas")
print("="*80)
display(df_silver_final.limit(20))

print("\n" + "="*80)
print("📋 MUESTRA DE TABLA CUARENTENA (rechazadas) - 20 filas, todas las columnas")
print("⚠️  Nota: columna 'motivo_rechazo' muestra TODAS las reglas que falló cada fila")
print("="*80)
display(df_cuarentena_final.limit(20))

print("\n" + "="*80)
print("📊 DISTRIBUCIÓN DE RECHAZOS POR REGLA")
print("="*80)
print("\nContando cuántas filas fallaron cada regla (una fila puede fallar múltiples):")
for regla in reglas:
    count = df_cuarentena_final.filter(
        col('motivo_rechazo').contains(regla['id'])
    ).count()
    print(f"  {regla['id']}: {count:,} filas - {regla['descripcion']}")

# COMMAND ----------

# DBTITLE 1,Análisis de rechazos por motivo
# MAGIC %sql
# MAGIC SELECT motivo_rechazo, COUNT(*) AS filas,
# MAGIC        ROUND(100.0 * COUNT(*) /
# MAGIC         (SELECT COUNT(*) FROM cee_aragonv2.bronze.certificados_raw), 2) AS pct
# MAGIC FROM cee_aragonv2.silver.cuarentena
# MAGIC GROUP BY motivo_rechazo
# MAGIC ORDER BY filas DESC;

# COMMAND ----------

# DBTITLE 1,7. Normalización de valores centinela (CORRECCIÓN)
# MAGIC %md
# MAGIC ### Por qué esta celda existe
# MAGIC
# MAGIC El origen codifica "sin calificación" con un guion `-`. Un valor centinela **no es una categoría**:
# MAGIC si se deja como texto, el motor lo trata como una octava letra de una escala de siete, y la
# MAGIC validación de la tarea 03 falla porque encuentra un valor fuera de {A..G, null}.
# MAGIC
# MAGIC Se normaliza a NULL **dentro del pipeline**, de modo que la corrección sobrevive a regenerar Silver.
# MAGIC Las filas se conservan: mantienen el consumo (que es el objetivo) y solo les falta una etiqueta derivada.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE cee_aragonv2.silver.certificados
# MAGIC SET clasificacion_consumo = NULL
# MAGIC WHERE trim(clasificacion_consumo) IN ('-', '', 'N/A', 'NA', 'NULL', '--');

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE cee_aragonv2.silver.certificados
# MAGIC SET clasificacion_emisiones = NULL
# MAGIC WHERE trim(clasificacion_emisiones) IN ('-', '', 'N/A', 'NA', 'NULL', '--');

# COMMAND ----------

# DBTITLE 1,7.1 Comprobación: solo letras A-G o NULL
from pyspark.sql.functions import col

df_chk = spark.table("cee_aragonv2.silver.certificados")

letras_ok = {'A', 'B', 'C', 'D', 'E', 'F', 'G'}

observadas = [
    r["clasificacion_consumo"]
    for r in df_chk.select("clasificacion_consumo").distinct().collect()
]
invalidas = [x for x in observadas if x is not None and x not in letras_ok]

n_nulas = df_chk.filter(col("clasificacion_consumo").isNull()).count()

print(f"Letras observadas : {sorted([x for x in observadas if x is not None])}")
print(f"Valores invalidos : {invalidas}")
print(f"Filas sin letra   : {n_nulas:,}  (conservan el consumo, solo falta la etiqueta)")

assert len(invalidas) == 0, (
    f"Quedan valores no normalizados en clasificacion_consumo: {invalidas}"
)
print("\nOK - clasificacion_consumo solo contiene A-G o NULL")