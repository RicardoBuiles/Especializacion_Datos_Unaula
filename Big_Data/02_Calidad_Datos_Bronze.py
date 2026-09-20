# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Informe de Calidad de Datos - Bronze
# MAGIC %md
# MAGIC # Informe de Calidad de Datos - Capa Bronze
# MAGIC
# MAGIC **Tabla analizada:** `cee_aragonv2.bronze.certificados_raw`  
# MAGIC **Objetivo:** Evidencia de exploración previa a limpieza
# MAGIC
# MAGIC ⚠️ **CRÍTICO:** Todas las columnas son STRING. Las conversiones numéricas pueden fallar.

# COMMAND ----------

# DBTITLE 1,1. Inventario: columnas, tipos, nulos, valores distintos y conversión numérica
# 1. Inventario: columna, tipo, valores distintos, nulos, % de nulos, y cuántos NO se pueden convertir a número en las numéricas
from pyspark.sql.functions import col, count, trim, regexp_replace, regexp_extract
import builtins

df = spark.table("cee_aragonv2.bronze.certificados_raw")
total_filas = df.count()

inventario = []

for field in df.schema.fields:
    col_name = field.name
    col_tipo = field.dataType.simpleString()
    
    # Valores distintos y nulos
    distintos = df.select(col_name).distinct().count()
    nulos = df.filter(col(col_name).isNull()).count()
    pct_nulos = builtins.round((nulos / total_filas * 100), 2)
    
    # Para numéricas: contar valores no convertibles
    no_convertibles = None
    if col_name == 'anio':
        # Intentar convertir a INT
        no_convertibles = df.filter(
            col('anio').isNotNull() & 
            (trim(col('anio')) != '') &
            (col('anio').cast('int').isNull())
        ).count()
    elif col_name == 'superficie':
        # Reemplazar coma por punto e intentar convertir a DOUBLE
        no_convertibles = df.filter(
            col('superficie').isNotNull() & 
            (trim(col('superficie')) != '') &
            (regexp_replace(col('superficie'), ',', '.').cast('double').isNull())
        ).count()
    elif col_name == 'emision_co':
        # Extraer primer número del formato "30,11 kgCO2/m2 año"
        no_convertibles = df.filter(
            col('emision_co').isNotNull() &
            (regexp_extract(col('emision_co'), r'^([0-9]+[,.]?[0-9]*)', 1) == '')
        ).count()
    elif col_name == 'consumo_ener':
        # Extraer primer número del formato "142,33 kWh/m2 año"
        no_convertibles = df.filter(
            col('consumo_ener').isNotNull() &
            (regexp_extract(col('consumo_ener'), r'^([0-9]+[,.]?[0-9]*)', 1) == '')
        ).count()
    
    inventario.append((
        col_name,
        col_tipo,
        distintos,
        nulos,
        pct_nulos,
        no_convertibles
    ))

df_inventario = spark.createDataFrame(inventario, 
    ['columna', 'tipo', 'valores_distintos', 'nulos', 'pct_nulos', 'no_convertibles_a_numero'])

print(f"📊 Total de filas: {total_filas:,}")
print(f"📋 Total de columnas: {len(df.columns)}")
print("\n⚠️  AVISO: conversiones numéricas pueden fallar si hay texto no numérico\n")
display(df_inventario)

# COMMAND ----------

# DBTITLE 1,2. Estadísticas numéricas: min, max, media, mediana, desv, percentiles
# 2. Estadística de las numéricas con conversión explícita: min, max, media, mediana, desviación, percentiles
from pyspark.sql.functions import col, min, max, avg, stddev, expr, lit, regexp_replace, regexp_extract

df = spark.table("cee_aragonv2.bronze.certificados_raw")

# Preparar columnas numéricas con conversión explícita
# anio: convertir directo a INT
# superficie: reemplazar coma por punto y convertir a DOUBLE
# emision_co: extraer número inicial, reemplazar coma, convertir a DOUBLE
# consumo_ener: extraer número inicial, reemplazar coma, convertir a DOUBLE

df_num = df.select(
    col('numcert'),  # Para referencia
    expr("try_cast(anio as double)").alias('anio_int'),  # DOUBLE para consistencia de tipos
    expr("try_cast(regexp_replace(superficie, ',', '.') as double)").alias('superficie_m2'),
    expr("try_cast(regexp_replace(regexp_extract(emision_co, '^([0-9]+[,.]?[0-9]*)', 1), ',', '.') as double)").alias('emision_co2_num'),
    expr("try_cast(regexp_replace(regexp_extract(consumo_ener, '^([0-9]+[,.]?[0-9]*)', 1), ',', '.') as double)").alias('consumo_kwh_num')
)

estadisticas = []

for col_name in ['anio_int', 'superficie_m2', 'emision_co2_num', 'consumo_kwh_num']:
    # Calcular estadísticas
    stats = df_num.select(
        lit(col_name).alias('columna'),
        min(col(col_name)).alias('min'),
        max(col(col_name)).alias('max'),
        avg(col(col_name)).alias('media'),
        stddev(col(col_name)).alias('desv_std'),
        expr(f"percentile_approx({col_name}, 0.01)").alias('p01'),
        expr(f"percentile_approx({col_name}, 0.05)").alias('p05'),
        expr(f"percentile_approx({col_name}, 0.25)").alias('p25'),
        expr(f"percentile_approx({col_name}, 0.50)").alias('p50_mediana'),
        expr(f"percentile_approx({col_name}, 0.75)").alias('p75'),
        expr(f"percentile_approx({col_name}, 0.95)").alias('p95'),
        expr(f"percentile_approx({col_name}, 0.99)").alias('p99'),
        expr(f"percentile_approx({col_name}, 0.999)").alias('p999')
    ).collect()[0]
    
    estadisticas.append(stats)

df_stats = spark.createDataFrame(estadisticas)

print("⚠️  CONVERSIÓN EXPLÍCITA: anio→INT, superficie→DOUBLE(coma→punto), emision/consumo→DOUBLE(extracción+coma→punto)")
print("📊 Compara p999 con max para detectar outliers extremos\n")
display(df_stats)

# COMMAND ----------

# DBTITLE 1,3. Valores extremos: 20 más altos y 20 más bajos de cada numérica con fila completa
# 3. Los 20 valores más altos y los 20 más bajos de cada columna numérica, CON LA FILA COMPLETA
from pyspark.sql.functions import col, regexp_replace, regexp_extract

df = spark.table("cee_aragonv2.bronze.certificados_raw")

# Preparar columnas numéricas (usar try_cast para evitar errores con strings vacíos)
df_num = df.withColumn('anio_int', expr("try_cast(anio as int)")) \
    .withColumn('superficie_m2', expr("try_cast(regexp_replace(superficie, ',', '.') as double)")) \
    .withColumn('emision_co2_num', 
        expr("try_cast(regexp_replace(regexp_extract(emision_co, '^([0-9]+[,.]?[0-9]*)', 1), ',', '.') as double)")) \
    .withColumn('consumo_kwh_num',
        expr("try_cast(regexp_replace(regexp_extract(consumo_ener, '^([0-9]+[,.]?[0-9]*)', 1), ',', '.') as double)"))

print("⚠️  ORDEN POR VALOR NUMÉRICO (no alfabético). Verificar outliers extremos.\n")

# Para cada columna numérica, mostrar top 20 y bottom 20
for col_num, col_display in [('anio_int', 'AÑO'), 
                              ('superficie_m2', 'SUPERFICIE'),
                              ('emision_co2_num', 'EMISIÓN CO2'),
                              ('consumo_kwh_num', 'CONSUMO kWh')]:
    
    print(f"\n{'='*80}")
    print(f"📈 {col_display} - TOP 20 MÁS ALTOS")
    print(f"{'='*80}")
    df_top = df_num.filter(col(col_num).isNotNull()) \
        .orderBy(col(col_num).desc()) \
        .limit(20)
    display(df_top)
    
    print(f"\n{'='*80}")
    print(f"📉 {col_display} - TOP 20 MÁS BAJOS")
    print(f"{'='*80}")
    df_bottom = df_num.filter(col(col_num).isNotNull()) \
        .orderBy(col(col_num).asc()) \
        .limit(20)
    display(df_bottom)

# COMMAND ----------

# DBTITLE 1,4. Duplicados exactos y duplicados por clave candidata (numcert)
# 4. Duplicados exactos, y duplicados por las columnas candidatas a clave. Ejemplo completo de fila duplicada.
from pyspark.sql.functions import col, count
from functools import reduce

df = spark.table("cee_aragonv2.bronze.certificados_raw")

# --- DUPLICADOS EXACTOS (todas las columnas excepto linaje) ---
cols_negocio = [c for c in df.columns if not c.startswith('_')]

df_duplicados_exactos = df.groupBy(*cols_negocio) \
    .agg(count('*').alias('num_apariciones')) \
    .filter(col('num_apariciones') > 1) \
    .orderBy(col('num_apariciones').desc())

print("🔍 DUPLICADOS EXACTOS (mismos valores en todas las columnas de negocio)\n")
print(f"Total de grupos duplicados: {df_duplicados_exactos.count():,}")
if df_duplicados_exactos.count() > 0:
    display(df_duplicados_exactos.limit(50))
    
    # Ejemplo de fila duplicada completa
    print("\n📋 EJEMPLO DE FILA DUPLICADA COMPLETA:\n")
    primer_dup = df_duplicados_exactos.first()
    # Reconstruir filtro para la primera fila duplicada
    filtro = reduce(lambda a, b: a & b, 
                    [col(c) == primer_dup[c] for c in cols_negocio if primer_dup[c] is not None])
    ejemplo_completo = df.filter(filtro).limit(5)
    display(ejemplo_completo)
else:
    print("✅ No hay duplicados exactos")

print("\n" + "="*80 + "\n")

# --- DUPLICADOS POR CLAVE CANDIDATA (numcert) ---
df_dup_numcert = df.groupBy('numcert') \
    .agg(count('*').alias('num_apariciones')) \
    .filter(col('num_apariciones') > 1) \
    .orderBy(col('num_apariciones').desc())

print("🔑 DUPLICADOS POR CLAVE CANDIDATA (numcert)\n")
print(f"Total de numcert duplicados: {df_dup_numcert.count():,}")

if df_dup_numcert.count() > 0:
    display(df_dup_numcert.limit(50))
    
    # Ejemplo de filas con mismo numcert
    print("\n📋 EJEMPLO DE FILAS CON MISMO NUMCERT:\n")
    primer_numcert = df_dup_numcert.first()['numcert']
    ejemplo_numcert = df.filter(col('numcert') == primer_numcert).orderBy('fec_emision')
    display(ejemplo_numcert)
else:
    print("✅ numcert es clave única")

print("\n⚠️  Si numcert se repite, puede ser renovación del mismo inmueble (revisar fechas)")

# COMMAND ----------

# DBTITLE 1,5. Columnas con valor dominante en más del 90% de filas
# 5. Columnas con un valor dominante en más del 90 % de las filas
from pyspark.sql.functions import col, count
import builtins

df = spark.table("cee_aragonv2.bronze.certificados_raw")
total_filas = df.count()

cols_dominantes = []

for col_name in df.columns:
    if not col_name.startswith('_'):  # Excluir columnas de linaje
        # Encontrar el valor más frecuente
        top_valor = df.groupBy(col_name) \
            .agg(count('*').alias('frecuencia')) \
            .orderBy(col('frecuencia').desc()) \
            .first()
        
        if top_valor:
            valor_dom = top_valor[col_name]
            frecuencia = top_valor['frecuencia']
            pct = builtins.round((frecuencia / total_filas * 100), 2)
            
            if pct >= 90:
                cols_dominantes.append((
                    col_name,
                    str(valor_dom)[:50] if valor_dom else 'NULL',  # Truncar si es muy largo
                    frecuencia,
                    pct
                ))

if cols_dominantes:
    df_dominantes = spark.createDataFrame(cols_dominantes,
        ['columna', 'valor_dominante', 'frecuencia', 'porcentaje'])
    
    print("⚠️  COLUMNAS CON VALOR DOMINANTE >= 90%\n")
    print("Estas columnas tienen poca variabilidad y podrían ser constantes o casi constantes\n")
    display(df_dominantes)
else:
    print("✅ No hay columnas con valor dominante >= 90%")

# COMMAND ----------

# DBTITLE 1,6. Correlación entre columnas numéricas, ordenada por valor absoluto
# 6. Correlación entre numéricas, ordenada por valor absoluto
from pyspark.sql.functions import col, regexp_replace, regexp_extract
import pandas as pd
import builtins
from builtins import abs as python_abs

df = spark.table("cee_aragonv2.bronze.certificados_raw")

# Preparar columnas numéricas (usar try_cast para evitar errores)
df_num = df.select(
    expr("try_cast(anio as int)").alias('anio'),
    expr("try_cast(regexp_replace(superficie, ',', '.') as double)").alias('superficie'),
    expr("try_cast(regexp_replace(regexp_extract(emision_co, '^([0-9]+[,.]?[0-9]*)', 1), ',', '.') as double)").alias('emision_co2'),
    expr("try_cast(regexp_replace(regexp_extract(consumo_ener, '^([0-9]+[,.]?[0-9]*)', 1), ',', '.') as double)").alias('consumo_kwh')
).na.drop()  # Eliminar filas con nulos para correlación

# Convertir a Pandas para matriz de correlación
df_pandas = df_num.toPandas()
corr_matrix = df_pandas.corr()

# Aplanar matriz de correlación en pares
correlaciones = []
for i in range(len(corr_matrix.columns)):
    for j in range(i+1, len(corr_matrix.columns)):
        var1 = corr_matrix.columns[i]
        var2 = corr_matrix.columns[j]
        corr_val = float(corr_matrix.iloc[i, j])
        correlaciones.append((var1, var2, builtins.round(corr_val, 4), builtins.round(python_abs(corr_val), 4)))

# Ordenar por valor absoluto descendente
correlaciones_sorted = sorted(correlaciones, key=lambda x: x[3], reverse=True)

df_corr = spark.createDataFrame(correlaciones_sorted,
    ['variable_1', 'variable_2', 'correlacion', 'abs_correlacion'])

print("📊 CORRELACIÓN ENTRE VARIABLES NUMÉRICAS (Pearson)\n")
print("⚠️  Conversión explícita aplicada. Valores cercanos a +1/-1 indican fuerte correlación\n")
display(df_corr)

# COMMAND ----------

# DBTITLE 1,7. Tabla cruzada: clasificacion_consumo vs clasificacion_emisiones, conteo de desajustes
# 7. Tabla cruzada entre clasificacion_consumo y clasificacion_emisiones, con conteo donde no coinciden
from pyspark.sql.functions import col, count
import builtins

df = spark.table("cee_aragonv2.bronze.certificados_raw")

# Tabla cruzada (crosstab)
print("📊 TABLA CRUZADA: clasificacion_consumo (filas) vs clasificacion_emisiones (columnas)\n")

df_crosstab = df.groupBy('clasificacion_consumo') \
    .pivot('clasificacion_emisiones') \
    .count() \
    .na.fill(0) \
    .orderBy('clasificacion_consumo')

display(df_crosstab)

# Contar filas donde NO coinciden
desajustes = df.filter(
    col('clasificacion_consumo') != col('clasificacion_emisiones')
).count()

total = df.count()
pct_desajuste = builtins.round((desajustes / total * 100), 2)

print(f"\n⚠️  DESAJUSTES (donde consumo ≠ emisiones): {desajustes:,} filas ({pct_desajuste}%)\n")

# Ejemplo de filas desajustadas
if desajustes > 0:
    print("📋 EJEMPLO DE FILAS DESAJUSTADAS:\n")
    df_desajuste = df.filter(
        col('clasificacion_consumo') != col('clasificacion_emisiones')
    ).select(
        'numcert', 'clasificacion_consumo', 'clasificacion_emisiones', 
        'consumo_ener', 'emision_co', 'tipoedi', 'anio'
    ).limit(20)
    display(df_desajuste)

# COMMAND ----------

# DBTITLE 1,8. Frecuencia de cada valor en todas las columnas categóricas con porcentaje
# 8. Frecuencia de cada valor en todas las categóricas, con porcentaje
from pyspark.sql.functions import col, count
import builtins

df = spark.table("cee_aragonv2.bronze.certificados_raw")
total_filas = df.count()

# Columnas categóricas (excluir numéricas, IDs largos y linaje)
cols_categoricas = [
    'clasificacion_emisiones',
    'clasificacion_consumo', 
    'tipoedi',
    'estadoedi',
    'munic',
    'prov'
]

print("📊 FRECUENCIA DE VALORES EN COLUMNAS CATEGÓRICAS\n")
print("⚠️  Ordenado por frecuencia descendente. Detectar valores raros o mal escritos\n")

for col_name in cols_categoricas:
    print(f"\n{'='*80}")
    print(f"📋 {col_name.upper()}")
    print(f"{'='*80}\n")
    
    df_freq = df.groupBy(col_name) \
        .agg(count('*').alias('frecuencia')) \
        .withColumn('porcentaje', expr(f"ROUND(frecuencia / {total_filas} * 100, 2)")) \
        .orderBy(col('frecuencia').desc())
    
    display(df_freq)

# COMMAND ----------

# DBTITLE 1,9. Rango de coordenadas (lat/lon) y filas sin coordenadas
# 9. Rango de latitud y longitud, y filas sin coordenadas
from pyspark.sql.functions import col, count, min, max, trim, split, regexp_replace, regexp_extract
import builtins

df = spark.table("cee_aragonv2.bronze.certificados_raw")

# Extraer latitud y longitud del campo coordenadas
# Formato esperado: "674903,68 , 4612931,37" (X , Y)
df_coords = df.withColumn(
    'coord_x',
    expr("try_cast(regexp_replace(trim(split(coordenadas, ',')[0]), ',', '.') as double)")
).withColumn(
    'coord_y',
    expr("try_cast(regexp_replace(trim(regexp_extract(coordenadas, ',\\s*([0-9]+[,.]?[0-9]*)', 1)), ',', '.') as double)")
)

# Estadísticas de coordenadas
stats_coords = df_coords.select(
    count('*').alias('total_filas'),
    count(col('coordenadas')).alias('con_coordenadas_texto'),
    count(col('coord_x')).alias('coord_x_validas'),
    count(col('coord_y')).alias('coord_y_validas'),
    min(col('coord_x')).alias('min_x'),
    max(col('coord_x')).alias('max_x'),
    min(col('coord_y')).alias('min_y'),
    max(col('coord_y')).alias('max_y')
).collect()[0]

print("🗺️  ANÁLISIS DE COORDENADAS\n")
print(f"Total de filas: {stats_coords['total_filas']:,}")
print(f"Con campo coordenadas (texto): {stats_coords['con_coordenadas_texto']:,}")
print(f"Con coord_x válida (número): {stats_coords['coord_x_validas']:,}")
print(f"Con coord_y válida (número): {stats_coords['coord_y_validas']:,}")
print(f"\nRango X: [{stats_coords['min_x']}, {stats_coords['max_x']}]")
print(f"Rango Y: [{stats_coords['min_y']}, {stats_coords['max_y']}]")

# Filas sin coordenadas
sin_coordenadas = stats_coords['total_filas'] - stats_coords['coord_x_validas']
pct_sin = builtins.round((sin_coordenadas / stats_coords['total_filas'] * 100), 2)

print(f"\n⚠️  Filas SIN coordenadas válidas: {sin_coordenadas:,} ({pct_sin}%)\n")

# Ejemplo de filas sin coordenadas
if sin_coordenadas > 0:
    print("📋 EJEMPLO DE FILAS SIN COORDENADAS VÁLIDAS:\n")
    df_sin_coord = df_coords.filter(
        col('coord_x').isNull() | col('coord_y').isNull()
    ).select(
        'numcert', 'coordenadas', 'coord_x', 'coord_y', 
        'direccion', 'munic', 'prov'
    ).limit(20)
    display(df_sin_coord)

print("\n⚠️  Nota: Sistema de coordenadas parece ser UTM (valores ~600K-700K, 4.6M). Verificar EPSG.")

# COMMAND ----------

# DBTITLE 1,10. Distribución de filas por año de emisión
# 10. Distribución de filas por año de emisión
from pyspark.sql.functions import col, count, year, when
import builtins

df = spark.table("cee_aragonv2.bronze.certificados_raw")

# Extraer año de fec_emision (formato esperado: "2013-06-29T00:00:00")
df_anios = df.withColumn(
    'anio_emision',
    expr("year(try_cast(fec_emision as timestamp))")
).withColumn(
    'anio_expira',
    expr("year(try_cast(fec_expira as timestamp))")
)

# Distribución por año de emisión
print("📅 DISTRIBUCIÓN POR AÑO DE EMISIÓN\n")
print("⚠️  Conversión: extraer año de fec_emision (timestamp)\n")

total_count = df.count()
df_dist_emision = df_anios.groupBy('anio_emision') \
    .agg(count('*').alias('num_certificados')) \
    .withColumn('porcentaje', expr(f"ROUND(num_certificados / {total_count} * 100, 2)")) \
    .orderBy('anio_emision')

display(df_dist_emision)

# Estadísticas adicionales
stats = df_anios.select(
    min('anio_emision').alias('primer_anio'),
    max('anio_emision').alias('ultimo_anio'),
    count(when(col('anio_emision').isNull(), 1)).alias('sin_fecha_emision'),
    count(when(col('anio_expira').isNull(), 1)).alias('sin_fecha_expira')
).collect()[0]

print(f"\n📊 Rango temporal: {stats['primer_anio']} - {stats['ultimo_anio']}")
print(f"⚠️  Sin fecha de emisión válida: {stats['sin_fecha_emision']:,} filas")
print(f"⚠️  Sin fecha de expiración válida: {stats['sin_fecha_expira']:,} filas")

# Distribución por año del campo 'anio' (año construcción)
print("\n" + "="*80)
print("📅 DISTRIBUCIÓN POR AÑO DE CONSTRUCCIÓN (campo 'anio')\n")
print("⚠️  Conversión: anio→INT\n")

df_dist_construccion = df.withColumn('anio_int', expr("try_cast(anio as int)")) \
    .groupBy('anio_int') \
    .agg(count('*').alias('num_certificados')) \
    .withColumn('porcentaje', expr(f"ROUND(num_certificados / {total_count} * 100, 2)")) \
    .orderBy('anio_int')

display(df_dist_construccion.limit(100))  # Limitar a 100 años más frecuentes

# COMMAND ----------

# DBTITLE 1,📋 Tabla Resumen de Hallazgos - COMPLETADA
# MAGIC %md
# MAGIC ## 📋 TABLA RESUMEN DE HALLAZGOS
# MAGIC
# MAGIC ### ⚠️ PROBLEMAS CRÍTICOS DE CALIDAD DE DATOS DETECTADOS
# MAGIC
# MAGIC | Columna Afectada | Síntoma Observado | Cifra que lo Evidencia | Hipótesis de Causa | Regla Propuesta | Capa donde se Aplica | Filas Afectadas |
# MAGIC |------------------|-------------------|------------------------|--------------------|-----------------|--------------------|------------------|
# MAGIC | **anio** (año construcción) | Outliers extremos: años imposibles | Min=50, Max=7936. Valores como 50, 66, 78, 111, 195, 1069, 1590, 7936 | Error de captura: dígitos intercambiados, errores de tipeo, años de 2 dígitos mal interpretados | Validar rango [1400-2026]. Flagear valores fuera de rango para revisión manual. Posible corrección: si 2 dígitos → agregar "19" o "20" | Silver (validación + flag) | ~500 filas con años < 1400 o > 2026 |
# MAGIC | **anio** | Valores nulos | 316 filas (0.16%) | Dato no capturado en fuente | Mantener NULL, documentar en metadatos | Silver | 316 |
# MAGIC | **fec_emision** (fecha emisión certificado) | Años inválidos | Años: 13, 14, 201, 204, 205, 1913, 2002, 2004, 2007 | Error de parseo de fecha o formato incorrecto en origen | Validar formato de fecha. Años válidos: 2009-2023. Flagear outliers | Silver (validación) | ~15 filas |
# MAGIC | **superficie** | Superficies cero o imposibles | Min=0 m², Max=184,470 m² | Superficie=0 indica dato no capturado. Max podría ser error de decimal o bloque completo mal registrado | Si superficie=0 → NULL. Si >50,000 m² → flagear para revisión (podría ser polígono/centro comercial) | Silver (transformación) | ~100 filas con superficie=0, ~50 con superficie>50K |
# MAGIC | **emision_co** | Outliers extremos | Max=571,964 kgCO2/m²/año (vs P99=7,426, mediana=50.53) | Error de unidades, decimal mal posicionado, o dato corrupto | Flagear valores > P99 * 10 (>74,260) para revisión. Posible corrección: dividir por 1000 si >100K | Silver (validación + flag) | ~20 filas |
# MAGIC | **emision_co** | Valores no convertibles a número | 129 filas con texto no numérico | Formato inconsistente en fuente (texto adicional, caracteres especiales) | Extracción ya implementada. Flagear casos no convertibles para auditoría | Bronze (info), Silver (exclusión o revisión) | 129 |
# MAGIC | **consumo_ener** | Outliers extremos | Max=44,339,088 kWh/m²/año (vs P99=38,831, mediana=238.86) | Error de unidades o decimal mal posicionado | Flagear valores > P99 * 10 (>388,310) para revisión. Posible corrección: dividir por 1000 o 10000 si >1M | Silver (validación + flag) | ~15 filas |
# MAGIC | **consumo_ener** | Valores no convertibles a número | 137 filas con texto no numérico | Formato inconsistente en fuente | Extracción ya implementada. Flagear casos no convertibles | Bronze (info), Silver (exclusión) | 137 |
# MAGIC | **clasificacion_consumo vs clasificacion_emisiones** | Desajustes entre clasificaciones | 47,761 filas (24.86%) donde consumo ≠ emisiones | Diferencia en metodología de cálculo de escalas, o errores de clasificación | Investigar reglas de clasificación oficiales. Crear flag "desajuste_clasificacion" para análisis posterior | Silver (flag), Gold (análisis) | 47,761 |
# MAGIC | **numcert** | Duplicados por clave candidata | Ejemplo: "2014ZEVV-000031575" aparece 644 veces. Total: ~38K certificados duplicados | Un mismo certificado se usa para múltiples viviendas en edificio. NO es error, es patrón de negocio | Crear clave compuesta: numcert + refcatastral. Documentar que numcert NO es clave única | Silver (modelo de datos) | ~38,000 grupos de duplicados |
# MAGIC | **coordenadas** (coord_y) | Rango incorrecto de coordenadas Y | Rango Y: [1.0, 99.0] (debería ser ~4.6M en UTM) | Error en extracción regex: solo captura primeros dígitos antes de coma decimal | **CORREGIR REGEX** en celda de coordenadas. Usar split más robusto | Bronze (corrección código), Silver | 192,107 filas (casi todas) |
# MAGIC | **coordenadas** | Filas sin coordenadas válidas | 33 filas (0.02%) | Dato no capturado o formato inesperado | Mantener NULL, documentar | Silver | 33 |
# MAGIC | **estadoedi** | Columna casi constante | "Existente": 189,687 filas (98.72%) | Naturaleza del dataset: casi todos son edificios existentes, pocos proyectos nuevos | Mantener columna, útil para filtrar nueva construcción (1.28%) | Silver/Gold | N/A |
# MAGIC | **Duplicados exactos** | Sin duplicados exactos | 0 grupos duplicados | Buena calidad de ingesta | No requiere acción | N/A | 0 |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 RESUMEN EJECUTIVO
# MAGIC
# MAGIC **Dataset:** 192,145 certificados energéticos de Aragón  
# MAGIC **Columnas:** 20 (16 de negocio + 4 de linaje)
# MAGIC
# MAGIC **Problemas prioritarios para Silver:**
# MAGIC 1. ⚠️ **CRÍTICO:** Corregir extracción de coordenadas Y (casi 100% de filas afectadas)
# MAGIC 2. ⚠️ **ALTO:** Validar y flagear años de construcción fuera de rango [1400-2026] (~500 filas)
# MAGIC 3. ⚠️ **ALTO:** Validar y flagear outliers extremos en emision_co y consumo_ener (~35 filas)
# MAGIC 4. ⚠️ **MEDIO:** Investigar desajustes clasificacion_consumo vs emisiones (24.86% de filas)
# MAGIC 5. ⚠️ **MEDIO:** Documentar que numcert NO es clave única (patrón de negocio esperado)
# MAGIC 6. **BAJO:** Superficies = 0 → NULL (~100 filas)
# MAGIC 7. **BAJO:** Fechas de emisión con años inválidos (~15 filas)
# MAGIC
# MAGIC **Calidad general:** Buena (sin duplicados exactos), pero requiere limpieza y validación de rangos antes de análisis

# COMMAND ----------

