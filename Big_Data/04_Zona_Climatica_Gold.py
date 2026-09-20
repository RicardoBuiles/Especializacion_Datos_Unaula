# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Título - Zona Climática y Capa Gold
# MAGIC %md
# MAGIC # Zona Climática y Capa Gold - Certificados Energéticos Aragón
# MAGIC
# MAGIC **Objetivos:**
# MAGIC 1. **PARTE A - Zona Climática**: Asignar zona climática CTE (C2, C3, D2, D3, E1) a cada certificado
# MAGIC    - Crear dimensión `cee_aragonv2.silver.dim_municipio`
# MAGIC    - Normalizar nombres de municipios
# MAGIC    - Validar cobertura de asignación
# MAGIC
# MAGIC 2. **PARTE B - Capa Gold**: Preparar datos para modelado predictivo
# MAGIC    - `cee_aragonv2.gold.features`: Variables predictoras (lista blanca estricta)
# MAGIC    - `cee_aragonv2.gold.parque_municipio`: Agregado municipal para análisis
# MAGIC    - Control anti-fuga de datos
# MAGIC
# MAGIC **Criterio de diseño Gold**: ¿Existiría este dato ANTES de la visita del técnico certificador?

# COMMAND ----------

# DBTITLE 1,PARTE A - Comparación de métodos para zona climática
# MAGIC %md
# MAGIC ## PARTE A · ZONA CLIMÁTICA
# MAGIC
# MAGIC ### Comparación de tres métodos
# MAGIC
# MAGIC La zona climática CTE (C2, C3, D2, D3, E1 en Aragón) determina las escalas de eficiencia energética A-G. Se asigna por capital de provincia corrigiendo por altitud.
# MAGIC
# MAGIC #### **MÉTODO 1: Zona de la capital → toda la provincia**
# MAGIC
# MAGIC **Descripción**: Asignar a todos los municipios la zona climática de su capital provincial.
# MAGIC - Zaragoza capital → C3 → toda la provincia de Zaragoza
# MAGIC - Huesca capital → D2 → toda la provincia de Huesca  
# MAGIC - Teruel capital → D3 → toda la provincia de Teruel
# MAGIC
# MAGIC **Precisión**: ⚠️ **BAJA** (20-40% error)  
# MAGIC Aragón tiene gran variación altitudinal (300-3.000 m). Municipios a >400m altitud respecto a la capital cambiarían de zona (cada 200m ≈ cambio de subzona). Ejemplo: Benasque (Huesca, 1.138m) es E1, no D2.
# MAGIC
# MAGIC **Esfuerzo**: ✅ Inmediato (3 valores hard-coded)
# MAGIC
# MAGIC **Limitaciones a declarar**:
# MAGIC > "Zona climática asignada por capital provincial (C3/D2/D3), sin corrección altitudinal. Estimación gruesa: ~30% de municipios pueden estar en zona incorrecta (especialmente en Pirineo, Sistema Ibérico y áreas montañosas). Válido para análisis provincial agregado, no para predicción a nivel municipal."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **MÉTODO 2: Tabla municipio-altitud-zona** [RECOMENDADO]
# MAGIC
# MAGIC **Descripción**: Cruzar con fuente externa: tabla `(municipio, codigo_ine, provincia, altitud_m, zona_climatica)`
# MAGIC
# MAGIC **Fuente requerida**:  
# MAGIC [VERIFICAR: Necesitas descargar del INE o Catastro la tabla oficial de altitudes por municipio de Aragón (~730 municipios). El CTE publica mapas zonales, pero necesitas trasladarlos a tabla. Alternativamente: OpenData Aragón puede tener "Municipios de Aragón" con altitud. Buscar: "altitud municipios Aragón INE" o "zonificación climática CTE Aragón".]
# MAGIC
# MAGIC **Precisión**: ✅ **ALTA** (95-98% correcta)  
# MAGIC Si la tabla viene de fuente oficial (CTE, Catastro, IDAE), la zona ya está calculada según normativa. Si solo traes altitud, aplicar regla CTE: capital + corrección cada 200m.
# MAGIC
# MAGIC **Esfuerzo**: ⏱️ Moderado (1-2 días)  
# MAGIC - Localizar/descargar tabla oficial
# MAGIC - Cargarla a Unity Catalog (volumen o tabla CSV)
# MAGIC - Normalizar nombres para el join (eliminar acentos, mayúsculas, artículos "El/La")
# MAGIC
# MAGIC **Limitaciones a declarar**:  
# MAGIC > "Zona climática obtenida de [FUENTE: especificar]. Cobertura: XX% de municipios (verificar en el join). Municipios sin match asignados a zona de capital provincial como fallback."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **MÉTODO 3: Derivar de coordenadas geoespaciales**
# MAGIC
# MAGIC **Descripción**: Usar `coord_x`, `coord_y` (EPSG:25830 UTM 30N) para:
# MAGIC 1. Calcular altitud via API de elevación (ej. Open-Elevation, Google Elevation API)
# MAGIC 2. Interpolar zona desde mapa vectorial del CTE
# MAGIC
# MAGIC **Precisión**: ✅ **MUY ALTA** (98-99% correcta)  
# MAGIC Usa la ubicación exacta del certificado, no solo el municipio.
# MAGIC
# MAGIC **Esfuerzo**: ⚠️ **ALTO** (1+ semana)  
# MAGIC - Consumir API de elevación para ~192k puntos (puede ser lento/costoso)
# MAGIC - Validar sistema de coordenadas y transformaciones
# MAGIC - Programar lógica de interpolación zonal
# MAGIC - O cargar mapa vectorial CTE como geometrías y hacer spatial join
# MAGIC
# MAGIC **Limitaciones a declarar**:  
# MAGIC > "Zona climática derivada de coordenadas geográficas via [API/mapa CTE]. Precisión limitada por calidad de coord_x/coord_y (~XX% de certificados tienen coordenadas válidas)."
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **RECOMENDACIÓN PARA 4 SEMANAS: MÉTODO 2**
# MAGIC
# MAGIC **Justificación**:
# MAGIC - ✅ Balance óptimo precisión/esfuerzo para el plazo
# MAGIC - ✅ Permite validación rápida (cobertura del join)
# MAGIC - ✅ La tabla de municipios es reutilizable para otros análisis
# MAGIC - ✅ Normalización de nombres resuelve problema de conteos (municipio con 3 grafías)
# MAGIC
# MAGIC **Método 1** es demasiado impreciso para modelado predictivo.  
# MAGIC **Método 3** requiere infraestructura geoespacial (APIs, librerías, validación de coordenadas) que consumiría >25% del tiempo del proyecto.
# MAGIC
# MAGIC **Implementación**: Ver celdas siguientes.

# COMMAND ----------

# DBTITLE 1,1. Extraer municipios únicos de Silver
# PASO 1: Extraer municipios únicos desde silver.certificados
# Necesitamos normalizar nombres para resolver el problema de "municipio con 3 grafías"

from pyspark.sql.functions import col, upper, trim, regexp_replace, count, countDistinct

df_certificados = spark.table("cee_aragonv2.silver.certificados")

print("📊 Municipios en Silver:")
df_municipios_raw = df_certificados.groupBy("municipio", "provincia").agg(
    count("*").alias("num_certificados")
)

print(f"   Combinaciones (municipio, provincia) únicas: {df_municipios_raw.count()}")
print(f"   Municipios distintos (sin provincia): {df_certificados.select('municipio').distinct().count()}")

# Muestra de variaciones en nombres (problema a resolver)
print("\n⚠️  Ejemplo de variaciones detectadas:")
df_municipios_raw.filter(col("municipio").like("%ZARAGOZA%")).show(10, False)

# COMMAND ----------

# DBTITLE 1,2. Crear dim_municipio con normalización
# PASO 2: Construir dim_municipio con normalización
# Estrategia: normalizar nombres (sin acentos, mayúsculas, sin artículos) para unificar grafías

from pyspark.sql.functions import col, upper, trim, regexp_replace, initcap, lit
from pyspark.sql.functions import translate  # para eliminar acentos

# [VERIFICAR: Necesitas una fuente externa para altitud y código INE]
# Opciones:
# 1. OpenData Aragón: https://opendata.aragon.es → buscar "Municipios de Aragón"
# 2. INE: https://www.ine.es → "Relación de municipios y códigos por provincias"
# 3. Catastro: tabla de municipios con altitud oficial
# 
# Para este ejercicio, crearemos la estructura sin datos reales de altitud/códigos.
# Los valores de altitud y código INE se marcarán como NULL.

# Función de normalización de nombres
def normalizar_nombre(col_name):
    """
    Normaliza nombres de municipios:
    - Elimina acentos
    - Convierte a mayúsculas
    - Elimina artículos iniciales (EL, LA, LOS, LAS)
    - Trim espacios
    """
    # Eliminar acentos (translate no soporta todos los casos, usar regexp_replace)
    sin_acentos = regexp_replace(col_name, "[ÁÀÄÂ]", "A")
    sin_acentos = regexp_replace(sin_acentos, "[ÉÈËÊ]", "E")
    sin_acentos = regexp_replace(sin_acentos, "[ÍÌÏÎ]", "I")
    sin_acentos = regexp_replace(sin_acentos, "[ÓÒÖÔ]", "O")
    sin_acentos = regexp_replace(sin_acentos, "[ÚÙÜÛ]", "U")
    sin_acentos = regexp_replace(sin_acentos, "[áàäâ]", "a")
    sin_acentos = regexp_replace(sin_acentos, "[éèëê]", "e")
    sin_acentos = regexp_replace(sin_acentos, "[íìïî]", "i")
    sin_acentos = regexp_replace(sin_acentos, "[óòöô]", "o")
    sin_acentos = regexp_replace(sin_acentos, "[úùüû]", "u")
    sin_acentos = regexp_replace(sin_acentos, "Ñ", "N")
    sin_acentos = regexp_replace(sin_acentos, "ñ", "n")
    
    # Mayúsculas y trim
    mayusculas = upper(trim(sin_acentos))
    
    # Eliminar artículos iniciales (con espacio después)
    sin_articulos = regexp_replace(mayusculas, "^(EL |LA |LOS |LAS )", "")
    
    return sin_articulos

# Crear dimensión con normalización
df_dim_municipio = df_certificados.select(
    "municipio",
    "provincia"
).distinct().withColumn(
    "municipio_normalizado", normalizar_nombre(col("municipio"))
).withColumn(
    "provincia_normalizada", normalizar_nombre(col("provincia"))
)

# Agregar código INE (NULL por ahora - requiere fuente externa)
df_dim_municipio = df_dim_municipio.withColumn(
    "codigo_ine", lit(None).cast("string")
)

# Agregar altitud (NULL por ahora - requiere fuente externa)
df_dim_municipio = df_dim_municipio.withColumn(
    "altitud_m", lit(None).cast("int")
)

# Asignar zona climática por PROVINCIA (método 1 como fallback)
# Zaragoza → C3, Huesca → D2, Teruel → D3
from pyspark.sql.functions import when

df_dim_municipio = df_dim_municipio.withColumn(
    "zona_climatica",
    when(col("provincia_normalizada") == "ZARAGOZA", "C3")
    .when(col("provincia_normalizada") == "HUESCA", "D2")
    .when(col("provincia_normalizada") == "TERUEL", "D3")
    .otherwise(None)
)

# Agregar ID único (row_number)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.orderBy("provincia", "municipio")
df_dim_municipio = df_dim_municipio.withColumn(
    "municipio_id", row_number().over(window_spec)
)

print(f"\n✅ dim_municipio construida:")
print(f"   Registros: {df_dim_municipio.count()}")
print(f"   Municipios únicos (normalizados): {df_dim_municipio.select('municipio_normalizado').distinct().count()}")

# Verificar si la normalización redujo duplicados
antes = df_certificados.select("municipio").distinct().count()
despues = df_dim_municipio.select("municipio_normalizado").distinct().count()
print(f"   Reducción de duplicados: {antes} → {despues} ({antes - despues} grafías unificadas)")

df_dim_municipio.show(10, False)

# COMMAND ----------

# DBTITLE 1,3. Guardar dim_municipio
# PASO 3: Guardar dim_municipio en Unity Catalog

# Crear schema silver si no existe
spark.sql("CREATE SCHEMA IF NOT EXISTS cee_aragonv2.silver")

# Guardar la dimensión
df_dim_municipio.write.mode("overwrite").saveAsTable("cee_aragonv2.silver.dim_municipio")

print("✅ Tabla guardada: cee_aragonv2.silver.dim_municipio")

# REPORTE OBLIGATORIO
print("\n" + "="*80)
print("📋 REPORTE: cee_aragonv2.silver.dim_municipio")
print("="*80)

# Contar filas
df_saved = spark.table("cee_aragonv2.silver.dim_municipio")
num_filas = df_saved.count()
print(f"\n🔢 Número de filas: {num_filas:,}")

# Lista de columnas con tipo
print("\n📊 Columnas:")
for field in df_saved.schema.fields:
    print(f"   • {field.name}: {field.dataType.simpleString()}")

# Display de 20 filas con todas las columnas
print("\n🔍 Display (20 filas, todas las columnas):")
print("="*80)
display(df_saved.limit(20))

# COMMAND ----------

# DBTITLE 1,4. Join con certificados - Verificar cobertura
# PASO 4: Join silver.certificados con dim_municipio
# OBJETIVO: Reportar cuántas filas quedaron SIN zona climática asignada

from pyspark.sql.functions import col, count, when, isnan, isnull

df_certificados = spark.table("cee_aragonv2.silver.certificados")
df_dim = spark.table("cee_aragonv2.silver.dim_municipio")

# Normalizar municipio en certificados para el join
df_certificados_norm = df_certificados.withColumn(
    "municipio_normalizado", normalizar_nombre(col("municipio"))
).withColumn(
    "provincia_normalizada", normalizar_nombre(col("provincia"))
)

# LEFT JOIN: todas las filas de certificados, agregar zona_climatica
df_certificados_zona = df_certificados_norm.alias("c").join(
    df_dim.select(
        "municipio_normalizado", 
        "provincia_normalizada", 
        "zona_climatica", 
        "altitud_m",
        "codigo_ine"
    ).alias("d"),
    on=["municipio_normalizado", "provincia_normalizada"],
    how="left"
)

# VERIFICAR COBERTURA
print("🔍 COBERTURA DE ZONA CLIMÁTICA:")
print("="*80)

total_certificados = df_certificados_zona.count()
con_zona = df_certificados_zona.filter(col("zona_climatica").isNotNull()).count()
sin_zona = df_certificados_zona.filter(col("zona_climatica").isNull()).count()

print(f"\n📊 Total certificados: {total_certificados:,}")
print(f"✅ CON zona climática asignada: {con_zona:,} ({100*con_zona/total_certificados:.2f}%)")
print(f"❌ SIN zona climática asignada: {sin_zona:,} ({100*sin_zona/total_certificados:.2f}%)")

if sin_zona > 0:
    print("\n⚠️  Municipios sin zona asignada:")
    df_sin_zona = df_certificados_zona.filter(col("zona_climatica").isNull()) \
        .groupBy("municipio", "provincia") \
        .agg(count("*").alias("num_certificados")) \
        .orderBy(col("num_certificados").desc())
    df_sin_zona.show(20, False)
else:
    print("\n✅ Todos los certificados tienen zona climática asignada.")

# Distribución por zona
print("\n📈 Distribución por zona climática:")
df_certificados_zona.groupBy("zona_climatica") \
    .agg(count("*").alias("num_certificados")) \
    .orderBy("zona_climatica") \
    .show()

# COMMAND ----------

# DBTITLE 1,PARTE B - Capa Gold
# MAGIC %md
# MAGIC ## PARTE B - CAPA GOLD
# MAGIC
# MAGIC ### Objetivo: Predecir consumo de inmuebles SIN certificado
# MAGIC
# MAGIC **Criterio único**: ¿Existe este dato ANTES de la visita del técnico?
# MAGIC
# MAGIC **Variables EXCLUIDAS** (fuga):
# MAGIC - emisiones CO2 (raw y calculada)
# MAGIC - clasificaciones A-G (consumo y emisiones)
# MAGIC - fechas de emisión/expiración
# MAGIC
# MAGIC **Por qué Silver SÍ conserva emisiones**: Silver es verdad histórica (auditoría). Gold es propósito específico (modelado).

# COMMAND ----------

# DBTITLE 1,5. Crear gold.features con lista blanca
# GOLD.FEATURES: LISTA BLANCA ESTRICTA (NUNCA drop)

from pyspark.sql.functions import col

# Leer datos con zona climática
df_cert = spark.table("cee_aragonv2.silver.certificados")
df_dim = spark.table("cee_aragonv2.silver.dim_municipio")

# Normalizar para join
df_cert_norm = df_cert.withColumn(
    "municipio_norm", normalizar_nombre(col("municipio"))
).withColumn(
    "provincia_norm", normalizar_nombre(col("provincia"))
)

# Join con zona climática
df_con_zona = df_cert_norm.alias("c").join(
    df_dim.select("municipio_normalizado", "provincia_normalizada", "zona_climatica").alias("d"),
    on=[
        col("c.municipio_norm") == col("d.municipio_normalizado"),
        col("c.provincia_norm") == col("d.provincia_normalizada")
    ],
    how="left"
).select("c.*", "d.zona_climatica")

# LISTA BLANCA (columnas admitidas)
columnas_gold = [
    # Claves
    "numero_certificado",
    "referencia_catastral",
    
    # Predictoras (conocidas ANTES de visita)
    "tipo_edificio",              # Catastro
    "estado_edificio",            # Catastro
    "anio_construccion",          # Catastro
    "superficie_m2",              # Catastro
    "municipio",                  # Ubicación
    "provincia",                  # Ubicación
    "zona_climatica",             # Derivada ubicación
    "direccion",                  # Dirección
    
    # TARGET
    "consumo_kwh_m2_anio"
]

# EXCLUIDAS (con motivo):
# ❌ emision_co2_kg_m2_anio → Se calcula DESPUÉS del consumo (fuga)
# ❌ emision_co_raw → Raw de emisiones (fuga)
# ❌ clasificacion_emisiones → Derivada de emisiones (fuga)
# ❌ clasificacion_consumo → FUGA DIRECTA: consumo binneado
# ❌ fecha_emision → No existe antes de visita
# ❌ fecha_expiracion → No existe antes de visita
# ❌ batch_id, source_file, etc. → Metadatos linaje
# ❌ coord_x / coord_y → INSERVIBLES: al separar el campo UTM del origen,
#    la componente Y quedo integramente vacia (187.570 valores en X, 0 en Y).
#    Sin las dos componentes no hay punto. La ubicacion queda representada
#    por municipio, provincia y zona_climatica.

df_gold_features = df_con_zona.select(*columnas_gold).filter(
    col("consumo_kwh_m2_anio").isNotNull()
)

print(f"✅ gold.features: {df_gold_features.count():,} filas")
print(f"   Predictoras: {len(columnas_gold) - 3}")
print(f"   Target: consumo_kwh_m2_anio")

# COMMAND ----------

# DBTITLE 1,6. Guardar gold.features + Reporte
# Guardar gold.features
spark.sql("CREATE SCHEMA IF NOT EXISTS cee_aragonv2.gold")
df_gold_features.write.mode("overwrite").saveAsTable("cee_aragonv2.gold.features")

print("✅ Tabla: cee_aragonv2.gold.features")

# REPORTE OBLIGATORIO
df_saved = spark.table("cee_aragonv2.gold.features")
print(f"\n🔢 Filas: {df_saved.count():,}")
print("\n📊 Columnas:")
for f in df_saved.schema.fields:
    print(f"   • {f.name}: {f.dataType.simpleString()}")

print("\n🔍 Display (20 filas):")
display(df_saved.limit(20))

# COMMAND ----------

# DBTITLE 1,7. Crear gold.parque_municipio
# GOLD.PARQUE_MUNICIPIO: Agregado municipal

from pyspark.sql.functions import count, sum as _sum, avg, expr, when

# Preparar datos con zona
df_full = spark.table("cee_aragonv2.silver.certificados").alias("c").join(
    spark.table("cee_aragonv2.silver.dim_municipio").select(
        "municipio_normalizado", "provincia_normalizada", "zona_climatica"
    ).alias("d"),
    on=[
        normalizar_nombre(col("c.municipio")) == col("d.municipio_normalizado"),
        normalizar_nombre(col("c.provincia")) == col("d.provincia_normalizada")
    ],
    how="left"
).select(col("c.*"), col("d.zona_climatica"))

# Agregación por municipio y año
df_parque = df_full.filter(
    col("consumo_kwh_m2_anio").isNotNull()
).groupBy(
    "municipio", "provincia", "zona_climatica", "anio_construccion"
).agg(
    # Conteos por letra
    count(when(col("clasificacion_consumo") == "A", 1)).alias("letra_A"),
    count(when(col("clasificacion_consumo") == "B", 1)).alias("letra_B"),
    count(when(col("clasificacion_consumo") == "C", 1)).alias("letra_C"),
    count(when(col("clasificacion_consumo") == "D", 1)).alias("letra_D"),
    count(when(col("clasificacion_consumo") == "E", 1)).alias("letra_E"),
    count(when(col("clasificacion_consumo") == "F", 1)).alias("letra_F"),
    count(when(col("clasificacion_consumo") == "G", 1)).alias("letra_G"),
    count("*").alias("num_certificados"),
    _sum("superficie_m2").alias("superficie_total_m2"),
    avg("consumo_kwh_m2_anio").alias("consumo_medio"),
    expr("percentile_approx(consumo_kwh_m2_anio, 0.5)").alias("consumo_mediano")
)

# Benchmark por zona
df_bench = df_full.filter(
    col("consumo_kwh_m2_anio").isNotNull()
).groupBy("zona_climatica").agg(
    avg("consumo_kwh_m2_anio").alias("consumo_medio_zona")
)

print("🌡️ Benchmark por zona:")
df_bench.show()

# Join con benchmark
df_parque = df_parque.alias("p").join(
    df_bench.alias("z"), on="zona_climatica", how="left"
).select("p.*", "z.consumo_medio_zona")

print(f"✅ Parque: {df_parque.count():,} combinaciones (municipio, año)")

# COMMAND ----------

# DBTITLE 1,8. Guardar gold.parque_municipio + Reporte
# Guardar gold.parque_municipio
df_parque.write.mode("overwrite").saveAsTable("cee_aragonv2.gold.parque_municipio")

print("✅ Tabla: cee_aragonv2.gold.parque_municipio")

# REPORTE OBLIGATORIO
df_saved = spark.table("cee_aragonv2.gold.parque_municipio")
print(f"\n🔢 Filas: {df_saved.count():,}")
print("\n📊 Columnas:")
for f in df_saved.schema.fields:
    print(f"   • {f.name}: {f.dataType.simpleString()}")

print("\n🔍 Display (20 filas, ordenado por certificados):")
display(df_saved.orderBy(col("num_certificados").desc()).limit(20))

# COMMAND ----------

# DBTITLE 1,9. Control anti-fuga (correlaciones)
# CONTROL ANTI-FUGA: Correlación con el target
# Alerta si alguna variable supera 0.95

import pandas as pd
from pyspark.sql.functions import col

df_features = spark.table("cee_aragonv2.gold.features")

# Columnas numéricas (predictoras)
cols_numericas = [
    "anio_construccion",
    "superficie_m2"
]

target = "consumo_kwh_m2_anio"

print("🔍 CONTROL ANTI-FUGA: Correlaciones con target")
print("="*80)

correlaciones = []
for col_name in cols_numericas:
    # Calcular correlación de Pearson
    df_temp = df_features.select(col_name, target).dropna()
    corr = df_temp.stat.corr(col_name, target) if df_temp.count() > 0 else None
    if corr is None:
        print(f"[AVISO] {col_name}: sin datos suficientes para correlacionar. Revisar la columna.")
        continue
    correlaciones.append({"variable": col_name, "correlacion": corr})
    
    # Alerta si supera 0.95
    if abs(corr) > 0.95:
        print(f"⚠️  ¡ALERTA FUGA! {col_name}: {corr:.4f} (>0.95)")
    else:
        print(f"✅ {col_name}: {corr:.4f}")

df_corr = pd.DataFrame(correlaciones).sort_values("correlacion", ascending=False, key=abs)
print("\n📊 Resumen de correlaciones:")
print(df_corr.to_string(index=False))

if all(abs(c["correlacion"]) <= 0.95 for c in correlaciones):
    print("\n✅ ¡TODO OK! Ninguna variable supera 0.95")
else:
    print("\n❌ ACCIÓN REQUERIDA: Eliminar variables con fuga")

# COMMAND ----------

# DBTITLE 1,Explicación: Fuga en Gold vs Silver
# MAGIC %md
# MAGIC ### 🔑 Por qué resolver la fuga en Gold (no en Silver)
# MAGIC
# MAGIC **Silver conserva TODO** (verdad histórica): emisiones, clasificaciones, fechas. Su propósito es auditoría, trazabilidad y uso general (dashboards, reportes de certificados existentes). Silver NO sabe para qué la usaremos.
# MAGIC
# MAGIC **Gold filtra según propósito**: diseñada para modelado predictivo ("predecir consumo ANTES de visita"). Aquí aplicamos lógica de negocio (lista blanca anti-fuga). Resolver la fuga en Silver perdería información para otros usos legítimos.

# COMMAND ----------

# DBTITLE 1,Consultas Gold
# MAGIC %sql
# MAGIC SELECT COUNT(*) AS filas FROM cee_aragonv2.gold.features;
# MAGIC SELECT * FROM cee_aragonv2.gold.features LIMIT 20;
# MAGIC SELECT * FROM cee_aragonv2.gold.parque_municipio LIMIT 20;
# MAGIC