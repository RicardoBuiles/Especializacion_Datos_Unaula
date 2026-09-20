# Databricks notebook source
# DBTITLE 1,Validaciones Silver - Certificados
# MAGIC %md
# MAGIC # Validaciones de Calidad - Silver Certificados
# MAGIC
# MAGIC **Proposito**: verificar invariantes de `cee_aragonv2.silver.certificados` ANTES de que Gold
# MAGIC consuma los datos. Si alguna falla, el notebook termina con error y el job SE DETIENE, de modo
# MAGIC que las tablas de consumo conservan su ultima version buena.
# MAGIC
# MAGIC **Invariantes comprobadas**
# MAGIC 1. Sin nulos en el objetivo (`consumo_kwh_m2_anio`)
# MAGIC 2. Objetivo dentro del rango fisico [20, 1000] kWh/m2/anio
# MAGIC 3. Anio de construccion dentro de [1800, 2024]
# MAGIC 4. `clasificacion_consumo` solo contiene {A..G, NULL}
# MAGIC 5. Conteo total dentro de la banda esperada [180k, 200k]
# MAGIC 6. **Reconciliacion**: validas + cuarentena = filas de Bronze
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Notas de esta version (corregida)
# MAGIC
# MAGIC * **Sin API de RDD.** La version anterior usaba `.rdd.flatMap(...)` en la validacion 4, que
# MAGIC   Databricks **bloquea en computo serverless**: el job fallaba en la tarea 03 por un motivo que
# MAGIC   no tenia nada que ver con los datos. Todo se hace ahora con la API de DataFrame.
# MAGIC * **Una sola lectura.** La version anterior leia la tabla y ejecutaba `.count()` cinco veces
# MAGIC   por separado (~5 min). Aqui se recogen todos los conteos en **una sola pasada** con
# MAGIC   agregaciones condicionales (~10 s).
# MAGIC * **Informe de fallo util.** Cuando una invariante no se cumple, se dice cual es, cuantas filas
# MAGIC   la incumplen y se muestran cinco ejemplos, en lugar de solo `AssertionError`.

# COMMAND ----------

# DBTITLE 1,0. Parametros y lectura unica
from pyspark.sql import functions as F

CATALOGO = "cee_aragonv2"

T_SILVER     = f"{CATALOGO}.silver.certificados"
T_CUARENTENA = f"{CATALOGO}.silver.cuarentena"
T_BRONZE     = f"{CATALOGO}.bronze.certificados_raw"

# Limites de dominio
MIN_CONSUMO, MAX_CONSUMO = 20, 1000        # rango fisico de un edificio
MIN_ANIO,    MAX_ANIO    = 1800, 2024
MIN_FILAS,   MAX_FILAS   = 180_000, 200_000

LETRAS_VALIDAS = ["A", "B", "C", "D", "E", "F", "G"]

df = spark.table(T_SILVER)

# Condiciones de incumplimiento. Ojo con la logica de tres valores:
# si la condicion es NULL, la fila no cae ni en verdadero ni en falso y desaparece
# del conteo. Por eso cada condicion se envuelve en coalesce(..., False).
def falla(cond):
    return F.coalesce(cond, F.lit(False))

cond_nulo_objetivo = falla(F.col("consumo_kwh_m2_anio").isNull())

cond_fuera_rango = falla(
    (F.col("consumo_kwh_m2_anio") < MIN_CONSUMO) |
    (F.col("consumo_kwh_m2_anio") > MAX_CONSUMO)
)

cond_anio_malo = falla(
    F.col("anio_construccion").isNotNull() &
    ((F.col("anio_construccion") < MIN_ANIO) | (F.col("anio_construccion") > MAX_ANIO))
)

cond_letra_mala = falla(
    F.col("clasificacion_consumo").isNotNull() &
    (~F.col("clasificacion_consumo").isin(LETRAS_VALIDAS))
)

# UNA sola pasada sobre la tabla para todos los conteos
r = df.agg(
    F.count(F.lit(1)).alias("total"),
    F.sum(cond_nulo_objetivo.cast("int")).alias("nulos_objetivo"),
    F.sum(cond_fuera_rango.cast("int")).alias("fuera_rango"),
    F.sum(cond_anio_malo.cast("int")).alias("anio_malo"),
    F.sum(cond_letra_mala.cast("int")).alias("letra_mala"),
    F.min("consumo_kwh_m2_anio").alias("min_consumo"),
    F.max("consumo_kwh_m2_anio").alias("max_consumo"),
).collect()[0]

total          = r["total"]
nulos_objetivo = r["nulos_objetivo"] or 0
fuera_rango    = r["fuera_rango"] or 0
anio_malo      = r["anio_malo"] or 0
letra_mala     = r["letra_mala"] or 0

print("=" * 78)
print(f"TABLA        : {T_SILVER}")
print(f"Filas        : {total:,}")
print(f"Consumo      : [{r['min_consumo']:.2f}, {r['max_consumo']:.2f}] kWh/m2/anio")
print("=" * 78)


def reportar_y_fallar(nombre, n_filas, condicion, columnas, ayuda):
    """Muestra ejemplos concretos antes de detener el job."""
    print(f"\n{'!' * 78}")
    print(f"VALIDACION FALLIDA: {nombre}")
    print(f"Filas que la incumplen: {n_filas:,} de {total:,}")
    print(f"Que revisar: {ayuda}")
    print(f"{'!' * 78}")
    print("\nCinco ejemplos:")
    df.filter(condicion).select(*columnas).show(5, truncate=False)
    raise AssertionError(f"{nombre} - {n_filas:,} filas incumplen la invariante. {ayuda}")

# COMMAND ----------

# DBTITLE 1,1. Sin nulos en el objetivo
if nulos_objetivo != 0:
    reportar_y_fallar(
        "Nulos en el objetivo (consumo_kwh_m2_anio)",
        nulos_objetivo,
        cond_nulo_objetivo,
        ["numero_certificado", "municipio", "consumo_kwh_m2_anio"],
        "El modelo no puede entrenar con objetivo nulo. Revisar la regla R08 en Silver: "
        "las filas no convertibles a numero deben ir a cuarentena, no quedarse en Silver.",
    )

print(f"OK  1/6  Sin nulos en el objetivo")

# COMMAND ----------

# DBTITLE 1,2. Objetivo dentro del rango fisico
if fuera_rango != 0:
    reportar_y_fallar(
        f"Consumo fuera del rango fisico [{MIN_CONSUMO}, {MAX_CONSUMO}]",
        fuera_rango,
        cond_fuera_rango,
        ["numero_certificado", "municipio", "superficie_m2", "consumo_kwh_m2_anio"],
        "Revisar la regla R06 en Silver. Si el umbral se calcula con un percentil, "
        "recordar que el propio percentil queda contaminado por los valores extremos: "
        "usar un rango de dominio, no una formula automatica.",
    )

print(f"OK  2/6  Objetivo dentro de [{MIN_CONSUMO}, {MAX_CONSUMO}] kWh/m2/anio")

# COMMAND ----------

# DBTITLE 1,3. Anio de construccion valido
if anio_malo != 0:
    reportar_y_fallar(
        f"Anio de construccion fuera de [{MIN_ANIO}, {MAX_ANIO}]",
        anio_malo,
        cond_anio_malo,
        ["numero_certificado", "municipio", "anio_construccion"],
        "Revisar la regla R01 en Silver.",
    )

print(f"OK  3/6  Anio de construccion dentro de [{MIN_ANIO}, {MAX_ANIO}]")

# COMMAND ----------

# DBTITLE 1,4. Letras A-G validas (sin API de RDD)
if letra_mala != 0:
    # Que valores concretos estan sobrando
    sobrantes = (df
        .filter(cond_letra_mala)
        .groupBy("clasificacion_consumo")
        .count()
        .orderBy(F.desc("count")))

    print("\nValores no permitidos encontrados:")
    sobrantes.show(20, truncate=False)

    reportar_y_fallar(
        "clasificacion_consumo con valores fuera de {A..G, NULL}",
        letra_mala,
        cond_letra_mala,
        ["numero_certificado", "clasificacion_consumo", "consumo_kwh_m2_anio"],
        "Causa habitual: el origen codifica la ausencia de calificacion con un guion '-'. "
        "Un valor centinela no es una categoria: normalizarlo a NULL en el notebook de Silver "
        "(celda 7), no aqui, para que la correccion sobreviva a regenerar la tabla.",
    )

# Que letras hay, para dejar constancia en el log del job
inventario = (df
    .groupBy("clasificacion_consumo")
    .count()
    .orderBy("clasificacion_consumo"))

print("Inventario de clasificacion_consumo:")
inventario.show(10, truncate=False)
print(f"OK  4/6  Solo letras A-G o NULL")

# COMMAND ----------

# DBTITLE 1,5. Conteo dentro de la banda esperada
if not (MIN_FILAS <= total <= MAX_FILAS):
    print(f"\n{'!' * 78}")
    print("VALIDACION FALLIDA: conteo fuera de la banda esperada")
    print(f"Filas en Silver : {total:,}")
    print(f"Banda esperada  : [{MIN_FILAS:,}, {MAX_FILAS:,}]")
    print(f"{'!' * 78}")
    raise AssertionError(
        f"Total de filas ({total:,}) fuera de la banda [{MIN_FILAS:,}, {MAX_FILAS:,}]. "
        f"Causas posibles: (1) Bronze no cargo el archivo completo, "
        f"(2) una regla de calidad esta mandando de mas a cuarentena, "
        f"(3) duplicacion por perdida de idempotencia en Bronze."
    )

print(f"OK  5/6  Conteo dentro de la banda esperada ({total:,} filas)")

# COMMAND ----------

# DBTITLE 1,6. Reconciliacion: validas + cuarentena = Bronze
# Esta es la validacion que de verdad prueba que la ingesta no pierde datos.
# Va como asercion, no como mensaje impreso: dentro de una tarea programada
# un print no detiene nada.

n_validas    = total
n_cuarentena = spark.table(T_CUARENTENA).count()
n_bronze     = spark.table(T_BRONZE).count()

suma = n_validas + n_cuarentena
descuadre = n_bronze - suma

print("RECONCILIACION")
print("-" * 78)
print(f"  Validas en Silver  : {n_validas:>10,}")
print(f"  En cuarentena      : {n_cuarentena:>10,}")
print(f"  {'-' * 34}")
print(f"  Suma               : {suma:>10,}")
print(f"  Filas en Bronze    : {n_bronze:>10,}")
print(f"  Descuadre          : {descuadre:>10,}")
print("-" * 78)

if descuadre != 0:
    print(f"\n{'!' * 78}")
    print("VALIDACION FALLIDA: la reconciliacion no cuadra")
    print(f"{'!' * 78}")
    raise AssertionError(
        f"Reconciliacion fallida: {n_validas:,} validas + {n_cuarentena:,} en cuarentena "
        f"= {suma:,}, pero Bronze tiene {n_bronze:,} filas (descuadre de {descuadre:,}). "
        f"Si el descuadre es positivo, hay filas que no llegaron ni a Silver ni a cuarentena: "
        f"revisar que la rama de rechazo cubra TODAS las filas que no pasan el filtro "
        f"(cuidado con la logica de tres valores: una condicion NULL hace que la fila "
        f"desaparezca de ambas ramas). Si es negativo, Silver o cuarentena estan duplicando."
    )

print(f"\nOK  6/6  Reconciliacion exacta: {n_validas:,} + {n_cuarentena:,} = {n_bronze:,}")

# COMMAND ----------

# DBTITLE 1,Resumen
print("=" * 78)
print("TODAS LAS VALIDACIONES PASARON")
print("=" * 78)
print(f"  {T_SILVER}")
print(f"  {n_validas:,} filas listas para consumirse en Gold")
print(f"  {n_cuarentena:,} filas rechazadas, auditables en {T_CUARENTENA}")
print("=" * 78)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado
# MAGIC
# MAGIC Si este notebook termina sin error, el job continua a la tarea `04_gold_features`.
# MAGIC
# MAGIC Si alguna validacion fallo, el notebook termino con `AssertionError`, el job se detuvo y
# MAGIC **las tablas Gold conservan su ultima version buena**. Esa es la razon de que la validacion
# MAGIC vaya antes de Gold y no despues.