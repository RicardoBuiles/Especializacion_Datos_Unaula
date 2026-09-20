# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Modelo de prediccion de consumo energetico
# MAGIC %md
# MAGIC # Prediccion del consumo energetico del parque no certificado
# MAGIC
# MAGIC **Objetivo**: estimar el consumo de energia primaria no renovable (kWh/m2/anio) de un inmueble
# MAGIC que todavia **no tiene certificado**, usando solo datos que existen sin visitarlo.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Que cambio respecto a la version anterior
# MAGIC
# MAGIC Esta version corrige cuatro defectos que hacian que el notebook anterior no fuera valido.
# MAGIC Se documentan aqui porque cada uno cambio una decision de diseno.
# MAGIC
# MAGIC ### 1. Habia fuga de datos masiva en las features
# MAGIC
# MAGIC La lista anterior incluia tres variables derivadas del propio objetivo:
# MAGIC
# MAGIC | Variable | Por que era fuga |
# MAGIC |---|---|
# MAGIC | `desviacion_zona` | Se calcula como `consumo - media_de_zona`. **Es literalmente el objetivo** desplazado por una constante. Con esta variable dentro, el R2 es ~1.0 y el modelo no predice nada: lee la respuesta. |
# MAGIC | `emision_co2_kg_m2_anio` | Las emisiones son el consumo multiplicado por el factor de emision del combustible. No es una medicion independiente. Ademas, un inmueble sin certificar tampoco tiene emisiones medidas: la columna no existe cuando hace falta la prediccion. |
# MAGIC | `clasificacion_emisiones` | Es el objetivo discretizado, por la via de las emisiones. |
# MAGIC
# MAGIC El criterio para admitir una variable es uno solo:
# MAGIC **?existiria este dato antes de que un tecnico visite el inmueble?**
# MAGIC
# MAGIC ### 2. La particion por inmueble no particionaba por inmueble
# MAGIC
# MAGIC El codigo anterior construia `inmueble_id` y despues asignaba el split con `rand(seed=42)`,
# MAGIC que genera **un numero aleatorio por fila, no por inmueble**. Dos certificados del mismo
# MAGIC inmueble podian caer uno en train y otro en test. La comprobacion
# MAGIC `inmuebles_train + inmuebles_test == inmuebles_total` daba False siempre que un inmueble
# MAGIC tuviera dos certificados, pero el resultado se imprimia sin detener nada.
# MAGIC Aqui el split se deriva **de forma determinista del hash del inmueble**.
# MAGIC
# MAGIC ### 3. El umbral de "ineficiente" estaba mal elegido
# MAGIC
# MAGIC La version anterior usaba el **percentil 70 del consumo de cada zona, calculado sobre el
# MAGIC conjunto de test**. Dos problemas: el umbral se calculaba con los mismos datos que se
# MAGIC evaluaban, y el percentil 70 no tiene ninguna relacion con la escala A-G.
# MAGIC
# MAGIC Aqui el umbral es el **percentil 95 del consumo de los edificios calificados A-D de cada zona**,
# MAGIC calculado sobre Silver completa. Define el techo del grupo eficiente en lugar del suelo del
# MAGIC ineficiente, que es lo que hacia degenerar la matriz de confusion.
# MAGIC
# MAGIC ### 4. MLflow fallaba en computo serverless
# MAGIC
# MAGIC `mlflow.spark.log_model` necesita un directorio temporal distribuido. En serverless hay que
# MAGIC pasarle `dfs_tmpdir` apuntando a un Volume de Unity Catalog. Se crea en la celda 0.

# COMMAND ----------

# DBTITLE 1,0. Parametros, Volume temporal y experimento MLflow
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOGO = "cee_aragonv2"

T_FEATURES     = f"{CATALOGO}.gold.features"
T_SILVER       = f"{CATALOGO}.silver.certificados"
T_PREDICCIONES = f"{CATALOGO}.gold.predicciones"

SEMILLA = 42
PCT_TEST = 20          # porcentaje al conjunto de prueba

# --- Volume temporal que necesita mlflow.spark en serverless ---
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.landing.mlflow_tmp")
DFS_TMPDIR = f"/Volumes/{CATALOGO}/landing/mlflow_tmp"
print(f"Directorio temporal MLflow: {DFS_TMPDIR}")

# --- Experimento bajo el usuario actual (no hardcodear el correo) ---
import mlflow
import mlflow.spark

usuario = (spark.sql("SELECT current_user() AS u").collect()[0]["u"])
EXPERIMENTO = f"/Users/{usuario}/AragonV2_ML_Consumo_Energetico"
mlflow.set_experiment(EXPERIMENTO)

print(f"Usuario     : {usuario}")
print(f"Experimento : {EXPERIMENTO}")

# COMMAND ----------

# DBTITLE 1,1. Carga y variables derivadas
df = spark.table(T_FEATURES)

print(f"Filas en {T_FEATURES}: {df.count():,}")
print("Columnas disponibles:")
for c in df.columns:
    print(f"   {c}")

# COMMAND ----------

# DBTITLE 1,1.1 Derivadas admisibles
# Solo se derivan variables a partir de columnas que existen SIN visitar el inmueble.
# Nada que dependa del consumo, ni directa ni indirectamente.

ANIO_REFERENCIA = 2024

df = (df
    .withColumn("antiguedad", F.lit(ANIO_REFERENCIA) - F.col("anio_construccion"))
    .withColumn(
        "log_superficie",
        F.when(F.col("superficie_m2") > 0, F.log(F.col("superficie_m2")))
    )
)

print("Derivadas creadas: antiguedad, log_superficie")
print("\nComprobacion anti-fuga de las derivadas:")
print("  antiguedad      <- anio_construccion (catastro)          OK")
print("  log_superficie  <- superficie_m2     (catastro)          OK")
print("  NO se deriva nada a partir de consumo_kwh_m2_anio")

df.select(
    "consumo_kwh_m2_anio", "antiguedad", "superficie_m2", "log_superficie",
    "zona_climatica", "tipo_edificio"
).show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,2. Seleccion de features (lista blanca explicita)
# MAGIC %md
# MAGIC La seleccion se hace con **lista blanca**, no descartando columnas. Si el origen anadiera un
# MAGIC campo nuevo, con descarte entraria solo al modelo; con lista blanca no entra nada que no se
# MAGIC haya decidido.
# MAGIC
# MAGIC | Columna | Decision | Motivo |
# MAGIC |---|---|---|
# MAGIC | `antiguedad`, `superficie_m2`, `log_superficie` | Admitidas | Catastro |
# MAGIC | `tipo_edificio` | Admitida | Catastro |
# MAGIC | `zona_climatica`, `provincia` | Admitidas | Se derivan de la direccion |
# MAGIC | `municipio` | **Excluida** | 730 valores distintos. Con `maxBins=32` por defecto el arbol no puede tratarla, y subir `maxBins` a 730 dispara el coste sin ganancia: la zona climatica ya captura el efecto geografico relevante |
# MAGIC | `estado_edificio` | **Excluida** | Un solo valor en el 99% de las filas: no aporta señal |
# MAGIC | `emision_co2_kg_m2_anio` | **Excluida** | Objetivo multiplicado por un factor |
# MAGIC | `clasificacion_consumo`, `clasificacion_emisiones` | **Excluidas** | Objetivo discretizado |
# MAGIC | `fecha_emision`, `fecha_expiracion` | **Excluidas** | Solo existen si ya hay certificado |
# MAGIC | `numero_certificado`, `referencia_catastral` | **Excluidas** | Identificadores, no señal |
# MAGIC | `coord_x`, `coord_y` | **Excluidas** | Al separar el campo UTM del origen, la componente Y quedo integramente vacia |

# COMMAND ----------

FEATURES_NUM = [
    "antiguedad",
    "superficie_m2",
    "log_superficie",
]

FEATURES_CAT = [
    "zona_climatica",
    "tipo_edificio",
    "provincia",
]

OBJETIVO = "consumo_kwh_m2_anio"

# Columnas que NO pueden aparecer nunca entre las features.
PROHIBIDAS = {
    "emision_co2_kg_m2_anio", "emision_co_raw",
    "clasificacion_consumo", "clasificacion_emisiones",
    "fecha_emision", "fecha_expiracion",
    "desviacion_zona", "media_zona",
    "numero_certificado", "referencia_catastral",
}

# Guardia automatica: si alguien vuelve a colar una variable derivada del objetivo,
# el notebook se detiene aqui en lugar de producir un R2 sospechosamente alto.
coladas = PROHIBIDAS.intersection(set(FEATURES_NUM + FEATURES_CAT))
assert not coladas, (
    f"FUGA DE DATOS: estas columnas estan derivadas del objetivo y no pueden ser features: {coladas}"
)

print(f"Features numericas  ({len(FEATURES_NUM)}): {FEATURES_NUM}")
print(f"Features categoricas ({len(FEATURES_CAT)}): {FEATURES_CAT}")
print(f"Objetivo: {OBJETIVO}")
print("\nGuardia anti-fuga: OK, ninguna variable prohibida entre las features")

# COMMAND ----------

# DBTITLE 1,2.1 Nulos: imputar, no descartar
# MAGIC %md
# MAGIC La version anterior filtraba las filas con nulos en `antiguedad` o `log_superficie`. Eso
# MAGIC descarta observaciones cuyo objetivo si conocemos, y sesga el conjunto hacia los inmuebles
# MAGIC mejor documentados en catastro.
# MAGIC
# MAGIC Aqui se imputa con la **mediana**, calculada **solo sobre el conjunto de entrenamiento** para
# MAGIC que el conjunto de prueba no filtre informacion hacia el modelo. Las categoricas usan
# MAGIC `handleInvalid="keep"`, que reserva un indice para "desconocido" en lugar de eliminar la fila.

# COMMAND ----------

nulos = df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in FEATURES_NUM + FEATURES_CAT + [OBJETIVO]
]).collect()[0].asDict()

print("Nulos por columna:")
for k, v in nulos.items():
    print(f"   {k:<24} {v:>8,}")

# El objetivo SI tiene que estar presente: sin el no hay nada que aprender ni que evaluar.
df = df.filter(F.col(OBJETIVO).isNotNull())
print(f"\nFilas con objetivo conocido: {df.count():,}")

# COMMAND ----------

# DBTITLE 1,3. Particion por inmueble (determinista)
# MAGIC %md
# MAGIC Un mismo inmueble puede tener varios certificados (recertificacion). Si uno cae en train y
# MAGIC otro en test, el modelo ve en entrenamiento practicamente la misma fila que luego evalua, y
# MAGIC las metricas salen infladas.
# MAGIC
# MAGIC El split se deriva del **hash del identificador de inmueble**, no de un aleatorio por fila.
# MAGIC Es determinista (misma particion en cada ejecucion, sin depender del orden de las filas) y
# MAGIC garantiza que todos los certificados de un inmueble van al mismo lado.

# COMMAND ----------

df = df.withColumn(
    "inmueble_id",
    F.sha2(F.concat_ws("||",
        F.coalesce(F.upper(F.trim(F.col("direccion"))), F.lit("")),
        F.coalesce(F.upper(F.trim(F.col("municipio"))),  F.lit("")),
    ), 256)
)

# Cubo 0-99 derivado del hash: mismo inmueble -> mismo cubo, siempre.
df = df.withColumn(
    "cubo",
    F.abs(F.hash(F.concat_ws("|", F.col("inmueble_id"), F.lit(str(SEMILLA))))) % 100
)

train_df = df.filter(F.col("cubo") >= PCT_TEST)
test_df  = df.filter(F.col("cubo") <  PCT_TEST)

n_train, n_test = train_df.count(), test_df.count()

print(f"Train : {n_train:,} certificados")
print(f"Test  : {n_test:,} certificados")
print(f"Total : {n_train + n_test:,}")

# Comprobacion REAL de que ningun inmueble esta en los dos lados.
inmuebles_compartidos = (train_df.select("inmueble_id").distinct()
    .intersect(test_df.select("inmueble_id").distinct())
    .count())

assert inmuebles_compartidos == 0, (
    f"FUGA POR PARTICION: {inmuebles_compartidos:,} inmuebles aparecen en train y en test."
)
print(f"\nInmuebles compartidos entre train y test: {inmuebles_compartidos}  OK")

# COMMAND ----------

# DBTITLE 1,4. Pipeline de preparacion
from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder, VectorAssembler

imputer = Imputer(
    inputCols=FEATURES_NUM,
    outputCols=[f"{c}_imp" for c in FEATURES_NUM],
    strategy="median",
)

indexers = [
    StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
    for c in FEATURES_CAT
]

encoders = [
    OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_vec", handleInvalid="keep")
    for c in FEATURES_CAT
]

assembler = VectorAssembler(
    inputCols=[f"{c}_imp" for c in FEATURES_NUM] + [f"{c}_vec" for c in FEATURES_CAT],
    outputCol="features",
    handleInvalid="keep",
)

prep = Pipeline(stages=[imputer] + indexers + encoders + [assembler])

# Se ajusta SOLO en train: la mediana del imputer y los indices de las categoricas
# no pueden aprenderse del conjunto de prueba.
prep_model = prep.fit(train_df)

# OJO: nada de .cache() ni .persist(). El computo serverless de Databricks los rechaza con
# [NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute.
# El motor gestiona el almacenamiento intermedio por su cuenta.
train_t = prep_model.transform(train_df)
test_t  = prep_model.transform(test_df)

print(f"Train transformado: {train_t.count():,}")
print(f"Test  transformado: {test_t.count():,}")
print(f"Dimension del vector de features: {len(train_t.select('features').first()[0])}")

# COMMAND ----------

# DBTITLE 1,5. Linea base: media por zona climatica
# MAGIC %md
# MAGIC Antes de entrenar nada se calcula la prediccion mas simple posible. Sin esa referencia,
# MAGIC ninguna metrica del modelo significa nada: un MAE de 72 no es bueno ni malo en abstracto.

# COMMAND ----------

media_zona = (train_t
    .groupBy("zona_climatica")
    .agg(F.avg(OBJETIVO).alias("pred_baseline")))

media_global = train_t.select(F.avg(OBJETIVO)).collect()[0][0]

test_base = (test_t
    .join(media_zona, on="zona_climatica", how="left")
    .withColumn("pred_baseline", F.coalesce(F.col("pred_baseline"), F.lit(media_global))))

base = test_base.select(
    F.avg(F.abs(F.col(OBJETIVO) - F.col("pred_baseline"))).alias("mae"),
    F.sqrt(F.avg(F.pow(F.col(OBJETIVO) - F.col("pred_baseline"), 2))).alias("rmse"),
).collect()[0]

mae_baseline  = base["mae"]
rmse_baseline = base["rmse"]

print("=" * 70)
print("LINEA BASE - media del consumo por zona climatica")
print("=" * 70)
print(f"  MAE  : {mae_baseline:.2f} kWh/m2/anio")
print(f"  RMSE : {rmse_baseline:.2f} kWh/m2/anio")
print("=" * 70)
print("\nEsta es la cifra a superar. Un modelo con MAE mayor es peor que predecir la media.")

media_zona.orderBy("zona_climatica").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,6. Entrenamiento de los tres modelos
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

ev_mae  = RegressionEvaluator(labelCol=OBJETIVO, predictionCol="prediction", metricName="mae")
ev_rmse = RegressionEvaluator(labelCol=OBJETIVO, predictionCol="prediction", metricName="rmse")
ev_r2   = RegressionEvaluator(labelCol=OBJETIVO, predictionCol="prediction", metricName="r2")

resultados = {
    "Linea base (media por zona)": {
        "mae": mae_baseline, "rmse": rmse_baseline, "r2": None, "modelo": None, "pred": None
    }
}


def entrenar(nombre, estimador, params):
    """Entrena, evalua y registra en MLflow. Devuelve (modelo, predicciones)."""
    with mlflow.start_run(run_name=nombre):
        mlflow.log_param("model_type", nombre)
        mlflow.log_params(params)
        mlflow.log_param("n_train", n_train)
        mlflow.log_param("n_test", n_test)
        mlflow.log_param("features_num", ",".join(FEATURES_NUM))
        mlflow.log_param("features_cat", ",".join(FEATURES_CAT))

        modelo = estimador.fit(train_t)
        pred   = modelo.transform(test_t)

        mae  = ev_mae.evaluate(pred)
        rmse = ev_rmse.evaluate(pred)
        r2   = ev_r2.evaluate(pred)
        sesgo = pred.select(F.avg(F.col("prediction") - F.col(OBJETIVO))).collect()[0][0]

        mlflow.log_metric("mae", mae)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("sesgo_medio", sesgo)
        # La linea base se registra en CADA corrida para que sean comparables entre si.
        mlflow.log_metric("mae_baseline", mae_baseline)
        mlflow.log_metric("mejora_vs_baseline_pct", (mae_baseline - mae) / mae_baseline * 100)

        # dfs_tmpdir es obligatorio en serverless
        mlflow.spark.log_model(modelo, "model", dfs_tmpdir=DFS_TMPDIR)

        print(f"\n{'=' * 70}")
        print(f"{nombre}")
        print(f"{'=' * 70}")
        print(f"  MAE   : {mae:8.2f}   (linea base {mae_baseline:.2f})")
        print(f"  RMSE  : {rmse:8.2f}")
        print(f"  R2    : {r2:8.4f}")
        print(f"  Sesgo : {sesgo:8.2f}   (cerca de 0 = no sobre ni infraestima de forma sistematica)")
        print(f"  Mejora: {(mae_baseline - mae) / mae_baseline * 100:7.1f}% sobre la linea base")

        if r2 > 0.95:
            print("\n  *** AVISO: R2 > 0.95. En este problema eso es senal de FUGA, no de exito. ***")
            print("  Revisar que ninguna feature derive del consumo.")

        resultados[nombre] = {"mae": mae, "rmse": rmse, "r2": r2, "modelo": modelo, "pred": pred}
        return modelo, pred

# COMMAND ----------

# DBTITLE 1,6.1 Regresion lineal
lr_params = {"maxIter": 100, "regParam": 0.1, "elasticNetParam": 0.0}

lr_model, lr_pred = entrenar(
    "Regresion lineal",
    LinearRegression(featuresCol="features", labelCol=OBJETIVO, **lr_params),
    lr_params,
)

# COMMAND ----------

# DBTITLE 1,6.2 Random forest
rf_params = {"numTrees": 100, "maxDepth": 10, "maxBins": 64, "seed": SEMILLA}

rf_model, rf_pred = entrenar(
    "Random forest",
    RandomForestRegressor(featuresCol="features", labelCol=OBJETIVO, **rf_params),
    rf_params,
)

# COMMAND ----------

# DBTITLE 1,6.3 Gradient boosting
gbt_params = {"maxIter": 100, "maxDepth": 6, "maxBins": 64, "seed": SEMILLA}

gbt_model, gbt_pred = entrenar(
    "Gradient boosting",
    GBTRegressor(featuresCol="features", labelCol=OBJETIVO, **gbt_params),
    gbt_params,
)

# COMMAND ----------

# DBTITLE 1,7. Comparacion contra la linea base
import pandas as pd

filas = []
for nombre, r in resultados.items():
    mejora = "" if r["mae"] == mae_baseline else f"{(mae_baseline - r['mae']) / mae_baseline * 100:+.1f}%"
    filas.append({
        "Modelo": nombre,
        "MAE": round(r["mae"], 2),
        "RMSE": round(r["rmse"], 2) if r["rmse"] else None,
        "R2": round(r["r2"], 4) if r["r2"] is not None else None,
        "Mejora vs linea base": mejora,
    })

tabla = pd.DataFrame(filas)
print(tabla.to_string(index=False))

mejor_nombre = min(
    (k for k in resultados if resultados[k]["modelo"] is not None),
    key=lambda k: resultados[k]["mae"],
)
mejor = resultados[mejor_nombre]
best_pred = mejor["pred"]

print(f"\nMejor modelo: {mejor_nombre}  (MAE {mejor['mae']:.2f})")

# COMMAND ----------

# DBTITLE 1,7.1 Grafico: modelos frente a la linea base
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

nombres = list(resultados.keys())
maes    = [resultados[n]["mae"] for n in nombres]
colores = ["#8D9A9F"] + ["#4E7E99"] * (len(nombres) - 1)
colores[nombres.index(mejor_nombre)] = "#1F5673"

fig, ax = plt.subplots(figsize=(9, 4.2))
barras = ax.barh(nombres, maes, color=colores)
ax.axvline(mae_baseline, color="#9C3226", linestyle="--", linewidth=1.2)
ax.text(mae_baseline, -0.65, f" linea base {mae_baseline:.1f}", color="#9C3226", fontsize=9)
for b, v in zip(barras, maes):
    ax.text(v + 1, b.get_y() + b.get_height() / 2, f"{v:.2f}", va="center", fontsize=9)
ax.set_xlabel("Error absoluto medio (kWh/m2/anio) — menos es mejor")
ax.set_title("Modelos frente a la linea base", fontsize=12, fontweight="bold")
ax.invert_yaxis()
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,8. Clasificacion derivada: umbral por zona climatica
# MAGIC %md
# MAGIC La rubrica pide matriz de confusion, que es de clasificacion, mientras que el objetivo es
# MAGIC continuo. Se resuelve sin cambiar de objetivo: el consumo predicho se convierte en una
# MAGIC decision binaria — *?es ineficiente?* — usando un umbral.
# MAGIC
# MAGIC **Por que el umbral no puede ser un numero unico.** La escala A-G no es absoluta: se calcula
# MAGIC contra un edificio de referencia de la misma zona climatica, de modo que la misma letra
# MAGIC corresponde a consumos distintos segun donde este el edificio.
# MAGIC
# MAGIC **Por que percentil 95 de A-D y no el minimo de la letra E.** La letra E representa mas de la
# MAGIC mitad del parque y su rango se solapa con las demas: basta un inmueble con letra E y consumo
# MAGIC bajo para hundir el umbral. Con ese criterio el 99,87% de los inmuebles quedaba clasificado
# MAGIC como ineficiente, con recall del 100% y **ningun verdadero negativo**: una matriz sin dos de
# MAGIC sus cuatro casillas.
# MAGIC
# MAGIC El percentil 95 de los edificios A-D define el **techo del grupo eficiente** en lugar del
# MAGIC suelo del ineficiente, y produce una matriz con las cuatro casillas pobladas.
# MAGIC
# MAGIC El umbral se calcula sobre **Silver completa**, no sobre el conjunto de prueba: es una
# MAGIC definicion de negocio, no un parametro ajustado a los datos que se evaluan.

# COMMAND ----------

# Zona climatica de cada municipio (la misma dimension que usa Gold)
zonas = (spark.table(T_FEATURES)
    .select("municipio", "zona_climatica")
    .distinct())

umbrales = (spark.table(T_SILVER)
    .join(zonas, on="municipio", how="left")
    .filter(F.col("clasificacion_consumo").isin("A", "B", "C", "D"))
    .filter(F.col("zona_climatica").isNotNull())
    .groupBy("zona_climatica")
    .agg(
        F.expr("percentile_approx(consumo_kwh_m2_anio, 0.95)").alias("umbral_ineficiente"),
        F.count(F.lit(1)).alias("n_edificios_AD"),
    ))

print("Umbral de ineficiencia por zona climatica")
print("(percentil 95 del consumo de los edificios A-D de esa zona)\n")
umbrales.orderBy("zona_climatica").show(truncate=False)

u = [r["umbral_ineficiente"] for r in umbrales.collect()]
if len(u) > 1:
    print(f"Separacion entre el umbral mas alto y el mas bajo: {max(u) - min(u):.1f} kWh/m2/anio")
    print("Esa diferencia es la razon de que la zona climatica entre al modelo como predictora.")

# COMMAND ----------

# DBTITLE 1,8.1 Aplicar el umbral a real y predicho
binario = (best_pred
    .join(umbrales.select("zona_climatica", "umbral_ineficiente"),
          on="zona_climatica", how="left")
    .filter(F.col("umbral_ineficiente").isNotNull())
    .withColumn("real_ineficiente",
                (F.col(OBJETIVO) > F.col("umbral_ineficiente")).cast("int"))
    .withColumn("pred_ineficiente",
                (F.col("prediction") > F.col("umbral_ineficiente")).cast("int")))

n_eval = binario.count()
print(f"Inmuebles evaluados: {n_eval:,}")

# COMMAND ----------

# DBTITLE 1,8.2 Matriz de confusion y metricas
m = binario.agg(
    F.sum(((F.col("real_ineficiente") == 1) & (F.col("pred_ineficiente") == 1)).cast("int")).alias("VP"),
    F.sum(((F.col("real_ineficiente") == 0) & (F.col("pred_ineficiente") == 1)).cast("int")).alias("FP"),
    F.sum(((F.col("real_ineficiente") == 1) & (F.col("pred_ineficiente") == 0)).cast("int")).alias("FN"),
    F.sum(((F.col("real_ineficiente") == 0) & (F.col("pred_ineficiente") == 0)).cast("int")).alias("VN"),
).collect()[0]

VP, FP, FN, VN = m["VP"], m["FP"], m["FN"], m["VN"]

precision = VP / (VP + FP) if (VP + FP) else 0.0
recall    = VP / (VP + FN) if (VP + FN) else 0.0
f1        = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
exactitud = (VP + VN) / n_eval

tasa_base = (VP + FN) / n_eval            # proporcion real de ineficientes
trivial   = max(tasa_base, 1 - tasa_base) # acertar diciendo siempre la clase mayoritaria
lift      = precision / tasa_base if tasa_base else 0.0

print("=" * 70)
print("MATRIZ DE CONFUSION")
print("=" * 70)
print(f"{'':<22}{'Pred. eficiente':>18}{'Pred. ineficiente':>20}")
print(f"{'Real eficiente':<22}{VN:>18,}{FP:>20,}")
print(f"{'Real ineficiente':<22}{FN:>18,}{VP:>20,}")
print("=" * 70)
print(f"  Precision       : {precision:6.1%}   de cada 100 marcados, {precision*100:.0f} lo son de verdad")
print(f"  Recall          : {recall:6.1%}   encuentra esa fraccion de los que existen")
print(f"  F1              : {f1:6.1%}")
print(f"  Exactitud       : {exactitud:6.1%}")
print(f"  Modelo trivial  : {trivial:6.1%}   decir siempre la clase mayoritaria")
print(f"  Tasa base real  : {tasa_base:6.1%}")
print(f"  LIFT            : {lift:6.2f}x  mejora sobre inspeccionar al azar")
print("=" * 70)
print("\nLa exactitud y el modelo trivial se presentan juntos a proposito: reportar")
print("la primera sin la segunda daria una impresion falsa del desempeno.")

# COMMAND ----------

# DBTITLE 1,8.3 Registrar las metricas de clasificacion en MLflow
with mlflow.start_run(run_name=f"{mejor_nombre} - clasificacion derivada"):
    mlflow.log_param("modelo_base", mejor_nombre)
    mlflow.log_param("criterio_umbral", "percentil 95 del consumo de edificios A-D por zona")
    for k, v in {
        "VP": VP, "FP": FP, "FN": FN, "VN": VN,
        "precision": precision, "recall": recall, "f1": f1,
        "exactitud": exactitud, "exactitud_trivial": trivial,
        "tasa_base": tasa_base, "lift": lift,
    }.items():
        mlflow.log_metric(k, v)

print("Metricas de clasificacion registradas en MLflow")

# COMMAND ----------

# DBTITLE 1,8.4 Grafico: azar frente a modelo
fig, ax = plt.subplots(figsize=(7, 4))
barras = ax.bar(
    ["Inspeccion al azar", "Guiada por el modelo"],
    [tasa_base * 100, precision * 100],
    color=["#8D9A9F", "#1F5673"], width=0.55,
)
for b, v in zip(barras, [tasa_base * 100, precision * 100]):
    ax.text(b.get_x() + b.get_width() / 2, v + 1, f"{v:.1f}%", ha="center", fontweight="bold")
ax.set_ylabel("% de inspecciones que encuentran un inmueble ineficiente")
ax.set_title(f"Eficacia de la inspeccion — lift {lift:.2f}x", fontsize=12, fontweight="bold")
ax.set_ylim(0, max(precision * 100, tasa_base * 100) * 1.25)
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,9. Rendimiento por zona climatica
# MAGIC %md
# MAGIC Una metrica global esconde que el error no se reparte por igual. El modelo rinde mejor donde
# MAGIC tiene mas datos, y esa asimetria desaconseja extrapolar el resultado global a las zonas con
# MAGIC menos observaciones.

# COMMAND ----------

por_zona = (binario
    .groupBy("zona_climatica")
    .agg(
        F.count(F.lit(1)).alias("evaluados"),
        F.avg(F.abs(F.col(OBJETIVO) - F.col("prediction"))).alias("mae"),
        F.sum(((F.col("real_ineficiente") == 1) & (F.col("pred_ineficiente") == 1)).cast("int")).alias("VP"),
        F.sum(((F.col("real_ineficiente") == 0) & (F.col("pred_ineficiente") == 1)).cast("int")).alias("FP"),
        F.sum(((F.col("real_ineficiente") == 1) & (F.col("pred_ineficiente") == 0)).cast("int")).alias("FN"),
    )
    .withColumn("precision",
        F.when(F.col("VP") + F.col("FP") > 0, F.col("VP") / (F.col("VP") + F.col("FP"))))
    .withColumn("recall",
        F.when(F.col("VP") + F.col("FN") > 0, F.col("VP") / (F.col("VP") + F.col("FN"))))
    .withColumn("tasa_base", (F.col("VP") + F.col("FN")) / F.col("evaluados"))
    .withColumn("lift",
        F.when(F.col("tasa_base") > 0, F.col("precision") / F.col("tasa_base")))
    .select("zona_climatica", "evaluados",
            F.round("mae", 2).alias("mae"),
            F.round(F.col("precision") * 100, 1).alias("precision_pct"),
            F.round(F.col("recall") * 100, 1).alias("recall_pct"),
            F.round("lift", 2).alias("lift"))
    .orderBy(F.desc("evaluados")))

print("Rendimiento por zona climatica")
por_zona.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,10. Escribir gold.predicciones
# MAGIC %md
# MAGIC Esta tabla es de **evaluacion, no de produccion**: contiene los inmuebles del conjunto de
# MAGIC prueba, que si tienen certificado. Un despliegue real exigiria puntuar el parque **no**
# MAGIC certificado, y para eso habria que cargar ese universo desde el catastro.

# COMMAND ----------

salida = binario.select(
    "numero_certificado",
    "municipio",
    "provincia",
    "zona_climatica",
    "tipo_edificio",
    "anio_construccion",
    "superficie_m2",
    F.col(OBJETIVO).alias("consumo_real"),
    F.col("prediction").alias("consumo_predicho"),
    (F.col("prediction") - F.col(OBJETIVO)).alias("error"),
    F.abs(F.col("prediction") - F.col(OBJETIVO)).alias("error_absoluto"),
    "umbral_ineficiente",
    "real_ineficiente",
    "pred_ineficiente",
    F.lit(mejor_nombre).alias("modelo"),
    F.current_timestamp().alias("_generado_en"),
)

(salida.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T_PREDICCIONES))

print(f"Escrita {T_PREDICCIONES}: {salida.count():,} filas")
display(spark.table(T_PREDICCIONES).limit(20))

# COMMAND ----------

# DBTITLE 1,11. Resumen final
print("=" * 78)
print("RESUMEN")
print("=" * 78)
print(f"  Entrenamiento      : {n_train:,} inmuebles")
print(f"  Prueba             : {n_test:,} inmuebles")
print(f"  Linea base (MAE)   : {mae_baseline:.2f} kWh/m2/anio")
print(f"  Mejor modelo       : {mejor_nombre}")
print(f"    MAE              : {mejor['mae']:.2f}  ({(mae_baseline - mejor['mae']) / mae_baseline * 100:.1f}% mejor que la linea base)")
print(f"    RMSE             : {mejor['rmse']:.2f}")
print(f"    R2               : {mejor['r2']:.4f}")
print(f"  Clasificacion derivada")
print(f"    Precision        : {precision:.1%}")
print(f"    Recall           : {recall:.1%}")
print(f"    Exactitud        : {exactitud:.1%}  (trivial: {trivial:.1%})")
print(f"    Lift             : {lift:.2f}x")
print("=" * 78)
print(f"\n  Experimento MLflow : {EXPERIMENTO}")
print(f"  Tabla de salida    : {T_PREDICCIONES}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Como leer un R2 de ~0,29
# MAGIC
# MAGIC Con las columnas excluidas dentro, el ajuste seria casi perfecto y el modelo no serviria para
# MAGIC nada: estaria leyendo la respuesta. **En este problema un R2 alto es senal de fuga, no de
# MAGIC exito.**
# MAGIC
# MAGIC Lo que se explica corresponde a lo que el catastro sabe de un edificio: cuando se construyo,
# MAGIC cuanto mide, de que tipo es y donde esta. El resto depende de la instalacion termica, los
# MAGIC cerramientos y el uso, informacion que por definicion solo aparece cuando alguien va a verlo.
# MAGIC
# MAGIC Y por eso la metrica que importa no es la exactitud sino el **lift**: el modelo no es un buen
# MAGIC clasificador, es un buen **priorizador**.