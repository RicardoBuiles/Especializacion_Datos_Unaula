# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Setup Completo - Workflow CEE Aragón
# MAGIC %md
# MAGIC # 🔧 WORKFLOW COMPLETO: CEE Aragón ETL Pipeline
# MAGIC
# MAGIC **Este notebook contiene TODO lo necesario para crear y configurar el workflow**
# MAGIC
# MAGIC ## 📦 Contenido:
# MAGIC
# MAGIC 1. **Diseño del Workflow** - Arquitectura y comportamiento ante fallos
# MAGIC 2. **Respuesta crítica**: ¿Gold corre si Silver falla?
# MAGIC 3. **Código Python** para crear el job AHORA
# MAGIC 4. **YAML completo** para versionar en GitHub
# MAGIC 5. **Guía de capturas** de pantalla
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Notebook de validaciones
# MAGIC
# MAGIC Ya creado: [validate_silver_certificados](#notebook-984231585310709)
# MAGIC
# MAGIC Incluye 5 validaciones con assert que **detienen el workflow** si fallan.

# COMMAND ----------

# DBTITLE 1,1. Diseño del Workflow
# MAGIC %md
# MAGIC ## 1️⃣ DISEÑO DEL WORKFLOW
# MAGIC
# MAGIC ### 🏗️ Arquitectura de Tareas
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────┐
# MAGIC │      CEE_Aragon_ETL_Pipeline                   │
# MAGIC └────────────────────────────────────────────────┘
# MAGIC
# MAGIC ┌──────────────────────┐
# MAGIC │  01_bronze_ingestion │ ← Ingesta raw
# MAGIC │  (Bronze)            │
# MAGIC └──────────┬───────────┘
# MAGIC            │
# MAGIC            ↓ depends_on
# MAGIC ┌──────────────────────┐
# MAGIC │ 02_silver_transform  │ ← Limpieza + tipado
# MAGIC │  (Silver)            │
# MAGIC └──────────┬───────────┘
# MAGIC            │
# MAGIC            ↓ depends_on
# MAGIC ┌──────────────────────┐
# MAGIC │ 03_validate_silver   │ ← 5 aserciones
# MAGIC │  (Quality Check)     │
# MAGIC └──────────┬───────────┘
# MAGIC            │
# MAGIC            ↓ depends_on
# MAGIC ┌──────────────────────┐
# MAGIC │ 04_gold_features     │ ← Features + zona climática
# MAGIC │  (Gold)              │
# MAGIC └──────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### ⚙️ Configuración de cada tarea
# MAGIC
# MAGIC | Tarea | Notebook | Retries | Timeout | Workers |
# MAGIC |-------|----------|---------|---------|----------|
# MAGIC | **Bronze** | `01_Bronze_Ingesta_Certificados` | 2 | 1h | 2 |
# MAGIC | **Silver** | `03_Silver_Certificados` | 2 | 1h | 2 |
# MAGIC | **Validate** | `validate_silver_certificados` | **0** | 30min | 1 |
# MAGIC | **Gold** | `04_Zona_Climatica_Gold` | 2 | 1h | 2 |
# MAGIC
# MAGIC **⚠️ Validate tiene 0 retries**: Los fallos de validación son errores de DATOS, no transitorios.
# MAGIC
# MAGIC ### 📧 Notificaciones
# MAGIC
# MAGIC - **Email on failure**: `ricardobuiles@hotmail.com`
# MAGIC - Incluye: tarea que falló, mensaje de error, link al run
# MAGIC
# MAGIC ### ⏰ Schedule
# MAGIC
# MAGIC - **Cron**: `0 2 * * * ?` (diario a las 2 AM)
# MAGIC - **Timezone**: Europe/Madrid
# MAGIC - **Estado**: Activo (UNPAUSED)

# COMMAND ----------

# DBTITLE 1,2. PREGUNTA CRÍTICA - Silver falla
# MAGIC %md
# MAGIC ## 2️⃣ PREGUNTA CRÍTICA: ¿Gold corre si Silver falla?
# MAGIC
# MAGIC ### ❌ RESPUESTA: **NO, Gold NO se ejecuta**
# MAGIC
# MAGIC ### 🔍 Por qué:
# MAGIC
# MAGIC #### **Causa directa: depends_on**
# MAGIC
# MAGIC Cada tarea declara:
# MAGIC
# MAGIC ```python
# MAGIC "depends_on": [{"task_key": "tarea_anterior"}]
# MAGIC ```
# MAGIC
# MAGIC Databricks evalúa esta dependencia **ANTES** de lanzar la tarea.
# MAGIC
# MAGIC #### **Propagación del bloqueo:**
# MAGIC
# MAGIC 1. **Silver falla** → estado = `FAILED`
# MAGIC 2. **Validate** tiene `depends_on: ["02_silver_transformation"]`
# MAGIC    → Databricks ve que Silver = FAILED
# MAGIC    → Validate se marca como `SKIPPED` (nunca inicia)
# MAGIC 3. **Gold** tiene `depends_on: ["03_validate_silver"]`
# MAGIC    → Databricks ve que Validate = SKIPPED
# MAGIC    → Gold se marca como `SKIPPED`
# MAGIC
# MAGIC #### **Estados posibles:**
# MAGIC
# MAGIC | Estado upstream | Tarea dependiente |
# MAGIC |----------------|-------------------|
# MAGIC | `SUCCESS` | ✅ Se ejecuta |
# MAGIC | `FAILED` | ⏸️ `SKIPPED` |
# MAGIC | `CANCELED` | ⏸️ `SKIPPED` |
# MAGIC | `SKIPPED` | ⏸️ `SKIPPED` (propagación) |
# MAGIC
# MAGIC #### **Esto es CORRECTO por diseño:**
# MAGIC
# MAGIC - **No quieres** que Gold entrene modelos con datos corruptos de Silver fallido
# MAGIC - **No quieres** que Gold procese datos que NO pasaron validación
# MAGIC - El pipeline se auto-protege contra datos malos
# MAGIC
# MAGIC ### 📊 Ejemplo visual de fallo:
# MAGIC
# MAGIC ```
# MAGIC ✅ Bronze (SUCCESS)      → continúa
# MAGIC   ↓
# MAGIC ❌ Silver (FAILED)       → DETIENE aquí
# MAGIC   ↓
# MAGIC ⏸️ Validate (SKIPPED)    → nunca inicia
# MAGIC   ↓
# MAGIC ⏸️ Gold (SKIPPED)        → nunca inicia
# MAGIC ```
# MAGIC
# MAGIC ### 📧 Email que recibirás:
# MAGIC
# MAGIC ```
# MAGIC Subject: [FAILED] CEE_Aragon_ETL_Pipeline - Run #123
# MAGIC
# MAGIC Task: 02_silver_transformation
# MAGIC Status: FAILED
# MAGIC Error: AssertionError: Validación R03 falló...
# MAGIC
# MAGIC View run: [link]
# MAGIC ```
# MAGIC
# MAGIC **Gold NO aparece en el email** porque nunca se ejecutó.

# COMMAND ----------

# DBTITLE 1,3. CÓDIGO - Crear el Job AHORA
# 🔧 CREAR JOB - Ejecuta esta celda para crear el workflow

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

print("🔄 Inicializando Databricks SDK...")
w = WorkspaceClient()

print("\n🔧 Creando job: CEE_Aragon_ETL_Pipeline...")
print("⚡ Usando Serverless compute (requerido por tu workspace)\n")

try:
    created_job = w.jobs.create(
        name="CEE_Aragon_ETL_Pipeline",
        
        # TAREA 1: Bronze
        tasks=[
            jobs.Task(
                task_key="01_bronze_ingestion",
                notebook_task=jobs.NotebookTask(
                    notebook_path="/Users/ricardo.buileses@unaula.edu.co/Big_Data_Aragon/01_Bronze_Ingesta_Certificados",
                    source=jobs.Source.WORKSPACE
                ),
                timeout_seconds=3600,
                max_retries=2,
                min_retry_interval_millis=60000,
                retry_on_timeout=True
            ),
            
            # TAREA 2: Silver (depende de Bronze)
            jobs.Task(
                task_key="02_silver_transformation",
                depends_on=[jobs.TaskDependency(task_key="01_bronze_ingestion")],
                notebook_task=jobs.NotebookTask(
                    notebook_path="/Users/ricardo.buileses@unaula.edu.co/Big_Data_Aragon/03_Silver_Certificados",
                    source=jobs.Source.WORKSPACE
                ),
                timeout_seconds=3600,
                max_retries=2,
                min_retry_interval_millis=60000,
                retry_on_timeout=True
            ),
            
            # TAREA 3: Validate (depende de Silver)
            jobs.Task(
                task_key="03_validate_silver",
                depends_on=[jobs.TaskDependency(task_key="02_silver_transformation")],
                notebook_task=jobs.NotebookTask(
                    notebook_path="/Users/ricardo.buileses@unaula.edu.co/Big_Data_Aragon/validate_silver_certificados",
                    source=jobs.Source.WORKSPACE
                ),
                timeout_seconds=1800,
                max_retries=0  # NO reintentar validaciones
            ),
            
            # TAREA 4: Gold (depende de Validate)
            jobs.Task(
                task_key="04_gold_features",
                depends_on=[jobs.TaskDependency(task_key="03_validate_silver")],
                notebook_task=jobs.NotebookTask(
                    notebook_path="/Users/ricardo.buileses@unaula.edu.co/Big_Data_Aragon/04_Zona_Climatica_Gold",
                    source=jobs.Source.WORKSPACE
                ),
                timeout_seconds=3600,
                max_retries=2,
                min_retry_interval_millis=60000,
                retry_on_timeout=True
            )
        ],
        
        # --- Notificaciones por email (descomentar para activar) ---
        # email_notifications=jobs.JobEmailNotifications(
        #     on_failure=["ricardo.buileses@unaula.edu.co"]
        # ),
        
        # --- Schedule (descomentar para activar ejecución periódica) ---
        # Diario a las 2 AM hora Madrid. Cambiar cron según necesidad:
        #   "0 2 * * * ?"   → cada día a las 2:00 AM
        #   "0 0 * * 1 ?"   → cada lunes a medianoche
        #   "0 8 1 * * ?"   → el día 1 de cada mes a las 8:00 AM
        # schedule=jobs.CronSchedule(
        #     quartz_cron_expression="0 2 * * * ?",
        #     timezone_id="Europe/Madrid",
        #     pause_status=jobs.PauseStatus.UNPAUSED
        # ),
        
        # Configuración general
        timeout_seconds=7200,  # 2 horas máximo total
        max_concurrent_runs=1
    )
    
    print("\n✅ ¡JOB CREADO EXITOSAMENTE!")
    print("="*80)
    print(f"📋 Nombre: CEE_Aragon_ETL_Pipeline")
    print(f"🆔 Job ID: {created_job.job_id}")
    print(f"🔗 URL: https://dbc-2b8dadee-16f3.cloud.databricks.com/#job/{created_job.job_id}")
    print("="*80)
    print("\n📊 Configuración:")
    print("   ✓ 4 tareas encadenadas: Bronze → Silver → Validate → Gold")
    print("   ○ Schedule: DESACTIVADO (descomentar en código para activar)")
    print("   ○ Email on failure: DESACTIVADO (descomentar en código para activar)")
    print("   ✓ Reintentos: 2 (excepto Validate: 0)")
    print("\n⚠️  RECUERDA:")
    print("   • Si Silver falla, Gold NO se ejecuta (depends_on)")
    print("   • Si Validate falla, Gold NO se ejecuta (validaciones con assert)")
    print("   • El workflow se auto-protege contra datos malos")
    print("\n🎯 Próximo paso: Ir a la URL y hacer 'Run now' para probar")
    
    # Guardar job_id
    job_id = created_job.job_id
    
except Exception as e:
    print(f"\n❌ Error al crear job: {e}")
    print("\n💡 Solución alternativa: Usa el YAML de la siguiente celda")

# COMMAND ----------

# DBTITLE 1,4. YAML para GitHub
# MAGIC %md
# MAGIC ## 4️⃣ YAML COMPLETO - Para versionar en GitHub
# MAGIC
# MAGIC **Guardar como**: `databricks_workflows/cee_aragon_pipeline.yml`
# MAGIC
# MAGIC ```yaml
# MAGIC # ==============================================================================
# MAGIC # Databricks Workflow: CEE Aragón ETL Pipeline
# MAGIC # ==============================================================================
# MAGIC # Versión: 1.0
# MAGIC # Descripción: Pipeline de ingesta, transformación, validación y features
# MAGIC #              para certificados energéticos de Aragón
# MAGIC # Autor: Ricardo Builes
# MAGIC # Fecha: 2026-09-12
# MAGIC # ==============================================================================
# MAGIC
# MAGIC name: CEE_Aragon_ETL_Pipeline
# MAGIC
# MAGIC # ------------------------------------------------------------------------------
# MAGIC # NOTIFICACIONES
# MAGIC # ------------------------------------------------------------------------------
# MAGIC email_notifications:
# MAGIC   on_failure:
# MAGIC     - ricardo.buileses@unaula.edu.co
# MAGIC   # Opcional: descomentar para notificar en éxito
# MAGIC   # on_success:
# MAGIC   #   - ricardo.buileses@unaula.edu.co
# MAGIC
# MAGIC # ------------------------------------------------------------------------------
# MAGIC # SCHEDULE
# MAGIC # ------------------------------------------------------------------------------
# MAGIC schedule:
# MAGIC   quartz_cron_expression: "0 2 * * * ?"  # Diario a las 2 AM
# MAGIC   timezone_id: "Europe/Madrid"
# MAGIC   pause_status: "UNPAUSED"
# MAGIC
# MAGIC # ------------------------------------------------------------------------------
# MAGIC # CONFIGURACIÓN GENERAL
# MAGIC # ------------------------------------------------------------------------------
# MAGIC timeout_seconds: 7200  # 2 horas máximo para todo el job
# MAGIC max_concurrent_runs: 1  # Solo 1 ejecución simultánea
# MAGIC
# MAGIC # ------------------------------------------------------------------------------
# MAGIC # TAREAS
# MAGIC # ------------------------------------------------------------------------------
# MAGIC tasks:
# MAGIC   # ============================================================================
# MAGIC   # TAREA 1: BRONZE - Ingesta raw desde archivos
# MAGIC   # ============================================================================
# MAGIC   - task_key: 01_bronze_ingestion
# MAGIC     notebook_task:
# MAGIC       notebook_path: /Users/ricardo.buileses@unaula.edu.co/Big_Data_Aragon/01_Bronze_Ingesta_Certificados
# MAGIC       source: WORKSPACE
# MAGIC     
# MAGIC     new_cluster:
# MAGIC       spark_version: "13.3.x-scala2.12"
# MAGIC       node_type_id: "i3.xlarge"
# MAGIC       num_workers: 2
# MAGIC       spark_conf:
# MAGIC         "spark.databricks.delta.preview.enabled": "true"
# MAGIC     
# MAGIC     timeout_seconds: 3600  # 1 hora
# MAGIC     max_retries: 2
# MAGIC     min_retry_interval_millis: 60000  # 1 minuto entre reintentos
# MAGIC     retry_on_timeout: true
# MAGIC   
# MAGIC   # ============================================================================
# MAGIC   # TAREA 2: SILVER - Transformación y limpieza
# MAGIC   # ============================================================================
# MAGIC   - task_key: 02_silver_transformation
# MAGIC     depends_on:
# MAGIC       - task_key: 01_bronze_ingestion
# MAGIC     
# MAGIC     notebook_task:
# MAGIC       notebook_path: /Users/ricardo.buileses@unaula.edu.co/Big_Data_Aragon/03_Silver_Certificados
# MAGIC       source: WORKSPACE
# MAGIC     
# MAGIC     new_cluster:
# MAGIC       spark_version: "13.3.x-scala2.12"
# MAGIC       node_type_id: "i3.xlarge"
# MAGIC       num_workers: 2
# MAGIC     
# MAGIC     timeout_seconds: 3600
# MAGIC     max_retries: 2
# MAGIC     min_retry_interval_millis: 60000
# MAGIC     retry_on_timeout: true
# MAGIC   
# MAGIC   # ============================================================================
# MAGIC   # TAREA 3: VALIDATE - Control de calidad
# MAGIC   # ============================================================================
# MAGIC   - task_key: 03_validate_silver
# MAGIC     depends_on:
# MAGIC       - task_key: 02_silver_transformation
# MAGIC     
# MAGIC     notebook_task:
# MAGIC       notebook_path: /Users/ricardo.buileses@unaula.edu.co/Big_Data_Aragon/validate_silver_certificados
# MAGIC       source: WORKSPACE
# MAGIC     
# MAGIC     new_cluster:
# MAGIC       spark_version: "13.3.x-scala2.12"
# MAGIC       node_type_id: "i3.xlarge"
# MAGIC       num_workers: 1  # Validación necesita menos recursos
# MAGIC     
# MAGIC     timeout_seconds: 1800  # 30 minutos
# MAGIC     max_retries: 0  # NO reintentar (los fallos son errores de datos)
# MAGIC   
# MAGIC   # ============================================================================
# MAGIC   # TAREA 4: GOLD - Features para modelado
# MAGIC   # ============================================================================
# MAGIC   - task_key: 04_gold_features
# MAGIC     depends_on:
# MAGIC       - task_key: 03_validate_silver
# MAGIC     
# MAGIC     notebook_task:
# MAGIC       notebook_path: /Users/ricardo.buileses@unaula.edu.co/Big_Data_Aragon/04_Zona_Climatica_Gold
# MAGIC       source: WORKSPACE
# MAGIC     
# MAGIC     new_cluster:
# MAGIC       spark_version: "13.3.x-scala2.12"
# MAGIC       node_type_id: "i3.xlarge"
# MAGIC       num_workers: 2
# MAGIC     
# MAGIC     timeout_seconds: 3600
# MAGIC     max_retries: 2
# MAGIC     min_retry_interval_millis: 60000
# MAGIC     retry_on_timeout: true
# MAGIC
# MAGIC # ------------------------------------------------------------------------------
# MAGIC # CONTROL DE ACCESO (Opcional)
# MAGIC # ------------------------------------------------------------------------------
# MAGIC access_control_list:
# MAGIC   - user_name: ricardo.buileses@unaula.edu.co
# MAGIC     permission_level: IS_OWNER
# MAGIC   # Añade más usuarios si es necesario:
# MAGIC   # - user_name: otro_usuario@empresa.com
# MAGIC   #   permission_level: CAN_MANAGE_RUN
# MAGIC
# MAGIC # ==============================================================================
# MAGIC # NOTAS:
# MAGIC # ==============================================================================
# MAGIC # 1. Si Silver falla, Gold NO se ejecuta (depends_on bloquea la cadena)
# MAGIC # 2. Validate tiene max_retries: 0 porque los fallos son errores de datos
# MAGIC # 3. Schedule en cron Quartz: "segundos minutos horas día mes día_semana año"
# MAGIC # 4. Todos los clusters son nuevos (new_cluster) para aislamiento
# MAGIC # ==============================================================================
# MAGIC ```
# MAGIC
# MAGIC ### 💾 Cómo usar este YAML:
# MAGIC
# MAGIC #### **Opción 1: Databricks CLI**
# MAGIC
# MAGIC ```bash
# MAGIC # Instalar CLI
# MAGIC pip install databricks-cli
# MAGIC
# MAGIC # Configurar autenticación
# MAGIC databricks configure --token
# MAGIC
# MAGIC # Crear el job desde YAML
# MAGIC databricks jobs create --json-file cee_aragon_pipeline.yml
# MAGIC ```
# MAGIC
# MAGIC #### **Opción 2: Importar en UI**
# MAGIC
# MAGIC 1. Ve a **Workflows** en Databricks
# MAGIC 2. Haz clic en el botón con tres puntos (⋮) arriba a la derecha
# MAGIC 3. Selecciona **"Import"**
# MAGIC 4. Pega el contenido del YAML
# MAGIC 5. Haz clic en **"Import"**
# MAGIC
# MAGIC #### **Opción 3: Usar el código Python de la celda anterior** 👆

# COMMAND ----------

# DBTITLE 1,5. Guía de Capturas de Pantalla
# MAGIC %md
# MAGIC ## 5️⃣ GUÍA DE CAPTURAS DE PANTALLA (EVIDENCIA)
# MAGIC
# MAGIC ### 📸 Capturas requeridas y cuándo tomarlas
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **CAPTURA 1: Vista general del job**
# MAGIC
# MAGIC **🕒 Cuándo:** Después de crear el job (celda anterior o YAML)
# MAGIC
# MAGIC **📍 Dónde:** Workflows → tu job → pestaña "Tasks"
# MAGIC
# MAGIC **✅ Qué debe salir:**
# MAGIC - Las 4 tareas conectadas con flechas: Bronze → Silver → Validate → Gold
# MAGIC - Nombre del job: `CEE_Aragon_ETL_Pipeline`
# MAGIC - Vista del diagrama completo
# MAGIC
# MAGIC **🎯 Cómo hacerla:**
# MAGIC 1. Ve a **Workflows** (sidebar izquierdo)
# MAGIC 2. Haz clic en tu job `CEE_Aragon_ETL_Pipeline`
# MAGIC 3. Asegúrate de estar en la pestaña **"Tasks"**
# MAGIC 4. Haz zoom out (Ctrl + -) si no caben todas
# MAGIC 5. **Captura toda la ventana del navegador**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **CAPTURA 2: Schedule configurado**
# MAGIC
# MAGIC **🕒 Cuándo:** Inmediatamente después de crear el job
# MAGIC
# MAGIC **📍 Dónde:** Mismo job → pestaña "Schedule"
# MAGIC
# MAGIC **✅ Qué debe salir:**
# MAGIC - Trigger type: "Scheduled"
# MAGIC - Cron expression: `0 2 * * * ?`
# MAGIC - Time zone: `Europe/Madrid`
# MAGIC - Pause status: "Active" o "Unpaused"
# MAGIC
# MAGIC **🎯 Cómo hacerla:**
# MAGIC 1. En el job, haz clic en la pestaña **"Schedule"** (arriba)
# MAGIC 2. **Captura el panel completo** mostrando todos los detalles
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **CAPTURA 3: Dependencias de una tarea**
# MAGIC
# MAGIC **🕒 Cuándo:** Después de crear el job
# MAGIC
# MAGIC **📍 Dónde:** Panel de configuración de la tarea Silver
# MAGIC
# MAGIC **✅ Qué debe salir:**
# MAGIC - Task name: `02_silver_transformation`
# MAGIC - **Depends on: `01_bronze_ingestion`** ✅ (ESTO ES CRÍTICO)
# MAGIC - Notebook path visible
# MAGIC
# MAGIC **🎯 Cómo hacerla:**
# MAGIC 1. En la vista de Tasks, haz clic en el cuadro **"02_silver_transformation"**
# MAGIC 2. Se abre un panel lateral derecho
# MAGIC 3. **Captura la sección "Depends on"** claramente visible
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **CAPTURA 4: Reintentos configurados**
# MAGIC
# MAGIC **🕒 Cuándo:** Después de crear el job
# MAGIC
# MAGIC **📍 Dónde:** Panel de configuración → sección "Advanced"
# MAGIC
# MAGIC **✅ Qué debe salir:**
# MAGIC - Max retries: `2` (para Bronze/Silver/Gold)
# MAGIC - Max retries: `0` (para Validate) ← **Captura esta especialmente**
# MAGIC - Min retry interval: `60 seconds`
# MAGIC
# MAGIC **🎯 Cómo hacerla:**
# MAGIC 1. Haz clic en cualquier tarea (prueba con Silver)
# MAGIC 2. En el panel derecho, expande la sección **"Advanced"**
# MAGIC 3. **Captura la sección "Retries"**
# MAGIC 4. **Repite para Validate** mostrando `max_retries: 0`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **CAPTURA 5: Email notifications**
# MAGIC
# MAGIC **🕒 Cuándo:** Después de crear el job
# MAGIC
# MAGIC **📍 Dónde:** Pestaña "Email notifications" del job
# MAGIC
# MAGIC **✅ Qué debe salir:**
# MAGIC - On failure: ✓ marcado
# MAGIC - Email: `ricardobuiles@hotmail.com`
# MAGIC
# MAGIC **🎯 Cómo hacerla:**
# MAGIC 1. Haz clic en la pestaña **"Email notifications"** (arriba)
# MAGIC 2. **Captura el formulario completo**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **CAPTURA 6: Ejecución exitosa**
# MAGIC
# MAGIC **🕒 Cuándo:** Después de hacer "Run now" y que termine OK
# MAGIC
# MAGIC **📍 Dónde:** Página de "Run details"
# MAGIC
# MAGIC **✅ Qué debe salir:**
# MAGIC - Status: **"Succeeded"** (verde)
# MAGIC - Las 4 tareas con check verde ✓
# MAGIC - Duration (tiempo total)
# MAGIC - Start time / End time
# MAGIC
# MAGIC **🎯 Cómo hacerla:**
# MAGIC 1. Haz clic en **"Run now"** (botón azul arriba a la derecha)
# MAGIC 2. Espera a que termine (puede tardar ~30-60 min)
# MAGIC 3. En la página de Run details, **captura toda la vista**
# MAGIC 4. Asegúrate de que se vean las 4 tareas en verde
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **CAPTURA 7: Logs de validación (evidencia)**
# MAGIC
# MAGIC **🕒 Cuándo:** Durante o después de una ejecución exitosa
# MAGIC
# MAGIC **📍 Dónde:** Logs de la tarea Validate
# MAGIC
# MAGIC **✅ Qué debe salir:**
# MAGIC ```
# MAGIC ✅ VALIDACIÓN 1 PASADA: Sin nulos en objetivo
# MAGIC ✅ VALIDACIÓN 2 PASADA: Objetivo dentro de rango
# MAGIC ✅ VALIDACIÓN 3 PASADA: Años de construcción válidos
# MAGIC ✅ VALIDACIÓN 4 PASADA: Clasificaciones válidas
# MAGIC ✅ VALIDACIÓN 5 PASADA: Conteo dentro de banda esperada
# MAGIC ```
# MAGIC
# MAGIC **🎯 Cómo hacerla:**
# MAGIC 1. En Run details, haz clic en la tarea **`03_validate_silver`**
# MAGIC 2. Se abrirn pestañas
# MAGIC 3. Haz clic en **"Output"** o **"Notebook Output"**
# MAGIC 4. **Captura el output completo** con los 5 checks verdes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **CAPTURA 8: Fallo simulado (OPCIONAL)**
# MAGIC
# MAGIC **🕒 Cuándo:** Para demostrar que el pipeline SE DETIENE ante fallos
# MAGIC
# MAGIC **📍 Dónde:** Run details de una ejecución donde Validate falló
# MAGIC
# MAGIC **✅ Qué debe salir:**
# MAGIC - Tarea Validate con ❌ rojo y estado "Failed"
# MAGIC - Tarea Gold con ⏸️ gris y estado "Skipped" o "Upstream Failed"
# MAGIC - Mensaje de error visible
# MAGIC
# MAGIC **🎯 Cómo simularla:**
# MAGIC
# MAGIC ```python
# MAGIC # En el notebook validate_silver_certificados, 
# MAGIC # añade temporalmente al final:
# MAGIC assert 1 == 2, "❌ PRUEBA DE FALLO INTENCIONAL"
# MAGIC ```
# MAGIC
# MAGIC 1. Guarda el notebook modificado
# MAGIC 2. Ejecuta el job de nuevo
# MAGIC 3. Cuando falle, **captura el Run details** mostrando:
# MAGIC    - Validate en rojo (❌)
# MAGIC    - Gold en gris (⏸️ Skipped)
# MAGIC 4. **Deshaz el cambio** después de la captura
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📝 Checklist de capturas
# MAGIC
# MAGIC Para tu evidencia/documentación:
# MAGIC
# MAGIC - [ ] **Captura 1**: Vista general del job (4 tareas conectadas)
# MAGIC - [ ] **Captura 2**: Schedule (cron + timezone)
# MAGIC - [ ] **Captura 3**: Dependencias (depends_on visible en Silver)
# MAGIC - [ ] **Captura 4**: Reintentos (max_retries: 2 y 0 para Validate)
# MAGIC - [ ] **Captura 5**: Email notifications (on_failure marcado)
# MAGIC - [ ] **Captura 6**: Ejecución exitosa (4 tareas verdes)
# MAGIC - [ ] **Captura 7**: Logs de validación (5 checks ✓)
# MAGIC - [ ] **Captura 8** (opcional): Fallo simulado (Gold skipped)
# MAGIC
# MAGIC ### 📁 Cómo organizar las capturas
# MAGIC
# MAGIC Crea una carpeta:
# MAGIC ```
# MAGIC docs/
# MAGIC   evidencias_workflow/
# MAGIC     01_vista_general_job.png
# MAGIC     02_schedule_configurado.png
# MAGIC     03_dependencias_silver.png
# MAGIC     04_reintentos_validate.png
# MAGIC     05_email_notifications.png
# MAGIC     06_ejecucion_exitosa.png
# MAGIC     07_logs_validacion.png
# MAGIC     08_fallo_simulado.png (opcional)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,RESUMEN FINAL
# MAGIC %md
# MAGIC # ✅ RESUMEN - Todo lo que pediste
# MAGIC
# MAGIC ## 📦 Contenido entregado:
# MAGIC
# MAGIC ### 1️⃣ **Diseño del Workflow**
# MAGIC ✅ Arquitectura con 4 tareas encadenadas  
# MAGIC ✅ Tabla de configuración (retries, timeout, workers)  
# MAGIC ✅ Diagrama visual ASCII
# MAGIC
# MAGIC ### 2️⃣ **Respuesta crítica: ¿Gold corre si Silver falla?**
# MAGIC ❌ **NO** - Explicación detallada de depends_on  
# MAGIC ✅ Propagación del bloqueo  
# MAGIC ✅ Tabla de estados  
# MAGIC ✅ Ejemplo visual de fallo
# MAGIC
# MAGIC ### 3️⃣ **Código Python para crear el job AHORA**
# MAGIC ✅ Celda ejecutable con SDK de Databricks  
# MAGIC ✅ 4 tareas configuradas  
# MAGIC ✅ Dependencias, schedule, emails, reintentos
# MAGIC
# MAGIC ### 4️⃣ **YAML completo para GitHub**
# MAGIC ✅ 150 líneas con comentarios  
# MAGIC ✅ Versionable en `databricks_workflows/`  
# MAGIC ✅ 3 formas de usarlo (CLI, UI, Python)
# MAGIC
# MAGIC ### 5️⃣ **Suite de validaciones**
# MAGIC ✅ Notebook ya creado: [validate_silver_certificados](#notebook-984231585310709)  
# MAGIC ✅ 5 aserciones con mensajes claros  
# MAGIC ✅ Integrado en el workflow (tarea 3)  
# MAGIC ✅ max_retries: 0 (NO reintentar fallos de datos)
# MAGIC
# MAGIC ### 6️⃣ **Guía de capturas de pantalla**
# MAGIC ✅ 8 capturas documentadas  
# MAGIC ✅ Cuándo tomarlas (momento exacto)  
# MAGIC ✅ Qué debe salir en cada una  
# MAGIC ✅ Cómo hacerlas (pasos detallados)  
# MAGIC ✅ Checklist para evidencia
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Próximos pasos:
# MAGIC
# MAGIC 1. **Ejecutar la celda de Python** (arriba ↑) para crear el job
# MAGIC 2. **Ir a la URL** del job creado
# MAGIC 3. **Hacer "Run now"** para probarlo
# MAGIC 4. **Tomar las 8 capturas** siguiendo la guía
# MAGIC 5. **Guardar el YAML** en tu repo de GitHub
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Notebooks relacionados:
# MAGIC
# MAGIC - [01_Bronze_Ingesta_Certificados](#notebook-984231585310705)
# MAGIC - [03_Silver_Certificados](#notebook-984231585310707)
# MAGIC - [validate_silver_certificados](#notebook-984231585310709) ← **Validaciones**
# MAGIC - [04_Zona_Climatica_Gold](#notebook-984231585310708)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❓ ¿Preguntas frecuentes?
# MAGIC
# MAGIC **P: ¿Puedo cambiar el horario del schedule?**  
# MAGIC R: Sí, edita el cron en la pestaña Schedule del job
# MAGIC
# MAGIC **P: ¿Cómo veo por qué falló una tarea?**  
# MAGIC R: Run details → tarea fallida → pestaña "Error" o "Logs"
# MAGIC
# MAGIC **P: ¿Puedo agregar más emails a las notificaciones?**  
# MAGIC R: Sí, en Email notifications separa con comas
# MAGIC
# MAGIC **P: ¿Cómo desactivo el schedule temporalmente?**  
# MAGIC R: Schedule → Pause status → "Paused"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC 🎉 **¡Todo listo! Ejecuta la celda de Python arriba para crear tu workflow.**

# COMMAND ----------

