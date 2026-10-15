# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00 · Delta Lake: cómo funciona una tabla
# MAGIC
# MAGIC **Laboratorio 0 · Semana 1 · Certificación Databricks Data Engineer Associate · TALENTO PARA TI**
# MAGIC
# MAGIC Antes de ingerir datos hay que entender dónde quedan guardados. En Databricks, casi todas las tablas son **tablas Delta**. Este laboratorio empieza con un recorrido por el workspace y después te muestra qué hay adentro de una tabla Delta y cómo se comporta cuando la modificas.
# MAGIC
# MAGIC **Cómo usar este notebook**
# MAGIC
# MAGIC * Ejecuta las celdas **en orden**, de arriba hacia abajo. Cada una depende de las anteriores.
# MAGIC * Lee el texto antes de ejecutar: la idea no es solo que corra, sino entender qué pasó.
# MAGIC * Las secciones **✏️ Tu turno** son ejercicios cortos. Intenta resolverlos antes de mirar la solución.
# MAGIC * Las secciones **🎯 Para el examen** resumen lo que suele preguntar la certificación.
# MAGIC
# MAGIC **Requisitos:** un workspace de Databricks Free Edition. Los datos son ficticios y se generan dentro del notebook, así que no necesitas subir nada.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recorrido: tu workspace en 10 minutos
# MAGIC
# MAGIC ### Las secciones del menú lateral
# MAGIC
# MAGIC | Sección | Para qué sirve |
# MAGIC |---|---|
# MAGIC | **Workspace** | Tus carpetas y notebooks, y los repositorios conectados con Git Folders |
# MAGIC | **Catalog** | El Catalog Explorer: catálogos, schemas, tablas, volumes y sus permisos |
# MAGIC | **SQL Editor** | Escribir consultas SQL sin abrir un notebook |
# MAGIC | **Jobs & Pipelines** | Programar y orquestar procesos (Semana 5) |
# MAGIC | **Compute** | Los recursos que ejecutan tu código |
# MAGIC
# MAGIC ### Cómo llegaste a este notebook
# MAGIC
# MAGIC Si lo abriste desde el repositorio del equipo, ya usaste **Git Folders**: *Workspace → Create → Git folder*, pegar la URL del repositorio y abrir el notebook. Cuando se actualice el repositorio, entra a la carpeta y usa **Pull** para traer los cambios.
# MAGIC
# MAGIC ### El compute
# MAGIC
# MAGIC Para ejecutar celdas, el notebook necesita compute. Arriba a la derecha, en el selector de compute, elige **Serverless**. La Free Edition solo ofrece compute serverless, pero el examen pregunta por todos los tipos:
# MAGIC
# MAGIC | Tipo | Para qué se usa |
# MAGIC |---|---|
# MAGIC | **Serverless** | Arranca en segundos y Databricks administra la infraestructura. Se paga por uso |
# MAGIC | **All-purpose cluster** | Trabajo interactivo en notebooks. Conviene configurarle *auto termination* para no pagar tiempo ocioso |
# MAGIC | **Job compute** | Procesos programados. Existe solo mientras corre el job y es más barato que un all-purpose |
# MAGIC | **SQL warehouse** | Consultas SQL y dashboards |
# MAGIC
# MAGIC ### Unity Catalog
# MAGIC
# MAGIC Todo dato en Databricks vive en una jerarquía de tres niveles: **catalog → schema → tabla** (o volume, vista o función). Por eso una tabla se nombra `catalogo.schema.tabla`. Por encima está el **metastore**, que se puede compartir entre varios workspaces de la misma región.
# MAGIC
# MAGIC En la Free Edition tienes dos catálogos: `workspace`, donde trabajas tú, y `samples`, con datos de ejemplo de solo lectura. 
# MAGIC
# MAGIC Ahora ejecuta tu primera celda (con `Shift + Enter`):

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     current_catalog() AS catalogo, 
# MAGIC     current_schema() AS schema_actual, 
# MAGIC     current_user() AS usuario

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 **Para el examen**
# MAGIC > * La jerarquía de Unity Catalog es catalog → schema → tabla, y el metastore puede asignarse a varios workspaces de una región.
# MAGIC > * Notebooks interactivos: all-purpose o serverless. Procesos programados: job compute o serverless. SQL y dashboards: SQL warehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Preparación
# MAGIC
# MAGIC Esta celda crea un schema propio para el laboratorio (`workspace.lab_delta`) y un volume para archivos. Si la ejecutas de nuevo, **borra las tablas del laboratorio y empieza de cero**, así que úsala también para reiniciar.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "lab_delta"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
spark.sql("CREATE VOLUME IF NOT EXISTS archivos")

# Reinicia el laboratorio: borra las tablas que crean las secciones siguientes
for t in ["transacciones", "clientes"]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")

print(f"Listo. Trabajarás en {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC Esta función genera transacciones ficticias de una billetera digital. La vas a usar en todo el laboratorio.

# COMMAND ----------

from pyspark.sql import functions as F

def generar_transacciones(n, inicio=0):
    """Genera n transacciones ficticias con id_transaccion desde `inicio`."""
    tipos = F.array(*[F.lit(t) for t in ["envio", "recarga", "pago"]])
    ciudades = F.array(*[F.lit(c) for c in ["Bogotá", "Medellín", "Cali", "Barranquilla", "Bucaramanga"]])
    return (
        spark.range(inicio, inicio + n)
        .select(
            F.col("id").alias("id_transaccion"),
            ((F.col("id") % 50) + 1).cast("int").alias("id_cliente"),
            F.element_at(tipos, (F.col("id") % 3 + 1).cast("int")).alias("tipo"),
            F.round((F.col("id") * 7919 % 100000) / 100, 2).alias("monto"),
            F.element_at(ciudades, (F.col("id") % 5 + 1).cast("int")).alias("ciudad"),
            F.date_add(F.lit("2026-01-01").cast("date"), (F.col("id") % 30).cast("int")).alias("fecha"),
        )
    )

display(generar_transacciones(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Delta Lake por dentro

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. El Delta Log
# MAGIC
# MAGIC Una tabla Delta tiene dos partes:
# MAGIC
# MAGIC 1. **Archivos Parquet** con los datos.
# MAGIC 2. **Una carpeta `_delta_log`** con un archivo JSON por cada cambio (*commit*). Cada archivo dice qué archivos Parquet se agregaron y cuáles se dejaron de usar.
# MAGIC
# MAGIC Ese registro es lo que hace posible todo lo demás: transacciones ACID, time travel y lectura de cambios. Cada commit crea una nueva **versión** de la tabla, empezando por la versión 0.
# MAGIC
# MAGIC Vamos a crear una tabla y hacerle varios cambios.

# COMMAND ----------

# Versión 0: se crea la tabla con 1.000 transacciones
generar_transacciones(1000).write.saveAsTable("transacciones")

# Versión 1: llegan 200 transacciones más
generar_transacciones(200, inicio=1000).write.mode("append").saveAsTable("transacciones")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Versión 2: se corrige el monto de los pagos
# MAGIC UPDATE transacciones SET monto = monto * 1.1 WHERE tipo = 'pago';
# MAGIC
# MAGIC -- Versión 3: se eliminan las transacciones de Cali
# MAGIC DELETE FROM transacciones WHERE ciudad = 'Cali';

# COMMAND ----------

# MAGIC %md
# MAGIC `DESCRIBE HISTORY` lee el Delta Log y muestra cada versión: qué operación se hizo, cuándo, quién y con qué métricas. Mira la columna `operationMetrics` de la versión 3: dice cuántas filas se eliminaron.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY transacciones

# COMMAND ----------

# MAGIC %md
# MAGIC `DESCRIBE DETAIL` muestra los datos físicos de la tabla: formato, ubicación, cantidad de archivos y tamaño.

# COMMAND ----------

# DBTITLE 1,Mostrar detalles físicos de la tabla
# MAGIC %sql
# MAGIC DESCRIBE DETAIL transacciones

# COMMAND ----------

# MAGIC %md
# MAGIC ### Opcional: mirar adentro del `_delta_log`
# MAGIC
# MAGIC En las tablas de Unity Catalog no puedes navegar los archivos directamente. Para ver el log por dentro, guardamos una pequeña tabla Delta como archivos en el volume del laboratorio. **Esto es solo para aprender**: en el trabajo real siempre se usan tablas.

# COMMAND ----------

ruta = f"/Volumes/{CATALOG}/{SCHEMA}/archivos/delta_log_demo"
try:
    dbutils.fs.rm(ruta, True)
    generar_transacciones(100).write.format("delta").mode("overwrite").save(ruta)
    generar_transacciones(50, inicio=100).write.format("delta").mode("append").save(ruta)

    print("Archivos del _delta_log:")
    display(dbutils.fs.ls(f"{ruta}/_delta_log"))

    print("Contenido del commit de la versión 1 (el append):")
    display(spark.read.json(f"{ruta}/_delta_log/00000000000000000001.json"))
except Exception as e:
    print("No se pudo crear la demo en este workspace. Puedes saltarte esta celda.")
    print(type(e).__name__, str(e)[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC En el commit verás filas con `commitInfo` (quién y qué operación) y con `add` (el archivo Parquet nuevo, su tamaño y estadísticas como mínimos y máximos por columna). Esas estadísticas permiten saltarse archivos al consultar: se llama **data skipping**.
# MAGIC
# MAGIC > 🎯 **Para el examen**
# MAGIC > * El Delta Log vive en la carpeta `_delta_log`, junto a los datos, y tiene un JSON por commit.
# MAGIC > * Cada cierta cantidad de commits se escribe un **checkpoint** en Parquet, para no tener que leer todos los JSON desde el principio.
# MAGIC > * `DESCRIBE HISTORY` muestra las versiones; `DESCRIBE DETAIL`, el detalle físico.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Time travel
# MAGIC
# MAGIC Como el log guarda todas las versiones, puedes consultar la tabla **tal como estaba** en una versión o en un momento anterior.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'actual' AS version, count(*) AS filas FROM transacciones -- Después de borrar las transacciones de Cali (240)
# MAGIC UNION ALL
# MAGIC SELECT 'versión 0', count(*) FROM transacciones VERSION AS OF 0
# MAGIC UNION ALL
# MAGIC SELECT 'versión 1', count(*) FROM transacciones VERSION AS OF 1

# COMMAND ----------

# MAGIC %md
# MAGIC También existe una sintaxis corta, `tabla@v1`, y se puede viajar por fecha y hora con `TIMESTAMP AS OF`:

# COMMAND ----------

historial = spark.sql("DESCRIBE HISTORY transacciones")
ts_v1 = historial.filter("version = 1").select("timestamp").first()[0]
print("La versión 1 se creó en:", ts_v1)

display(spark.sql(f"SELECT count(*) AS filas FROM transacciones TIMESTAMP AS OF '{ts_v1}'"))
display(spark.sql("SELECT count(*) AS filas FROM transacciones@v1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recuperarse de un error con `RESTORE`
# MAGIC
# MAGIC Simulemos un accidente: alguien borra **todas** las filas de la tabla.

# COMMAND ----------

version_antes = spark.sql("DESCRIBE HISTORY transacciones").agg(F.max("version")).first()[0]
print("Última versión buena:", version_antes)

spark.sql("DELETE FROM transacciones") 
print("Filas después del accidente:", spark.table("transacciones").count())

# COMMAND ----------

spark.sql(f"RESTORE TABLE transacciones TO VERSION AS OF {version_antes}") # Versión 5
print("Filas después de RESTORE:", spark.table("transacciones").count()) # Volvemos a la misma cantidad de filas antes de borrar

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY transacciones

# COMMAND ----------

# MAGIC %md
# MAGIC Fíjate en que `RESTORE` **no borra** la versión del accidente: agrega una versión nueva con el contenido anterior. El historial nunca se reescribe.
# MAGIC
# MAGIC > ✏️ **Tu turno**
# MAGIC > Escribe una consulta que muestre las transacciones de **Cali** que existían en la versión 2 (antes del `DELETE`). ¿Cuántas eran?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reemplaza esta línea por tu consulta
# MAGIC SELECT 'Escribe tu consulta aquí' AS pendiente

# COMMAND ----------

# MAGIC %md
# MAGIC <details><summary>Ver solución</summary>
# MAGIC
# MAGIC ```sql
# MAGIC SELECT count(*) FROM transacciones VERSION AS OF 2 WHERE ciudad = 'Cali'
# MAGIC ```
# MAGIC Deberían ser 240: las 200 iniciales de Cali más 40 del segundo lote.
# MAGIC </details>
# MAGIC
# MAGIC > 🎯 **Para el examen**
# MAGIC > * `VERSION AS OF n`, `TIMESTAMP AS OF '...'` y `tabla@vN` consultan versiones anteriores.
# MAGIC > * `RESTORE TABLE ... TO VERSION AS OF n` vuelve a una versión creando un commit nuevo.
# MAGIC > * El time travel funciona mientras existan los archivos de esa versión. `VACUUM` los borra (lo verás en el laboratorio de la Semana 6).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Leer los cambios con `table_changes()`
# MAGIC
# MAGIC Time travel te muestra una **foto** de la tabla. El **Change Data Feed (CDF)** te muestra **qué cambió** entre versiones, fila por fila. Es la base para propagar cambios de bronze a silver sin releer toda la tabla.
# MAGIC
# MAGIC El CDF hay que **activarlo** en la tabla. Solo registra cambios desde que se activa.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS clientes;
# MAGIC
# MAGIC -- Versión 0: tabla con CDF activado
# MAGIC CREATE TABLE clientes (id_cliente INT, nombre STRING, segmento STRING)
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- Versión 1
# MAGIC INSERT INTO 
# MAGIC     clientes 
# MAGIC VALUES 
# MAGIC     (1, 'Ana', 'basico'), 
# MAGIC     (2, 'Luis', 'premium'), 
# MAGIC     (3, 'Carla', 'basico');
# MAGIC
# MAGIC -- Versión 2
# MAGIC UPDATE clientes SET segmento = 'premium' WHERE id_cliente = 1; -- Cambiamos Ana a premium
# MAGIC
# MAGIC -- Versión 3
# MAGIC DELETE FROM clientes WHERE id_cliente = 3; -- Borramos a Carla (id_cliente = 3)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM table_changes('clientes', 1)
# MAGIC ORDER BY _commit_version, id_cliente

# COMMAND ----------

# MAGIC %md
# MAGIC Cada fila trae tres columnas extra:
# MAGIC
# MAGIC | Columna | Qué indica |
# MAGIC |---|---|
# MAGIC | `_change_type` | `insert`, `delete`, `update_preimage` (cómo era antes) o `update_postimage` (cómo quedó) |
# MAGIC | `_commit_version` | En qué versión ocurrió el cambio |
# MAGIC | `_commit_timestamp` | Cuándo ocurrió |
# MAGIC
# MAGIC Un `UPDATE` genera **dos filas**: la imagen anterior y la posterior. Desde PySpark se lee igual con la opción `readChangeFeed`:

# COMMAND ----------

cambios = (
    spark.read
    .option("readChangeFeed", "true")
    .option("startingVersion", 2)
    .table("clientes")
)
display(cambios)

# COMMAND ----------

# MAGIC %md
# MAGIC > ✏️ **Tu turno**
# MAGIC > Usando `table_changes()`, muestra solo los clientes **eliminados**, con la versión en que se eliminaron.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reemplaza esta línea por tu consulta
# MAGIC SELECT 'Escribe tu consulta aquí' AS pendiente

# COMMAND ----------

# MAGIC %md
# MAGIC <details><summary>Ver solución</summary>
# MAGIC
# MAGIC ```sql
# MAGIC SELECT id_cliente, nombre, _commit_version
# MAGIC FROM table_changes('clientes', 1)
# MAGIC WHERE _change_type = 'delete'
# MAGIC ```
# MAGIC </details>
# MAGIC
# MAGIC > 🎯 **Para el examen**
# MAGIC > * El CDF se activa con `delta.enableChangeDataFeed = true` y no registra cambios anteriores a su activación.
# MAGIC > * `table_changes('tabla', inicio [, fin])` acepta versiones o timestamps.
# MAGIC > * Un `UPDATE` produce `update_preimage` y `update_postimage`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Schema enforcement y schema evolution
# MAGIC
# MAGIC **Schema enforcement:** Delta rechaza escrituras que no coinciden con el schema de la tabla. Así un archivo mal formado no ensucia los datos en silencio.
# MAGIC
# MAGIC Intentemos agregar transacciones que traen una columna nueva, `canal`:

# COMMAND ----------

nuevas = generar_transacciones(10, inicio=5000).withColumn("canal", F.lit("app"))

try:
    nuevas.write.mode("append").saveAsTable("transacciones")
except Exception as e:
    print("❌ Escritura rechazada por schema enforcement:")
    print(str(e).split("\n")[0][:300])

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Equivalente en SQL: INSERT con la columna extra "canal" que no existe en la tabla
# MAGIC -- Delta rechaza la escritura por schema enforcement (igual que en PySpark)
# MAGIC INSERT INTO transacciones
# MAGIC VALUES (5000, 'Bogotá', 150000.0, '2024-01-01', 'app')

# COMMAND ----------

# MAGIC %md
# MAGIC También se rechaza un tipo incompatible, por ejemplo un monto que llega como texto:

# COMMAND ----------

monto_texto = generar_transacciones(10, inicio=6000).withColumn("monto", F.lit("cien mil"))

try:
    monto_texto.write.mode("append").saveAsTable("transacciones")
    print("⚠️ La escritura se aceptó: revisa con tu instructor por qué no se aplicó el enforcement.")
except Exception as e:
    print("❌ Escritura rechazada:")
    print(str(e).split("\n")[0][:300])

# COMMAND ----------

# MAGIC %md
# MAGIC **Schema evolution:** si la columna nueva es esperada, le pides a Delta que la agregue con `mergeSchema`. Las filas anteriores quedan con `NULL` en la columna nueva.

# COMMAND ----------

# Agregamos la columna canal, pero esta vez con mergeSchema (Schema Evolution), esto permite que se agreguen nuevas columnas a la tabla
nuevas.write.mode("append").option("mergeSchema", "true").saveAsTable("transacciones")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM transacciones LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- El cambio de schema también queda en el historial
# MAGIC DESCRIBE HISTORY transacciones

# COMMAND ----------

# MAGIC %md
# MAGIC > 🎯 **Para el examen**
# MAGIC > * **Enforcement** rechaza columnas extra y tipos incompatibles. Es el comportamiento por defecto.
# MAGIC > * **Evolution** con `.option("mergeSchema", "true")` agrega columnas nuevas. No sirve para cambiar el tipo de una columna existente.
# MAGIC > * `.option("overwriteSchema", "true")` con `mode("overwrite")` reemplaza el schema completo.
# MAGIC > * En SQL, una columna se agrega con `ALTER TABLE ... ADD COLUMN`.
# MAGIC > * Auto Loader (Semana 3) tiene su propio manejo de schema evolution, que se basa en esta misma idea.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Repaso
# MAGIC
# MAGIC Responde mentalmente antes de cerrar el notebook:
# MAGIC
# MAGIC 1. ¿Qué tipo de compute usarías para un proceso que corre todas las noches? ¿Y para un dashboard?
# MAGIC 2. ¿Qué hay en la carpeta `_delta_log` y para qué sirve?
# MAGIC 3. Corriste un `DELETE` por error. ¿Con qué comando vuelves atrás y qué pasa con el historial?
# MAGIC 4. ¿Qué necesitas hacer antes de poder usar `table_changes()` en una tabla?
# MAGIC 5. Llega un archivo con una columna nueva que sí quieres guardar. ¿Qué opción usas?
# MAGIC
# MAGIC El viernes, el **Cuestionario 1** evalúa justamente estos temas.
# MAGIC
# MAGIC No borres el schema `lab_delta`: los próximos laboratorios lo reutilizan.