"""
GameInsight - ETL Pipeline
Lee archivos CSV, JSON, TXT y XML,
limpia y transforma datos,
y guarda resultados procesados en Parquet.
"""

import os
import xml.etree.ElementTree as ET

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ─────────────────────────────────────────────────────────────
# 1. INICIALIZAR SPARK
# ─────────────────────────────────────────────────────────────

os.environ["PYSPARK_PYTHON"] = "python3"

spark = (
    SparkSession.builder
    .appName("GameInsight-ETL")
    .master("spark://spark-master:7077")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext

BASE_PATH = "/opt/spark-data/raw"
OUT_PATH = "/opt/spark-data/processed"

print("=" * 60)
print("  GAMEINSIGHT — ETL PIPELINE")
print("=" * 60)

# ─────────────────────────────────────────────────────────────
# 2. LEER CSV
# ─────────────────────────────────────────────────────────────

print("\n[1/5] Leyendo ventas.csv ...")

ventas_df = spark.read.csv(
    f"{BASE_PATH}/ventas.csv",
    header=True,
    inferSchema=True
)

print(f"  → {ventas_df.count()} registros de ventas cargados")
ventas_df.printSchema()

print("\n[2/5] Leyendo clientes.csv ...")

clientes_df = spark.read.csv(
    f"{BASE_PATH}/clientes.csv",
    header=True,
    inferSchema=True
)

print(f"  → {clientes_df.count()} clientes cargados")

# ─────────────────────────────────────────────────────────────
# 3. LEER JSON
# ─────────────────────────────────────────────────────────────

print("\n[3/5] Leyendo catalogo_juegos.json ...")

catalogo_df = spark.read.option(
    "multiLine",
    "true"
).json(f"{BASE_PATH}/catalogo_juegos.json")

print(f"  → {catalogo_df.count()} juegos en catálogo")
catalogo_df.printSchema()

print("\n[4/5] Leyendo resenas.json ...")

resenas_df = spark.read.option(
    "multiLine",
    "true"
).json(f"{BASE_PATH}/resenas.json")

print(f"  → {resenas_df.count()} reseñas cargadas")

# ─────────────────────────────────────────────────────────────
# 4. LEER TXT CON RDD
# ─────────────────────────────────────────────────────────────

print("\n[5/5] Leyendo logs_tienda.txt con RDD ...")

logs_rdd = sc.textFile(f"{BASE_PATH}/logs_tienda.txt")

# PURCHASE
compras_rdd = (
    logs_rdd
    .filter(lambda line: "PURCHASE" in line and "SISTEMA" not in line)
    .map(lambda line: line.split())
    .filter(lambda parts: len(parts) > 2)
    .map(lambda parts: (parts[2], 1))
    .reduceByKey(lambda a, b: a + b)
)

# LOGIN
sesiones_rdd = (
    logs_rdd
    .filter(lambda line: "LOGIN" in line and "SISTEMA" not in line)
    .map(lambda line: line.split())
    .filter(lambda parts: len(parts) > 2)
    .map(lambda parts: (parts[2], 1))
    .reduceByKey(lambda a, b: a + b)
)

print(f"  → Clientes con compras detectadas en logs: {compras_rdd.count()}")

compras_log_df = spark.createDataFrame(
    compras_rdd,
    schema=["cliente_id", "compras_en_log"]
)

sesiones_log_df = spark.createDataFrame(
    sesiones_rdd,
    schema=["cliente_id", "sesiones_log"]
)

# ─────────────────────────────────────────────────────────────
# 5. LEER XML
# ─────────────────────────────────────────────────────────────

print("\nParsing inventario.xml ...")

xml_content = sc.wholeTextFiles(
    f"{BASE_PATH}/inventario.xml"
).collect()[0][1]

root = ET.fromstring(xml_content)

inventario_rows = []

for juego_elem in root.findall("juego"):

    juego_id = juego_elem.get("id")
    titulo = juego_elem.find("titulo").text

    for plat in juego_elem.find("plataformas").findall("plataforma"):

        inventario_rows.append({
            "juego_id": juego_id,
            "titulo_inv": titulo,
            "plataforma": plat.get("nombre"),
            "stock": int(plat.find("stock").text),
            "precio_inv": float(plat.find("precio").text),
            "umbral": int(plat.find("umbral_reposicion").text)
        })

inventario_df = spark.createDataFrame(inventario_rows)

print(f"  → {inventario_df.count()} combinaciones juego/plataforma en inventario")

# ─────────────────────────────────────────────────────────────
# 6. LIMPIEZA Y TRANSFORMACIÓN
# ─────────────────────────────────────────────────────────────

print("\nLimpiando y transformando datos ...")

# VENTAS
ventas_clean = (
    ventas_df

    .withColumn("fecha", F.to_date("fecha", "yyyy-MM-dd"))
    .withColumn("mes", F.month("fecha"))
    .withColumn("anio", F.year("fecha"))
    .withColumn("trimestre", F.quarter("fecha"))

    .withColumn(
        "total",
        F.round(F.col("precio") * F.col("cantidad"), 2)
    )

    .withColumn(
        "juego_id",
        F.upper(F.trim(F.col("juego_id")))
    )

    .withColumn(
        "plataforma",
        F.upper(F.trim(F.col("plataforma")))
    )

    # NORMALIZAR NOMBRES
    .withColumn(
        "plataforma",
        F.when(F.col("plataforma").contains("XBOX"), "XBOX")
         .when(F.col("plataforma").contains("PS5"), "PS5")
         .when(F.col("plataforma").contains("PS4"), "PS4")
         .when(F.col("plataforma").contains("SWITCH"), "SWITCH")
         .when(F.col("plataforma").contains("PC"), "PC")
         .otherwise(F.col("plataforma"))
    )

    .filter(F.col("precio") > 0)

    .dropDuplicates(["venta_id"])
)

# CLIENTES
clientes_clean = (
    clientes_df

    .withColumn(
        "nivel_membresia",
        F.trim(F.col("nivel_membresia"))
    )

    .withColumn(
        "distrito",
        F.trim(F.col("distrito"))
    )

    .dropDuplicates(["cliente_id"])
)

# CATÁLOGO
catalogo_exploded = (
    catalogo_df

    .withColumn(
        "plataforma",
        F.explode("plataformas")
    )

    .withColumn(
        "juego_id",
        F.upper(F.trim(F.col("juego_id")))
    )

    .withColumn(
        "plataforma",
        F.upper(F.trim(F.col("plataforma")))
    )

    # NORMALIZAR NOMBRES
    .withColumn(
        "plataforma",
        F.when(F.col("plataforma").contains("XBOX"), "XBOX")
         .when(F.col("plataforma").contains("PLAYSTATION 5"), "PS5")
         .when(F.col("plataforma").contains("PLAYSTATION 4"), "PS4")
         .when(F.col("plataforma").contains("NINTENDO"), "SWITCH")
         .when(F.col("plataforma").contains("PC"), "PC")
         .otherwise(F.col("plataforma"))
    )

    .select(
        "juego_id",
        "titulo",
        "categoria",
        "plataforma",
        "precio_base",
        "desarrollador",
        "anio_lanzamiento",
        "pegi",
        "tags"
    )
)

# RESEÑAS
puntuacion_promedio = (
    resenas_df
    .groupBy("juego_id")
    .agg(
        F.round(F.avg("puntuacion"), 2).alias("puntuacion_promedio"),
        F.count("resena_id").alias("total_resenas"),
        F.sum("util_votos").alias("total_votos_util")
    )
)

# ─────────────────────────────────────────────────────────────
# 7. JOINS
# ─────────────────────────────────────────────────────────────

print("Integrando fuentes de datos ...")

ventas_enriquecidas = (
    ventas_clean

    .join(
        clientes_clean.select(
            "cliente_id",
            "nombre",
            "edad",
            "genero",
            "distrito",
            "nivel_membresia"
        ),
        on="cliente_id",
        how="left"
    )

    .join(
        catalogo_exploded.select(
            "juego_id",
            "plataforma",
            "titulo",
            "categoria",
            "desarrollador",
            "anio_lanzamiento",
            "tags"
        ),
        on=["juego_id", "plataforma"],
        how="left"
    )

    .join(
        puntuacion_promedio,
        on="juego_id",
        how="left"
    )

    .join(
        compras_log_df,
        on="cliente_id",
        how="left"
    )

    .join(
        sesiones_log_df,
        on="cliente_id",
        how="left"
    )
)

print(f"  → Dataset integrado: {ventas_enriquecidas.count()} registros")

# REVISAR NULLS
nulls = ventas_enriquecidas.filter(
    F.col("titulo").isNull()
).count()

print(f"  → Registros con titulo NULL: {nulls}")

# MOSTRAR EJEMPLO
print("\nMuestra del dataset integrado:")

ventas_enriquecidas.select(
    "venta_id",
    "fecha",
    "nombre",
    "titulo",
    "categoria",
    "plataforma",
    "total",
    "nivel_membresia"
).show(10, truncate=False)

# ─────────────────────────────────────────────────────────────
# 8. GUARDAR RESULTADOS
# ─────────────────────────────────────────────────────────────

print("\nGuardando resultados procesados en Parquet ...")

ventas_enriquecidas.write.mode("overwrite").parquet(
    f"{OUT_PATH}/ventas_enriquecidas"
)

inventario_df.write.mode("overwrite").parquet(
    f"{OUT_PATH}/inventario"
)

puntuacion_promedio.write.mode("overwrite").parquet(
    f"{OUT_PATH}/puntuaciones"
)

compras_log_df.write.mode("overwrite").csv(
    f"{OUT_PATH}/actividad_logs",
    header=True
)

print("  ✓ ventas_enriquecidas.parquet guardado")
print("  ✓ inventario.parquet guardado")
print("  ✓ puntuaciones.parquet guardado")
print("  ✓ actividad_logs.csv guardado")

print("\n[ETL completado exitosamente]\n")

spark.stop()