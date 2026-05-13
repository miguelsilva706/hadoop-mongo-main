"""
GameInsight - Análisis con Spark SQL
Lee el dataset procesado y ejecuta consultas SQL para responder las
4 preguntas de negocio clave de la tienda de videojuegos.

Demuestra:
- Spark SQL
- Vistas temporales
- Window Functions
- GroupBy avanzado
- Exportación de KPIs en CSV
"""

import os
os.environ["PYSPARK_PYTHON"] = "python3"

from pyspark.sql import SparkSession

# ─────────────────────────────────────────────────────────────
# INICIALIZAR SPARK
# ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("GameInsight-SparkSQL") \
    .master("spark://gamestore-spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

OUT_PATH = "/opt/spark-data/processed"
SQL_PATH = "/outputs/kpis"

print("=" * 60)
print("  GAMEINSIGHT — ANÁLISIS CON SPARK SQL")
print("=" * 60)

# ─────────────────────────────────────────────────────────────
# CARGAR DATOS PROCESADOS
# ─────────────────────────────────────────────────────────────
ventas_df = spark.read.parquet(f"{OUT_PATH}/ventas_enriquecidas")
inventario_df = spark.read.parquet(f"{OUT_PATH}/inventario")

ventas_df.createOrReplaceTempView("ventas")
inventario_df.createOrReplaceTempView("inventario")

print(f"\nDataset cargado: {ventas_df.count()} registros de ventas\n")

# ═════════════════════════════════════════════════════════════
# KPI 1 — TOP JUEGOS MÁS VENDIDOS
# ═════════════════════════════════════════════════════════════
print("━" * 60)
print("TOP 10 JUEGOS MÁS VENDIDOS")
print("━" * 60)

top_juegos = spark.sql("""
    SELECT
        juego_id,
        titulo,
        categoria,
        SUM(cantidad)                    AS total_unidades,
        COUNT(venta_id)                  AS num_transacciones,
        ROUND(SUM(total), 2)             AS ingresos_total,
        ROUND(AVG(puntuacion_promedio), 2) AS rating_promedio
    FROM ventas
    WHERE titulo IS NOT NULL
    GROUP BY juego_id, titulo, categoria
    ORDER BY total_unidades DESC
    LIMIT 10
""")

top_juegos.show(truncate=False)

top_juegos.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{SQL_PATH}/top_juegos")

# ═════════════════════════════════════════════════════════════
# KPI 2 — CATEGORÍAS MÁS POPULARES
# ═════════════════════════════════════════════════════════════
print("━" * 60)
print("CATEGORÍAS MÁS POPULARES")
print("━" * 60)

categorias = spark.sql("""
    SELECT
        categoria,
        COUNT(venta_id)              AS num_ventas,
        SUM(cantidad)                AS total_unidades,
        ROUND(SUM(total), 2)         AS ingresos_total,
        ROUND(AVG(precio), 2)        AS precio_promedio,
        ROUND(
            COUNT(venta_id) * 100.0 /
            SUM(COUNT(venta_id)) OVER (), 1
        )                            AS pct_ventas
    FROM ventas
    WHERE categoria IS NOT NULL
    GROUP BY categoria
    ORDER BY ingresos_total DESC
""")

categorias.show(truncate=False)

categorias.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{SQL_PATH}/categorias")

# ═════════════════════════════════════════════════════════════
# KPI 3 — PLATAFORMAS MÁS POPULARES
# ═════════════════════════════════════════════════════════════
print("━" * 60)
print("PLATAFORMAS MÁS POPULARES")
print("━" * 60)

plataformas = spark.sql("""
    SELECT
        v.plataforma,
        COUNT(v.venta_id)              AS num_ventas,
        SUM(v.cantidad)                AS total_unidades,
        ROUND(SUM(v.total), 2)         AS ingresos_total,
        ROUND(AVG(v.precio), 2)        AS ticket_promedio,
        ROUND(
            COUNT(v.venta_id) * 100.0 /
            SUM(COUNT(v.venta_id)) OVER (), 1
        )                              AS cuota_mercado_pct,
        COALESCE(SUM(i.stock), 0)      AS stock_actual
    FROM ventas v
    LEFT JOIN (
        SELECT plataforma, SUM(stock) AS stock
        FROM inventario
        GROUP BY plataforma
    ) i ON v.plataforma = i.plataforma
    GROUP BY v.plataforma
    ORDER BY ingresos_total DESC
""")

plataformas.show(truncate=False)

plataformas.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{SQL_PATH}/plataformas")

# ═════════════════════════════════════════════════════════════
# KPI 4 — VENTAS MENSUALES
# ═════════════════════════════════════════════════════════════
print("━" * 60)
print("VENTAS MENSUALES")
print("━" * 60)

ventas_mensuales = spark.sql("""
    SELECT
        anio,
        mes,
        COUNT(venta_id)               AS num_transacciones,
        SUM(cantidad)                 AS total_unidades,
        ROUND(SUM(total), 2)          AS ingresos_mes,
        ROUND(AVG(total), 2)          AS ticket_promedio,
        COUNT(DISTINCT cliente_id)    AS clientes_unicos
    FROM ventas
    GROUP BY anio, mes
    ORDER BY anio, mes
""")

ventas_mensuales.show(12, truncate=False)

ventas_mensuales.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{SQL_PATH}/ventas_mensuales")

# ═════════════════════════════════════════════════════════════
# KPI 5 — TOP CLIENTES
# ═════════════════════════════════════════════════════════════
print("━" * 60)
print("TOP CLIENTES")
print("━" * 60)

top_clientes = spark.sql("""
    SELECT
        cliente_id,
        nombre,
        nivel_membresia,
        distrito,
        COUNT(venta_id)          AS num_compras,
        ROUND(SUM(total), 2)     AS gasto_total,
        ROUND(AVG(total), 2)     AS ticket_promedio,
        MAX(fecha)               AS ultima_compra
    FROM ventas
    WHERE nombre IS NOT NULL
    GROUP BY cliente_id, nombre, nivel_membresia, distrito
    ORDER BY gasto_total DESC
    LIMIT 10
""")

top_clientes.show(truncate=False)

top_clientes.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{SQL_PATH}/top_clientes")

# ═════════════════════════════════════════════════════════════
# KPI 6 — MÉTODO DE PAGO
# ═════════════════════════════════════════════════════════════
print("━" * 60)
print("MÉTODO DE PAGO POR MEMBRESÍA")
print("━" * 60)

pago_membresia = spark.sql("""
    SELECT
        nivel_membresia,
        metodo_pago,
        COUNT(*) AS frecuencia
    FROM ventas
    WHERE nivel_membresia IS NOT NULL
    GROUP BY nivel_membresia, metodo_pago
    ORDER BY nivel_membresia, frecuencia DESC
""")

pago_membresia.show(truncate=False)

pago_membresia.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{SQL_PATH}/pago_membresia")

# ═════════════════════════════════════════════════════════════
# KPI 7 — STOCK BAJO
# ═════════════════════════════════════════════════════════════
print("━" * 60)
print("ALERTAS DE STOCK")
print("━" * 60)

stock_bajo = spark.sql("""
    SELECT
        juego_id,
        titulo_inv AS titulo,
        plataforma,
        stock,
        umbral,
        precio_inv AS precio
    FROM inventario
    WHERE stock <= umbral
    ORDER BY stock ASC
""")

stock_bajo.show(truncate=False)

stock_bajo.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{SQL_PATH}/stock_bajo")

# ═════════════════════════════════════════════════════════════
# KPI 8 — RESUMEN EJECUTIVO
# ═════════════════════════════════════════════════════════════
print("=" * 60)
print("RESUMEN EJECUTIVO")
print("=" * 60)

resumen = spark.sql("""
    SELECT
        COUNT(venta_id)             AS total_ventas,
        SUM(cantidad)               AS total_unidades_vendidas,
        ROUND(SUM(total), 2)        AS ingresos_totales_soles,
        ROUND(AVG(total), 2)        AS ticket_promedio,
        COUNT(DISTINCT cliente_id)  AS clientes_activos,
        COUNT(DISTINCT juego_id)    AS juegos_vendidos
    FROM ventas
""")

resumen.show(truncate=False)

resumen.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{SQL_PATH}/resumen_ejecutivo")

print("\n[ANÁLISIS SQL COMPLETADO EXITOSAMENTE]\n")

spark.stop()