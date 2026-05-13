"""
Archivo: streaming_spark_kafka.py
Proyecto: GameInsight - Big Data VideoGame Streaming

Objetivo:
Leer eventos desde Kafka usando Spark Structured Streaming,
procesarlos en micro-batches y generar alertas de ventas.

Entrada:
- Kafka topic: "Game-Store"

Salida:
- outputs/streaming/events/
- outputs/streaming/alerts/
- outputs/streaming/summary_by_platform.csv
- outputs/streaming/summary_by_payment.csv

Comando:
docker exec -it gamestore-spark-master \
/opt/spark/bin/spark-submit \
/opt/spark-apps/streaming_spark_kafka.py --duration 120
"""

from pathlib import Path
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    BooleanType
)


# ============================================================
# 1. CONFIGURACIÓN GENERAL
# ============================================================

STREAMING_OUTPUT_DIR = Path("/outputs/streaming")

EVENTS_OUTPUT_DIR = STREAMING_OUTPUT_DIR / "events"
ALERTS_OUTPUT_DIR = STREAMING_OUTPUT_DIR / "alerts"

CHECKPOINT_DIR = Path("/outputs/checkpoints/gamestore_streaming")

STREAMING_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
EVENTS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ALERTS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

KAFKA_TOPIC = "Game-Store"

KAFKA_BOOTSTRAP_SERVERS = "broker:9092"

KAFKA_SPARK_PACKAGE = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
)


# ============================================================
# 2. ESQUEMA DEL JSON
# ============================================================

event_schema = StructType([

    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),

    StructField("venta_id", StringType(), True),
    StructField("cliente_id", StringType(), True),
    StructField("juego_id", StringType(), True),

    StructField("plataforma", StringType(), True),
    StructField("metodo_pago", StringType(), True),
    StructField("vendedor", StringType(), True),

    StructField("precio", DoubleType(), True),
    StructField("cantidad", IntegerType(), True),
    StructField("total", DoubleType(), True),

    StructField("cliente_nombre", StringType(), True),
    StructField("nivel_membresia", StringType(), True),
    StructField("distrito", StringType(), True),

    StructField("riesgo_fraude", DoubleType(), True),
    StructField("venta_riesgosa", BooleanType(), True)
])


# ============================================================
# 3. CREAR SESIÓN SPARK
# ============================================================

def create_spark_session() -> SparkSession:

    spark = (
        SparkSession.builder
        .appName("GameInsightKafkaStructuredStreaming")
        .master("spark://gamestore-spark-master:7077")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", KAFKA_SPARK_PACKAGE)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# 4. GUARDAR CSV
# ============================================================

def append_csv_with_header(pdf, output_file: Path):

    write_header = not output_file.exists()

    pdf.to_csv(
        output_file,
        mode="a",
        header=write_header,
        index=False,
        encoding="utf-8"
    )


# ============================================================
# 5. PROCESAR MICRO-BATCH
# ============================================================

def process_batch(batch_df, batch_id: int):

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: sin eventos")
        return

    print("\n" + "=" * 80)
    print(f"Procesando batch streaming: {batch_id}")
    print("=" * 80)

    batch_df.cache()

    total_events = batch_df.count()

    print(f"Eventos recibidos: {total_events}")

    print("\nMuestra de eventos:")

    batch_df.select(
        "event_id",
        "venta_id",
        "event_type",
        "plataforma",
        "metodo_pago",
        "total",
        "riesgo_fraude",
        "venta_riesgosa"
    ).show(10, truncate=False)

    # ========================================================
    # RESUMEN POR PLATAFORMA
    # ========================================================

    summary_platform_df = (
        batch_df
        .groupBy("plataforma")
        .agg(
            F.count("*").alias("total_ventas"),
            F.round(F.sum("total"), 2).alias("ingresos"),
            F.round(F.avg("total"), 2).alias("ticket_promedio"),
            F.sum(F.col("venta_riesgosa").cast("int")).alias("ventas_riesgo")
        )
        .orderBy(F.desc("ingresos"))
    )

    print("\nResumen por plataforma:")

    summary_platform_df.show(truncate=False)

    # ========================================================
    # RESUMEN POR MÉTODO DE PAGO
    # ========================================================

    summary_payment_df = (
        batch_df
        .groupBy("metodo_pago")
        .agg(
            F.count("*").alias("total_transacciones"),
            F.round(F.sum("total"), 2).alias("monto_total"),
            F.round(F.avg("riesgo_fraude"), 2).alias("riesgo_promedio")
        )
        .orderBy(F.desc("monto_total"))
    )

    print("\nResumen por método de pago:")

    summary_payment_df.show(truncate=False)

    # ========================================================
    # ALERTAS DE FRAUDE
    # ========================================================

    alerts_df = (
        batch_df
        .filter(
            (F.col("venta_riesgosa") == True)
            | (F.col("riesgo_fraude") >= 0.75)
        )
        .select(
            "event_id",
            "venta_id",
            "cliente_nombre",
            "plataforma",
            "metodo_pago",
            "total",
            "riesgo_fraude",
            "event_timestamp"
        )
        .orderBy(F.desc("riesgo_fraude"))
    )

    total_alerts = alerts_df.count()

    print("\nAlertas detectadas:")

    print(f"Total alertas: {total_alerts}")

    if total_alerts > 0:
        alerts_df.show(10, truncate=False)

    # ========================================================
    # GUARDAR EVENTS CSV
    # ========================================================

    events_pdf = batch_df.toPandas()

    events_pdf["batch_id"] = batch_id

    events_pdf.to_csv(
        EVENTS_OUTPUT_DIR / f"events_batch_{batch_id}.csv",
        index=False,
        encoding="utf-8"
    )

    # ========================================================
    # GUARDAR ALERTS CSV
    # ========================================================

    if total_alerts > 0:

        alerts_pdf = alerts_df.toPandas()

        alerts_pdf["batch_id"] = batch_id

        alerts_pdf.to_csv(
            ALERTS_OUTPUT_DIR / f"alerts_batch_{batch_id}.csv",
            index=False,
            encoding="utf-8"
        )

    # ========================================================
    # GUARDAR SUMMARY PLATAFORMA
    # ========================================================

    summary_platform_pdf = summary_platform_df.toPandas()

    summary_platform_pdf["batch_id"] = batch_id

    append_csv_with_header(
        summary_platform_pdf,
        STREAMING_OUTPUT_DIR / "summary_by_platform.csv"
    )

    # ========================================================
    # GUARDAR SUMMARY PAYMENT
    # ========================================================

    summary_payment_pdf = summary_payment_df.toPandas()

    summary_payment_pdf["batch_id"] = batch_id

    append_csv_with_header(
        summary_payment_pdf,
        STREAMING_OUTPUT_DIR / "summary_by_payment.csv"
    )

    batch_df.unpersist()

    print(f"\nArchivos generados batch {batch_id}:")

    print(f"- outputs/streaming/events/events_batch_{batch_id}.csv")

    if total_alerts > 0:
        print(f"- outputs/streaming/alerts/alerts_batch_{batch_id}.csv")

    print("- outputs/streaming/summary_by_platform.csv")

    print("- outputs/streaming/summary_by_payment.csv")


# ============================================================
# 6. MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description="Spark Structured Streaming GameInsight"
    )

    parser.add_argument(
        "--duration",
        type=int,
        default=120,
        help="Duración streaming en segundos"
    )

    args = parser.parse_args()

    print("=" * 80)
    print("Spark Structured Streaming - GameInsight")
    print("=" * 80)

    print(f"Kafka topic: {KAFKA_TOPIC}")

    print(f"Kafka server: {KAFKA_BOOTSTRAP_SERVERS}")

    print(f"Duración: {args.duration} segundos")

    print("=" * 80)

    spark = create_spark_session()

    # ========================================================
    # LEER STREAM KAFKA
    # ========================================================

    kafka_stream_df = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS
        )
        .option(
            "subscribe",
            KAFKA_TOPIC
        )
        .option(
            "startingOffsets",
            "latest"
        )
        .load()
    )

    # ========================================================
    # PARSEAR JSON
    # ========================================================

    parsed_events_df = (
        kafka_stream_df
        .select(
            F.col("key").cast("string").alias("message_key"),
            F.col("value").cast("string").alias("message_value"),
            F.col("timestamp").alias("kafka_timestamp")
        )
        .withColumn(
            "json_data",
            F.from_json(
                F.col("message_value"),
                event_schema
            )
        )
        .select(
            "message_key",
            "kafka_timestamp",
            "json_data.*"
        )
        .withColumn(
            "event_timestamp",
            F.to_timestamp("event_timestamp")
        )
        .withColumn(
            "processing_timestamp",
            F.current_timestamp()
        )
    )

    # ========================================================
    # INICIAR STREAMING
    # ========================================================

    query = (
        parsed_events_df
        .writeStream
        .foreachBatch(process_batch)
        .option(
            "checkpointLocation",
            str(CHECKPOINT_DIR)
        )
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("\nStreaming iniciado")

    print("Ejecuta el productor en otra terminal")

    print("Resultados en outputs/streaming/\n")

    query.awaitTermination(args.duration)

    query.stop()

    print("=" * 80)
    print("Streaming finalizado")
    print("Revisa outputs/streaming/")
    print("=" * 80)

    spark.stop()


# ============================================================
# 7. EJECUCIÓN
# ============================================================

if __name__ == "__main__":
    main()

