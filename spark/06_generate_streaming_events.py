"""
GameInsight - Generador de Eventos Streaming con Kafka

Objetivo:
Generar eventos simulados de ventas de videojuegos y enviarlos a Kafka.

Este script funciona como productor de Kafka para el proyecto GameInsight.

Topic usado:
- "Game-Store"

Dónde se ejecuta:
Dentro del contenedor spark-master.

Comando de ejemplo:
docker exec -it gamestore-spark-master python3 /opt/spark-apps/generate_streaming_events.py --events 500 --delay 0.1
"""

from pathlib import Path
from datetime import datetime
import argparse
import json
import random
import time
import csv

from kafka import KafkaProducer


# ============================================================
# 1. CONFIGURACIÓN DE RUTAS
# ============================================================

BASE_DIR = Path("/opt/spark-data")
RAW_DIR = BASE_DIR / "raw"


# ============================================================
# 2. CONFIGURACIÓN GENERAL
# ============================================================

KAFKA_TOPIC = "Game-Store"

# Nombre del servicio Kafka en Docker
KAFKA_BOOTSTRAP_SERVERS = "broker:9092"

EVENT_TYPES = [
    "venta_creada",
    "venta_confirmada",
    "pago_aprobado",
    "envio_preparado",
    "venta_completada",
    "venta_cancelada"
]

EVENT_WEIGHTS = [
    0.25,
    0.20,
    0.20,
    0.15,
    0.15,
    0.05
]


# ============================================================
# 3. CARGAR DATOS BASE
# ============================================================

def load_reference_data():
    """
    Carga los CSV del proyecto para generar eventos realistas.
    """

    ventas_path = RAW_DIR / "ventas.csv"
    clientes_path = RAW_DIR / "clientes.csv"

    if not ventas_path.exists():
        raise FileNotFoundError(f"No existe: {ventas_path}")

    if not clientes_path.exists():
        raise FileNotFoundError(f"No existe: {clientes_path}")

    ventas = []
    clientes = {}

    # Leer ventas
    with open(ventas_path, "r", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)

        for row in reader:
            ventas.append(row)

    # Leer clientes
    with open(clientes_path, "r", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)

        for row in reader:
            clientes[row["cliente_id"]] = row

    return {
        "ventas": ventas,
        "clientes": clientes
    }


# ============================================================
# 4. CALLBACK DE ENTREGA
# ============================================================

def delivery_report(err, msg):
    """
    Confirmación de envío a Kafka.
    """

    if err is not None:
        print(f"Error enviando mensaje: {err}")


# ============================================================
# 5. CREAR EVENTO
# ============================================================

def create_event(event_number, reference_data):
    """
    Genera un evento streaming basado en ventas reales.
    """

    ventas = reference_data["ventas"]
    clientes = reference_data["clientes"]

    venta = random.choice(ventas)

    cliente_id = venta["cliente_id"]

    cliente = clientes.get(cliente_id, {})

    event_type = random.choices(
        EVENT_TYPES,
        weights=EVENT_WEIGHTS,
        k=1
    )[0]

    precio = float(venta["precio"])
    cantidad = int(venta["cantidad"])

    total = round(precio * cantidad, 2)

    riesgo_fraude = round(random.uniform(0.01, 0.95), 2)

    evento = {
        "event_id": f"EVT-{event_number:06d}",
        "event_type": event_type,
        "event_timestamp": datetime.now().isoformat(timespec="seconds"),

        "venta_id": venta["venta_id"],
        "cliente_id": cliente_id,
        "juego_id": venta["juego_id"],

        "plataforma": venta["plataforma"],
        "metodo_pago": venta["metodo_pago"],
        "vendedor": venta["vendedor"],

        "precio": precio,
        "cantidad": cantidad,
        "total": total,

        "cliente_nombre": cliente.get("nombre", "desconocido"),
        "nivel_membresia": cliente.get("nivel_membresia", "Normal"),
        "distrito": cliente.get("distrito", "Desconocido"),

        "riesgo_fraude": riesgo_fraude,
        "venta_riesgosa": riesgo_fraude >= 0.75
    }

    return evento


# ============================================================
# 6. MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description="Generador Streaming - GameInsight"
    )

    parser.add_argument(
        "--events",
        type=int,
        default=500,
        help="Cantidad de eventos a enviar"
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=0.1,
        help="Tiempo entre eventos"
    )

    args = parser.parse_args()

    print("=" * 70)
    print(" GAMEINSIGHT — PRODUCTOR STREAMING KAFKA ")
    print("=" * 70)

    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Servidor Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Eventos a enviar: {args.events}")
    print(f"Delay: {args.delay} segundos")

    print("=" * 70)

    # Cargar datos base
    reference_data = load_reference_data()

    # Crear productor Kafka
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Enviar eventos
    for event_number in range(1, args.events + 1):

        evento = create_event(event_number, reference_data)

        producer.send(
            KAFKA_TOPIC,
            key=evento["venta_id"].encode("utf-8"),
            value=evento
        )

        # Mostrar algunos eventos
        if event_number <= 5 or event_number % 100 == 0:
            print(f"Evento enviado {event_number}")
            print(json.dumps(evento, indent=2, ensure_ascii=False))
            print("-" * 70)

        time.sleep(args.delay)

    producer.flush()

    print("=" * 70)
    print("Streaming finalizado correctamente")
    print("=" * 70)


# ============================================================
# 7. EJECUCIÓN
# ============================================================

if __name__ == "__main__":
    main()