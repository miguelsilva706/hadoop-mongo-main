"""
GameInsight - Visualización de KPIs
Genera gráficos PNG a partir de los KPIs creados con Spark SQL.

Salida:
output/charts/
"""

import os
import pandas as pd
import matplotlib.pyplot as plt

# ── CONFIGURACIÓN ─────────────────────────────────────────────────────────────
BASE_PATH = "/outputs/kpis"
OUT_PATH = "/outputs/charts"

os.makedirs(OUT_PATH, exist_ok=True)

print("=" * 60)
print("  GAMEINSIGHT — VISUALIZACIÓN DE KPIs")
print("=" * 60)

# ── FUNCIÓN AUXILIAR ──────────────────────────────────────────────────────────
def leer_csv_spark(path):
    """
    Spark genera carpetas con archivos part-xxxxx.csv.
    Esta función encuentra automáticamente el CSV correcto.
    """

    archivos = [
        f for f in os.listdir(path)
        if f.endswith(".csv")
    ]

    if not archivos:
        raise Exception(f"No se encontraron CSV en {path}")

    archivo_csv = os.path.join(path, archivos[0])

    return pd.read_csv(archivo_csv)

# ═══════════════════════════════════════════════════════════════════════════════
# KPI 1 — TOP JUEGOS
# ═══════════════════════════════════════════════════════════════════════════════
print("\n[1/6] Generando gráfico: Top Juegos")

top_juegos = leer_csv_spark(f"{BASE_PATH}/top_juegos")

plt.figure(figsize=(14, 7))

bars = plt.bar(
    top_juegos["titulo"],
    top_juegos["total_unidades"]
)

# Mostrar valores encima de las barras
for bar in bars:
    altura = bar.get_height()

    plt.text(
        bar.get_x() + bar.get_width()/2,
        altura,
        f'{int(altura)}',
        ha='center',
        va='bottom'
    )

plt.xticks(rotation=45, ha="right")
plt.ylabel("Unidades Vendidas")
plt.title("Top Juegos Más Vendidos")
plt.tight_layout()

plt.savefig(f"{OUT_PATH}/top_juegos.png")
plt.close()

# ═══════════════════════════════════════════════════════════════════════════════
# KPI 2 — CATEGORÍAS
# ═══════════════════════════════════════════════════════════════════════════════
print("[2/6] Generando gráfico: Categorías")

categorias = leer_csv_spark(f"{BASE_PATH}/categorias")

plt.figure(figsize=(10, 8))

plt.pie(
    categorias["ingresos_total"],
    labels=categorias["categoria"],
    autopct="%1.1f%%"
)

plt.title("Participación por Categoría")
plt.tight_layout()

plt.savefig(f"{OUT_PATH}/categorias.png")
plt.close()

# ═══════════════════════════════════════════════════════════════════════════════
# KPI 3 — PLATAFORMAS
# ═══════════════════════════════════════════════════════════════════════════════
print("[3/6] Generando gráfico: Plataformas")

plataformas = leer_csv_spark(f"{BASE_PATH}/plataformas")

plt.figure(figsize=(12, 6))

bars = plt.bar(
    plataformas["plataforma"],
    plataformas["ingresos_total"]
)

# Mostrar valores encima
for bar in bars:
    altura = bar.get_height()

    plt.text(
        bar.get_x() + bar.get_width()/2,
        altura,
        f'S/{altura:,.0f}',
        ha='center',
        va='bottom'
    )

plt.ylabel("Ingresos Totales")
plt.title("Ingresos por Plataforma")
plt.tight_layout()

plt.savefig(f"{OUT_PATH}/plataformas.png")
plt.close()

# ═══════════════════════════════════════════════════════════════════════════════
# KPI 4 — VENTAS MENSUALES
# ═══════════════════════════════════════════════════════════════════════════════
print("[4/6] Generando gráfico: Ventas Mensuales")

ventas_mensuales = leer_csv_spark(f"{BASE_PATH}/ventas_mensuales")

plt.figure(figsize=(12, 6))

plt.plot(
    ventas_mensuales["mes"],
    ventas_mensuales["ingresos_mes"],
    marker="o"
)

# Mostrar valores en cada punto
for x, y in zip(
    ventas_mensuales["mes"],
    ventas_mensuales["ingresos_mes"]
):
    plt.text(
        x,
        y,
        f'S/{y:,.0f}',
        ha='center',
        va='bottom'
    )

plt.xlabel("Mes")
plt.ylabel("Ingresos")
plt.title("Tendencia Mensual de Ventas")
plt.grid(True)

plt.tight_layout()

plt.savefig(f"{OUT_PATH}/ventas_mensuales.png")
plt.close()

# ═══════════════════════════════════════════════════════════════════════════════
# KPI 5 — TOP CLIENTES
# ═══════════════════════════════════════════════════════════════════════════════
print("[5/6] Generando gráfico: Top Clientes")

top_clientes = leer_csv_spark(f"{BASE_PATH}/top_clientes")

plt.figure(figsize=(14, 7))

bars = plt.bar(
    top_clientes["nombre"],
    top_clientes["gasto_total"]
)

# Mostrar valores encima
for bar in bars:
    altura = bar.get_height()

    plt.text(
        bar.get_x() + bar.get_width()/2,
        altura,
        f'S/{altura:,.0f}',
        ha='center',
        va='bottom'
    )

plt.xticks(rotation=45, ha="right")
plt.ylabel("Gasto Total")
plt.title("Top Clientes por Gasto")
plt.tight_layout()

plt.savefig(f"{OUT_PATH}/top_clientes.png")
plt.close()

# ═══════════════════════════════════════════════════════════════════════════════
# KPI 6 — STOCK BAJO
# ═══════════════════════════════════════════════════════════════════════════════
print("[6/6] Generando gráfico: Stock Bajo")

stock_bajo = leer_csv_spark(f"{BASE_PATH}/stock_bajo")

plt.figure(figsize=(14, 7))

bars = plt.bar(
    stock_bajo["titulo"],
    stock_bajo["stock"]
)

# Mostrar valores encima
for bar in bars:
    altura = bar.get_height()

    plt.text(
        bar.get_x() + bar.get_width()/2,
        altura,
        f'{int(altura)}',
        ha='center',
        va='bottom'
    )

plt.xticks(rotation=45, ha="right")
plt.ylabel("Stock")
plt.title("Juegos con Stock Bajo")
plt.tight_layout()

plt.savefig(f"{OUT_PATH}/stock_bajo.png")
plt.close()

# ── FINAL ─────────────────────────────────────────────────────────────────────
print("\n✓ Gráficos generados correctamente")
print(f"✓ Ubicación: {OUT_PATH}")

print("\nArchivos generados:")

for archivo in os.listdir(OUT_PATH):
    print(" -", archivo)

print("\n[Visualización completada]")