import pandas as pd
import sys

if len(sys.argv) < 2:
    print("Uso: python analizar_metricas.py archivo.csv")
    sys.exit(1)

archivo = sys.argv[1]
try:
    df = pd.read_csv(archivo)
except FileNotFoundError:
    print(f"Error: No se encontró el archivo {archivo}")
    sys.exit(1)

if df.empty:
    print("El archivo de métricas está vacío.")
    sys.exit(0)

# Contar eventos
hits = (df["event_type"] == "hit").sum()
misses = (df["event_type"] == "miss").sum()
retries = (df["event_type"] == "retry").sum()
dlqs = (df["event_type"] == "dlq").sum()
recovered = (df["event_type"] == "recovered").sum()

# Cálculos globales
total_intentos = len(df)
total_exitosos = hits + misses + recovered

hit_rate = hits / total_exitosos if total_exitosos > 0 else 0
miss_rate = misses / total_exitosos if total_exitosos > 0 else 0

# Tasas solicitadas en la pauta
retry_rate = retries / total_intentos if total_intentos > 0 else 0
dlq_rate = dlqs / total_intentos if total_intentos > 0 else 0
# Recovery rate: Porcentaje de consultas recuperadas exitosamente vs las que fallaron 
recovery_rate = recovered / (retries + recovered) if (retries + recovered) > 0 else 0 

# Filtrar solo latencias de eventos exitosos para las métricas de tiempo
df_exitosos = df[df["event_type"].isin(["hit", "miss", "recovered"])]

lat_prom = df_exitosos["latency_ms"].mean() if not df_exitosos.empty else 0
p50 = df_exitosos["latency_ms"].quantile(0.50) if not df_exitosos.empty else 0
p95 = df_exitosos["latency_ms"].quantile(0.95) if not df_exitosos.empty else 0

print("-" * 45)
print("Metricas: ")
print(f"Archivo analizado: {archivo}")
print(f"Total eventos registrados: {total_intentos}")
print(f"Consultas completadas con éxito: {total_exitosos}")
print("-" * 45)
print(f"Hits en Caché: {hits} ({round(hit_rate * 100, 2)}%)")
print(f"Misses directos: {misses} ({round(miss_rate * 100, 2)}%)")
print(f"Recuperados tras fallo: {recovered}")
print(f"Enviados a Reintento: {retries}")
print(f"Enviados a DLQ (Pérdida): {dlqs}")
print("-" * 45)
print(f"Retry rate: {round(retry_rate * 100, 2)}%")
print(f"Recovery rate: {round(recovery_rate * 100, 2)}%")
print(f"DLQ rate: {round(dlq_rate * 100, 2)}%")
print("-" * 45)
print(f"Latencia promedio: {round(lat_prom, 2)} ms")
print(f"Latencia p50: {round(p50, 2)} ms")
print(f"Latencia p95: {round(p95, 2)} ms")
print("-" * 45)