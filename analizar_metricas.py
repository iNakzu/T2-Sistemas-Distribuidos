import pandas as pd
import sys

if len(sys.argv) < 2:
    print("Uso: python3 analizar_metricas.py archivo.csv")
    sys.exit(1)

archivo = sys.argv[1]

df = pd.read_csv(archivo)

hits = (df["event_type"] == "hit").sum()
misses = (df["event_type"] == "miss").sum()
total = hits + misses

hit_rate = hits / total if total > 0 else 0
miss_rate = misses / total if total > 0 else 0

lat_prom = df["latency_ms"].mean()
p50 = df["latency_ms"].quantile(0.50)
p95 = df["latency_ms"].quantile(0.95)

print("Archivo:", archivo)
print("Total consultas:", total)
print("Hits:", hits)
print("Misses:", misses)
print("Hit rate:", round(hit_rate, 4))
print("Miss rate:", round(miss_rate, 4))
print("Latencia promedio ms:", round(lat_prom, 4))
print("Latencia p50 ms:", round(p50, 4))
print("Latencia p95 ms:", round(p95, 4))
