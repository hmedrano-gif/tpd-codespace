"""Verifica la PRECISIÓN del flujo: compara lo que llegó a Kafka con el archivo de origen de la TLC.

Lee del tema «viajes» los eventos que publicó simulador.py para un día y los compara con los mismos viajes leídos
directamente del Parquet: viajes, eventos por tipo, duplicados, y tarifa y pago al conductor por empresa, al centavo.

Uso (con los mismos --dia y --viajes que se usaron en el simulador):
    python verificar_flujo.py ..\\datos\\fhvhv_tripdata_2025-06.parquet --dia 2025-06-05 --viajes 1000
"""
import argparse
import json
from collections import Counter, defaultdict

import pandas as pd
from kafka import KafkaConsumer

import kafka_conexion as K

EMPRESA = {"HV0003": "Uber", "HV0005": "Lyft"}
TIPOS = ["solicitud", "asignado", "llegada", "recogida", "fin_viaje", "pago"]

a = argparse.ArgumentParser()
a.add_argument("archivo")
a.add_argument("--dia", default="2025-06-02")
a.add_argument("--viajes", type=int, default=500)
args = a.parse_args()
prefijo = f"tlc{args.dia.replace('-', '')}_"
centavos = lambda x: round(float(x) * 100)

# 1) Origen: los mismos viajes que selecciona el simulador (mismo filtro, mismo orden)
inicio = pd.Timestamp(args.dia)
origen = pd.read_parquet(args.archivo, columns=["hvfhs_license_num", "request_datetime", "base_passenger_fare", "driver_pay"],
                         filters=[("request_datetime", ">=", inicio), ("request_datetime", "<", inicio + pd.Timedelta(days=1))])
origen = origen.sort_values("request_datetime").head(args.viajes)
esperado = defaultdict(lambda: [0, 0, 0])
for r in origen.itertuples():
    e = esperado[EMPRESA.get(r.hvfhs_license_num, r.hvfhs_license_num)]
    e[0] += 1
    e[1] += centavos(r.base_passenger_fare)
    e[2] += centavos(r.driver_pay)

# 2) Flujo: todo el tema desde el principio, sin grupo (no altera los offsets de nadie); termina tras 5 s sin eventos
consumidor = KafkaConsumer(K.TEMA, auto_offset_reset="earliest", group_id=None, consumer_timeout_ms=5000, **K.parametros())
por_tipo, vistos, duplicados, recibido = Counter(), Counter(), 0, defaultdict(lambda: [0, 0, 0])
for m in consumidor:
    try:
        ev = json.loads(m.value.decode("utf-8"))
    except ValueError:
        continue
    if not str(ev.get("viaje_id", "")).startswith(prefijo):
        continue                                   # eventos de la app o de otros días
    clave = (ev["viaje_id"], ev["tipo"])
    vistos[clave] += 1
    if vistos[clave] > 1:
        duplicados += 1
        continue
    por_tipo[ev["tipo"]] += 1
    if ev["tipo"] == "pago":
        x = recibido[ev["empresa"]]
        x[0] += 1
        x[1] += centavos(ev["base_passenger_fare"])
        x[2] += centavos(ev["driver_pay"])
consumidor.close()

# 3) Comparación
print(f"Viajes del {args.dia} publicados por el simulador: {args.viajes:,} (origen: {len(origen):,})")
print("Eventos por tipo en Kafka: " + " · ".join(f"{t} {por_tipo[t]:,}" for t in TIPOS))
print(f"Eventos duplicados: {duplicados}")
print(f"\n{'Empresa':8} {'Viajes (origen / Kafka)':>25} {'Tarifa base US$ (origen / Kafka)':>36} {'Pago conductor US$ (origen / Kafka)':>38}")
ok = duplicados == 0 and all(por_tipo[t] == len(origen) for t in TIPOS)
for emp in sorted(set(esperado) | set(recibido)):
    e, r = esperado[emp], recibido[emp]
    ok &= e == r
    print(f"{emp:8} {e[0]:>12,} / {r[0]:<10,} {e[1] / 100:>17,.2f} / {r[1] / 100:<16,.2f} {e[2] / 100:>18,.2f} / {r[2] / 100:<16,.2f}")
if ok:
    print("\nRESULTADO: PRECISO — ningún evento perdido ni duplicado y montos idénticos al centavo.")
elif duplicados:
    print(f"\nRESULTADO: NO COINCIDE — {duplicados} eventos repetidos: el simulador publicó el {args.dia} más de una vez. "
          "Repite la prueba con otro --dia.")
else:
    print("\nRESULTADO: NO COINCIDE — faltan eventos o montos: revisa si el simulador terminó y si usaste los mismos --dia y --viajes.")
