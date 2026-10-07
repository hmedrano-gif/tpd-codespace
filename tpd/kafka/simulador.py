r"""Simulador de volumen: publica en Kafka viajes REALES de la TLC como los seis eventos de la app.

Toma los viajes de un día del archivo de la TLC (Parquet), ordena todos los eventos por su hora real y los publica
a la velocidad indicada. La central y Spark Streaming los reciben igual que los viajes hechos a mano en la app.

Ejemplo:  python simulador.py ..\datos\fhvhv_tripdata_2025-06.parquet --dia 2025-06-02 --viajes 500 --eventos-por-segundo 100
"""
import argparse
import json
import time

import pandas as pd
from kafka import KafkaProducer

import kafka_conexion as K

EMPRESA = {"HV0003": "Uber", "HV0005": "Lyft"}
COLS = ["hvfhs_license_num", "request_datetime", "on_scene_datetime", "pickup_datetime", "dropoff_datetime",
        "PULocationID", "DOLocationID", "trip_miles", "trip_time", "base_passenger_fare", "tips", "driver_pay"]

a = argparse.ArgumentParser()
a.add_argument("archivo")
a.add_argument("--dia", default="2025-06-02")
a.add_argument("--viajes", type=int, default=500)
a.add_argument("--eventos-por-segundo", type=float, default=100)
a.add_argument("--fecha-original", action="store_true", help="conservar la fecha real de 2025 (por defecto los viajes se trasladan a hoy)")
args = a.parse_args()

inicio = pd.Timestamp(args.dia)
df = pd.read_parquet(args.archivo, columns=COLS,
                     filters=[("request_datetime", ">=", inicio), ("request_datetime", "<", inicio + pd.Timedelta(days=1))])
df = df.sort_values("request_datetime").head(args.viajes).reset_index(drop=True)
# Traslado a hoy: se conserva la hora del día y la duración real de cada viaje, pero con la fecha de hoy,
# para que los viajes simulados y los hechos a mano en la app queden en la misma línea de tiempo.
corrimiento = pd.Timedelta(0) if args.fecha_original else pd.Timestamp.now().normalize() - inicio
iso = lambda x: (pd.Timestamp(x) + corrimiento).isoformat(timespec="seconds")

eventos = []
for i, r in df.iterrows():
    vid, emp = f"tlc{args.dia.replace('-', '')}_{i:06d}", EMPRESA.get(r.hvfhs_license_num, r.hvfhs_license_num)
    llegada = r.on_scene_datetime if pd.notna(r.on_scene_datetime) else r.pickup_datetime   # on_scene solo existe en algunos viajes
    base = {"viaje_id": vid, "empresa": emp, "conductor": "Conductor TLC"}
    eventos += [
        {**base, "tipo": "solicitud", "ts": iso(r.request_datetime), "cliente": "Pasajero TLC", "PULocationID": int(r.PULocationID),
         "DOLocationID": int(r.DOLocationID), "millas": float(r.trip_miles), "tarifa_estimada": float(r.base_passenger_fare)},
        {**base, "tipo": "asignado", "ts": iso(r.request_datetime)},
        {**base, "tipo": "llegada", "ts": iso(llegada), "PULocationID": int(r.PULocationID), "request_datetime": iso(r.request_datetime)},
        {**base, "tipo": "recogida", "ts": iso(r.pickup_datetime)},
        {**base, "tipo": "fin_viaje", "ts": iso(r.dropoff_datetime), "trip_miles": float(r.trip_miles), "trip_time": int(r.trip_time)},
        {**base, "tipo": "pago", "ts": iso(r.dropoff_datetime), "base_passenger_fare": float(r.base_passenger_fare), "cargos": 0.0,
         "tips": float(r.tips), "driver_pay": float(r.driver_pay)},
    ]
orden = {"solicitud": 0, "asignado": 1, "llegada": 2, "recogida": 3, "fin_viaje": 4, "pago": 5}
eventos.sort(key=lambda e: (e["ts"], orden[e["tipo"]]))

p = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode("utf-8"), key_serializer=lambda k: k.encode("utf-8"),
                  **K.parametros())
K.asegurar_tema()
pausa, t0 = 1 / args.eventos_por_segundo, time.time()
for n, e in enumerate(eventos, 1):
    p.send(K.TEMA, key=e["viaje_id"], value=e)
    if n % 500 == 0:
        print(f"{n:,} eventos enviados · hora simulada {e['ts'][11:]}")
    time.sleep(pausa)
p.flush()
print(f"Listo: {len(df):,} viajes reales = {len(eventos):,} eventos en {time.time() - t0:.0f} s")
