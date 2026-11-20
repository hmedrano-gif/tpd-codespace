"""Prepara los datos del caso: viajes de Uber y Lyft en Nueva York, junio de 2025 u otro mes (TLC, High Volume FHV).

Uso (PowerShell, desde la carpeta datos):
    pip install pyarrow
    python preparar_datos.py            -> mes completo: fhvhv_2025-06.csv (19.868.009 viajes, 4,04 GB)
    python preparar_datos.py --semana   -> plan B: fhvhv_2025-06_semana.csv (2 al 8 de junio, mismas 25 columnas)
    python preparar_datos.py --mes 2025-09  -> otro mes de 2025: fhvhv_2025-09.csv (el mes de cada grupo)

1. Si no está el Parquet oficial (491 MB), lo descarga del servidor de la TLC.
2. Lo convierte a CSV por grupos de filas, así la memoria no pasa de unos cientos de MB
   (cargar el mes entero de una vez en pandas pediría cerca de 12 GB).
El CSV conserva las 25 columnas y su orden oficial: mapper.py y job_spark.py dependen de ese orden.
"""
import argparse
import re
import time
import urllib.request
from datetime import datetime
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.csv as pv
import pyarrow.parquet as pq

a = argparse.ArgumentParser()
a.add_argument("--semana", action="store_true", help="solo del 2 al 8 de junio (plan B para equipos con poco disco o memoria)")
a.add_argument("--mes", default="2025-06", help="mes de 2025 con el formato AAAA-MM, por ejemplo 2025-09 (por defecto 2025-06)")
args = a.parse_args()
if not re.fullmatch(r"2025-(0[1-9]|1[0-2])", args.mes):
    raise SystemExit(f"Mes no válido: «{args.mes}». Escríbelo así: 2025-01, 2025-04, 2025-09 o 2025-12.")
if args.semana and args.mes != "2025-06":
    raise SystemExit("La opción --semana solo existe para junio (2025-06).")
MES = args.mes
URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{MES}.parquet"
PARQUET = Path(f"fhvhv_tripdata_{MES}.parquet")
# junio quedó medido en clase; para los demás meses el tamaño lo dice el servidor y los viajes, el propio Parquet
BYTES_PARQUET = 490_819_474 if MES == "2025-06" else None
VIAJES = {"mes": 19_868_009, "semana": 4_576_158} if MES == "2025-06" else {}
porcion = "semana" if args.semana else "mes"
salida = Path(f"fhvhv_{MES}_semana.csv" if args.semana else f"fhvhv_{MES}.csv")

if BYTES_PARQUET is None:
    with urllib.request.urlopen(urllib.request.Request(URL, method="HEAD")) as r:
        BYTES_PARQUET = int(r.headers["Content-Length"])

if not PARQUET.exists() or PARQUET.stat().st_size != BYTES_PARQUET:
    print(f"Descargando {URL} ({BYTES_PARQUET / 1e6:.0f} MB)...")
    t0 = time.time()

    def avance(bloques, tam_bloque, total):
        hecho = min(bloques * tam_bloque, total)
        if bloques % 6000 == 0 or hecho == total:   # una línea cada ~50 MB
            print(f"  {hecho / 1e6:6.0f} de {total / 1e6:.0f} MB")

    urllib.request.urlretrieve(URL, PARQUET, avance)
    if PARQUET.stat().st_size != BYTES_PARQUET:
        raise SystemExit(f"Descarga incompleta: {PARQUET.stat().st_size:,} bytes (se esperaban {BYTES_PARQUET:,}). Vuelve a ejecutar.")
    print(f"Descargado en {time.time() - t0:.0f} s")

archivo = pq.ParquetFile(PARQUET)
print(f"Parquet: {archivo.metadata.num_rows:,} viajes, {archivo.metadata.num_columns} columnas, "
      f"{archivo.metadata.num_row_groups} grupos de filas")
t0, filas = time.time(), 0
with pv.CSVWriter(salida, archivo.schema_arrow) as escritor:
    for g in range(archivo.metadata.num_row_groups):
        tabla = archivo.read_row_group(g)
        if args.semana:
            recogida = tabla.column("pickup_datetime")
            tabla = tabla.filter(pc.and_(pc.greater_equal(recogida, datetime(2025, 6, 2)),
                                         pc.less(recogida, datetime(2025, 6, 9))))
        escritor.write_table(tabla)
        filas += tabla.num_rows
        print(f"  grupo {g + 1:2d} de {archivo.metadata.num_row_groups}: {filas:,} viajes escritos")

print(f"{salida}: {filas:,} viajes, {salida.stat().st_size / 1e9:.2f} GB, {time.time() - t0:.0f} s")
if filas != VIAJES.get(porcion, archivo.metadata.num_rows if porcion == "mes" else filas):
    raise SystemExit(f"ATENCIÓN: se esperaban {VIAJES[porcion]:,} viajes. Borra el CSV y vuelve a ejecutar.")
print("Punto de control correcto: la cantidad de viajes es la esperada para esta porción.")
