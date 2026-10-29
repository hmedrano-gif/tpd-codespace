#!/usr/bin/env python3
"""Mapper del Proyecto Final: una función por pregunta de negocio (p1 a p8).

Uso en Hadoop Streaming:  -mapper "python3 mapper_proyecto.py p3"
Cada función recibe las 25 columnas de un viaje y devuelve la clave y los valores que se van a sumar.
Todos los valores son enteros (centavos, centésimas de milla, segundos, conteos): así la suma es exacta y el
combiner puede sumar por partes sin cambiar el resultado. Los promedios y porcentajes los calcula el reducer al final.

Columnas del CSV de la TLC (posición empezando en 0):
    0 hvfhs_license_num  5 pickup_datetime  7 PULocationID  8 DOLocationID  9 trip_miles  10 trip_time
   11 base_passenger_fare  16 airport_fee  17 tips  18 driver_pay  24 cbd_congestion_fee
"""
import csv
import os
import sys
from datetime import date

EMPRESA = {"HV0003": "Uber", "HV0005": "Lyft"}
DIAS = ["1-lunes", "2-martes", "3-miercoles", "4-jueves", "5-viernes", "6-sabado", "7-domingo"]
AEROPUERTOS = {"1": "Newark", "132": "JFK", "138": "LaGuardia"}   # zonas oficiales de la TLC
BOROUGH = {}
if os.path.exists("zonas.csv"):                                   # llega al nodo con -files
    with open("zonas.csv", newline="", encoding="utf-8") as f:
        # la zona 265 («Outside of NYC») trae borough «N/A»
        BOROUGH = {r["LocationID"]: r["Borough"].replace("N/A", "Fuera de NYC") for r in csv.DictReader(f)}


def centavos(texto):
    # round() y no int(): 17.82 * 100 da 1781.9999... en binario
    return round(float(texto or 0) * 100)


def empresa(c):
    return EMPRESA.get(c[0], c[0])


def hora(c):
    return c[5][11:13]                                            # «2025-06-01 00:09:41» → «00»


def p1(c):
    """Horas pico y margen: viajes, tarifa base y pago al conductor por hora de recogida."""
    yield hora(c), (1, centavos(c[11]), centavos(c[18]))


def p2(c):
    """Día de la semana: viajes y tarifa base por día y empresa."""
    dia = DIAS[date.fromisoformat(c[5][:10]).weekday()]
    yield f"{dia}|{empresa(c)}", (1, centavos(c[11]))


def p3(c):
    """Propinas: viajes, viajes con propina y monto de propinas por borough de origen y empresa."""
    propina = centavos(c[17])
    yield f"{BOROUGH.get(c[7], 'Desconocido')}|{empresa(c)}", (1, 1 if propina > 0 else 0, propina)


def p4(c):
    """Aeropuertos: viajes desde y hacia Newark, JFK y LaGuardia, con tarifa, cargo de aeropuerto y pago."""
    for col, sentido in ((7, "desde"), (8, "hacia")):
        if c[col] in AEROPUERTOS:
            yield f"{AEROPUERTOS[c[col]]}|{sentido}", (1, centavos(c[11]), centavos(c[16]), centavos(c[18]))


def p5(c):
    """Pago al conductor por milla, por borough de origen y empresa (solo viajes con millas > 0)."""
    millas = centavos(c[9])                                       # centésimas de milla
    if millas > 0:
        yield f"{BOROUGH.get(c[7], 'Desconocido')}|{empresa(c)}", (1, centavos(c[18]), millas)


def p6(c):
    """Velocidad por hora: millas y segundos de viaje por hora de recogida (solo viajes con millas y tiempo > 0)."""
    millas, segundos = centavos(c[9]), int(c[10] or 0)
    if millas > 0 and segundos > 0:
        yield hora(c), (1, millas, segundos)


def p7(c):
    """Zonas que más dejan a la plataforma: viajes, tarifa base y pago al conductor por zona de origen."""
    yield c[7], (1, centavos(c[11]), centavos(c[18]))


def p8(c):
    """Cargo de congestión de Manhattan (CBD): viajes, viajes con cargo y recaudo por hora de recogida."""
    cargo = centavos(c[24])
    yield hora(c), (1, 1 if cargo > 0 else 0, cargo)


PREGUNTA = {"p1": p1, "p2": p2, "p3": p3, "p4": p4, "p5": p5, "p6": p6, "p7": p7, "p8": p8}[sys.argv[1]]

for campos in csv.reader(sys.stdin):
    if len(campos) != 25 or campos[0] == "hvfhs_license_num":
        continue                                                  # encabezado o fila incompleta
    try:
        for clave, valores in list(PREGUNTA(campos)):   # list(): si un valor falla, no imprime nada del viaje
            print(clave, *valores, sep="\t")
    except ValueError:
        # contador propio de Hadoop Streaming: aparece junto a los contadores del job
        sys.stderr.write("reporter:counter:Caso viajes,Filas descartadas,1\n")
