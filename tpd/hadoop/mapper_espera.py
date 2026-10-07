#!/usr/bin/env python3
"""Mapper del job propio: espera del pasajero por zona de recogida.

Espera = pickup_datetime − request_datetime (desde que pide el viaje hasta que lo recogen), en segundos.
Emite «zona TAB segundos_de_espera TAB 1»: la suma y el conteo, no el promedio, para que el promedio se pueda
calcular por partes (combiner) y juntar al final (reducer).

Columnas del CSV de la TLC que usa (posición empezando en 0):
    3 request_datetime   5 pickup_datetime   7 PULocationID
"""
import csv
import sys
from datetime import datetime

for campos in csv.reader(sys.stdin):
    if len(campos) != 25 or campos[0] == "hvfhs_license_num":
        continue
    try:
        pedido = datetime.fromisoformat(campos[3])
        recogida = datetime.fromisoformat(campos[5])
    except ValueError:
        sys.stderr.write("reporter:counter:Caso viajes,Fechas invalidas,1\n")
        continue
    espera = (recogida - pedido).total_seconds()
    if espera < 0:
        sys.stderr.write("reporter:counter:Caso viajes,Esperas negativas,1\n")
        continue                                   # registro inconsistente: recogido antes de pedirlo
    print(f"{campos[7]}\t{int(espera)}\t1")
