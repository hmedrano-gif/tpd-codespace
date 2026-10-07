#!/usr/bin/env python3
"""Reducer del job propio: espera promedio por zona de recogida.

Recibe «zona TAB suma_segundos TAB conteo» ordenado por zona y suma mientras la zona se repite.
Salida: zona TAB nombre_de_la_zona TAB viajes TAB espera_promedio_min
Con --combinar devuelve el mismo formato de entrada (suma y conteo), para usarlo como combiner.
El nombre de la zona sale de zonas.csv, que se envía junto con -files (tabla oficial de zonas de la TLC).
"""
import csv
import os
import sys

COMBINAR = "--combinar" in sys.argv
NOMBRES = {}
if not COMBINAR and os.path.exists("zonas.csv"):
    with open("zonas.csv", newline="", encoding="utf-8") as f:
        NOMBRES = {r["LocationID"]: f'{r["Zone"]} ({r["Borough"]})' for r in csv.DictReader(f)}


def emitir(zona, suma, conteo):
    if COMBINAR:
        print(f"{zona}\t{suma}\t{conteo}")
    else:
        print(f"{zona}\t{NOMBRES.get(zona, 'zona ' + zona)}\t{conteo}\t{suma / conteo / 60:.2f}")


actual, suma, conteo = None, 0, 0
for linea in sys.stdin:
    zona, s, c = linea.rstrip("\n").split("\t")
    if zona != actual:
        if actual is not None:
            emitir(actual, suma, conteo)
        actual, suma, conteo = zona, 0, 0
    suma += int(s)
    conteo += int(c)
if actual is not None:
    emitir(actual, suma, conteo)
