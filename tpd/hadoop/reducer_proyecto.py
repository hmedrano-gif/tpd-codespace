#!/usr/bin/env python3
"""Reducer del Proyecto Final: suma los valores de cada clave y, al final, calcula los indicadores de la pregunta.

Uso en Hadoop Streaming:  -reducer "python3 reducer_proyecto.py p3"
                          -combiner "python3 reducer_proyecto.py p3 --combinar"
Recibe «clave TAB v1 TAB v2 ...» ordenado por clave y suma columna por columna mientras la clave se repite.
Con --combinar devuelve el mismo formato de entrada (las sumas), para usarlo como combiner.
Sin --combinar escribe una fila con encabezado por clave: las columnas de cada pregunta están en SALIDA.
"""
import csv
import os
import sys

PREGUNTA = sys.argv[1]
COMBINAR = "--combinar" in sys.argv
ZONA = {}
if not COMBINAR and os.path.exists("zonas.csv"):
    with open("zonas.csv", newline="", encoding="utf-8") as f:
        ZONA = {r["LocationID"]: (r["Zone"], r["Borough"]) for r in csv.DictReader(f)}


def usd(centavos):
    return f"{centavos / 100:.2f}"


def pct(parte, total):
    return f"{100 * parte / total:.2f}" if total else "0.00"


# Por pregunta: columnas de la salida y cómo se calculan a partir de la clave y las sumas (v)
SALIDA = {
    "p1": ("hora,viajes,tarifa_base_usd,plataforma_usd,plataforma_pct",
           lambda k, v: [k, v[0], usd(v[1]), usd(v[1] - v[2]), pct(v[1] - v[2], v[1])]),
    "p2": ("dia,empresa,viajes,tarifa_base_usd,tarifa_promedio_usd",
           lambda k, v: [*k.split("|"), v[0], usd(v[1]), usd(v[1] / v[0])]),
    "p3": ("borough,empresa,viajes,viajes_con_propina,pct_con_propina,propina_total_usd,propina_promedio_cuando_hay_usd",
           lambda k, v: [*k.split("|"), v[0], v[1], pct(v[1], v[0]), usd(v[2]), usd(v[2] / v[1] if v[1] else 0)]),
    "p4": ("aeropuerto,sentido,viajes,tarifa_promedio_usd,cargo_aeropuerto_usd,plataforma_usd",
           lambda k, v: [*k.split("|"), v[0], usd(v[1] / v[0]), usd(v[2]), usd(v[1] - v[3])]),
    "p5": ("borough,empresa,viajes,pago_conductor_usd,millas,pago_por_milla_usd",
           lambda k, v: [*k.split("|"), v[0], usd(v[1]), f"{v[2] / 100:.0f}", f"{v[1] / v[2]:.2f}"]),
    "p6": ("hora,viajes,millas,horas_de_viaje,velocidad_mph,duracion_promedio_min",
           lambda k, v: [k, v[0], f"{v[1] / 100:.0f}", f"{v[2] / 3600:.0f}", f"{(v[1] / 100) / (v[2] / 3600):.2f}",
                         f"{v[2] / v[0] / 60:.2f}"]),
    "p7": ("zona_id,zona,borough,viajes,tarifa_base_usd,plataforma_usd,plataforma_por_viaje_usd",
           lambda k, v: [k, *ZONA.get(k, ("zona " + k, "Desconocido")), v[0], usd(v[1]), usd(v[1] - v[2]),
                         usd((v[1] - v[2]) / v[0])]),
    "p8": ("hora,viajes,viajes_con_cargo,pct_con_cargo,recaudo_usd",
           lambda k, v: [k, v[0], v[1], pct(v[1], v[0]), usd(v[2])]),
}
ENCABEZADO, FILA = SALIDA[PREGUNTA]


def emitir(clave, sumas):
    print(clave, *sumas, sep="\t") if COMBINAR else print(*FILA(clave, sumas), sep="\t")


if not COMBINAR:
    # el encabezado empieza con «#» para ordenarlo arriba y para que el tablero lo reconozca
    print("#" + ENCABEZADO.replace(",", "\t"))
actual, sumas = None, None
for linea in sys.stdin:
    clave, *valores = linea.rstrip("\n").split("\t")
    if clave != actual:
        if actual is not None:
            emitir(actual, sumas)
        actual, sumas = clave, [0] * len(valores)
    sumas = [s + int(x) for s, x in zip(sumas, valores)]
if actual is not None:
    emitir(actual, sumas)
