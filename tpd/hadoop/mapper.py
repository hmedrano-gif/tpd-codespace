#!/usr/bin/env python3
"""Mapper: por cada viaje emite «empresa TAB 1 TAB tarifa_base TAB pago_conductor» (montos en centavos).

Columnas del CSV de la TLC que usa (posición empezando en 0):
    0 hvfhs_license_num   (HV0003 = Uber, HV0005 = Lyft)
   11 base_passenger_fare (tarifa base, US$)
   18 driver_pay          (pago al conductor, US$)
"""
import csv
import sys

EMPRESA = {"HV0003": "Uber", "HV0005": "Lyft"}


def centavos(texto):
    # round() y no int(): 17.82 * 100 da 1781.9999... en binario
    return round(float(texto) * 100)


# csv.reader y no split(","): los textos van entre comillas y así una coma dentro de un campo
# no desalinea las columnas siguientes
for campos in csv.reader(sys.stdin):
    if len(campos) != 25 or campos[0] == "hvfhs_license_num":
        continue  # encabezado (solo llega al mapper del primer bloque) o fila incompleta
    try:
        tarifa, pago = centavos(campos[11]), centavos(campos[18])
    except ValueError:
        # contador propio de Hadoop Streaming: aparece junto a los contadores del job
        sys.stderr.write("reporter:counter:Caso viajes,Filas descartadas,1\n")
        continue
    print(f"{EMPRESA.get(campos[0], campos[0])}\t1\t{tarifa}\t{pago}")
