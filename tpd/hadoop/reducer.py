#!/usr/bin/env python3
"""Reducer: por empresa suma viajes, tarifa base y pago al conductor, y calcula la parte que retiene la plataforma.

Asume las claves ordenadas: Hadoop lo garantiza después del shuffle (en la prueba local lo hace sort).
Salida: empresa TAB viajes TAB tarifa_base_US$ TAB pago_conductor_US$ TAB parte_plataforma_%

Con --combinar hace la misma suma pero devuelve el formato del mapper (montos en centavos, sin porcentaje):
así puede usarse como combiner, que suma dentro de cada map antes del shuffle.
"""
import sys

COMBINAR = "--combinar" in sys.argv


def emitir(empresa, viajes, tarifa, pago):
    if COMBINAR:
        print(f"{empresa}\t{viajes}\t{tarifa}\t{pago}")
    else:
        plataforma = 100 * (tarifa - pago) / tarifa if tarifa else 0.0
        print(f"{empresa}\t{viajes}\t{tarifa / 100:.2f}\t{pago / 100:.2f}\t{plataforma:.1f}")


actual, viajes, tarifa, pago = None, 0, 0, 0
for linea in sys.stdin:
    empresa, v, t, p = linea.rstrip("\n").split("\t")
    if empresa != actual:
        if actual is not None:
            emitir(actual, viajes, tarifa, pago)
        actual, viajes, tarifa, pago = empresa, 0, 0, 0
    viajes += int(v)
    tarifa += int(t)
    pago += int(p)
if actual is not None:  # la última empresa no tiene una clave siguiente que la cierre
    emitir(actual, viajes, tarifa, pago)
