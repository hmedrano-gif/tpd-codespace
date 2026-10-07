"""Ensayo: ¿cabe el mes de viajes en la memoria de un computador con pandas?

No carga el mes entero (podría dejar el equipo sin responder): carga el primer millón de viajes del CSV, mide cuánta
memoria ocupan en pandas y proyecta el mes completo. Luego compara con la memoria RAM de este equipo.

Uso (PowerShell, desde la carpeta datos):
    pip install pandas
    python ensayo_python.py
"""
import ctypes
import os
import time

import pandas as pd

ARCHIVO = "fhvhv_2025-06.csv"
VIAJES_MES = 19_868_009
MUESTRA = 1_000_000


def ram_del_equipo_gb():
    """Memoria física total del equipo (Windows o Linux); None si no se puede leer."""
    class Estado(ctypes.Structure):
        _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]
    try:
        e = Estado()
        e.dwLength = ctypes.sizeof(Estado)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(e))
        return e.ullTotalPhys / 1e9
    except (AttributeError, OSError):
        pass
    try:  # Linux (por ejemplo, GitHub Codespaces)
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1e9
    except (AttributeError, ValueError, OSError):
        return None


print(f"Archivo: {ARCHIVO}, {os.path.getsize(ARCHIVO) / 1e9:.2f} GB en disco")
t0 = time.time()
# las cuatro fechas como fechas (8 bytes cada una) y no como texto: así ocupan un tercio menos de memoria
FECHAS = ["request_datetime", "on_scene_datetime", "pickup_datetime", "dropoff_datetime"]
muestra = pd.read_csv(ARCHIVO, nrows=MUESTRA, parse_dates=FECHAS)
seg = time.time() - t0
gb = muestra.memory_usage(deep=True).sum() / 1e9
print(f"Primer millón de viajes: {gb:.2f} GB en memoria, leídos en {seg:.1f} s")

proyeccion = gb * VIAJES_MES / MUESTRA
minutos = round(seg * VIAJES_MES / MUESTRA / 60)
lectura = "cerca de 1 minuto" if minutos <= 1 else f"unos {minutos} minutos"
print(f"Proyección del mes ({VIAJES_MES:,} viajes): {proyeccion:.1f} GB en memoria, y {lectura} solo en leerlo")
ram = ram_del_equipo_gb()
if ram:
    sistema = "Windows" if os.name == "nt" else "el sistema"
    print(f"Memoria RAM de este equipo: {ram:.1f} GB ({sistema} y los programas abiertos ya usan una parte)")
    if proyeccion > ram * 0.6:
        print("Conclusión: el mes no cabe con holgura en la memoria de este equipo.")
    else:
        print("Conclusión: este equipo alcanza para el mes, pero no para un año (12 meses): el problema vuelve al crecer.")
