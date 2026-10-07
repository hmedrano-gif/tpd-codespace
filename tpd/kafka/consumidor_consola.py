"""Consumidor de consola del tema «viajes». Tres modos:

  python consumidor_consola.py            -> VER: muestra cada evento y cómo se completa la fila del viaje
                                             (lee desde el principio, sin grupo: no guarda por dónde va)
  python consumidor_consola.py medir      -> MEDIR: throughput (eventos por segundo) y latencia
                                             (desde que el productor envió el evento hasta que llega aquí);
                                             ábrelo ANTES del simulador: solo cuenta los eventos nuevos
  python consumidor_consola.py caida      -> CAÍDA: procesa despacio y en el grupo «central-caida»; detenlo con
                                             Ctrl+C a mitad y vuelve a ejecutarlo: retoma donde quedó, sin perder
                                             ni repetir eventos

Se detiene con Ctrl+C. En los modos medir y caída también termina solo tras 10 segundos sin eventos nuevos,
contados desde el primer evento que recibe: antes de ese primer evento espera lo que haga falta.
"""
import json
import sys
import time

from kafka import KafkaConsumer

import kafka_conexion as K

modo = sys.argv[1] if len(sys.argv) > 1 else "ver"
if modo not in ("ver", "medir", "caida"):
    raise SystemExit("Uso: python consumidor_consola.py [ver | medir | caida]")

COLUMNAS = ["request_datetime", "on_scene_datetime", "pickup_datetime", "dropoff_datetime",
            "trip_miles", "base_passenger_fare", "tips", "driver_pay"]
CAMPO = {"solicitud": "request_datetime", "llegada": "on_scene_datetime", "recogida": "pickup_datetime",
         "fin_viaje": "dropoff_datetime"}
# medir va sin grupo: así nunca retoma una posición guardada y siempre empieza en lo más nuevo (latest)
GRUPO = {"ver": None, "medir": None, "caida": "central-caida"}[modo]

# medir: solo eventos nuevos (los viejos tendrían una «latencia» igual a su antigüedad); abrirlo ANTES del simulador.
# Espera sin límite el primer evento (el simulador tarda en cargar el Parquet y en abrirse en otra ventana);
# en medir y caída, desde el primer evento termina solo tras 10 s sin eventos (kafka-python relee este valor cada vez).
consumidor = KafkaConsumer(K.TEMA, auto_offset_reset="latest" if modo == "medir" else "earliest", group_id=GRUPO,
                           consumer_timeout_ms=float("inf"), **K.parametros())
print(f"Modo «{modo}» · grupo {GRUPO or '(ninguno)'} · esperando eventos del tema «viajes»... (Ctrl+C para salir)\n")

filas, n, inicio, ultimo, latencias = {}, 0, None, None, []
try:
    for m in consumidor:
        n += 1
        if n == 1 and modo != "ver":
            consumidor.config["consumer_timeout_ms"] = 10_000
        inicio = inicio or time.time()
        ultimo = time.time()
        try:
            e = json.loads(m.value.decode("utf-8"))
        except ValueError:
            print(f"[{n}] partición {m.partition} · offset {m.offset} · MENSAJE DESCARTADO (no es JSON válido)\n")
            continue

        if modo == "medir":
            # m.timestamp: milisegundos en que el productor creó el evento (lo pone Kafka en cada mensaje)
            latencias.append(time.time() * 1000 - m.timestamp)
            if n % 500 == 0:
                seg = time.time() - inicio
                ult = sorted(latencias[-500:])
                print(f"{n:,} eventos en {seg:.1f} s · {n / seg:,.0f} eventos/s · latencia de los últimos 500: "
                      f"mediana {ult[len(ult) // 2]:.0f} ms, máxima {ult[-1]:.0f} ms")
            continue

        if modo == "caida":
            if n == 1:
                print(f"Primer evento de esta ejecución: partición {m.partition}, offset {m.offset}, "
                      f"viaje {e['viaje_id']}, evento «{e['tipo']}»")
            time.sleep(0.05)  # despacio a propósito, para alcanzar a detenerlo a mitad
            if n % 100 == 0:
                print(f"{n} eventos procesados · último: partición {m.partition}, offset {m.offset}")
            continue

        fila = filas.setdefault(e["viaje_id"], dict.fromkeys(COLUMNAS, "—"))
        if e["tipo"] in CAMPO:
            fila[CAMPO[e["tipo"]]] = e["ts"][11:]          # solo la hora, para que quepa en la línea
        for col in ("trip_miles", "base_passenger_fare", "tips", "driver_pay"):
            if col in e:
                fila[col] = e[col]
        llenas = sum(1 for x in fila.values() if x != "—")
        print(f"[{n}] partición {m.partition} · offset {m.offset} · viaje {e['viaje_id']} · evento «{e['tipo']}»")
        print("     " + " | ".join(f"{c}={v}" for c, v in fila.items()) + f"   ({llenas}/{len(COLUMNAS)} columnas)\n")
except KeyboardInterrupt:
    print("\nDetenido con Ctrl+C.")
finally:
    consumidor.close()  # con grupo, confirma (commit) el último offset leído antes de salir
    print(f"Total leído en esta ejecución: {n:,} eventos")
    if modo == "medir" and latencias:
        seg = max(ultimo - inicio, 0.001)   # hasta el último evento: sin contar la espera final de 10 s
        lat = sorted(latencias)
        print(f"Throughput promedio: {n / seg:,.0f} eventos/s · latencia mediana {lat[len(lat) // 2]:.0f} ms "
              f"· percentil 95: {lat[int(len(lat) * 0.95)]:.0f} ms")
