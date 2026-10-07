"""Spark Structured Streaming sobre los eventos de la app de viajes (tema «viajes»).

Cada 10 segundos muestra, en ventanas de 15 minutos del tiempo de los viajes (con watermark de 30 minutos):
  1) la ESPERA promedio por zona (solicitud → llega el conductor): la señal que usa una tarifa dinámica;
  2) los INGRESOS por empresa y la parte que retiene la plataforma.

Ejecutar (PowerShell, carpeta kafka, con docker compose up -d; todo en una sola línea):
  docker compose exec spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 /opt/kafka-taller/streaming_viajes.py
"""
import os
import threading

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType

CARPETA = os.path.dirname(os.path.abspath(__file__))
spark = SparkSession.builder.appName("StreamingViajes").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

esquema = (StructType().add("viaje_id", StringType()).add("tipo", StringType()).add("ts", StringType())
           .add("empresa", StringType()).add("PULocationID", IntegerType()).add("request_datetime", StringType())
           .add("base_passenger_fare", DoubleType()).add("driver_pay", DoubleType()).add("tips", DoubleType()))

# startingOffsets = latest: procesa solo lo que llega desde que arranca, como un flujo en tiempo real. Con earliest,
# el primer lote leería también los viajes de la app (hora real de hoy) y el watermark saltaría a esa hora: todo lo
# que publique después el simulador (horas de madrugada) llegaría «tarde» y se descartaría.
eventos = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", os.getenv("KAFKA_INTERNO", "kafka:29092"))
           .option("subscribe", "viajes").option("startingOffsets", "latest").load()
           .select(F.from_json(F.col("value").cast("string"), esquema).alias("e")).select("e.*")
           .where(F.col("tipo").isNotNull())                     # descarta mensajes que no son JSON válido
           .withColumn("event_time", F.to_timestamp("ts"))
           # watermark: se aceptan eventos que lleguen hasta 30 minutos tarde; las ventanas más viejas se cierran y
           # Spark libera su estado (sin esto, la memoria crecería sin límite en un flujo que nunca termina)
           .withWatermark("event_time", "30 minutes"))

zonas = (spark.read.option("header", True).csv(os.path.join(CARPETA, "zonas.csv"))
         .select(F.col("LocationID").cast("int").alias("PULocationID"), F.col("Zone").alias("zona")))

# 1) espera por zona: el evento «llegada» trae la hora de solicitud, así que no hace falta cruzar dos flujos
espera = (eventos.where(F.col("tipo") == "llegada")
          .withColumn("espera_min", (F.col("event_time").cast("long") - F.to_timestamp("request_datetime").cast("long")) / 60)
          # la TLC trae viajes con la llegada registrada antes que la solicitud: espera negativa = error de datos,
          # se excluye (el MapReduce de la mañana los cuenta como «Esperas negativas»)
          .where(F.col("espera_min") >= 0)
          .groupBy(F.window("event_time", "15 minutes").alias("ventana"), "PULocationID")
          .agg(F.count("*").alias("viajes"), F.round(F.avg("espera_min"), 1).alias("espera_prom_min")))

# 2) ingresos por empresa
ingresos = (eventos.where(F.col("tipo") == "pago")
            .groupBy(F.window("event_time", "15 minutes").alias("ventana"), "empresa")
            .agg(F.count("*").alias("viajes_pagados"), F.round(F.sum("base_passenger_fare"), 2).alias("tarifa"),
                 F.round(F.sum("driver_pay"), 2).alias("pago_conductor"))
            .withColumn("plataforma_pct", F.round(100 * (F.col("tarifa") - F.col("pago_conductor")) / F.col("tarifa"), 1)))


IMPRESION = threading.Lock()      # las dos consultas corren en paralelo: se imprime una tabla a la vez


def mostrar_espera(lote, n):
    tabla = (lote.join(F.broadcast(zonas), "PULocationID", "left")
             .select(F.date_format("ventana.start", "dd/MM HH:mm").alias("desde"), "zona", "viajes", "espera_prom_min")
             .orderBy(F.desc("espera_prom_min")))
    with IMPRESION:
        print(f"\n=== Lote {n} · ESPERA por zona (ventanas de 15 min, las 10 con más espera) ===", flush=True)
        tabla.show(10, truncate=False)


def mostrar_ingresos(lote, n):
    tabla = (lote.select(F.date_format("ventana.start", "dd/MM HH:mm").alias("desde"), "empresa", "viajes_pagados",
                         "tarifa", "pago_conductor", "plataforma_pct").orderBy("desde", "empresa"))
    with IMPRESION:
        print(f"\n=== Lote {n} · INGRESOS por empresa (ventanas de 15 min) ===", flush=True)
        tabla.show(20, truncate=False)


q1 = espera.writeStream.outputMode("update").trigger(processingTime="10 seconds").foreachBatch(mostrar_espera).start()
q2 = ingresos.writeStream.outputMode("update").trigger(processingTime="10 seconds").foreachBatch(mostrar_ingresos).start()
spark.streams.awaitAnyTermination()
