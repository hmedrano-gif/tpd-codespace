"""Job Spark del Proyecto Final: la misma pregunta de negocio que el MapReduce (p1 a p8), con el mes completo.

Ejecutar (terminal del codespace, desde tpd/spark), por ejemplo la pregunta 3:
    docker run --rm -it -p 4040:4040 -v "$PWD/..:/opt/tpd" -w /opt/tpd/spark spark:3.5.9-python3 \\
        /opt/spark/bin/spark-submit job_proyecto.py --pregunta p3
Desde el Parquet oficial:     ... job_proyecto.py --pregunta p3 --parquet
Con N núcleos:                ... job_proyecto.py --pregunta p3 --parquet --nucleos 2
El resultado queda en spark/output_proyecto_p3 (un CSV con encabezado) y se imprime en pantalla.
"""
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructType, TimestampType

PREGUNTA = sys.argv[sys.argv.index("--pregunta") + 1]
PARQUET = "--parquet" in sys.argv
NUCLEOS = sys.argv[sys.argv.index("--nucleos") + 1] if "--nucleos" in sys.argv else "*"
ARCHIVO = "../datos/fhvhv_tripdata_2025-06.parquet" if PARQUET else "../datos/fhvhv_2025-06.csv"

TEXTO, FECHA, ENTERO, LARGO, DECIMAL = StringType(), TimestampType(), IntegerType(), LongType(), DoubleType()
esquema = StructType()
for nombre, tipo in [
    ("hvfhs_license_num", TEXTO), ("dispatching_base_num", TEXTO), ("originating_base_num", TEXTO),
    ("request_datetime", FECHA), ("on_scene_datetime", FECHA), ("pickup_datetime", FECHA), ("dropoff_datetime", FECHA),
    ("PULocationID", ENTERO), ("DOLocationID", ENTERO), ("trip_miles", DECIMAL), ("trip_time", LARGO),
    ("base_passenger_fare", DECIMAL), ("tolls", DECIMAL), ("bcf", DECIMAL), ("sales_tax", DECIMAL),
    ("congestion_surcharge", DECIMAL), ("airport_fee", DECIMAL), ("tips", DECIMAL), ("driver_pay", DECIMAL),
    ("shared_request_flag", TEXTO), ("shared_match_flag", TEXTO), ("access_a_ride_flag", TEXTO),
    ("wav_request_flag", TEXTO), ("wav_match_flag", TEXTO), ("cbd_congestion_fee", DECIMAL)]:
    esquema = esquema.add(nombre, tipo)

inicio = time.time()
spark = SparkSession.builder.appName(f"Proyecto_{PREGUNTA}").master(f"local[{NUCLEOS}]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
viajes = spark.read.parquet(ARCHIVO) if PARQUET else spark.read.csv(ARCHIVO, header=True, schema=esquema)
zonas = spark.read.csv("../hadoop/zonas.csv", header=True, inferSchema=True)
# la zona 265 («Outside of NYC») trae borough «N/A»
borough = zonas.select(F.col("LocationID").alias("PULocationID"),
                       F.when(F.col("Borough") == "N/A", "Fuera de NYC").otherwise(F.col("Borough")).alias("borough"))

# montos como decimal(14,2): sumas exactas al centavo, como los centavos enteros del MapReduce
dinero = lambda c: F.col(c).cast("decimal(14,2)")
v = (viajes
     .withColumn("empresa", F.when(F.col("hvfhs_license_num") == "HV0003", "Uber")
                 .when(F.col("hvfhs_license_num") == "HV0005", "Lyft").otherwise(F.col("hvfhs_license_num")))
     .withColumn("hora", F.date_format("pickup_datetime", "HH")))
viajes_n = F.count("*").alias("viajes")
pct = lambda parte, total: F.round(100 * F.col(parte) / F.col(total), 2)

if PREGUNTA == "p1":      # horas pico y margen
    r = (v.groupBy("hora").agg(viajes_n, F.sum(dinero("base_passenger_fare")).alias("tarifa_base_usd"),
                               F.sum(dinero("driver_pay")).alias("pago"))
         .withColumn("plataforma_usd", F.col("tarifa_base_usd") - F.col("pago"))
         .withColumn("plataforma_pct", pct("plataforma_usd", "tarifa_base_usd")).drop("pago").orderBy("hora"))
elif PREGUNTA == "p2":    # día de la semana (dayofweek: 1 = domingo … 7 = sábado)
    nombres = F.element_at(F.array(*[F.lit(d) for d in ["7-domingo", "1-lunes", "2-martes", "3-miercoles", "4-jueves",
                                                        "5-viernes", "6-sabado"]]), F.dayofweek("pickup_datetime"))
    r = (v.withColumn("dia", nombres).groupBy("dia", "empresa")
         .agg(viajes_n, F.sum(dinero("base_passenger_fare")).alias("tarifa_base_usd"))
         .withColumn("tarifa_promedio_usd", F.round(F.col("tarifa_base_usd") / F.col("viajes"), 2)).orderBy("dia", "empresa"))
elif PREGUNTA == "p3":    # propinas
    r = (v.join(F.broadcast(borough), "PULocationID", "left").fillna({"borough": "Desconocido"})
         .groupBy("borough", "empresa")
         .agg(viajes_n, F.sum((F.col("tips") > 0).cast("int")).alias("viajes_con_propina"),
              F.sum(dinero("tips")).alias("propina_total_usd"))
         .withColumn("pct_con_propina", pct("viajes_con_propina", "viajes"))
         .withColumn("propina_promedio_cuando_hay_usd", F.round(F.col("propina_total_usd") / F.col("viajes_con_propina"), 2))
         .orderBy("borough", "empresa"))
elif PREGUNTA == "p4":    # aeropuertos: un viaje entre dos aeropuertos cuenta una vez en «desde» y otra en «hacia»
    aero = {1: "Newark", 132: "JFK", 138: "LaGuardia"}
    nombre = lambda c: F.coalesce(*[F.when(F.col(c) == k, F.lit(n)) for k, n in aero.items()])
    partes = [v.withColumn("aeropuerto", nombre(c)).withColumn("sentido", F.lit(s)).where(F.col("aeropuerto").isNotNull())
              for c, s in (("PULocationID", "desde"), ("DOLocationID", "hacia"))]
    r = (partes[0].unionByName(partes[1]).groupBy("aeropuerto", "sentido")
         .agg(viajes_n, F.sum(dinero("base_passenger_fare")).alias("tarifa"), F.sum(dinero("airport_fee")).alias("cargo_aeropuerto_usd"),
              F.sum(dinero("driver_pay")).alias("pago"))
         .withColumn("tarifa_promedio_usd", F.round(F.col("tarifa") / F.col("viajes"), 2))
         .withColumn("plataforma_usd", F.col("tarifa") - F.col("pago")).drop("tarifa", "pago").orderBy("aeropuerto", "sentido"))
elif PREGUNTA == "p5":    # pago al conductor por milla
    r = (v.where(F.round(F.col("trip_miles") * 100) > 0).join(F.broadcast(borough), "PULocationID", "left")
         .fillna({"borough": "Desconocido"}).groupBy("borough", "empresa")
         .agg(viajes_n, F.sum(dinero("driver_pay")).alias("pago_conductor_usd"), F.sum(dinero("trip_miles")).alias("millas"))
         .withColumn("pago_por_milla_usd", F.round(F.col("pago_conductor_usd") / F.col("millas"), 2)).orderBy("borough", "empresa"))
elif PREGUNTA == "p6":    # velocidad por hora
    r = (v.where((F.round(F.col("trip_miles") * 100) > 0) & (F.col("trip_time") > 0)).groupBy("hora")
         .agg(viajes_n, F.sum(dinero("trip_miles")).alias("millas"), F.sum("trip_time").alias("segundos"))
         .withColumn("velocidad_mph", F.round(F.col("millas") / (F.col("segundos") / 3600), 2))
         .withColumn("duracion_promedio_min", F.round(F.col("segundos") / F.col("viajes") / 60, 2)).orderBy("hora"))
elif PREGUNTA == "p7":    # zonas que más dejan a la plataforma
    r = (v.groupBy("PULocationID").agg(viajes_n, F.sum(dinero("base_passenger_fare")).alias("tarifa_base_usd"),
                                       F.sum(dinero("driver_pay")).alias("pago"))
         .withColumn("plataforma_usd", F.col("tarifa_base_usd") - F.col("pago"))
         .withColumn("plataforma_por_viaje_usd", F.round(F.col("plataforma_usd") / F.col("viajes"), 2)).drop("pago")
         .join(F.broadcast(zonas.select(F.col("LocationID").alias("PULocationID"), F.col("Zone").alias("zona"),
                                        F.col("Borough").alias("borough"))), "PULocationID", "left")
         .orderBy(F.desc("plataforma_usd")))
elif PREGUNTA == "p8":    # cargo de congestión de Manhattan
    r = (v.groupBy("hora").agg(viajes_n, F.sum((F.col("cbd_congestion_fee") > 0).cast("int")).alias("viajes_con_cargo"),
                               F.sum(dinero("cbd_congestion_fee")).alias("recaudo_usd"))
         .withColumn("pct_con_cargo", pct("viajes_con_cargo", "viajes")).orderBy("hora"))
else:
    sys.exit(f"Pregunta desconocida: {PREGUNTA} (use p1 a p8)")

r.show(30, truncate=False)
r.coalesce(1).write.mode("overwrite").csv(f"output_proyecto_{PREGUNTA}", header=True)
print(f"Pregunta {PREGUNTA} · {'Parquet' if PARQUET else 'CSV'} · núcleos: {spark.sparkContext.defaultParallelism} · "
      f"tiempo total (incluye arranque de Spark): {time.time() - inicio:.1f} s")

# La Spark UI (puerto 4040) solo existe mientras la sesión esté abierta; espera Enter si hay terminal (docker run -it).
try:
    teclado = open("/dev/tty")
except OSError:
    teclado = None
if teclado:
    with teclado:
        print("Revisa la Spark UI (puerto 4040) y presiona Enter para cerrar Spark...", flush=True)
        teclado.readline()
spark.stop()
