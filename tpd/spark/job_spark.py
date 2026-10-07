"""Job Spark equivalente al MapReduce: por empresa, viajes, tarifa base, pago al conductor y parte de la plataforma.

Ejecutar (PowerShell, desde la carpeta tpd):
    docker run --rm -it -p 4040:4040 -v "${PWD}:/opt/tpd" -w /opt/tpd/spark spark:3.5.9-python3 /opt/spark/bin/spark-submit job_spark.py
Plan B (la semana):  ... spark-submit job_spark.py --semana
Desde el Parquet oficial (sin convertir):  ... spark-submit job_spark.py --parquet
Con N núcleos (escalabilidad):             ... spark-submit job_spark.py --parquet --nucleos 2
"""
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructType, TimestampType

PARQUET = "--parquet" in sys.argv
# --nucleos N: cuántos núcleos usa Spark (local[N]); sin la opción usa todos (local[*])
NUCLEOS = sys.argv[sys.argv.index("--nucleos") + 1] if "--nucleos" in sys.argv else "*"
if PARQUET:
    ARCHIVO = "../datos/fhvhv_tripdata_2025-06.parquet"
else:
    ARCHIVO = "../datos/fhvhv_2025-06_semana.csv" if "--semana" in sys.argv else "../datos/fhvhv_2025-06.csv"

# Esquema explícito, en el orden oficial de la TLC. Con inferSchema, Spark leería los 4 GB una vez más solo para
# adivinar los tipos.
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
spark = SparkSession.builder.appName("CasoViajes").master(f"local[{NUCLEOS}]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# El Parquet trae su propio esquema y se lee por columnas: Spark solo lee las 3 que usa el cálculo.
viajes = spark.read.parquet(ARCHIVO) if PARQUET else spark.read.csv(ARCHIVO, header=True, schema=esquema)

# decimal(12,2) en las sumas: montos exactos al centavo, como los centavos enteros del MapReduce
resultado = (
    viajes.withColumn("empresa", F.when(F.col("hvfhs_license_num") == "HV0003", "Uber")
                      .when(F.col("hvfhs_license_num") == "HV0005", "Lyft")
                      .otherwise(F.col("hvfhs_license_num")))
    .groupBy("empresa")
    .agg(F.count("*").alias("viajes"),
         F.sum(F.col("base_passenger_fare").cast("decimal(12,2)")).alias("tarifa_base"),
         F.sum(F.col("driver_pay").cast("decimal(12,2)")).alias("pago_conductor"))
    .withColumn("parte_plataforma_pct",
                F.round(100 * (F.col("tarifa_base") - F.col("pago_conductor")) / F.col("tarifa_base"), 1))
    .orderBy(F.desc("viajes"))
)

resultado.show(truncate=False)
resultado.coalesce(1).write.mode("overwrite").csv("output_spark", header=True)
print(f"Tiempo total (incluye arranque de Spark): {time.time() - inicio:.1f} s · núcleos: {spark.sparkContext.defaultParallelism}")

# La Spark UI (http://localhost:4040) solo existe mientras la sesión esté abierta.
# Solo espera si el contenedor tiene terminal (docker run -it); sin terminal cierra de una vez.
# Se lee de /dev/tty y no de sys.stdin: spark-submit lanza este script desde Java, y para Python
# la entrada estándar nunca es una terminal, aunque se use -it.
try:
    teclado = open("/dev/tty")
except OSError:
    teclado = None
if teclado:
    with teclado:
        print("Revisa la Spark UI en http://localhost:4040 y presiona Enter para cerrar Spark...", flush=True)
        teclado.readline()
spark.stop()
