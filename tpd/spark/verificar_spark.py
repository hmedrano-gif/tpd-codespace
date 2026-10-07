"""Prueba mínima de Spark: arranca una SparkSession local y cuenta un rango de números.

Ejecutar (PowerShell, desde la carpeta tpd):
    docker run --rm -v "${PWD}:/opt/tpd" -w /opt/tpd/spark spark:3.5.9-python3 /opt/spark/bin/spark-submit verificar_spark.py
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("VerificarSpark").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
n = spark.range(1_000_000).count()
print(f"Spark {spark.version} funcionando: contó {n:,} filas.")
spark.stop()
