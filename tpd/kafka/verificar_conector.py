"""Verifica que Spark (en Docker) llega a Kafka y que el conector Spark-Kafka está instalado.

Ejecutar (PowerShell, desde la carpeta kafka, con docker compose up -d ya hecho):
    docker compose exec spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 /opt/kafka-taller/verificar_conector.py
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("VerificarConector").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = (spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:29092")
      .option("subscribe", "verificacion")
      .option("startingOffsets", "earliest")
      .load())

mensajes = [r.value.decode("utf-8") for r in df.select("value").collect()]
print(f"Conector Spark-Kafka OK: {len(mensajes)} mensaje(s) en el topic 'verificacion': {mensajes}")
spark.stop()
