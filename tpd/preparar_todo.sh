#!/usr/bin/env bash
# preparar_todo.sh — deja el codespace listo para el Taller 1 con un solo comando.
# Hace, en orden: descarga y convierte los datos del caso, enciende y prueba Hadoop, prueba Spark,
# enciende y prueba Kafka, apaga todo y corre el verificador. Se puede repetir: lo que ya está hecho no se rehace.
# Uso (terminal del codespace):  bash tpd/preparar_todo.sh
TPD="$(cd "$(dirname "$0")" && pwd)"
inicio=$(date +%s)
paso() { printf '\n\033[36m=== %s ===\033[0m\n' "$1"; }
falla() {
  printf '\n\033[31mSe detuvo en: %s\033[0m\n' "$1"
  echo "Vuelve a pegar el mismo comando: lo que ya quedó hecho no se repite."
  echo "Si se detiene otra vez en el mismo punto, toma una captura de esta pantalla y muéstrasela al docente."
  exit 1
}

paso "1 de 5 · Datos del caso: el mes de junio de 2025 (unos 2 minutos)"
cd "$TPD/datos" || falla "la carpeta tpd/datos"
if [ "$(stat -c %s fhvhv_2025-06.csv 2>/dev/null)" = "4042229115" ]; then echo "El mes ya estaba preparado."
else python preparar_datos.py || falla "los datos del mes"; fi
if [ "$(stat -c %s fhvhv_2025-06_semana.csv 2>/dev/null)" = "932199265" ]; then echo "La semana ya estaba preparada."
else python preparar_datos.py --semana || falla "los datos de la semana"; fi

paso "2 de 5 · Hadoop (unos 2 minutos la primera vez)"
cd "$TPD/hadoop" || falla "la carpeta tpd/hadoop"
bash encender_hadoop.sh || falla "el encendido de Hadoop"
docker compose exec -T namenode hdfs dfs -mkdir -p /prueba &&
  docker compose exec -T namenode hdfs dfs -put -f /opt/codigo/zonas.csv /prueba/ &&
  docker compose exec -T namenode hdfs dfs -ls /prueba || falla "la prueba de HDFS"
docker compose exec -T nodemanager python3 --version || falla "el Python del nodo de Hadoop"
docker compose stop

paso "3 de 5 · Spark (unos 30 segundos)"
cd "$TPD/spark" || falla "la carpeta tpd/spark"
salida=$(docker run --rm -v "$TPD":/opt/tpd -w /opt/tpd/spark spark:3.5.9-python3 \
  /opt/spark/bin/spark-submit verificar_spark.py 2>&1)
grep -i "funcionando" <<< "$salida" || { echo "$salida" | tail -20; falla "la prueba de Spark"; }

paso "4 de 5 · Kafka (unos 2 minutos la primera vez)"
cd "$TPD/kafka" || falla "la carpeta tpd/kafka"
bash encender_kafka.sh || falla "el encendido de Kafka"
python -c "from kafka import KafkaProducer; p=KafkaProducer(bootstrap_servers='localhost:9092'); p.send('verificacion', b'hola desde Codespaces'); p.flush(); print('Productor OK')" \
  || falla "el productor de Kafka"
echo "Spark lee el mensaje desde Kafka (la primera vez descarga el conector)..."
salida=$(docker compose exec -T spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 /opt/kafka-taller/verificar_conector.py 2>&1)
grep "Conector Spark-Kafka OK" <<< "$salida" || { echo "$salida" | tail -20; docker compose down; falla "el conector Spark-Kafka"; }
docker compose down

paso "5 de 5 · Verificación final"
s=$(( $(date +%s) - inicio ))
echo "Preparación terminada en $((s / 60)) min $((s % 60)) s."
bash "$TPD/verificar_codespace.sh"
