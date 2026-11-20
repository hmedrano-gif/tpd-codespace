#!/usr/bin/env bash
# mi_mes.sh — la misma pregunta, con el mes de tu grupo, en Hadoop y en Spark (CSV y Parquet).
# Así puedes comparar los tiempos y las cifras con lo que ya te dio BigQuery para ese mismo mes.
#
# Uso (terminal del codespace):   bash tpd/mi_mes.sh 2025-09
#   Grupo 1 = 2025-01 · Grupo 2 = 2025-04 · Grupo 3 = 2025-09 · Grupo 4 = 2025-12
#   Pregunta opcional (por defecto p2, viajes y tarifa por día de la semana y empresa):  bash tpd/mi_mes.sh 2025-09 p1
#
# Pasos: 1) baja y convierte tu mes · 2) Hadoop · 3) Spark con CSV · 4) Spark con Parquet · 5) tabla de tiempos.
# Entre paso y paso espera Enter, para que el docente explique. Se puede repetir: lo ya hecho no se rehace.
TPD="$(cd "$(dirname "$0")" && pwd)"
MES="$1"; PREG="${2:-p2}"
JAR="/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar"

titulo() { printf '\n\033[36m=== %s ===\033[0m\n' "$1"; }
falla()  { printf '\n\033[31mSe detuvo en: %s\033[0m\n' "$1"
           echo "Vuelve a pegar el mismo comando: lo que ya quedó hecho no se repite."
           echo "Si se detiene otra vez en el mismo punto, toma una captura de esta pantalla y muéstrasela al docente."; exit 1; }
pausa()  { if [ -r /dev/tty ] && [ -z "$SIN_PAUSA" ]; then printf '\n\033[33m>>> %s (presiona Enter)\033[0m' "$1"; read -r _ < /dev/tty; fi; }
mmss()   { printf '%d min %02d s' $(( $1 / 60 )) $(( $1 % 60 )); }

if ! [[ "$MES" =~ ^2025-(0[1-9]|1[0-2])$ ]]; then
  echo "Escribe el mes de tu grupo así:  bash tpd/mi_mes.sh 2025-09"
  echo "  Grupo 1 = 2025-01 · Grupo 2 = 2025-04 · Grupo 3 = 2025-09 · Grupo 4 = 2025-12"; exit 1
fi
[[ "$PREG" =~ ^p[1-8]$ ]] || { echo "La pregunta va de p1 a p8 (por defecto p2)."; exit 1; }
CSV="fhvhv_${MES}.csv"; PARQ="fhvhv_tripdata_${MES}.parquet"
RES="$TPD/resultados_${MES}_${PREG}.txt"

titulo "1 de 5 · Tu mes: $MES (unos 3 minutos)"
cd "$TPD/datos" || falla "la carpeta tpd/datos"
libre=$(df -BG --output=avail /workspaces | tail -1 | tr -dc '0-9')
if [ ! -f "$CSV.ok" ] && [ "$MES" != "2025-06" ] && [ "$libre" -lt 14 ]; then
  echo "Hay poco disco libre ($libre GB): borro el CSV de junio (se puede volver a crear con preparar_datos.py)."
  rm -f fhvhv_2025-06.csv fhvhv_2025-06_semana.csv
fi
if [ -f "$CSV.ok" ] && [ -f "$CSV" ] && [ -f "$PARQ" ]; then echo "Tu mes ya estaba preparado."
else
  rm -f "$CSV.ok"
  python preparar_datos.py --mes "$MES" || falla "la descarga o conversión del mes"
  touch "$CSV.ok"
fi
VIAJES=$(python -c "import pyarrow.parquet as pq; print(pq.ParquetFile('$PARQ').metadata.num_rows)")
printf 'Tu mes tiene %s viajes. CSV: %s · Parquet: %s\n' "$VIAJES" "$(du -h "$CSV" | cut -f1)" "$(du -h "$PARQ" | cut -f1)"
pausa "Sigue Hadoop: MapReduce sobre HDFS"

titulo "2 de 5 · Hadoop con tu mes (subir a HDFS ~2 min + MapReduce ~4 min)"
cd "$TPD/hadoop" || falla "la carpeta tpd/hadoop"
bash encender_hadoop.sh || falla "el encendido de Hadoop"
NN="docker compose exec -T namenode"
tam_local=$(stat -c %s "$TPD/datos/$CSV")
tam_hdfs=$($NN hdfs dfs -stat %b "/mes/input/$CSV" 2>/dev/null | tr -dc '0-9')
if [ "$tam_hdfs" = "$tam_local" ]; then echo "Tu mes ya estaba en HDFS."
else
  # en HDFS solo se deja un mes a la vez, para no llenar el disco
  $NN hdfs dfs -rm -r -f -skipTrash /mes/input /caso/input >/dev/null 2>&1
  t0=$(date +%s)
  $NN hdfs dfs -mkdir -p /mes/input && $NN hdfs dfs -put -f "/opt/datos/$CSV" /mes/input/ || falla "la copia del mes a HDFS"
  echo "Copiado a HDFS en $(mmss $(( $(date +%s) - t0 )))"
fi
$NN hdfs dfs -rm -r -f -skipTrash "/mes/$PREG" >/dev/null 2>&1
echo "Ejecutando MapReduce (pregunta $PREG). Mira el avance: map 0% ... reduce 100%"
t0=$(date +%s)
$NN bash -c "hadoop jar $JAR -files /opt/codigo/mapper_proyecto.py,/opt/codigo/reducer_proyecto.py,/opt/codigo/zonas.csv \
  -mapper 'python3 mapper_proyecto.py $PREG' -combiner 'python3 reducer_proyecto.py $PREG --combinar' \
  -reducer 'python3 reducer_proyecto.py $PREG' -input /mes/input/$CSV -output /mes/$PREG" 2>&1 \
  | grep --line-buffered -E "map [0-9]+% reduce|completed successfully|ERROR|Exception" || true
T_HADOOP=$(( $(date +%s) - t0 ))
$NN hdfs dfs -test -e "/mes/$PREG/_SUCCESS" || { docker compose stop; falla "el MapReduce de Hadoop"; }
SAL_HADOOP="resultado_${MES}_${PREG}_hadoop.tsv"
$NN hdfs dfs -cat "/mes/$PREG/part-*" > "$SAL_HADOOP"
echo; tr -d "#" < "$SAL_HADOOP" | column -t -s $'\t'
echo; echo "Hadoop: $(mmss $T_HADOOP) · resultado guardado en tpd/hadoop/$SAL_HADOOP"
docker compose stop >/dev/null 2>&1
pausa "Sigue Spark con el mismo CSV"

spark() {  # spark "--parquet" o "" · deja en SEG el tiempo que reporta el propio Spark
  docker run --rm -v "$TPD":/opt/tpd -w /opt/tpd/spark spark:3.5.9-python3 \
    /opt/spark/bin/spark-submit job_proyecto.py --pregunta "$PREG" --mes "$MES" $1 2>&1 \
    | grep -v -E "^\s*$|WARN|INFO|NativeCodeLoader|Setting default log level|To adjust logging level" | tee /tmp/spark_salida.txt
  SEG=$(grep -o 'arranque de Spark): [0-9.]* s' /tmp/spark_salida.txt | grep -o '[0-9.]*' | tr . ,)
}
titulo "3 de 5 · Spark con el CSV de tu mes (~1 min)"
cd "$TPD/spark" || falla "la carpeta tpd/spark"
spark "" || falla "Spark con CSV"; T_CSV="$SEG s"
ls "output_proyecto_${PREG}_${MES}_csv"/_SUCCESS >/dev/null 2>&1 || falla "Spark con CSV (no dejó resultado)"
pausa "Sigue Spark con Parquet: el mismo mes en formato por columnas"

titulo "4 de 5 · Spark con el Parquet de tu mes (~30 s)"
spark "--parquet" || falla "Spark con Parquet"; T_PARQ="$SEG s"
ls "output_proyecto_${PREG}_${MES}_parquet"/_SUCCESS >/dev/null 2>&1 || falla "Spark con Parquet (no dejó resultado)"

titulo "5 de 5 · Tus tiempos con $MES ($VIAJES viajes, pregunta $PREG)"
# comprobación: la suma de viajes de Hadoop y de Spark debe ser la misma
V_H=$(awk -F'\t' '$0 !~ /^#/ {s+=$3} END {print s+0}' "$TPD/hadoop/$SAL_HADOOP")
V_S=$(python -c "
import csv,glob
f=glob.glob('$TPD/spark/output_proyecto_${PREG}_${MES}_parquet/part-*.csv')[0]
print(sum(int(r['viajes']) for r in csv.DictReader(open(f))))")
{
  echo "Mes $MES · $VIAJES viajes · pregunta $PREG"
  printf '%-22s %s\n' "Hadoop (MapReduce)" "$(mmss $T_HADOOP)"
  printf '%-22s %s\n' "Spark con CSV" "$T_CSV"
  printf '%-22s %s\n' "Spark con Parquet" "$T_PARQ"
  printf '%-22s %s\n' "BigQuery" "(el de tu Taller 2A, en Información del trabajo)"
  if [ "$PREG" = "p2" ]; then
    if [ "$V_H" = "$V_S" ]; then echo "Cifras: Hadoop y Spark suman los mismos $V_S viajes ✓"
    else echo "Cifras: Hadoop suma $V_H viajes y Spark $V_S: revisa con el docente"; fi
  fi
  echo "Compara estas cifras con tu P2 de BigQuery: deben coincidir."
} | tee "$RES"
echo; echo "Quedó guardado en tpd/resultados_${MES}_${PREG}.txt"
