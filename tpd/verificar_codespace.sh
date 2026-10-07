#!/usr/bin/env bash
# verificar_codespace.sh — revisa si el codespace tiene todo lo necesario para el Taller 1
# (Hadoop, Spark, Kafka y la app de viajes). Solo lee: no instala, no descarga ni enciende contenedores.
# Uso (terminal del codespace, desde cualquier carpeta):  bash /workspaces/tpd-codespace/tpd/verificar_codespace.sh
TPD="$(cd "$(dirname "$0")" && pwd)"
fallos=0; avisos=0
ok()    { printf '  \033[32m[OK]\033[0m    %s\n' "$1"; }
aviso() { printf '  \033[33m[AVISO]\033[0m %s\n' "$1"; avisos=$((avisos+1)); }
falla() { printf '  \033[31m[FALTA]\033[0m %s\n' "$1"; fallos=$((fallos+1)); }
titulo(){ printf '\n\033[36m%s\033[0m\n' "$1"; }

echo "Verificación del codespace para el Taller 1 · Tecnologías de Procesamiento de Datos"
echo "Carpeta de trabajo revisada: $TPD"

titulo "1. Máquina"
nucleos=$(nproc)
ram=$(awk '/MemTotal/ {printf "%.1f", $2/1048576}' /proc/meminfo)
if [ "$nucleos" -ge 4 ]; then ok "$nucleos núcleos"; else falla "$nucleos núcleos: crea el codespace con la máquina de 4 núcleos (16 GB)"; fi
if awk "BEGIN{exit !($ram >= 14)}"; then ok "RAM: $ram GB"; else falla "RAM: $ram GB: crea el codespace con la máquina de 4 núcleos (16 GB)"; fi
libre=$(df -BG --output=avail /workspaces | tail -1 | tr -dc '0-9')
if [ "$libre" -ge 8 ]; then ok "Disco libre: $libre GB"; else falla "Disco libre: solo $libre GB (Hadoop copia 3,8 GB más)"; fi

titulo "2. Docker"
if docker version --format '{{.Server.Version}}' >/dev/null 2>&1; then
  ok "Docker encendido (motor $(docker version --format '{{.Server.Version}}'))"
  imagenes=$(docker images --format '{{.Repository}}:{{.Tag}}')
  for img in apache/hadoop:3.4.3 spark:3.5.9-python3 confluentinc/cp-kafka:7.6.0 confluentinc/cp-zookeeper:7.6.0; do
    if grep -qx "$img" <<< "$imagenes"; then ok "Imagen $img"
    else falla "Falta la imagen $img: ejecuta encender_hadoop.sh / encender_kafka.sh o docker pull $img"; fi
  done
else falla "Docker no responde: espera a que termine de abrir el codespace y repite"; fi
if sudo -n iptables-legacy -S FORWARD 2>/dev/null | head -1 | grep -q ACCEPT; then
  ok "Red entre contenedores habilitada"
else falla "Red entre contenedores bloqueada: ejecuta  sudo iptables-legacy -P FORWARD ACCEPT"; fi

titulo "3. Python y librerías"
if command -v python >/dev/null; then
  ok "$(python --version 2>&1)"
  python - <<'EOF' | while IFS='=' read -r n v; do
import importlib.metadata as m
for p in ["pyarrow", "pandas", "streamlit", "kafka-python"]:
    try: print(p + "=" + m.version(p))
    except Exception: print(p + "=NO")
EOF
    if [ "$v" = "NO" ]; then
      [ "$n" = "kafka-python" ] && i="kafka-python==2.3.2" || i="$n"
      printf '  \033[31m[FALTA]\033[0m Falta la librería %s: pip install %s\n' "$n" "$i"; echo x >> /tmp/.verif_fallos
    elif [ "$n" = "kafka-python" ] && [ "$v" != "2.3.2" ]; then
      printf '  \033[33m[AVISO]\033[0m kafka-python %s: la guía fija la 2.3.2\n' "$v"
    else printf '  \033[32m[OK]\033[0m    %s %s\n' "$n" "$v"; fi
  done
  [ -f /tmp/.verif_fallos ] && fallos=$((fallos + $(wc -l < /tmp/.verif_fallos))) && rm -f /tmp/.verif_fallos
else falla "Python no responde"; fi

titulo "4. Carpeta de trabajo $TPD"
faltan=0
for f in datos/ensayo_python.py datos/preparar_datos.py hadoop/docker-compose.yml hadoop/config hadoop/mapper.py hadoop/reducer.py \
         hadoop/mapper_espera.py hadoop/reducer_espera.py hadoop/zonas.csv spark/job_spark.py spark/verificar_spark.py \
         kafka/docker-compose.yml kafka/app_viajes.py kafka/kafka_conexion.py kafka/simulador.py kafka/consumidor_consola.py \
         kafka/verificar_flujo.py kafka/streaming_viajes.py kafka/verificar_conector.py kafka/zonas.csv kafka/zonas_centros.csv; do
  [ -e "$TPD/$f" ] || { falla "Falta $f"; faltan=1; }
done
[ "$faltan" -eq 0 ] && ok "Los 21 archivos de la práctica están en su lugar"

titulo "5. Datos del caso"
for par in "fhvhv_tripdata_2025-06.parquet:490819474" "fhvhv_2025-06_semana.csv:932199265" "fhvhv_2025-06.csv:4042229115"; do
  n=${par%%:*}; b=${par##*:}; p="$TPD/datos/$n"
  if [ -f "$p" ]; then
    t=$(stat -c %s "$p")
    if [ "$t" -eq "$b" ]; then ok "$n ($(awk "BEGIN{printf \"%.2f\", $t/1e9}") GB, tamaño exacto)"
    else falla "$n incompleto: $t bytes, debería tener $b; repite preparar_datos.py"; fi
  else falla "Falta $n: ejecuta preparar_datos.py"; fi
done

echo
if [ "$fallos" -eq 0 ]; then printf '\033[32mRESULTADO: LISTO para el Taller 1 (%d avisos).\033[0m\n' "$avisos"
else printf '\033[31mRESULTADO: FALTAN %d cosas (%d avisos). Corrígelas y vuelve a ejecutar este comando.\033[0m\n' "$fallos" "$avisos"; fi
