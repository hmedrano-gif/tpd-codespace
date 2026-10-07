#!/usr/bin/env bash
# encender_kafka.sh — enciende Kafka (con ZooKeeper y el contenedor de Spark) y espera a que acepte conexiones.
# Al reabrir un codespace, Kafka puede quedar varios minutos reiniciándose porque ZooKeeper conserva el registro
# del broker de la sesión anterior; mientras tanto los programas fallan con «NoBrokersAvailable».
# Este script lo detecta: si Kafka no responde en 60 s, recrea los contenedores (docker compose down + up -d),
# lo que borra ese registro viejo, y vuelve a esperar.
# Uso (terminal del codespace):  bash /workspaces/tpd-codespace/tpd/kafka/encender_kafka.sh
cd "$(dirname "$0")" || exit 1
inicio=$(date +%s)
responde() { docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; }
esperar() {  # esperar segundos_máximos
  for _ in $(seq 1 $(( $1 / 5 ))); do responde && return 0; sleep 5; done
  return 1
}
echo "Encendiendo Kafka, ZooKeeper y Spark..."
docker compose up -d || exit 1
printf '  %-58s' "Kafka acepta conexiones (localhost:9092)"
if esperar 60; then
  echo "OK ($(( $(date +%s) - inicio )) s)"
else
  echo "no responde: se reinicia en bucle"
  echo "  Recreando los contenedores para borrar el registro viejo (docker compose down + up -d)..."
  docker compose down >/dev/null 2>&1
  docker compose up -d >/dev/null 2>&1
  printf '  %-58s' "Kafka acepta conexiones (localhost:9092)"
  if esperar 120; then echo "OK ($(( $(date +%s) - inicio )) s)"; else echo "NO RESPONDE"; exit 1; fi
fi
echo
echo "KAFKA LISTO en $(( $(date +%s) - inicio )) s · temas actuales: $(docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list | tr '\n' ' ')"
