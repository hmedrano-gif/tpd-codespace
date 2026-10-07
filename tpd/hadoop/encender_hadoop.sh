#!/usr/bin/env bash
# encender_hadoop.sh — enciende el clúster de Hadoop y espera a que esté listo para trabajar.
# Hace lo mismo que «docker compose up -d», pero no devuelve el control hasta que:
#   1) el NameNode responde, 2) el DataNode está registrado y 3) HDFS salió del modo seguro (safe mode).
# Si se lanza un job antes de eso, falla con «Cannot delete /tmp/hadoop-yarn/staging…» o «0 datanode(s) running».
# Uso (terminal del codespace):  bash /workspaces/tpd-codespace/tpd/hadoop/encender_hadoop.sh
cd "$(dirname "$0")" || exit 1
inicio=$(date +%s)
echo "Encendiendo Hadoop (namenode, datanode, resourcemanager, nodemanager, historyserver)..."
docker compose up -d || exit 1
paso() { printf '  %-58s' "$1"; }
espera() {  # espera() "descripción" "comando" segundos_máximos
  paso "$1"
  for _ in $(seq 1 $(( $3 / 5 ))); do
    if eval "$2" >/dev/null 2>&1; then echo "OK ($(( $(date +%s) - inicio )) s)"; return 0; fi
    sleep 5
  done
  echo "NO RESPONDE"; return 1
}
espera "NameNode responde" "docker compose exec -T namenode hdfs dfsadmin -safemode get" 240 || exit 1
espera "DataNode registrado (Live datanodes 1)" \
  "docker compose exec -T namenode hdfs dfsadmin -report | grep -q 'Live datanodes (1)'" 240 || exit 1
espera "HDFS fuera del modo seguro (Safe mode is OFF)" \
  "docker compose exec -T namenode hdfs dfsadmin -safemode get | grep -q OFF" 240 || exit 1
espera "YARN con su nodo activo" \
  "docker compose exec -T resourcemanager yarn node -list 2>/dev/null | grep -q RUNNING" 240 || exit 1
echo
echo "HADOOP LISTO en $(( $(date +%s) - inicio )) s · consolas: HDFS puerto 9870 · YARN puerto 8088 (pestaña Puertos)"
