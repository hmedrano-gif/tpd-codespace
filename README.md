# Tecnologías de Procesamiento de Datos · Taller 1

Entorno listo para usar con **Hadoop, Spark y Kafka** en GitHub Codespaces, para los talleres del seminario *Tecnologías de Procesamiento de Datos*. Los estudiantes no instalan nada: el codespace descarga los datos del caso, enciende cada tecnología y comprueba que funcione.

> Autor: **Heyder Medrano Olier** · Especialización en Analítica de Datos, Corporación Universitaria Rafael Núñez (2026). La institución se menciona como contexto académico. · [Página del seminario](https://hmedrano-gif.github.io/CURN/tpd/)

## Para estudiantes: 3 pasos

1. Crea tu codespace con este enlace: https://codespaces.new/hmedrano-gif/tpd-codespace?quickstart=1
2. Cuando el codespace termine de crearse, pega en la terminal:

```bash
bash tpd/preparar_todo.sh
```

3. Espera la última línea: «RESULTADO: LISTO para el Taller 1».

Para volver a entrar, usa el mismo enlace: abre tu codespace sin crear otro.

## Qué hace `preparar_todo.sh`

| Paso | Qué ocurre |
|---|---|
| 1 · Datos | Descarga los viajes de Uber y Lyft en Nueva York (TLC) de **junio de 2025** y los convierte a CSV: el mes completo y una semana |
| 2 · Hadoop | Enciende HDFS y YARN con Docker y prueba que se pueda escribir en HDFS |
| 3 · Spark | Prueba un job de Spark sobre los datos |
| 4 · Kafka | Enciende Kafka y prueba que el flujo de eventos funcione |
| 5 · Verificación | Apaga todo y ejecuta `verificar_codespace.sh` |

Puedes repetir el script: lo que ya quedó hecho no se vuelve a hacer. Si falla, te dice en qué paso se detuvo.

## Estructura

```
.devcontainer/devcontainer.json   Imagen universal, 4 núcleos y 16 GB, puertos de las interfaces web
tpd/datos/      Descarga y preparación del mes del caso
tpd/hadoop/     docker-compose del clúster y MapReduce en Python (mapper y reducer del proyecto y de la espera)
tpd/spark/      Jobs de Spark: el mismo cálculo del proyecto
tpd/kafka/      Simulador de la app de viajes, productor y consumidor, y Spark Structured Streaming
tpd/preparar_todo.sh · tpd/verificar_codespace.sh · tpd/verificar_equipo.ps1
```

**Interfaces que el codespace publica:**

| Puerto | Interfaz |
|---|---|
| 9870 | HDFS |
| 8088 | YARN |
| 19888 | Historial de trabajos |
| 4040 | Spark UI |
| 8501 | Streamlit |

## Requisitos

- Una cuenta de GitHub con Codespaces. El plan gratuito de estudiante alcanza para el taller.
- Hay que **detener el codespace** al terminar, para no gastar horas del plan.

## Sin Codespaces

El mismo paquete se descarga como `tpd-codespace.zip` desde [tpd-materiales · Releases](https://github.com/hmedrano-gif/tpd-materiales/releases/tag/v1) y se ejecuta con Docker en tu propio equipo.
