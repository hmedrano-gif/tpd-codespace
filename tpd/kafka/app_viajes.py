"""App de viajes — caso integrador de Tecnologías de Procesamiento de Datos.

Simula la app de un servicio de transporte: el CLIENTE pide y paga el viaje, el CONDUCTOR lo acepta y lo realiza,
y la CENTRAL ve todo en vivo. Cliente y conductor NO se hablan directamente: cada paso es un evento que se publica
en Kafka (tema «viajes») y todas las pantallas leen el estado desde Kafka.

Los seis eventos siguen las columnas reales de los viajes de la TLC de Nueva York:
  solicitud (request_datetime) · asignado · llegada (on_scene_datetime) · recogida (pickup_datetime)
  · fin_viaje (dropoff_datetime) · pago (base_passenger_fare, tips, driver_pay)

Ejecutar:  streamlit run app_viajes.py
Pago SIMULADO: la app nunca pide datos de tarjeta.
"""
import json
import math
import os
import random
import threading
import time
import uuid
from collections import deque
from datetime import datetime

import pandas as pd
import pydeck as pdk            # viene incluido con Streamlit: no hay que instalarlo aparte
import streamlit as st
from kafka import KafkaConsumer, KafkaProducer

import kafka_conexion as K

st.set_page_config(page_title="App de viajes", page_icon="🚕", layout="wide")
CARPETA = os.path.dirname(os.path.abspath(__file__))
PARTE_CONDUCTOR = 0.77          # en junio de 2025 las plataformas retuvieron ≈ 21–23 % de la tarifa base (datos TLC)
PASOS = ["solicitado", "asignado", "conductor llegó", "en viaje", "finalizado", "pagado"]


# ---------------------------------------------------------------- datos de referencia
@st.cache_data
def zonas():
    z = pd.read_csv(os.path.join(CARPETA, "zonas.csv"))
    z = z[z.Zone.notna() & z.Borough.notna() & ~z.Borough.isin(["Unknown", "N/A"])]
    return {int(i): f"{zona} ({distrito})" for i, zona, distrito in zip(z.LocationID, z.Zone, z.Borough)}


def nombre_zona(i):
    return zonas().get(i, f"zona {i}")


def ahora():
    return datetime.now().isoformat(timespec="seconds")


@st.cache_data
def centros():
    """Un punto dentro de cada zona (latitud, longitud), calculado del mapa oficial de zonas de la TLC (taxi_zones.zip)."""
    c = pd.read_csv(os.path.join(CARPETA, "zonas_centros.csv"))
    return {int(i): (la, lo) for i, la, lo in zip(c.LocationID, c.lat, c.lon)}


def millas_estimadas(origen, destino):
    """Distancia en línea recta entre las dos zonas × 1,3, porque por calles el recorrido es más largo."""
    if origen not in centros() or destino not in centros():
        return round(random.Random(origen * 1000 + destino).uniform(1.2, 12.0), 1)   # zona sin coordenadas
    (la1, lo1), (la2, lo2) = centros()[origen], centros()[destino]
    p1, p2, dla, dlo = map(math.radians, (la1, la2, la2 - la1, lo2 - lo1))
    recta = 2 * 3958.8 * math.asin(math.sqrt(math.sin(dla / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlo / 2) ** 2))
    return max(round(recta * 1.3, 1), 0.5)       # 3958,8 = radio de la Tierra en millas; mínimo 0,5 dentro de una zona


def zoom_para(la1, lo1, la2, lo2):
    """Zoom que deja ver los dos puntos con margen, aunque el mapa sea angosto (columna de la vista integrada)."""
    grados = max(abs(la1 - la2), abs(lo1 - lo2), 0.01)
    return max(8.5, min(12.5, math.log2(0.35 / grados) + 8.3))


def mapa_viaje(origen, destino, alto=260):
    """Mapa de Nueva York con el origen (verde), el destino (rojo) y la línea recta del viaje (no es la ruta por calles)."""
    if origen not in centros() or destino not in centros():
        return
    (la1, lo1), (la2, lo2) = centros()[origen], centros()[destino]
    puntos = pd.DataFrame([{"lat": la1, "lon": lo1, "color": [0, 150, 70], "zona": nombre_zona(origen)},
                           {"lat": la2, "lon": lo2, "color": [200, 30, 30], "zona": nombre_zona(destino)}])
    linea = pd.DataFrame([{"desde": [lo1, la1], "hasta": [lo2, la2]}])
    st.pydeck_chart(pdk.Deck(
        map_provider="carto", map_style=pdk.map_styles.CARTO_LIGHT,
        initial_view_state=pdk.ViewState(latitude=(la1 + la2) / 2, longitude=(lo1 + lo2) / 2, zoom=zoom_para(la1, lo1, la2, lo2)),
        layers=[pdk.Layer("LineLayer", linea, get_source_position="desde", get_target_position="hasta",
                          get_color=[30, 90, 200], get_width=4),
                pdk.Layer("ScatterplotLayer", puntos, get_position="[lon, lat]", get_fill_color="color",
                          get_radius=350, pickable=True)],
        tooltip={"text": "{zona}"}), height=alto)


def tarifa_estimada(millas):
    return round(3.0 + 2.4 * millas, 2)      # tarifa simulada, no es la tarifa oficial de ninguna empresa


# ---------------------------------------------------------------- la fila del archivo de la TLC
COLUMNAS_TLC = [("hvfhs_license_num", "solicitud"), ("request_datetime", "solicitud"), ("PULocationID", "solicitud"),
                ("DOLocationID", "solicitud"), ("on_scene_datetime", "llegada"), ("pickup_datetime", "recogida"),
                ("dropoff_datetime", "fin_viaje"), ("trip_miles", "fin_viaje"), ("trip_time", "fin_viaje"),
                ("base_passenger_fare", "pago"), ("tips", "pago"), ("driver_pay", "pago")]
LICENCIA = {"Uber": "HV0003", "Lyft": "HV0005"}     # códigos del diccionario de datos de la TLC


def fila_tlc(v):
    """Las columnas del archivo de la TLC que ya se conocen para este viaje; «—» = aún no llega el evento."""
    datos = {**v, "hvfhs_license_num": LICENCIA.get(v.get("empresa"))}
    datos = {k: (x.replace("T", " ") if isinstance(x, str) and k.endswith("_datetime") else x) for k, x in datos.items()}
    return {col: (datos.get(col) if datos.get(col) is not None else "—") for col, _ in COLUMNAS_TLC}


def mostrar_fila(v, titulo="Así se construye la fila de este viaje en el archivo de la TLC"):
    st.caption(titulo + " — cada evento que llega por Kafka completa columnas:")
    llenas = sum(1 for x in fila_tlc(v).values() if x != "—")
    st.dataframe(pd.DataFrame([fila_tlc(v)]).astype(str), hide_index=True, width="stretch")
    st.caption(f"{llenas} de {len(COLUMNAS_TLC)} columnas completas")


# ---------------------------------------------------------------- Kafka: productor y estado compartido
@st.cache_resource
def productor():
    K.asegurar_tema()
    return KafkaProducer(value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                         key_serializer=lambda k: k.encode("utf-8"), **K.parametros())


def publicar(evento):
    evento.setdefault("ts", ahora())
    productor().send(K.TEMA, key=evento["viaje_id"], value=evento)
    productor().flush()


class Estado:
    """Lee el tema «viajes» desde el principio, en un hilo aparte, y reconstruye el estado de cada viaje."""

    def __init__(self):
        self.lock = threading.Lock()
        self.viajes, self.eventos = {}, deque(maxlen=300)
        self.recibidos = self.descartados = 0
        self.sin_conexion = False
        threading.Thread(target=self._leer, daemon=True).start()

    def _leer(self):
        # Si la app se abre antes de que Kafka termine de arrancar, reintenta cada 3 s en vez de quedar sin datos.
        while True:
            try:
                K.asegurar_tema()
                c = KafkaConsumer(K.TEMA, auto_offset_reset="earliest", enable_auto_commit=False, group_id=None,
                                  consumer_timeout_ms=-1, **K.parametros())
                break
            except Exception:
                self.sin_conexion = True
                time.sleep(3)
        self.sin_conexion = False
        for m in c:
            try:
                e = json.loads(m.value.decode("utf-8"))
                self._aplicar(e)
            except Exception:
                with self.lock:
                    self.descartados += 1     # mensaje mal formado: llega a Kafka, pero aquí se descarta

    def _aplicar(self, e):
        with self.lock:
            self.recibidos += 1
            self.eventos.appendleft(e)
            v, t = self.viajes.get(e["viaje_id"]), e["tipo"]
            if t == "solicitud":
                self.viajes[e["viaje_id"]] = {**e, "estado": "solicitado", "request_datetime": e["ts"]}
            elif v is None:
                return
            elif t == "asignado" and v["estado"] == "solicitado":
                v.update(estado="asignado", conductor=e["conductor"])
            elif t == "llegada" and v.get("conductor") == e.get("conductor"):
                v.update(estado="conductor llegó", on_scene_datetime=e["ts"])
            elif t == "recogida" and v.get("conductor") == e.get("conductor"):
                v.update(estado="en viaje", pickup_datetime=e["ts"])
            elif t == "fin_viaje" and v.get("conductor") == e.get("conductor"):
                v.update(estado="finalizado", dropoff_datetime=e["ts"], trip_miles=e["trip_miles"], trip_time=e["trip_time"])
            elif t == "pago" and v["estado"] == "finalizado":
                v.update(estado="pagado", **{k: e[k] for k in ("base_passenger_fare", "tips", "driver_pay", "cargos")})

    def viaje(self, vid):
        with self.lock:
            return dict(self.viajes.get(vid, {}))

    def copia(self):
        with self.lock:
            return [dict(v) for v in self.viajes.values()], list(self.eventos), self.recibidos, self.descartados


@st.cache_resource
def estado():
    return Estado()


def conductor_automatico(vid):
    """Para quien trabaja sin pareja: un conductor simulado avanza el viaje solo."""
    def avanzar():
        for pausa, tipo in [(3, "asignado"), (6, "llegada"), (4, "recogida"), (8, "fin_viaje")]:
            time.sleep(pausa)
            v = estado().viaje(vid)
            e = {"viaje_id": vid, "tipo": tipo, "conductor": "Conductor automático", "empresa": v.get("empresa"),
                 "PULocationID": v.get("PULocationID"), "request_datetime": v.get("request_datetime")}
            if tipo == "fin_viaje":
                e.update(trip_miles=v["millas"], trip_time=int(v["millas"] * 180))
            publicar(e)
    threading.Thread(target=avanzar, daemon=True).start()


# ---------------------------------------------------------------- acceso con código de clase
codigo = K._secreto("CODIGO_CLASE")
if codigo and st.session_state.get("codigo_ok") != codigo:
    st.title("🚕 App de viajes")
    if st.text_input("Código de la clase", type="password") == codigo:
        st.session_state.codigo_ok = codigo
        st.rerun()
    st.stop()

pantalla = st.sidebar.radio("Vista", ["🖥️ Vista integrada", "🙋 Cliente", "🚗 Conductor", "📊 Central de operaciones"],
                            help="La vista integrada muestra las tres pantallas a la vez; las otras sirven para usar la app desde el celular.")
COMPACTO = pantalla.startswith("🖥️")


def titulo(texto):
    (st.subheader if COMPACTO else st.title)(texto)
st.sidebar.caption("Cada acción es un evento que viaja por Kafka (tema «viajes»). Pago simulado.")


# ---------------------------------------------------------------- CLIENTE
def pantalla_cliente():
    titulo("🙋 Cliente")
    vid = st.session_state.get("viaje_cliente")
    if not vid:
        z = zonas()
        nombre = st.text_input("Tu nombre (cliente)", "María", key="cli_nombre")
        empresa = st.radio("Empresa", ["Uber", "Lyft"], horizontal=True, key="cli_empresa")
        c1, c2 = st.columns(2)
        origen = c1.selectbox("¿Dónde estás?", list(z), index=list(z).index(161), format_func=nombre_zona)
        destino = c2.selectbox("¿A dónde vas?", list(z), index=list(z).index(138), format_func=nombre_zona)
        mi = millas_estimadas(origen, destino)
        st.info(f"Distancia estimada: {mi} millas · Tarifa estimada: **US\\$ {tarifa_estimada(mi):.2f}** (simulada)")
        mapa_viaje(origen, destino)
        st.caption("Verde: origen · rojo: destino · la línea es recta; por calles el recorrido es más largo.")
        auto = st.checkbox("Trabajo sin pareja: usar un conductor automático")
        if st.button("Pedir viaje", type="primary"):
            vid = uuid.uuid4().hex[:8]
            publicar({"viaje_id": vid, "tipo": "solicitud", "cliente": nombre, "empresa": empresa,
                      "PULocationID": int(origen), "DOLocationID": int(destino), "millas": mi,
                      "tarifa_estimada": tarifa_estimada(mi)})
            st.session_state.viaje_cliente = vid
            if auto:
                conductor_automatico(vid)
            st.rerun()
        return
    seguimiento_cliente(vid)


@st.fragment(run_every=2)
def seguimiento_cliente(vid):
    v = estado().viaje(vid)
    if not v:
        st.write("Enviando la solicitud a Kafka…")
        return
    paso = PASOS.index(v["estado"])
    st.progress((paso + 1) / len(PASOS), text=f"Estado: **{v['estado']}**")
    st.write(f"Viaje `{vid}` · {v['empresa']} · de {nombre_zona(v['PULocationID'])} a {nombre_zona(v['DOLocationID'])}")
    if v.get("conductor"):
        st.write(f"Tu conductor: **{v['conductor']}**")
    mapa_viaje(v["PULocationID"], v["DOLocationID"], alto=200)
    mostrar_fila(v)
    if v["estado"] == "solicitado":
        st.write("Buscando conductor… (tu pareja debe aceptarlo en la pantalla del conductor)")
    if v["estado"] == "finalizado":
        tarifa = tarifa_estimada(v["trip_miles"])
        cargos = round(tarifa * 0.08, 2)
        propina_pct = st.radio("Propina", [0, 10, 15, 20], format_func=lambda p: f"{p} %", horizontal=True, key=f"prop_{vid}")
        propina = round(tarifa * propina_pct / 100, 2)
        st.write(f"Tarifa base US\\$ {tarifa:.2f} · cargos (simulados) US\\$ {cargos:.2f} · propina US\\$ {propina:.2f}")
        st.subheader(f"Total: US\\$ {tarifa + cargos + propina:.2f}")
        if st.button("Pagar (simulado)", type="primary", key=f"pagar_{vid}"):
            publicar({"viaje_id": vid, "tipo": "pago", "empresa": v["empresa"], "base_passenger_fare": tarifa,
                      "cargos": cargos, "tips": propina, "driver_pay": round(tarifa * PARTE_CONDUCTOR, 2)})
    if v["estado"] == "pagado":
        st.success(f"¡Pago recibido! Total US\\$ {v['base_passenger_fare'] + v['cargos'] + v['tips']:.2f}. Gracias por viajar.")
        if st.button("Pedir otro viaje", key=f"nuevo_{vid}"):
            del st.session_state["viaje_cliente"]
            st.rerun(scope="app")


# ---------------------------------------------------------------- CONDUCTOR
def pantalla_conductor():
    titulo("🚗 Conductor")
    c1, c2 = st.columns(2)
    st.session_state.nombre_conductor = c1.text_input("Tu nombre (conductor)", "Carlos", key="con_nombre")
    st.session_state.empresa_conductor = c2.radio("Trabajas para", ["Uber", "Lyft"], horizontal=True, key="con_empresa")
    panel_conductor()


@st.fragment(run_every=2)
def panel_conductor():
    yo, empresa = st.session_state.nombre_conductor, st.session_state.empresa_conductor
    vid = st.session_state.get("viaje_conductor")
    if vid:
        v = estado().viaje(vid)
        if v.get("conductor") and v["conductor"] != yo:
            st.warning("Otro conductor tomó este viaje primero.")
            del st.session_state["viaje_conductor"]
            return
        st.write(f"Viaje `{vid}` de **{v.get('cliente', '')}**: {nombre_zona(v['PULocationID'])} → {nombre_zona(v['DOLocationID'])} · estado **{v['estado']}**")
        mostrar_fila(v)
        siguiente = {"asignado": ("Llegué al punto de recogida", "llegada"),
                     "conductor llegó": ("El pasajero subió: iniciar viaje", "recogida"),
                     "en viaje": ("Llegamos: terminar viaje", "fin_viaje")}.get(v["estado"])
        if siguiente and st.button(siguiente[0], type="primary", key=f"{vid}_{siguiente[1]}"):
            e = {"viaje_id": vid, "tipo": siguiente[1], "conductor": yo, "empresa": empresa,
                 "PULocationID": v["PULocationID"], "request_datetime": v["request_datetime"]}
            if siguiente[1] == "fin_viaje":
                e.update(trip_miles=v["millas"], trip_time=int(v["millas"] * 180))
            publicar(e)
        if v["estado"] in ("finalizado", "pagado"):
            st.success("Viaje terminado." + (f" Tu pago: US\\$ {v['driver_pay']:.2f} + propina US\\$ {v['tips']:.2f}." if v["estado"] == "pagado" else " Esperando el pago del cliente…"))
            if st.button("Buscar otro viaje", key=f"otro_{vid}"):
                del st.session_state["viaje_conductor"]
        return
    viajes, *_ = estado().copia()
    pendientes = [v for v in viajes if v["estado"] == "solicitado" and v["empresa"] == empresa]
    if not pendientes:
        st.info(f"No hay solicitudes de {empresa} por ahora. La pantalla se actualiza sola.")
    for v in sorted(pendientes, key=lambda x: x["request_datetime"]):
        c1, c2 = (st.container(), st.container()) if COMPACTO else st.columns([4, 1])
        c1.write(f"**{v.get('cliente', '')}** · {nombre_zona(v['PULocationID'])} → {nombre_zona(v['DOLocationID'])} · US\\$ {v['tarifa_estimada']:.2f}")
        if c2.button("Aceptar", key=f"aceptar_{v['viaje_id']}"):
            publicar({"viaje_id": v["viaje_id"], "tipo": "asignado", "conductor": yo, "empresa": empresa})
            st.session_state.viaje_conductor = v["viaje_id"]


# ---------------------------------------------------------------- CENTRAL
@st.fragment(run_every=2)
def pantalla_central():
    titulo("📊 Central de operaciones")
    if estado().sin_conexion:
        st.warning("Sin conexión con Kafka (localhost:9092). Revisa docker compose ps en C:\\tpd\\kafka; la app reintenta sola cada 3 segundos.")
    viajes, eventos, recibidos, descartados = estado().copia()
    df = pd.DataFrame(viajes)
    c = st.columns(3) + st.columns(2) if COMPACTO else st.columns(5)
    c[0].metric("Eventos recibidos de Kafka", recibidos)
    c[1].metric("Viajes solicitados", len(df))
    if df.empty:
        st.info("Aún no hay viajes. Pide uno desde la pantalla del cliente.")
        return
    en_curso = df.estado.isin(PASOS[1:4]).sum()
    c[2].metric("En curso", int(en_curso))
    pagados = df[df.estado == "pagado"]
    c[3].metric("Pagados", len(pagados))
    c[4].metric("Mensajes descartados", descartados)
    if "on_scene_datetime" in df:
        esp = (pd.to_datetime(df.on_scene_datetime, format="mixed") - pd.to_datetime(df.request_datetime, format="mixed")).dt.total_seconds().dropna()
        if len(esp):
            st.metric("Espera promedio hasta que llega el conductor", f"{esp.mean() / 60:.1f} min")
    if len(pagados):
        tarifa, conductor = pagados.base_passenger_fare.sum(), pagados.driver_pay.sum()
        d = st.columns(3)
        d[0].metric("Tarifa base cobrada", f"US$ {tarifa:,.0f}")
        d[1].metric("Pago a conductores", f"US$ {conductor:,.0f}")
        d[2].metric("Parte de la plataforma", f"{100 * (tarifa - conductor) / tarifa:.1f} %")
    izq, der = (st.container(), st.container()) if COMPACTO else st.columns(2)
    por_zona = df.assign(zona=df.PULocationID.map(nombre_zona)).groupby("zona").size().sort_values(ascending=False).head(10)
    izq.subheader("Viajes por zona de origen")
    izq.bar_chart(por_zona)
    der.subheader("Viajes por estado")
    der.bar_chart(df.estado.value_counts().reindex(PASOS).fillna(0))
    st.subheader("Las filas que se van construyendo (columnas del archivo de la TLC)")
    st.dataframe(pd.DataFrame([fila_tlc(v) for v in sorted(viajes, key=lambda x: x["request_datetime"], reverse=True)]).astype(str),
                 hide_index=True, width="stretch")
    st.subheader("Últimos eventos que llegaron por Kafka")
    st.dataframe(pd.DataFrame(eventos).head(15), width="stretch", hide_index=True)


if COMPACTO:
    st.title("🚕 Un viaje, tres miradas — todo pasa por Kafka")
    c_cli, c_con, c_cen = st.columns([1, 1, 1.5], gap="large")
    with c_cli:
        pantalla_cliente()
    with c_con:
        pantalla_conductor()
    with c_cen:
        pantalla_central()
else:
    {"🙋 Cliente": pantalla_cliente, "🚗 Conductor": pantalla_conductor, "📊 Central de operaciones": pantalla_central}[pantalla]()
