"""Conexión a Kafka para la app de viajes.

Local (Docker):   no hay que configurar nada; usa localhost:9092.
Nube (Aiven):     los datos de acceso van en los «secrets» de Streamlit (nunca en el código):
    KAFKA_BOOTSTRAP = "mi-servicio.aivencloud.com:12345"
    KAFKA_SASL_USERNAME = "avnadmin"
    KAFKA_SASL_PASSWORD = "..."
    KAFKA_SSL_CA = "-----BEGIN CERTIFICATE----- ..."   (certificado CA del servicio)
"""
import os
import tempfile

TEMA = "viajes"


def _secreto(clave, defecto=None):
    """Busca primero en los secrets de Streamlit y luego en variables de entorno."""
    try:
        import streamlit as st
        if clave in st.secrets:
            return st.secrets[clave]
    except Exception:
        pass
    return os.getenv(clave, defecto)


def parametros():
    """Parámetros comunes para KafkaProducer, KafkaConsumer y KafkaAdminClient."""
    p = {"bootstrap_servers": _secreto("KAFKA_BOOTSTRAP", "localhost:9092")}
    usuario = _secreto("KAFKA_SASL_USERNAME")
    if usuario:
        ca = tempfile.NamedTemporaryFile("w", suffix=".pem", delete=False)
        ca.write(_secreto("KAFKA_SSL_CA", ""))
        ca.close()
        p.update(security_protocol="SASL_SSL", sasl_mechanism="SCRAM-SHA-256",
                 sasl_plain_username=usuario, sasl_plain_password=_secreto("KAFKA_SASL_PASSWORD"),
                 ssl_cafile=ca.name)
    return p


def asegurar_tema():
    """Crea el tema «viajes» si todavía no existe (en Docker también se crearía solo al primer mensaje)."""
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
    admin = KafkaAdminClient(**parametros())
    try:
        if TEMA not in admin.list_topics():
            admin.create_topics([NewTopic(TEMA, num_partitions=2, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()
