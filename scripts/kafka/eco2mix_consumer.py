"""
eco2mix_consumer.py — Consumer Kafka pour éCO2mix → OpenSearch
==============================================================
Lit les messages du topic Kafka 'eco2mix-regional' et les indexe
dans OpenSearch pour visualisation en temps réel dans Dashboards.

Usage:
  docker exec spark-master python3 /scripts/kafka/eco2mix_consumer.py

Dépendances:
  pip install kafka-python requests opensearch-py
"""

import json
import time
import requests
from datetime import datetime, timezone
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from opensearchpy import OpenSearch, helpers

# ============================================================
# Configuration
# ============================================================

KAFKA_BROKER        = "kafka:9092"
KAFKA_TOPIC         = "eco2mix-regional"
KAFKA_GROUP_ID      = "eco2mix-opensearch-consumer"
OS_HOST             = "opensearch"
OS_PORT             = 9200
OS_INDEX            = "eco2mix-tr"
BATCH_SIZE          = 50    # indexer par lots de 50 documents
POLL_TIMEOUT_MS     = 5000  # attendre 5s max si pas de nouveaux messages

# ============================================================
# Couleurs pour les logs
# ============================================================
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
RESET  = "\033[0m"

def ok(msg):   print(f"{GREEN}  [OK]{RESET} {msg}", flush=True)
def fail(msg): print(f"{RED}  [FAIL]{RESET} {msg}", flush=True)
def info(msg): print(f"{BLUE}  [INFO]{RESET} {msg}", flush=True)
def warn(msg): print(f"{YELLOW}  [WARN]{RESET} {msg}", flush=True)

# ============================================================
# Connexion à OpenSearch
# ============================================================

def create_opensearch_client():
    """Crée et retourne un client OpenSearch."""
    info(f"Connexion à OpenSearch: {OS_HOST}:{OS_PORT}...")
    client = OpenSearch(
        hosts=[{"host": OS_HOST, "port": OS_PORT}],
        http_auth=("admin", "admin"),
        use_ssl=False,
        verify_certs=False,
        timeout=30,
    )
    info_resp = client.info()
    ok(f"Connecté à OpenSearch {info_resp['version']['number']}")
    return client

# ============================================================
# Création de l'index avec mapping
# ============================================================

def create_index_if_not_exists(client):
    """
    Crée l'index eco2mix-tr avec le bon mapping si il n'existe pas.
    Le mapping définit les types de chaque champ — important pour
    que les visualisations fonctionnent correctement dans Dashboards.
    """
    if client.indices.exists(index=OS_INDEX):
        info(f"Index '{OS_INDEX}' existe déjà")
        return

    info(f"Création de l'index '{OS_INDEX}' avec mapping...")

    mapping = {
        "mappings": {
            "properties": {
                # Temporel
                "date_heure":           {"type": "date"},
                "ingestion_timestamp":  {"type": "date"},

                # Géographique
                "libelle_region":       {"type": "keyword"},
                "code_insee_region":    {"type": "keyword"},

                # Consommation et production (en MW)
                "consommation":         {"type": "double"},
                "production":           {"type": "double"},

                # Mix énergétique par source (en MW)
                "thermique":            {"type": "double"},
                "nucleaire":            {"type": "double"},
                "eolien":               {"type": "double"},
                "solaire":              {"type": "double"},
                "hydraulique":          {"type": "double"},
                "pompage":              {"type": "double"},
                "bioenergies":          {"type": "double"},

                # Échanges et émissions
                "ech_physiques":        {"type": "double"},
                "taux_co2":             {"type": "double"},
            }
        },
        "settings": {
            "number_of_shards":   1,
            "number_of_replicas": 0,   # 0 réplicas — cluster single node
        }
    }

    client.indices.create(index=OS_INDEX, body=mapping)
    ok(f"Index '{OS_INDEX}' créé avec mapping")

# ============================================================
# Connexion au consumer Kafka
# ============================================================

def create_consumer():
    """Crée et retourne un KafkaConsumer."""
    info(f"Connexion au topic Kafka: {KAFKA_TOPIC}...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="earliest",   # relire depuis le début si nouveau groupe
        enable_auto_commit=True,        # commit automatique des offsets
        auto_commit_interval_ms=5000,
        consumer_timeout_ms=POLL_TIMEOUT_MS,
    )
    ok(f"Consumer connecté au topic '{KAFKA_TOPIC}'")
    return consumer

# ============================================================
# Indexation dans OpenSearch
# ============================================================

def index_batch(client, batch):
    """
    Indexe un lot de documents dans OpenSearch en utilisant
    l'API bulk pour de meilleures performances.
    """
    if not batch:
        return 0, 0

    actions = []
    for doc in batch:
        # Utiliser région + date_heure comme ID pour éviter les doublons
        region = doc.get("libelle_region", "unknown")
        date_heure = doc.get("date_heure", "unknown")
        doc_id = f"{region}_{date_heure}".replace(" ", "_").replace(":", "-")

        actions.append({
            "_index": OS_INDEX,
            "_id": doc_id,
            "_source": doc,
        })

    try:
        nb_ok, errors = helpers.bulk(
            client,
            actions,
            raise_on_error=False,
            stats_only=False,
        )
        nb_fail = len(errors) if errors else 0
        return nb_ok, nb_fail

    except Exception as e:
        fail(f"Erreur bulk indexation: {e}")
        return 0, len(batch)

# ============================================================
# Boucle principale
# ============================================================

def main():
    print(f"\n{'='*55}")
    print(f"  Consumer Kafka → OpenSearch")
    print(f"  Topic:  {KAFKA_TOPIC}")
    print(f"  Index:  {OS_INDEX}")
    print(f"  Group:  {KAFKA_GROUP_ID}")
    print(f"{'='*55}\n")

    # Connexions
    os_client = create_opensearch_client()
    create_index_if_not_exists(os_client)
    consumer = create_consumer()

    # Statistiques globales
    total_messages  = 0
    total_indexed   = 0
    total_errors    = 0

    info("En attente de messages Kafka...")

    batch = []

    try:
        for message in consumer:
            doc = message.value

            region     = doc.get("libelle_region", "?")
            date_heure = doc.get("date_heure", "?")
            conso      = doc.get("consommation", "?")

            info(f"Message reçu: {region} | {date_heure} | conso={conso} MW")

            batch.append(doc)
            total_messages += 1

            # Indexer par lots de BATCH_SIZE
            if len(batch) >= BATCH_SIZE:
                nb_ok, nb_fail = index_batch(os_client, batch)
                total_indexed += nb_ok
                total_errors  += nb_fail
                ok(f"Lot indexé: {nb_ok} OK, {nb_fail} erreurs "
                   f"(total: {total_indexed} indexés)")
                batch = []

    except StopIteration:
        # consumer_timeout_ms atteint — plus de messages pour l'instant
        if batch:
            nb_ok, nb_fail = index_batch(os_client, batch)
            total_indexed += nb_ok
            total_errors  += nb_fail
            batch = []

    except KeyboardInterrupt:
        info("Arrêt demandé par l'utilisateur")

    finally:
        # Indexer ce qui reste dans le batch
        if batch:
            nb_ok, nb_fail = index_batch(os_client, batch)
            total_indexed += nb_ok
            total_errors  += nb_fail

        consumer.close()

        print(f"\n{'='*55}")
        print(f"  Résumé final")
        print(f"  Messages reçus  : {total_messages:,}")
        print(f"  Indexés         : {total_indexed:,}")
        print(f"  Erreurs         : {total_errors:,}")
        print(f"{'='*55}\n")

if __name__ == "__main__":
    main()
