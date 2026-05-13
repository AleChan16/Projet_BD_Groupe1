"""
eco2mix_producer.py — Producer Kafka pour éCO2mix
==================================================
Interroge l'API éCO2mix (RTE) toutes les 15 minutes et publie
les données de consommation énergétique régionale dans un topic Kafka.

Usage:
  docker exec spark-master python3 /scripts/kafka/eco2mix_producer.py

Dépendances (dans l'image Spark):
  pip install kafka-python requests
"""

import json
import time
import requests
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ============================================================
# Configuration
# ============================================================

KAFKA_BROKER    = "kafka:9092"
KAFKA_TOPIC     = "eco2mix-regional"
API_BASE_URL    = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-tr/records"
POLL_INTERVAL   = 900   # 15 minutes en secondes
BATCH_SIZE      = 100   # nombre de records par appel API

# Champs utiles à conserver (l'API retourne ~40 champs)
CHAMPS_UTILES = [
    "date_heure",
    "libelle_region",
    "code_insee_region",
    "consommation",
    "production",
    "thermique",
    "nucleaire",
    "eolien",
    "solaire",
    "hydraulique",
    "pompage",
    "bioenergies",
    "ech_physiques",
    "taux_co2",
]

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
# Connexion au broker Kafka
# ============================================================

def create_producer():
    """Crée et retourne un KafkaProducer avec sérialisation JSON."""
    info(f"Connexion au broker Kafka: {KAFKA_BROKER}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",           # attend confirmation de tous les replicas
            retries=3,            # retry automatique si envoi échoue
            request_timeout_ms=30000,
        )
        ok(f"Connecté au broker Kafka")
        return producer
    except KafkaError as e:
        fail(f"Impossible de se connecter à Kafka: {e}")
        raise

# ============================================================
# Appel à l'API éCO2mix
# ============================================================

def fetch_eco2mix(start_datetime=None, limit=BATCH_SIZE):
    """
    Interroge l'API éCO2mix et retourne une liste de records.

    Args:
        start_datetime: datetime à partir de laquelle récupérer les données.
                        Si None, récupère les données des dernières 24h.
        limit: nombre maximum de records à récupérer.
    """
    if start_datetime is None:
        start_datetime = datetime.now(timezone.utc) - timedelta(hours=24)

    # Format ISO 8601 requis par l'API
    start_str = start_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "limit": limit,
        "order_by": "date_heure DESC",
        "where": f"date_heure >= '{start_str}' AND consommation IS NOT NULL",
        "timezone": "Europe/Paris",
    }

    try:
        response = requests.get(API_BASE_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        records = data.get("results", [])
        info(f"API éCO2mix: {len(records)} records récupérés depuis {start_str}")
        return records

    except requests.exceptions.RequestException as e:
        fail(f"Erreur API éCO2mix: {e}")
        return []

# ============================================================
# Nettoyage et enrichissement d'un record
# ============================================================

def clean_record(record):
    """
    Nettoie un record éCO2mix et ne garde que les champs utiles.
    Ajoute un timestamp d'ingestion pour traçabilité.
    """
    cleaned = {}

    for champ in CHAMPS_UTILES:
        valeur = record.get(champ)
        cleaned[champ] = valeur

    # Timestamp d'ingestion (quand on a reçu le message, pas quand il a été produit)
    cleaned["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()

    # Conversion explicite des valeurs numériques
    for champ_num in ["consommation", "production", "thermique", "nucleaire",
                       "eolien", "solaire", "hydraulique", "pompage",
                       "bioenergies", "ech_physiques", "taux_co2"]:
        val = cleaned.get(champ_num)
        if val is not None:
            try:
                cleaned[champ_num] = float(val)
            except (ValueError, TypeError):
                cleaned[champ_num] = None

    return cleaned

# ============================================================
# Publication dans Kafka
# ============================================================

def publish_records(producer, records):
    """
    Publie une liste de records dans le topic Kafka.
    Utilise 'region_date_heure' comme clé pour garantir l'ordre
    des messages d'une même région.
    """
    nb_ok = 0
    nb_fail = 0

    records_valides = [r for r in records if r.get("consommation") is not None]

    if len(records_valides) < len(records):
      warn(f"{len(records) - len(records_valides)} records ignorés (consommation null)")

    for record in records_valides:
        cleaned = clean_record(record)

        # Clé du message = région + date_heure (garantit l'ordre par région)
        region = cleaned.get("libelle_region", "unknown")
        date_heure = cleaned.get("date_heure", "unknown")
        key = f"{region}_{date_heure}"

        try:
            future = producer.send(
                KAFKA_TOPIC,
                key=key,
                value=cleaned
            )
            future.get(timeout=10)  # attend confirmation
            nb_ok += 1

        except KafkaError as e:
            fail(f"Erreur envoi message [{key}]: {e}")
            nb_fail += 1

    return nb_ok, nb_fail

# ============================================================
# Boucle principale
# ============================================================

def main():
    print(f"\n{'='*55}")
    print(f"  Producer éCO2mix → Kafka")
    print(f"  Topic: {KAFKA_TOPIC}")
    print(f"  Intervalle: {POLL_INTERVAL//60} minutes")
    print(f"{'='*55}\n")

    # Créer le producer
    producer = create_producer()

    # Première exécution — charger les dernières 24h
    info("Chargement initial des dernières 24h...")
    records = fetch_eco2mix(limit=100)
    if records:
        nb_ok, nb_fail = publish_records(producer, records)
        ok(f"Chargement initial: {nb_ok} messages publiés, {nb_fail} erreurs")
    else:
        warn("Aucun record récupéré lors du chargement initial")

    # Boucle infinie — poll toutes les 15 minutes
    info(f"Démarrage de la boucle de poll (toutes les {POLL_INTERVAL//60} min)...")

    while True:
        next_poll = datetime.now(timezone.utc) + timedelta(seconds=POLL_INTERVAL)
        info(f"Prochain poll: {next_poll.strftime('%H:%M:%S')} UTC")
        time.sleep(POLL_INTERVAL)

        info("Poll éCO2mix...")
        # Récupérer seulement les 20 dernières minutes pour éviter les doublons
        since = datetime.now(timezone.utc) - timedelta(minutes=20)
        records = fetch_eco2mix(start_datetime=since, limit=BATCH_SIZE)

        if records:
            nb_ok, nb_fail = publish_records(producer, records)
            ok(f"Poll: {nb_ok} messages publiés, {nb_fail} erreurs")
        else:
            warn("Aucun nouveau record disponible")

        producer.flush()

if __name__ == "__main__":
    main()
