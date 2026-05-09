"""
index_opensearch.py — Re-indexation des données DVF dans OpenSearch
====================================================================
Lit les données enrichies depuis le warehouse Parquet (s3a://warehouse/dvf_enrichi/)
et les indexe dans OpenSearch avec un mapping geo_point.

Usage:
    docker exec spark-master spark-submit /scripts/data/index_opensearch.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import requests
import urllib3
import sys

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def ok(msg):    print(f"{GREEN}  [OK]{RESET} {msg}")
def fail(msg):  print(f"{RED}  [FAIL]{RESET} {msg}")
def info(msg):  print(f"{BLUE}  [INFO]{RESET} {msg}")
def section(t): print(f"\n{BOLD}{YELLOW}{'='*55}{RESET}\n{BOLD}{YELLOW}  {t}{RESET}\n{BOLD}{YELLOW}{'='*55}{RESET}")

S3_WAREHOUSE = "s3a://warehouse/dvf_enrichi/"
OS_INDEX     = "dvf-mutations"
OS_HOST      = "opensearch"
OS_PORT      = "9200"
OS_BASE_URL  = f"http://{OS_HOST}:{OS_PORT}"

OS_OPTIONS = {
    "opensearch.nodes":              OS_HOST,
    "opensearch.port":               OS_PORT,
    "opensearch.nodes.wan.only":     "true",
    "opensearch.net.ssl":            "false",
    "opensearch.batch.size.bytes":   "5mb",
    "opensearch.batch.size.entries": "1000",
}

# ============================================================
# SparkSession
# ============================================================
section("Initialisation SparkSession")

try:
    spark = SparkSession.builder \
        .appName("index_opensearch_dvf") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    ok(f"SparkSession créée — Spark {spark.version}")
except Exception as e:
    fail(f"Impossible de créer la SparkSession: {e}")
    sys.exit(1)

# ============================================================
# Lecture du warehouse
# ============================================================
section("Lecture du warehouse Parquet")

try:
    info(f"Lecture depuis {S3_WAREHOUSE}...")
    df = spark.read.parquet(S3_WAREHOUSE)
    count = df.count()
    ok(f"{count:,} transactions chargées depuis le warehouse")
except Exception as e:
    fail(f"Erreur lecture warehouse: {e}")
    sys.exit(1)

# ============================================================
# Préparation du DataFrame (geo_point)
# ============================================================
section("Préparation du DataFrame pour OpenSearch")

try:
    df_os = df.select(
        "code_commune", "nom_commune", "code_departement",
        "type_local", "annee", "mois", "date_mutation",
        "valeur_fonciere", "prix_m2", "surface_bati", "nb_pieces",
        "nb_entreprises_actives", "diversite_sectorielle",
        "nb_creations_annee", "taux_creation_entreprises",
        "latitude", "longitude",
        F.when(
            F.col("latitude").isNotNull() &
            F.col("longitude").isNotNull() &
            (F.col("latitude") != 0.0) &
            (F.col("longitude") != 0.0),
            F.struct(
                F.col("latitude").alias("lat"),
                F.col("longitude").alias("lon")
            )
        ).alias("location")
    )
    ok("DataFrame préparé avec champ geo_point 'location'")
except Exception as e:
    fail(f"Erreur préparation DataFrame: {e}")
    sys.exit(1)

# ============================================================
# Création du mapping OpenSearch
# ============================================================
section("Création du mapping OpenSearch")

try:
    resp = requests.delete(f"{OS_BASE_URL}/{OS_INDEX}", verify=False)
    info(f"Suppression index existant: HTTP {resp.status_code}")

    mapping = {
        "mappings": {
            "properties": {
                "code_commune":              {"type": "keyword"},
                "nom_commune":               {"type": "text"},
                "code_departement":          {"type": "keyword"},
                "type_local":                {"type": "keyword"},
                "annee":                     {"type": "integer"},
                "mois":                      {"type": "integer"},
                "date_mutation":             {"type": "date"},
                "valeur_fonciere":           {"type": "double"},
                "prix_m2":                   {"type": "double"},
                "surface_bati":              {"type": "double"},
                "nb_pieces":                 {"type": "integer"},
                "nb_entreprises_actives":    {"type": "integer"},
                "diversite_sectorielle":     {"type": "integer"},
                "nb_creations_annee":        {"type": "long"},
                "taux_creation_entreprises": {"type": "double"},
                "latitude":                  {"type": "double"},
                "longitude":                 {"type": "double"},
                "location":                  {"type": "geo_point"},
            }
        }
    }
    resp = requests.put(f"{OS_BASE_URL}/{OS_INDEX}", json=mapping, verify=False)
    if resp.status_code in (200, 201):
        ok(f"Index '{OS_INDEX}' créé avec mapping geo_point")
    else:
        fail(f"Erreur création index: {resp.text}")
        sys.exit(1)
except Exception as e:
    fail(f"Erreur mapping OpenSearch: {e}")
    sys.exit(1)

# ============================================================
# Indexation
# ============================================================
section("Indexation dans OpenSearch")

try:
    df_with_loc    = df_os.filter(F.col("location").isNotNull())
    df_without_loc = df_os.filter(F.col("location").isNull()).drop("location")

    count_with    = df_with_loc.count()
    count_without = df_without_loc.count()
    info(f"Documents avec coordonnées GPS : {count_with:,}")
    info(f"Documents sans coordonnées GPS : {count_without:,}")

    info("Indexation des documents avec geo_point...")
    df_with_loc.write \
        .format("opensearch") \
        .option("opensearch.resource", OS_INDEX) \
        .options(**OS_OPTIONS) \
        .mode("append") \
        .save()
    ok(f"{count_with:,} documents avec location indexés")

    if count_without > 0:
        info("Indexation des documents sans geo_point...")
        df_without_loc.write \
            .format("opensearch") \
            .option("opensearch.resource", OS_INDEX) \
            .options(**OS_OPTIONS) \
            .mode("append") \
            .save()
        ok(f"{count_without:,} documents sans location indexés")

    ok(f"Index '{OS_INDEX}' complet — {count_with + count_without:,} documents au total")

except Exception as e:
    fail(f"Erreur indexation: {e}")
    sys.exit(1)

# ============================================================
# Vérification finale
# ============================================================
section("Vérification")

try:
    resp = requests.get(f"{OS_BASE_URL}/_cat/indices/{OS_INDEX}?v", verify=False)
    info(f"État de l'index:\n{resp.text}")
    ok("Indexation DVF dans OpenSearch terminée avec succès !")
except Exception as e:
    info(f"Vérification impossible: {e}")

spark.stop()
sys.exit(0)
