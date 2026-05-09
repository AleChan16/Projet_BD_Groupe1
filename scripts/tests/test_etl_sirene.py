from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import struct
import sys

# ==== Couleurs ====

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

# ==== Configuration ====

S3_SIRENE_PATH = "s3a://raw-data/sirene/sirene_etablissements.csv"
OS_INDEX_SIRENE_COMMUNES   = "sirene-communes"
OS_INDEX_SIRENE_CREATIONS  = "sirene-creations"

# ==== Initialisation de SparkSession ====

section("Initialisation SparkSession")
 
try:
    spark = SparkSession.builder \
        .appName("etl_sirene_test") \
        .getOrCreate()
 
    spark.sparkContext.setLogLevel("WARN")
    ok(f"SparkSession créée — Spark {spark.version}")
 
except Exception as e:
    fail(f"Impossible de créer la SparkSession: {e}")
    sys.exit(1)

# ==== Lecture et nettoyage SIRENE ====

try:
    info("Lecture du fichier SIRENE depuis SeaweedFS...")
    info("Attention: fichier ~4GB, cela peut prendre quelques minutes...")
 
    df_sirene_raw = spark.read \
        .option("sep", ",") \
        .option("header", "true") \
        .option("encoding", "utf-8") \
        .option("multiline", "true") \
        .option("quote", '"') \
        .csv(S3_SIRENE_PATH)
 
    total_raw = df_sirene_raw.count()
    ok(f"SIRENE brut: {total_raw:,} établissements lus")
 
    # Sélection et renommage des colonnes utiles
    info("Sélection des colonnes utiles...")
    df_sirene = df_sirene_raw.select(
        F.col("siret"),
        F.col("codeCommuneEtablissement").alias("code_commune"),
        F.col("libelleCommuneEtablissement").alias("nom_commune"),
        F.col("codePostalEtablissement").alias("code_postal"),
        F.col("activitePrincipaleEtablissement").alias("code_naf"),
        F.col("activitePrincipaleEtablissement").substr(1, 2).alias("section_naf"),
        F.col("etatAdministratifEtablissement").alias("etat"),
        F.col("dateCreationEtablissement").alias("date_creation"),
        F.year(
            F.to_date(F.col("dateCreationEtablissement"), "yyyy-MM-dd")
        ).alias("annee_creation"),
        F.col("trancheEffectifsEtablissement").alias("tranche_effectifs"),
        F.col("etablissementSiege").alias("est_siege"),
        F.col("caractereEmployeurEtablissement").alias("est_employeur"),
    ).filter(
        F.col("code_commune").isNotNull() &
        (F.length(F.col("code_commune")) == 5)
    )
 
    # Mise en cache pour réutilisation dans les deux agrégations
    info("Mise en cache du DataFrame nettoyé...")
    df_sirene.cache()
 
    count_clean = df_sirene.count()
    ok(f"SIRENE nettoyé: {count_clean:,} établissements avec code commune valide")
 
    # Répartition actifs / fermés
    info("Répartition par état administratif:")
    df_sirene.groupBy("etat").count().orderBy("count", ascending=False).show()
 
except Exception as e:
    fail(f"Erreur lecture SIRENE: {e}")
    sys.exit(1)

# ==== Métriques SIRENE par commune ====

try:
    info("Calcul des métriques par commune...")
 
    df_communes = df_sirene \
        .filter(F.col("etat") == "A") \
        .groupBy("code_commune", "nom_commune") \
        .agg(
            F.count("siret").alias("nb_entreprises_actives"),
            F.countDistinct("section_naf").alias("diversite_sectorielle"),
 
            # Top secteurs pour l'analyse de corrélation avec DVF
            F.sum(F.when(F.col("section_naf") == "68", 1).otherwise(0))
             .alias("nb_immobilier"),
            F.sum(F.when(F.col("section_naf") == "41", 1).otherwise(0))
             .alias("nb_construction"),
            F.sum(F.when(F.col("section_naf") == "62", 1).otherwise(0))
             .alias("nb_tech"),
            F.sum(F.when(F.col("section_naf") == "47", 1).otherwise(0))
             .alias("nb_commerce"),
            F.sum(F.when(F.col("section_naf") == "35", 1).otherwise(0))
             .alias("nb_energie"),
 
            # Part d'employeurs
            F.round(
                F.sum(F.when(F.col("est_employeur") == "O", 1).otherwise(0)) * 100.0
                / F.count("siret"), 2
            ).alias("pct_employeurs"),
        )
 
    count_communes = df_communes.count()
    ok(f"Métriques calculées pour {count_communes:,} communes")
 
    info("Top 10 communes par nombre d'entreprises actives:")
    df_communes.orderBy(F.col("nb_entreprises_actives").desc()).show(10, truncate=False)
 
except Exception as e:
    fail(f"Erreur calcul métriques communes: {e}")
    sys.exit(1)

# ==== Chargement des coordonnées depuis INSEE ====

df_coords = spark.read \
    .option("sep", ",") \
    .option("header", "true") \
    .csv("s3a://raw-data/insee/communes_france_2025.csv") \
    .select(
        F.col("code_commune_INSEE").alias("code_commune"),
        F.col("latitude").cast("double").alias("lat"),
        F.col("longitude").cast("double").alias("lon")
    ).filter(F.col("code_commune").isNotNull())

# Join avec les métriques communes
df_communes_geo = df_communes.join(df_coords, on="code_commune", how="left")

# Créer le champ geo_point au format OpenSearch

df_communes_geo = df_communes_geo.withColumn(
    "location",
    F.when(
        F.col("lat").isNotNull() & F.col("lon").isNotNull(),
        F.struct(
            F.col("lat").alias("lat"),
            F.col("lon").alias("lon")
        )
    )
)
# ==== Indexation dans OpenSearch ====

OS_OPTIONS = {
    "opensearch.nodes":           "opensearch",
    "opensearch.port":            "9200",
    "opensearch.nodes.wan.only":  "true",
    "opensearch.net.ssl":         "false",
    "opensearch.batch.size.bytes": "5mb",
    "opensearch.batch.size.entries": "1000",
}
 
# Index 1 — métriques par commune
try:
    info(f"Indexation des métriques communes dans '{OS_INDEX_SIRENE_COMMUNES}'...")
 
    df_communes_geo.write \
        .format("opensearch") \
        .option("opensearch.resource", OS_INDEX_SIRENE_COMMUNES) \
        .options(**OS_OPTIONS) \
        .mode("overwrite") \
        .save()
 
    ok(f"Index '{OS_INDEX_SIRENE_COMMUNES}' créé — {count_communes:,} documents")
 
except Exception as e:
    fail(f"Erreur indexation communes: {e}")

# Index 2 — créations par commune/année
try:
    info(f"Indexation des créations dans '{OS_INDEX_SIRENE_CREATIONS}'...")
 
    df_creations.write \
        .format("opensearch") \
        .option("opensearch.resource", OS_INDEX_SIRENE_CREATIONS) \
        .options(**OS_OPTIONS) \
        .mode("overwrite") \
        .save()
 
    ok(f"Index '{OS_INDEX_SIRENE_CREATIONS}' créé — {count_creations:,} documents")
 
except Exception as e:
    fail(f"Erreur indexation créations: {e}")

df_sirene.unpersist()
spark.stop()


