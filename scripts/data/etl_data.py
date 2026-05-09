from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, DateType
)

# Nécessaire pour créer un mapping explicite avant d'indexer les données SIRENE
import requests
from requests.auth import HTTPBasicAuth
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import sys

# ============================================================
# Couleurs pour les logs
# ============================================================

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
 
# ============================================================
# Configuration
# ============================================================
 
# Chemins S3
S3_DVF_PATH      = "s3a://raw-data/dvf/"
S3_INSEE_PATH    = "s3a://raw-data/insee/communes_france_2025.csv"
S3_SIRENE_PATH   = "s3a://raw-data/sirene/sirene_etablissements.csv"
S3_WAREHOUSE     = "s3a://warehouse/dvf_enrichi/"
 
# Hive
HIVE_DB          = "dvf"
HIVE_TABLE       = "mutations_enrichies"
 
# OpenSearch
OS_INDEX         = "dvf-mutations"
 
# Filtres
TYPES_BIENS      = ["Maison", "Appartement"]
NATURES_VALIDES  = ["Vente", "Vente en l'état futur d'achèvement"]
 
# Codes NAF secteurs d'intérêt pour l'analyse de corrélation
# (immobilier, construction, services, tech)
SECTEURS_NAF = {
    "immobilier":   "68",
    "construction": "41",
    "tech":         "62",
    "commerce":     "47",
    "finance":      "64",
}

OS_OPTIONS = {
    "opensearch.nodes":           "opensearch",
    "opensearch.port":            "9200",
    "opensearch.nodes.wan.only":  "true",
    "opensearch.net.ssl":         "false",
    "opensearch.batch.size.bytes": "5mb",
    "opensearch.batch.size.entries": "1000",
    }
 
# ============================================================
# Initialisation de SparkSession
# ============================================================

section("Initialisation SparkSession")
 
try:
    spark = SparkSession.builder \
        .appName("etl_dvf_enrichi") \
        .enableHiveSupport() \
        .getOrCreate()
 
    spark.sparkContext.setLogLevel("WARN")
    ok(f"SparkSession créée — Spark {spark.version}")
 
except Exception as e:
    fail(f"Impossible de créer la SparkSession: {e}")
    sys.exit(1)
 
# ============================================================
# ÉTAPE 1 — Lecture et nettoyage DVF
# ============================================================

section("ÉTAPE 1 — Lecture et nettoyage DVF")
 
try:
    info("Lecture des fichiers DVF depuis SeaweedFS...")
 
    # DVF utilise | comme séparateur, encodage latin-1
    df_dvf_raw = spark.read \
        .option("sep", "|") \
        .option("header", "true") \
        .option("encoding", "latin1") \
        .option("inferSchema", "false") \
        .csv(S3_DVF_PATH)
 
    total_raw = df_dvf_raw.count()
    ok(f"DVF brut: {total_raw:,} lignes lues")
    info(f"Colonnes DVF: {df_dvf_raw.columns[:10]}...")
 
    # Nettoyage et sélection des colonnes utiles
    info("Nettoyage DVF...")
    df_dvf = df_dvf_raw.select(
        # Identifiants
        F.col("No disposition").alias("id_disposition"),
        F.col("Nature mutation").alias("nature_mutation"),
 
        # Date — conversion en date et extraction année/mois
        F.to_date(F.col("Date mutation"), "dd/MM/yyyy").alias("date_mutation"),
        F.year(F.to_date(F.col("Date mutation"), "dd/MM/yyyy")).alias("annee"),
        F.month(F.to_date(F.col("Date mutation"), "dd/MM/yyyy")).alias("mois"),
 
        # Valeur
        F.regexp_replace(F.col("Valeur fonciere"), ",", ".").cast(DoubleType()).alias("valeur_fonciere"),
 
        # Localisation
        F.col("Code departement").alias("code_departement"),
        F.lpad(F.concat_ws("",
            F.col("Code departement"),
            F.col("Code commune")
        ), 5, "0").alias("code_commune"),
        F.col("Commune").alias("nom_commune"),
        F.col("Code postal").alias("code_postal"),
 
        # Bien
        F.col("Type local").alias("type_local"),
        F.col("Surface reelle bati").cast(DoubleType()).alias("surface_bati"),
        F.col("Nombre pieces principales").cast(IntegerType()).alias("nb_pieces"),
        F.col("Surface terrain").cast(DoubleType()).alias("surface_terrain"),
    )
 
    # Filtres qualité
    info(f"Filtrage: conservation de {TYPES_BIENS} uniquement...")
    df_dvf = df_dvf.filter(
        (F.col("type_local").isin(TYPES_BIENS)) &
        (F.col("nature_mutation").isin(NATURES_VALIDES)) &
        (F.col("valeur_fonciere") > 1000) &       # valeurs aberrantes
        (F.col("valeur_fonciere") < 50_000_000) & # valeurs aberrantes
        (F.col("surface_bati") > 5) &              # surfaces impossibles
        (F.col("surface_bati") < 2000) &           # surfaces impossibles
        (F.col("annee").isNotNull()) &
        (F.col("code_commune").isNotNull())
    )
 
    # Calcul du prix au m²
    info("Calcul du prix_m2...")
    df_dvf = df_dvf.withColumn(
        "prix_m2",
        F.round(F.col("valeur_fonciere") / F.col("surface_bati"), 2)
    ).filter(
        (F.col("prix_m2") > 100) &    # prix_m2 aberrants
        (F.col("prix_m2") < 50_000)   # prix_m2 aberrants
    )
 
    count_dvf = df_dvf.count()
    ok(f"DVF nettoyé: {count_dvf:,} transactions valides "
       f"({100*count_dvf/total_raw:.1f}% conservées)")
 
except Exception as e:
    fail(f"Erreur lecture DVF: {e}")
    sys.exit(1)
 
# ============================================================
# ÉTAPE 2 — Lecture et nettoyage INSEE
# ============================================================

section("ÉTAPE 2 — Lecture et nettoyage INSEE")
 
try:
    info("Lecture du fichier communes INSEE...")
 
    df_insee_raw = spark.read \
        .option("sep", ",") \
        .option("header", "true") \
        .option("encoding", "utf-8") \
        .csv(S3_INSEE_PATH)
 
    info(f"Colonnes INSEE disponibles: {df_insee_raw.columns[:15]}...")
 
    # Sélection des colonnes utiles
    # Note: les noms de colonnes peuvent varier selon la version du fichier
    # On sélectionne par position si les noms diffèrent
    df_insee = df_insee_raw.select(
        F.col("code_commune_INSEE").alias("code_commune"),
        F.col("nom_commune_postal").alias("nom_commune_insee"),
        F.col("code_departement").alias("departement"),
        F.col("latitude").cast(DoubleType()).alias("latitude"),
        F.col("longitude").cast(DoubleType()).alias("longitude"),
    ).filter(
        F.col("code_commune").isNotNull() &
        (F.length(F.col("code_commune")) == 5)
    )
 
    count_insee = df_insee.count()
    ok(f"INSEE: {count_insee:,} communes chargées")
 
except Exception as e:
    fail(f"Erreur lecture INSEE: {e}")
    fail("Vérifiez les noms de colonnes avec: df_insee_raw.printSchema()")
    sys.exit(1)
 
# ============================================================
# ÉTAPE 3 — Lecture et nettoyage SIRENE + métriques par commune/année
# ============================================================
section("ÉTAPE 3 — Lecture SIRENE et métriques par commune/année")
 
try:
    info("Lecture du fichier SIRENE...")
 
    df_sirene_raw = spark.read \
        .option("sep", ",") \
        .option("header", "true") \
        .option("encoding", "utf-8") \
        .option("multiline", "true") \
        .option("quote", '"') \
        .csv(S3_SIRENE_PATH)
 
    info(f"SIRENE brut: {df_sirene_raw.count():,} établissements")
 
    # Sélection des colonnes utiles
    df_sirene = df_sirene_raw.select(
        F.col("siret"),
        F.col("codeCommuneEtablissement").alias("code_commune"),
        F.col("activitePrincipaleEtablissement").alias("code_naf"),
        F.col("activitePrincipaleEtablissement").substr(1, 2).alias("section_naf"),
        F.col("etatAdministratifEtablissement").alias("etat"),
        F.col("trancheEffectifsEtablissement").alias("date_creation"),
        F.year(F.to_date(
            F.col("dateCreationEtablissement"), "yyyy-MM-dd"
        )).alias("annee_creation"),
        F.col("trancheEffectifsEtablissement").alias("tranche_effectifs"),
    ).filter(
        F.col("code_commune").isNotNull() &
        (F.length(F.col("code_commune")) == 5)
    )
 
    # ---------------------------------------------------------
    # Métrique 1: entreprises ACTIVES par commune (snapshot actuel)
    # ---------------------------------------------------------
    
    info("Calcul des entreprises actives par commune...")
    df_actives = df_sirene \
        .filter(F.col("etat") == "A") \
        .groupBy("code_commune") \
        .agg(
            F.count("siret").alias("nb_entreprises_actives"),
            F.countDistinct("section_naf").alias("diversite_sectorielle"),
 
            # Décompte par secteur d'intérêt
            F.sum(F.when(F.col("section_naf") == "68", 1).otherwise(0))
             .alias("nb_entreprises_immobilier"),
            F.sum(F.when(F.col("section_naf") == "41", 1).otherwise(0))
             .alias("nb_entreprises_construction"),
            F.sum(F.when(F.col("section_naf") == "62", 1).otherwise(0))
             .alias("nb_entreprises_tech"),
            F.sum(F.when(F.col("section_naf") == "47", 1).otherwise(0))
             .alias("nb_entreprises_commerce"),
        )
 
    ok(f"Métriques actives calculées: {df_actives.count():,} communes")
 
    # ---------------------------------------------------------
    # Métrique 2: créations d'entreprises par commune et par année
    # ---------------------------------------------------------
    
    info("Calcul des créations par commune et par année...")
    df_creations = df_sirene \
        .filter(
            F.col("annee_creation").isNotNull() &
            F.col("annee_creation").between(2019, 2025)
        ) \
        .groupBy("code_commune", "annee_creation") \
        .agg(
            F.count("siret").alias("nb_creations_annee"),
            F.sum(F.when(F.col("section_naf") == "62", 1).otherwise(0))
             .alias("nb_creations_tech"),
            F.sum(F.when(F.col("section_naf") == "41", 1).otherwise(0))
             .alias("nb_creations_construction"),
        ) \
        .withColumnRenamed("annee_creation", "annee")
 
    ok(f"Métriques créations calculées: {df_creations.count():,} paires commune/année")
 
except Exception as e:
    fail(f"Erreur lecture SIRENE: {e}")
    fail("Assurez-vous que le fichier SIRENE est dans s3a://raw-data/sirene/")
    sys.exit(1)
 
# ============================================================
# ÉTAPE 4 — Join triple DVF + INSEE + SIRENE
# ============================================================

section("ÉTAPE 4 — Join triple DVF + INSEE + SIRENE")
 
try:
    info("Join DVF + INSEE par code_commune...")
    df_join1 = df_dvf.join(
        df_insee,
        on="code_commune",
        how="left"   # left pour conserver toutes les transactions DVF
    )
    ok(f"Après join DVF+INSEE: {df_join1.count():,} lignes")
 
    info("Join + SIRENE actives par code_commune...")
    df_join2 = df_join1.join(
        df_actives,
        on="code_commune",
        how="left"
    )
 
    info("Join + SIRENE créations par code_commune ET année...")
    df_final = df_join2.join(
        df_creations,
        on=["code_commune", "annee"],
        how="left"
    )
 
    count_final = df_final.count()
    ok(f"Table enrichie finale: {count_final:,} transactions")
 
    # Métriques dérivées post-join
    info("Calcul des métriques dérivées...")
    df_final = df_final.withColumn(
        # Taux de création (créations année N / total actives)
        "taux_creation_entreprises",
        F.when(
            F.col("nb_entreprises_actives") > 0,
            F.round(
                F.col("nb_creations_annee") * 100 / F.col("nb_entreprises_actives"), 2
            )
        ).otherwise(None)
    )
 
    ok("Métriques dérivées calculées")
 
    # Aperçu des données finales
    info("Aperçu de la table enrichie:")
    df_final.select(
        "code_commune", "nom_commune", "type_local", "annee",
        "valeur_fonciere", "prix_m2", "surface_bati",
        "nb_entreprises_actives","nb_creations_annee", 
        "taux_creation_entreprises"
    ).show(5, truncate=True)
 
except Exception as e:
    fail(f"Erreur lors des joins: {e}")
    sys.exit(1)
 
# ============================================================
# ÉTAPE 5 — Écriture Parquet dans SeaweedFS
# ============================================================
section("ÉTAPE 5 — Écriture Parquet dans SeaweedFS")
 
try:
    info(f"Écriture Parquet dans {S3_WAREHOUSE}...")
    info("Partitionnement par code_departement et annee...")
 
    df_final.write \
        .mode("overwrite") \
        .partitionBy("code_departement", "annee") \
        .parquet(S3_WAREHOUSE)
 
    ok("Données écrites en Parquet avec succès")
 
    # Vérification
    df_check = spark.read.parquet(S3_WAREHOUSE)
    ok(f"Vérification: {df_check.count():,} lignes lisibles depuis S3")
 
    info("Partitions disponibles:")
    df_check.select("code_departement", "annee") \
        .distinct() \
        .orderBy("code_departement", "annee") \
        .show(10)
 
except Exception as e:
    fail(f"Erreur écriture Parquet: {e}")
    sys.exit(1)
 
# ============================================================
# ÉTAPE 6 — Enregistrement dans Hive Metastore
# ============================================================
section("ÉTAPE 6 — Enregistrement dans Hive Metastore")
 
try:
    info(f"Création de la base de données '{HIVE_DB}'...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
 
    info(f"Suppression de l'ancienne table si elle existe...")
    spark.sql(f"DROP TABLE IF EXISTS {HIVE_DB}.{HIVE_TABLE}")
 
    info(f"Création de la table externe '{HIVE_DB}.{HIVE_TABLE}'...")
    spark.sql(f"""
        CREATE EXTERNAL TABLE {HIVE_DB}.{HIVE_TABLE} (
            id_disposition          STRING,
            nature_mutation         STRING,
            date_mutation           DATE,
            mois                    INT,
            valeur_fonciere         DOUBLE,
            code_commune            STRING,
            nom_commune             STRING,
            code_postal             STRING,
            type_local              STRING,
            surface_bati            DOUBLE,
            nb_pieces               INT,
            surface_terrain         DOUBLE,
            prix_m2                 DOUBLE,
            nom_commune_insee       STRING,
            departement             STRING,
            latitude                DOUBLE,
            longitude               DOUBLE,
            nb_entreprises_actives  INT,
            diversite_sectorielle   INT,
            nb_entreprises_immobilier INT,
            nb_entreprises_construction INT,
            nb_entreprises_tech     INT,
            nb_entreprises_commerce INT,
            nb_creations_annee      LONG,
            nb_creations_tech       LONG,
            nb_creations_construction LONG,
            taux_creation_entreprises DOUBLE
        )
        PARTITIONED BY (code_departement STRING, annee INT)
        STORED AS PARQUET
        LOCATION '{S3_WAREHOUSE}'
    """)
 
    info("Récupération des partitions Parquet dans Hive...")
    spark.sql(f"MSCK REPAIR TABLE {HIVE_DB}.{HIVE_TABLE}")
 
    # Vérification
    result = spark.sql(f"SELECT COUNT(*) as total FROM {HIVE_DB}.{HIVE_TABLE}").collect()
    ok(f"Table Hive '{HIVE_DB}.{HIVE_TABLE}': {result[0]['total']:,} lignes indexées")
 
    info("Partitions Hive enregistrées:")
    spark.sql(f"SHOW PARTITIONS {HIVE_DB}.{HIVE_TABLE}").show(10)
 
except Exception as e:
    fail(f"Erreur Hive Metastore: {e}")
    sys.exit(1)
 
# ============================================================
# ÉTAPE 7 — Indexation dans OpenSearch
# ============================================================
section("ÉTAPE 7 — Indexation dans OpenSearch")
 
try:
    info(f"Indexation dans OpenSearch (index: '{OS_INDEX}')...")
    info("Sélection des champs pertinents pour la recherche...")
 
    # On n'indexe pas tout dans OpenSearch — seulement les champs utiles
    # pour les dashboards et les recherches
    df_os = df_final.select(
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

    # Supprimer l'index s'il existe déjà
    requests.delete(
        f"http://opensearch:9200/{OS_INDEX}",
        auth=HTTPBasicAuth("admin", "admin"),
        verify=False
    )

    # Créer l'index avec le bon mapping geo_point
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
                "nb_creations_annee":        {"type": "long"},
                "taux_creation_entreprises": {"type": "double"},
                "latitude":                  {"type": "double"},
                "longitude":                 {"type": "double"},
                "location":                  {"type": "geo_point"}
            }
        }
    }

    response = requests.put(
        f"http://opensearch:9200/{OS_INDEX}",
        json=mapping,
        auth=HTTPBasicAuth("admin", "admin"),
        verify=False
    )

    if response.status_code == 200:
        ok(f"Mapping geo_point créé pour l'index '{OS_INDEX}'")
    else:
        fail(f"Erreur création mapping: {response.text}")  
      
    # Filtrer les documents avec location vide AVANT d'indexer
    # pour ignorer location pour les communes sans coordonnées
    df_os_with_loc = df_os.filter(F.col("location").isNotNull())
    df_os_without_loc = df_os.filter(F.col("location").isNull()).drop("location")
    
    # Indexer les deux séparément
    df_os_with_loc.write \
        .format("opensearch") \
        .option("opensearch.resource", OS_INDEX) \
        .options(**OS_OPTIONS) \
        .mode("append") \
        .save()

    df_os_without_loc.write \
        .format("opensearch") \
        .option("opensearch.resource", OS_INDEX) \
        .options(**OS_OPTIONS) \
        .mode("append") \
        .save()
 
    ok(f"Index '{OS_INDEX}' créé dans OpenSearch")
 
except Exception as e:
    fail(f"Erreur OpenSearch: {e}")
    fail("Le pipeline Parquet/Hive est complet — OpenSearch est optionnel")
 
# ============================================================
# RÉSUMÉ FINAL
# ============================================================
section("RÉSUMÉ FINAL")
 
try:
    stats = spark.sql(f"""
        SELECT
            COUNT(*)                            AS total_transactions,
            COUNT(DISTINCT code_commune)        AS nb_communes,
            COUNT(DISTINCT code_departement)    AS nb_departements,
            MIN(annee)                          AS annee_debut,
            MAX(annee)                          AS annee_fin,
            ROUND(AVG(prix_m2), 0)              AS prix_m2_moyen,
            ROUND(MIN(prix_m2), 0)              AS prix_m2_min,
            ROUND(MAX(prix_m2), 0)              AS prix_m2_max,
            COUNT(DISTINCT type_local)          AS nb_types_biens
        FROM {HIVE_DB}.{HIVE_TABLE}
    """).collect()[0]
 
    print(f"""
  Transactions totales    : {stats['total_transactions']:>12,}
  Communes couvertes      : {stats['nb_communes']:>12,}
  Départements            : {stats['nb_departements']:>12,}
  Période                 : {stats['annee_debut']} → {stats['annee_fin']}
  Prix m² moyen           : {stats['prix_m2_moyen']:>12,.0f} €
  Prix m² min/max         : {stats['prix_m2_min']:,.0f} € / {stats['prix_m2_max']:,.0f} €
    """)
 
    ok("Pipeline ETL DVF + INSEE + SIRENE terminé avec succès !")
    print(f"\n  Prochaine étape:")
    print(f"  Lancer les analyses SQL avec Trino:")
    print(f"  docker exec -it trino trino --catalog hive --schema dvf")
 
except Exception as e:
    ok("Pipeline terminé — statistiques non disponibles")
 
spark.stop()
