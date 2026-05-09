"""
test_stack.py — Script de validation du stack Big Data
=======================================================
Tester la connectivité et les operations de base de:
  1. SeaweedFS (lecture/écriture S3 via s3a://)
  2. Hive Metastore (création de table externe Parquet)
  3. OpenSearch (indexation et lecture de documents)
 
Usage (depuis le conteneur spark-master):
  docker exec -it spark-master bash
  cd /data/scripts
  spark-submit test_stack.py
 
Ou directement depuis Linux:
  docker exec spark-master spark-submit /data/scripts/test_stack.py
"""
 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, avg, count
import sys
 
# ============================================================
# Couleurs pour la sortie sur le terminal
# ============================================================
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
RESET  = "\033[0m"
BOLD   = "\033[1m"
 
def ok(msg):
    print(f"{GREEN}  [OK]{RESET} {msg}")
 
def fail(msg):
    print(f"{RED}  [FAIL]{RESET} {msg}")
 
def info(msg):
    print(f"{BLUE}  [INFO]{RESET} {msg}")
 
def section(title):
    print(f"\n{BOLD}{YELLOW}{'='*55}{RESET}")
    print(f"{BOLD}{YELLOW}  {title}{RESET}")
    print(f"{BOLD}{YELLOW}{'='*55}{RESET}")
 
# ============================================================
# Test data
# ============================================================
TEST_DATA = [
    ("Alice",   "Engineering", 85000.0, 5),
    ("Bob",     "Marketing",   72000.0, 3),
    ("Clara",   "Engineering", 91000.0, 7),
    ("David",   "HR",          65000.0, 2),
    ("Elena",   "Engineering", 88000.0, 6),
    ("Frank",   "Marketing",   70000.0, 4),
    ("Grace",   "HR",          67000.0, 3),
    ("Hector",  "Engineering", 95000.0, 9),
]
 
SCHEMA = StructType([
    StructField("Name",       StringType(),  True),
    StructField("Department", StringType(),  True),
    StructField("Salary",     DoubleType(),  True),
    StructField("Years_exp",  IntegerType(), True),
])
 
S3_BUCKET     = "s3a://warehouse/test-stack/"
HIVE_DB       = "test_db"
HIVE_TABLE    = "employees"
OS_INDEX      = "test-employees"
 
results = {"seaweedfs": False, "hive": False, "opensearch": False}
 
# ============================================================
# Initialisation de SparkSession
# ============================================================
section("Initialisation de SparkSession")
 
try:
    spark = SparkSession.builder \
        .appName("test_stack") \
        .enableHiveSupport() \
        .getOrCreate()
 
    spark.sparkContext.setLogLevel("WARN")
    ok(f"SparkSession créée — version Spark: {spark.version}")
 
except Exception as e:
    fail(f"Impossible de créer la SparkSession: {e}")
    sys.exit(1)
 
# ============================================================
# TEST 1 — SeaweedFS
# ============================================================
section("TEST 1 — SeaweedFS (S3 via s3a://)")
 
try:
    info(f"Création du DataFrame de test ({len(TEST_DATA)} lignes)...")
    df = spark.createDataFrame(TEST_DATA, schema=SCHEMA)
 
    info(f"Écriture en Parquet dans {S3_BUCKET}...")
    df.write \
        .mode("overwrite") \
        .parquet(S3_BUCKET)
    ok("Écriture Parquet dans SeaweedFS réussie.")
 
    info(f"Lecture depuis {S3_BUCKET}...")
    df_read = spark.read.parquet(S3_BUCKET)
    count = df_read.count()
 
    if count == len(TEST_DATA):
        ok(f"Lecture réussie — {count} lignes récupérées (attendu: {len(TEST_DATA)}).")
        results["seaweedfs"] = True
    else:
        fail(f"Nombre de lignes incorrect: {count} lu, {len(TEST_DATA)} attendu.")
 
    info("Apercu des données lues depuis SeaweedFS:")
    df_read.show(3, truncate=False)
 
except Exception as e:
    fail(f"Erreur SeaweedFS: {e}")
    print(f"\n  Vérifications à faire:")
    print(f"  - SeaweedFS filer accessible sur http://seaweedfs-filer:8333")
    print(f"  - Bucket 'warehouse' existant (mc ls mysdfs)")
    print(f"  - Credentials s3a correctes dans spark-defaults.conf")
 
# ============================================================
# TEST 2 — Hive Metastore
# ============================================================
section("TEST 2 — Hive Metastore (table externe Parquet)")
 
if not results["seaweedfs"]:
    fail("Test Hive ignoré car SeaweedFS a échoué (les données n'ont pas été écrites).")
else:
    try:
        info(f"Création de la base de données '{HIVE_DB}' si elle n'existe pas...")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
        ok(f"Base de données '{HIVE_DB}' disponible.")
 
        info(f"Suppression de la table '{HIVE_TABLE}' si elle existe (nettoyage)...")
        spark.sql(f"DROP TABLE IF EXISTS {HIVE_DB}.{HIVE_TABLE}")
 
        info(f"Création de la table externe '{HIVE_DB}.{HIVE_TABLE}' pointant vers {S3_BUCKET}...")
        spark.sql(f"""
            CREATE EXTERNAL TABLE {HIVE_DB}.{HIVE_TABLE} (
                name       STRING,
                department STRING,
                salary     DOUBLE,
                years_exp  INT
            )
            STORED AS PARQUET
            LOCATION '{S3_BUCKET}'
        """)
        ok(f"Table externe '{HIVE_DB}.{HIVE_TABLE}' créée dans le Metastore.")
 
        info(f"Requête SQL sur la table Hive...")
        df_hive = spark.sql(f"""
            SELECT   department,
                     COUNT(*)        AS nb_employees,
                     AVG(salary)     AS avg_salary,
                     AVG(years_exp)  AS avg_experience
            FROM     {HIVE_DB}.{HIVE_TABLE}
            GROUP BY department
            ORDER BY avg_salary DESC
        """)
 
        count_hive = df_hive.count()
        if count_hive > 0:
            ok(f"Requête SQL réussie — {count_hive} départements trouvés.")
            results["hive"] = True
        else:
            fail("La requête SQL n'a retourné aucun résultat.")
 
        info("Résultat de l'agrégation par département:")
        df_hive.show(truncate=False)
 
        info("Tables disponibles dans le Metastore:")
        spark.sql(f"SHOW TABLES IN {HIVE_DB}").show()
 
    except Exception as e:
        fail(f"Erreur Hive Metastore: {e}")
        print(f"\n  Vérifications à faire:")
        print(f"  - Hive Metastore accessible sur thrift://hive-metastore:9083")
        print(f"  - PostgreSQL opérationnel (docker logs postgres)")
        print(f"  - hive-site.xml correctement monté dans le conteneur Hive")
 
# ============================================================
# TEST 3 — OpenSearch
# ============================================================
section("TEST 3 — OpenSearch (indexation depuis Spark)")
 
if not results["seaweedfs"]:
    fail("Test OpenSearch ignoré car SeaweedFS a échoué.")
else:
    try:
        info(f"Indexation des données dans OpenSearch (index: '{OS_INDEX}')...")
 
        df_read = spark.read.parquet(S3_BUCKET)
        df_read.write \
            .format("opensearch") \
            .option("opensearch.resource",     OS_INDEX) \
            .option("opensearch.nodes",        "opensearch") \
            .option("opensearch.port",         "9200") \
            .option("opensearch.nodes.wan.only", "true") \
            .option("opensearch.net.ssl",      "false") \
            .mode("overwrite") \
            .save()
        ok(f"Indexation dans '{OS_INDEX}' réussie.")
 
        info(f"Lecture depuis l'index OpenSearch '{OS_INDEX}'...")
        df_os = spark.read \
            .format("opensearch") \
            .option("opensearch.resource",     OS_INDEX) \
            .option("opensearch.nodes",        "opensearch") \
            .option("opensearch.port",         "9200") \
            .option("opensearch.nodes.wan.only", "true") \
            .option("opensearch.net.ssl",      "false") \
            .load()
 
        count_os = df_os.count()
        if count_os > 0:
            ok(f"Lecture depuis OpenSearch réussie — {count_os} documents trouvés.")
            results["opensearch"] = True
        else:
            fail("Aucun document récupéré depuis OpenSearch.")
 
        info("Apercu des documents lus depuis OpenSearch:")
        df_os.show(3, truncate=False)
 
    except Exception as e:
        fail(f"Erreur OpenSearch: {e}")
        print(f"\n  Vérifications à faire:")
        print(f"  - OpenSearch accessible sur http://opensearch:9200")
        print(f"  - opensearch-spark JAR present dans $SPARK_HOME/jars/")
        print(f"  - spark.opensearch.* correctement configuré dans spark-defaults.conf")
 
# ============================================================
# Résumé final
# ============================================================
section("RÉSUMÉ")
 
all_ok = all(results.values())
 
for service, status in results.items():
    if status:
        ok(f"{service.upper():20s} operationnel")
    else:
        fail(f"{service.upper():20s} en erreur")
 
print()
if all_ok:
    print(f"{GREEN}{BOLD}  Stack 100% operationnel — prêt pour le projet !{RESET}\n")
else:
    failed = [s for s, v in results.items() if not v]
    print(f"{RED}{BOLD}  Problemes detectes: {', '.join(failed)}{RESET}")
    print(f"{YELLOW}  Consultez les logs ci-dessus pour plus de details.{RESET}\n")
 
spark.stop()
sys.exit(0 if all_ok else 1)
