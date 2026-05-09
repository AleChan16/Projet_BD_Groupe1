from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("DVF-Enrichissement").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("[INFO] Lecture du warehouse dvf_enrichi...")
df = spark.read.parquet("s3a://warehouse/dvf_enrichi/")

# prix_m2 existe deja dans les donnees, on l'utilise directement

# Qualite de la donnee
df = df.withColumn("qualite_donnee", F.when(
    (F.col("valeur_fonciere") <= 0) |
    (F.col("surface_bati") <= 0) |
    (F.col("prix_m2") < 500) |
    (F.col("prix_m2") > 30000) |
    F.col("type_local").isNull(),
    "SUSPECTE"
).otherwise("OK"))

# Categorie de prix
df = df.withColumn("categorie_prix",
    F.when(F.col("prix_m2") < 1500, "bas")
    .when(F.col("prix_m2") < 3500, "moyen")
    .when(F.col("prix_m2") < 7000, "eleve")
    .when(F.col("prix_m2").isNotNull(), "tres_eleve")
    .otherwise(None)
)

# Zone Ile-de-France
df = df.withColumn("zone_idf",
    F.when(F.col("code_departement") == "75", "Paris")
    .when(F.col("code_departement").isin("92","93","94"), "Petite_couronne")
    .when(F.col("code_departement").isin("77","78","91","95"), "Grande_couronne")
    .otherwise(None)
)

total = df.count()
ok    = df.filter(F.col("qualite_donnee") == "OK").count()
susp  = df.filter(F.col("qualite_donnee") == "SUSPECTE").count()
print(f"[INFO] Total      : {total:,}")
print(f"[INFO] OK         : {ok:,}")
print(f"[INFO] SUSPECTES  : {susp:,}")

df.write.mode("overwrite").partitionBy("code_departement", "annee") \
    .parquet("s3a://warehouse/dvf_enrichi_v2/")

print("[OK] Enrichissement termine -> s3a://warehouse/dvf_enrichi_v2/")
spark.stop()
