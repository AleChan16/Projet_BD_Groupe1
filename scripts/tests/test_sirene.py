from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test_sirene").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read \
    .option("sep", ",") \
    .option("header", "true") \
    .option("encoding", "utf-8") \
    .csv("s3a://raw-data/sirene/sirene_etablissements.csv")

print(f"Colonnes SIRENE ({len(df.columns)}):")
for col in df.columns:
    print(f"  - {col}")

print(f"\nNombre de lignes: {df.count():,}")
print("\nAperçu:")
df.show(3, truncate=True)

spark.stop()
