from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Spark session avec Delta
spark = SparkSession.builder \
    .appName("ConsommateurKafka") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Schéma des ventes
schema = StructType([
    StructField("vente_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("pays", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("produit", StringType(), True),
    StructField("quantite", IntegerType(), True),
    StructField("prix_unitaire", DoubleType(), True),
    StructField("total", DoubleType(), True)
])

# Lire depuis Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ventes_stream") \
    .load()

# Convertir les données JSON de Kafka en DataFrame Spark
df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Écriture en Delta Lake Bronze
df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/workspace/delta/bronze/_checkpoints") \
    .outputMode("append") \
    .start("/workspace/delta/bronze/ventes") \
    .awaitTermination()
