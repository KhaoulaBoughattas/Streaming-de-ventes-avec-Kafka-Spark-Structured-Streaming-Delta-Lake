from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

import plotly.express as px

# --- Spark Session en mode local ---
spark = SparkSession.builder \
    .appName("DashboardSilver") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# --- Simuler des données Silver manuellement ---
schema = StructType([
    StructField("pays", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("total_quantite", LongType(), True),
    StructField("ca_total", DoubleType(), True)
])

data = [
    ("FR", "Retail", 120, 3500.0),
    ("FR", "Online", 80, 2200.0),
    ("US", "Retail", 200, 5400.0),
    ("US", "Online", 150, 4100.0),
]

df_silver = spark.createDataFrame(data, schema)
df_silver.show(truncate=False)
df_silver.printSchema()

# --- Créer un dashboard avec Plotly ---
# Convertir en Pandas pour Plotly
df_pd = df_silver.toPandas()

# Graphique CA par pays et segment
fig_ca = px.bar(
    df_pd,
    x="pays",
    y="ca_total",
    color="segment",
    text="ca_total",
    title="Chiffre d'affaires par pays et segment"
)
fig_ca.show()

# Graphique Quantité par pays et segment
fig_qty = px.bar(
    df_pd,
    x="pays",
    y="total_quantite",
    color="segment",
    text="total_quantite",
    title="Quantité totale vendue par pays et segment"
)
fig_qty.show()
