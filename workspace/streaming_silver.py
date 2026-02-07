# streaming_silver.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
from delta.tables import DeltaTable
import sys
import os

# -------------------------------
# 1. Crée la session Spark avec support Delta
# -------------------------------
spark = SparkSession.builder \
    .appName("DeltaBronzeToSilver") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# -------------------------------
# 2. Définition des chemins Bronze et Silver
# -------------------------------
# ⚠️ Utiliser un chemin compatible WSL2 ou Windows
bronze_path = "/workspace/delta/bronze/ventes"
silver_path = "/workspace/delta/silver/ventes_aggreges"


# Vérification si la table Bronze existe
if not DeltaTable.isDeltaTable(spark, bronze_path):
    print(f"Erreur : la table Bronze n'existe pas ou n'est pas Delta à l'emplacement {bronze_path}")
    sys.exit(1)

# -------------------------------
# 3. Lecture des données Bronze
# -------------------------------
df_bronze = spark.read.format("delta").load(bronze_path)
print("Lecture Bronze réussie !")
df_bronze.show(5, truncate=False)

# Vérification que les colonnes nécessaires existent
required_cols = {"pays", "segment", "quantite", "total"}
missing_cols = required_cols - set(df_bronze.columns)
if missing_cols:
    print(f"Erreur : Colonnes manquantes dans Bronze: {missing_cols}")
    sys.exit(1)

# -------------------------------
# 4. Agrégation pour la table Silver
# -------------------------------
df_silver = df_bronze.groupBy("pays", "segment") \
    .agg(
        _sum("quantite").alias("total_quantite"),
        _sum("total").alias("ca_total")
    )

# -------------------------------
# 5. Écriture dans Delta Silver
# -------------------------------
# Crée le dossier Silver si nécessaire
os.makedirs(os.path.dirname(silver_path), exist_ok=True)

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_path)

print("Écriture Silver terminée !")

# -------------------------------
# 6. Vérification et affichage
# -------------------------------
df_silver.show(truncate=False)
print("Bronze → Silver terminé avec succès !")
