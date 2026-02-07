{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": None,
   "id": "3d559805-6ff5-4321-aeb6-39bed15a709d",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql import SparkSession\n",
    "from pyspark.sql.functions import from_json, col\n",
    "from pyspark.sql.types import *\n",
    "\n",
    "spark = SparkSession.builder \\\n",
    "    .appName(\"KafkaToDeltaBronze\") \\\n",
    "    .config(\"spark.sql.extensions\", \"io.delta.sql.DeltaSparkSessionExtension\") \\\n",
    "    .config(\"spark.sql.catalog.spark_catalog\", \"org.apache.spark.sql.delta.catalog.DeltaCatalog\") \\\n",
    "    .getOrCreate()\n",
    "\n",
    "schema = StructType([\n",
    "    StructField(\"vente_id\", IntegerType()),\n",
    "    StructField(\"timestamp\", StringType()),\n",
    "    StructField(\"pays\", StringType()),\n",
    "    StructField(\"segment\", StringType()),\n",
    "    StructField(\"produit\", StringType()),\n",
    "    StructField(\"quantite\", IntegerType()),\n",
    "    StructField(\"prix_unitaire\", DoubleType())\n",
    "])\n",
    "\n",
    "df_kafka = spark.readStream \\\n",
    "    .format(\"kafka\") \\\n",
    "    .option(\"kafka.bootstrap.servers\", \"kafka:9092\") \\\n",
    "    .option(\"subscribe\", \"ventes_stream\") \\\n",
    "    .load()\n",
    "\n",
    "df_json = df_kafka.select(\n",
    "    from_json(col(\"value\").cast(\"string\"), schema).alias(\"data\")\n",
    ").select(\"data.*\")\n",
    "\n",
    "df_bronze = df_json.withColumn(\n",
    "    \"total\", col(\"quantite\") * col(\"prix_unitaire\")\n",
    ")\n",
    "\n",
    "query = df_bronze.writeStream \\\n",
    "    .format(\"delta\") \\\n",
    "    .outputMode(\"append\") \\\n",
    "    .option(\"checkpointLocation\", \"/workspace/delta/checkpoints/bronze\") \\\n",
    "    .start(\"/workspace/delta/bronze/ventes\")\n",
    "\n",
    "query.awaitTermination()\n"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "PySpark",
   "language": "python",
   "name": "pyspark"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.12.3"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
