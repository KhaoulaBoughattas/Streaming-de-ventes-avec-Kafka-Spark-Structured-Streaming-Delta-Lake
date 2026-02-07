{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": None,
   "id": "490507b0-8abe-4cb9-bff0-240f9568dc8d",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql import SparkSession\n",
    "\n",
    "spark = SparkSession.builder \\\n",
    "    .appName(\"SilverAggregation\") \\\n",
    "    .config(\"spark.sql.extensions\", \"io.delta.sql.DeltaSparkSessionExtension\") \\\n",
    "    .config(\"spark.sql.catalog.spark_catalog\", \"org.apache.spark.sql.delta.catalog.DeltaCatalog\") \\\n",
    "    .getOrCreate()\n",
    "\n",
    "df = spark.read.format(\"delta\").load(\"/workspace/delta/bronze/ventes\")\n",
    "\n",
    "df_agg = df.groupBy(\"pays\", \"segment\") \\\n",
    "    .sum(\"total\") \\\n",
    "    .withColumnRenamed(\"sum(total)\", \"ca_total\")\n",
    "\n",
    "df_agg.write.format(\"delta\") \\\n",
    "    .mode(\"overwrite\") \\\n",
    "    .save(\"/workspace/delta/silver/ventes_aggreges\")\n"
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
