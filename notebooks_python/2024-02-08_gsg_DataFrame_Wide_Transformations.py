# Databricks notebook source
# MAGIC %md
# MAGIC # Aggregating data
# MAGIC In this session, we'll learning how to aggregate data using Apache Spark operations.
# MAGIC We are going to use the Flights Dataset in order to illustrate the commands below.

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/flights/

# COMMAND ----------

df_flights = spark.read.format('csv').option("header", True).load('dbfs:/databricks-datasets/flights/departuredelays.csv')

# COMMAND ----------

display(df_flights.show(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Group by
# MAGIC To group by your DataFrame with Apache Spark you leverage the `groupBy` command. Group by is an computacional intensive task (wide transformation) as it's performed a shuffle between keys to colocate on the same partitions over workers.

# COMMAND ----------

df_flights.groupBy('destination').count().sort('count', ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrame Statistics
# MAGIC In order to get general statistics for your DataFrame you can use `describe` or `summary`(more details).<br>
# MAGIC You can also apply Spark functions like `max`, `min`, `mean`, `stddev` and `count` to calculate the metrics for desired columns.
# MAGIC

# COMMAND ----------

df_flights.describe().show()

# COMMAND ----------

df_flights.summary().show()

# COMMAND ----------

from pyspark.sql.functions import min, max, avg, mean, count
df_flights.agg(
    min('distance'),
    max('distance'),
    avg('distance'),
    mean('distance'),
    count('distance'),
).show()


# COMMAND ----------


