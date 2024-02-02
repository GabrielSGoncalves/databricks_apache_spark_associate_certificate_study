# Databricks notebook source
# MAGIC %md
# MAGIC # DataFrame Basics
# MAGIC In this notebook we are going to explore basic commands used to manipulate date with PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Acessing the Spark Session
# MAGIC On every Databricks notebook you have a Spark Session represented by the variable `spark` that can be accessed.
# MAGIC Through the Spark Session we can communicate with the Spark Clusters Driver and execute commands.

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC The Spark Session offers a DataFrame reader that enables us to read files and tables.

# COMMAND ----------

spark.read

# COMMAND ----------

# MAGIC %md
# MAGIC Before reading a data object, we can list the available files and tables.

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/

# COMMAND ----------

# MAGIC %md
# MAGIC And we can read a Delta table from our Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading a table as a Spark DataFrame

# COMMAND ----------

df_books = spark.read.format('delta').load('dbfs:/user/hive/warehouse/books')

# COMMAND ----------

display(df_books)

# COMMAND ----------

# MAGIC %md
# MAGIC We can also query the table using a SQL approach.

# COMMAND ----------

df_customer_sql = spark.sql("""select * from default.websales""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Understanding the properties of a DataFrame
# MAGIC The first we'll do is check the DataFrame schema.

# COMMAND ----------

df_books.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC We can also define the schema while reading the table.

# COMMAND ----------

books_schema = 'book_id string, title string, author string, category string, price double'
df_books = spark.read.format('delta').schema(books_schema).load('dbfs:/user/hive/warehouse/books')

# COMMAND ----------

# MAGIC %md
# MAGIC We can also use the StructType to define the schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

custom_schema = StructType([
    StructField("book_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("author", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True)
])

# Define the file path
file_path = "dbfs:/user/hive/warehouse/books"

# Read the CSV file with the specified schema
df_books = spark.read.csv(file_path, schema=custom_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC We can also set Spark Reader to infer the schema. It can take longer as Spark is reading and inferring the dtype for each column.<br>
# MAGIC For a production pipeline, always define the schema when reading files.

# COMMAND ----------

df_books = spark.read.format('delta').option("inferSchema", True).load('dbfs:/user/hive/warehouse/books')

# COMMAND ----------

# MAGIC %md
# MAGIC # Selecting collumns
# MAGIC In this session we'll explore how to select and manipulate columns with PySpark.
# MAGIC We are going to use NYC trip datasets, that is stored as compressed CSV to illustrate the commands.

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/nyctaxi/tripdata/green/

# COMMAND ----------

df_nyc_taxi_green = spark.read.format('csv').option("header", True).load('dbfs:/databricks-datasets/nyctaxi/tripdata/green/')

# COMMAND ----------

df_nyc_taxi_green.count()

# COMMAND ----------

display(df_nyc_taxi_green)

# COMMAND ----------

df_nyc_taxi_green.printSchema()

# COMMAND ----------

len(df_nyc_taxi_green.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC As we can see, this is a large dataset with 21 columns. <br>
# MAGIC So we use the `select` method to restrict the desired columns.

# COMMAND ----------

df_nyc_taxi_green_select = df_nyc_taxi_green.select(["VendorID","lpep_pickup_datetime", "Trip_distance", "Fare_amount", "Passenger_count", "Tip_amount"])

# COMMAND ----------

display(df_nyc_taxi_green_select)

# COMMAND ----------

As we can see we created a new DataFrame selecting the desired columns.<br>
