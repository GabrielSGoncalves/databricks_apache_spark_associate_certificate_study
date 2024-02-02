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


