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

# MAGIC %md
# MAGIC As we can see we created a new DataFrame selecting the desired columns.<br>
# MAGIC There other ways to select columns from a DataFrame, using the column objects from Spark, `col` or `column`. 

# COMMAND ----------

from pyspark.sql.functions import col, column

df_nyc_taxi_green_select2 = df_nyc_taxi_green.select(
    col("VendorID"),
    col("lpep_pickup_datetime"),
    col("Trip_distance"), 
    col("Fare_amount"),
    col("Passenger_count"),
    col("Tip_amount")
    )
display(df_nyc_taxi_green_select2)

# COMMAND ----------

# MAGIC %md
# MAGIC And finally, you can also use a SQL expression to select columns from a DataFrame by leveraging the `selectExpr` method.

# COMMAND ----------

display(df_nyc_taxi_green.selectExpr(
    "lpep_pickup_datetime pickup_dt",
    "year(lpep_pickup_datetime) year",
    "month(lpep_pickup_datetime) month",
    "day(lpep_pickup_datetime) day"
    ))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Concatenating columns
# MAGIC Another important operation you can perform over your DataFrame is concatenating columns. You can use both the function `concat` or the `expr`.

# COMMAND ----------

from pyspark.sql.functions import concat, expr

display(df_nyc_taxi_green.select(
    expr("concat(Store_and_fwd_flag, VendorID) concat_cols"
         )
    )
)
        

# COMMAND ----------

display(df_nyc_taxi_green.select(
    concat(
        df_nyc_taxi_green.Store_and_fwd_flag, 
        df_nyc_taxi_green.VendorID
        )
         )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming columns
# MAGIC To rename columns of a DataFrame we can use the method `withColumnRename`. Important to note that if you specify a column name that does not exist, Apache Spark won't change anything over your DataFrame.

# COMMAND ----------

df_nyc_taxi_green.columns

# COMMAND ----------

display(df_nyc_taxi_green.withColumnRenamed("VendorID", "VendorIdentifier"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Changing columns dtypes (casting)
# MAGIC To change the dtype for a specific column we can use the `cast` method, passing the desired new dtype as parameter.

# COMMAND ----------

df_nyc_taxi_green.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

display(df_nyc_taxi_green.select(col("Fare_amount").cast('double')))

# COMMAND ----------

# MAGIC %md
# MAGIC If we want to change the dtype of a column from an original DataFrame we can use the expression:

# COMMAND ----------

df_nyc_taxi_green = df_nyc_taxi_green.withColumn("Fare_amount", col("Fare_amount").cast("double"))
df_nyc_taxi_green.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC And finally, you can use the `expr` and leverage the SQL synthax.
# MAGIC -> FIX THIS

# COMMAND ----------

df_nyc_taxi_green = df_nyc_taxi_green.withColumn("Fare_amount", expr("CAST(Fare_amount AS DOUBLE)"))
df_nyc_taxi_green.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding new columns to a DataFrame
# MAGIC One important operations you must master is creating/adding new columns to a DataFrame.
# MAGIC To do it, you must leverage the `withColumn` expression, and pass a column object to the DataFrame.
# MAGIC For instance, if you want to create a new column with the same string value for all rows, you need to import the `lit` to do it.

# COMMAND ----------

from pyspark.sql.functions import lit
df_nyc_taxi_green_add_col = df_nyc_taxi_green.withColumn("new_col", lit("literal string"))
display(df_nyc_taxi_green_add_col.select("new_col"))

# COMMAND ----------

# MAGIC %md
# MAGIC We can create new columns based on existing columns operations like transformations or concatenations.

# COMMAND ----------

df_nyc_taxi_green_add_col_concat = df_nyc_taxi_green.withColumn(
    "concat_columns", concat(
        df_nyc_taxi_green.Store_and_fwd_flag, 
        df_nyc_taxi_green.VendorID
        )
         )
display(df_nyc_taxi_green_add_col_concat.select("concat_columns"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Removing columns from a DataFrame
# MAGIC Dropping columns is another essencial operation when processing data through the DataFrame API.
# MAGIC To do it, you simply need to use the `drop` command and specify the name of the columns. You can use both a string with the name of the columns, or columns Objects (`col`).<br>
# MAGIC Important to note that if you pass a column name that's not in the DataFrame schema, nothing will happen, you would get the DataFrame with the original columns, in other words, Spark won't raise an error.

# COMMAND ----------

df_nyc_taxi_green.printSchema()

# COMMAND ----------

df_nyc_taxi_green_drop_col = df_nyc_taxi_green.drop(
    "VendorID", 
    "lpep_pickup_datetime",
    "Lpep_dropoff_datetime",
    "Store_and_fwd_flag"
)
df_nyc_taxi_green_drop_col.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
df_nyc_taxi_green_drop_col = df_nyc_taxi_green.drop(
    col("VendorID"), 
    col("lpep_pickup_datetime"),
    col("Lpep_dropoff_datetime"),
    col("Store_and_fwd_flag"),
    col("123") # this column does not exist, but it does not affect the result
)
df_nyc_taxi_green_drop_col.printSchema()

# COMMAND ----------

df_nyc_taxi_green_drop_col.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Arithmetic operations
# MAGIC To expand the possibilities of data manipulation for your Spark DataFrame, you can perform arithmetic over your columns.

# COMMAND ----------

from pyspark.sql.functions import col
df_nyc_taxi_green_numeric = df_nyc_taxi_green.select(
    col("VendorID").cast('integer'),
    col("lpep_pickup_datetime").cast('timestamp'),
    col("Fare_amount").cast('double'),
    col("Tip_amount").cast('double'),
    col("Trip_distance").cast('double')
    )
display(df_nyc_taxi_green_numeric)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's now convert the column `Trip_distance` from miles to kilometers.

# COMMAND ----------

df_nyc_taxi_green_numeric = df_nyc_taxi_green_numeric.withColumn(
    "Trip_distance_km", col("Trip_distance") * 1.609344)
display(df_nyc_taxi_green_numeric)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding the DataFrame lineage graph
# MAGIC Apache Spark executes its commands by optimizing each step and creating a Direct Acyclic Graph in order to return the results as performant as possible. To understand this DAG planning, you can use the command `explain`.

# COMMAND ----------

df_nyc_taxi_green_numeric.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Types of action commands
# MAGIC As Apache Spark uses a lazy evalution execution model, it's important to understand the differences between transformations and actions. One way to understand it is by viewing that whenever you execute a transformation to your DataFrame, Apache Spark will only execute it when an action is executed. Apache Spark is "lazy" about transformations, only storing its history, but when an action is called upon a DataFrame, it builds a execution plan contemplating all the transformations, optimizing its order, distribution and shuffling.<br>
# MAGIC There are 3 types of Apache Spark actions:
# MAGIC 1. View: display a subset of the DataFrame (`show`)
# MAGIC 2. Collect: Fetch data from the workers to the driver ()`take`, `takeAsList`, `collect`)
# MAGIC 3. Write to output: Saves you DataFrame to a specific format (`save`) 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering rows of a DataFrame
# MAGIC You can filter your DataFrame based on specific conditions using the `where` or `filter` statement.

# COMMAND ----------

df_nyc_taxi_green_numeric.show(3)

# COMMAND ----------

df_nyc_taxi_green_filtered = df_nyc_taxi_green_numeric.where(col("Fare_amount") > 20)
display(df_nyc_taxi_green_filtered)

# COMMAND ----------

df_nyc_taxi_green_filtered.count()

# COMMAND ----------

df_nyc_taxi_green_filtered = df_nyc_taxi_green_numeric.filter(col("Fare_amount") == 20)
display(df_nyc_taxi_green_filtered)

# COMMAND ----------

df_nyc_taxi_green_filtered.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Types of transformations
# MAGIC Transformations can be divided into Narrow or Wide. <br>
# MAGIC
# MAGIC **Narrow transformations** (or dependencies) generate one partition for each original partition (1 to 1 relationship). These types of transformation requires less processing, as there are no shuffles. `filter`, `select`, `union`, `map`, `flatmap`, `mapPartitions` are examples of narrow transformations.
# MAGIC
# MAGIC **Wide transformations** require shuffling of data between partitions. These transformations require the exchange of data between partitions and can be more expensive compared to narrow transformations.
# MAGIC Examples of wide transformations in Spark include `reduceByKey`, `groupByKey`, and `join`.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dropping rows
# MAGIC Another important transformations to execute over your DataFrame is the ones related to removing duplicates. You can either use the `distinct` or `drop_duplicates`.

# COMMAND ----------

df_distinct = df_nyc_taxi_green_filtered.select(col("VendorID")).distinct()

# COMMAND ----------

display(df_distinct)

# COMMAND ----------

df_drop_dup = df_nyc_taxi_green_filtered.drop_duplicates(["Trip_distance"])

# COMMAND ----------

display(df_drop_dup)

# COMMAND ----------


