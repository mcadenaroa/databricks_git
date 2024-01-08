# Databricks notebook source
# MAGIC %md
# MAGIC #### Get latest COVID-19 hospitalization data

# COMMAND ----------

# Download file from the internet
!wget -q https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/hospitalizations/covid-hospitalizations.csv -O /tmp/covid-hospitalizations.csv

#Move file to DBFS
dbutils.fs.cp("file:/tmp/covid-hospitalizations.csv", "/mnt/covid-hospitalizations.csv")


# COMMAND ----------

# MAGIC %md #### Transform

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType
from pyspark.sql.functions import when

# Create a Spark session
spark = SparkSession.builder.appName("Covid-19 cases").getOrCreate()

# define schema
# Load the data
user_schema = 'entity STRING, iso_code STRING, date STRING, indicator STRING, value DOUBLE'
psdf = spark.read.csv('/mnt/covid-hospitalizations.csv', header=True, schema = user_schema)

# Filter the data
psdf = psdf.filter(psdf.iso_code == 'USA')\
     .groupBy("date")\
     .pivot("indicator")\
     .agg({"value": "first"})

# Fill NA values with 0
psdf = psdf.na.fill(0)

# Display the DataFrame
display(psdf)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualize 

# COMMAND ----------

# Visualize - 
pandas_df = psdf.toPandas()
sampled_pandas_df = psdf.sample(False, 0.8).toPandas()
sampled_pandas_df.plot(figsize=(12,6), grid=True).legend(loc='upper left')


# COMMAND ----------

# MAGIC  %md
# MAGIC #### Save to Delta Lake
# MAGIC The current schema has spaces in the column names, which are incompatible with Delta Lake.  To save our data as a table, we'll replace the spaces with underscores.  We also need to add the date index as its own column or it won't be available to others who might query this table.

# COMMAND ----------

# import pyspark.pandas as ps

clean_cols = psdf.columns.str.replace(' ', '_')

# # Create pandas on Spark dataframe
# psdf = ps.from_pandas(pandas_df)

psdf.columns = clean_cols

# Write to Delta table, overwrite with latest data each time
psdf.to_table(name='dev_covid_analysis', mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC #### View table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev_covid_analysis
