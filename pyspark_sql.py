import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import time

import pyspark # only run this after findspark.init()
from pyspark.sql import SparkSession, SQLContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.sql.functions import countDistinct
from pyspark.sql import functions as F


sc = SparkSession.builder.appName("PysparkExample")\
    .config ("spark.sql.shuffle.partitions", "50")\
    .config("spark.driver.maxResultSize","5g")\
    .config ("spark.sql.execution.arrow.enabled", "true").getOrCreate()


#JSON
dataframe = sc.read.json('/home/ubuntu/workspace/data/nyt2.json')
# dataframe.show(10)

# show the duplicates
duplicates=dataframe.select(countDistinct("amazon_product_url", "_id"))
# duplicates.show(10)

#Duplicate values in a table can be eliminated by using dropDuplicates() function.
dataframe_dropdup = dataframe.dropDuplicates()
# dataframe_dropdup.show(10)

#Show all entries in title column
dataframe.select("title")

#Show all entries in title, author, rank, price columns
dataframe.select("author", "title", "rank", "price")

# Show title and assign 0 or 1 depending on title
# THE HOST is present pirnt 1 else 0
dataframe.select("title", when(dataframe.title != 'THE HOST', 1).otherwise(0))

# Show rows with specified authors if in the given options
# author row contain both name
dataframe [dataframe.author.isin("John Sandford", "Emily Giffin")]


#'Startswith' — 'Endswith' Operation
# Show author and title is TRUE if title has " THE " word in titles
dataframe.select("author", "title", dataframe.title.like("% THE %"))
dataframe.select("author", "title", dataframe.title.startswith("THE"))
dataframe.select("author", "title", dataframe.title.endswith("NT"))

"'Substring' Operation"
dataframe.select(dataframe.author.substr(1, 3).alias("title"))
dataframe.select(dataframe.author.substr(3, 6).alias("title"))
dataframe.select(dataframe.author.substr(1, 6).alias("title"))

"Adding Columns"
# Lit() is required while we are creating columns with exact values.
dataframe = dataframe.withColumn('new_column', F.lit('This is a new column'))


# Update column 'amazon_product_url' with 'URL'
dataframe = dataframe.withColumnRenamed('amazon_product_url', 'URL')
# dataframe.show(5)

"Removing Columns"
# Removal of a column can be achieved in two ways:
# 1.Adding the list of column names in the drop() function
# 2.Specifying columns by pointing in the drop function

# 1.
dataframe_remove = dataframe.drop("publisher", "published_date")

# 2.
dataframe_remove2 = dataframe.drop(dataframe.publisher).drop(dataframe.published_date)


" 'GroupBy' Operation"
dataframe.groupBy("author").count()


"'Filter' Operation"
# Filtering entries of title
# Only keeps records having value 'THE HOST'
dataframe.filter(dataframe["title"] == 'THE HOST').show(5)

"Handling Missing Values"

# Replacing null values
dataframe.na.fill()
dataFrame.fillna()
dataFrameNaFunctions.fill()

# Returning new dataframe restricting rows with null valuesdataframe.na.drop()
dataFrame.dropna()
dataFrameNaFunctions.drop()

# Return new dataframe replacing one value with another
dataframe.na.replace(5, 15)
dataFrame.replace()
dataFrameNaFunctions.replace()

"Repartitioning"

# Dataframe with 10 partitions
dataframe.repartition(10).rdd.getNumPartitions()

# Dataframe with 1 partition
dataframe.coalesce(1).rdd.getNumPartitions()

"Running SQL Commnads In Spark"

# Registering a table
dataframe.registerTempTable("df")

sc.sql("select * from df").show(3)

"Output in Data"
# Obtaining contents of df as Pandas
dataFramedataframe.toPandas()


"Write & Save to Files"

# Write & Save File in .parquet format
dataframe.select("author", "title", "rank", "description") \
        .write \
        .save("Rankings_Descriptions.parquet")

# Write & Save File in .json format
dataframe.select("author", "title") \
        .write \
        .save("Authors_Titles.json",format="json")


sc.stop()

"https://github.com/hyunjoonbok/PySpark/blob/master/PySpark%20and%20SparkSQL%20Complete%20Guide.ipynb"
