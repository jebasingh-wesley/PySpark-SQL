# pyspark-SQL
PySpark and SparkSQL Complete Guide

Apache Spark is a cluster computing system that offers comprehensive libraries and APIs for developers, and SparkSQL can be represented as the module in Apache Spark for processing unstructured data with the help of DataFrame API.

In this notebook, we will cover the basics how to run Spark Jobs with PySpark (Python API) and execute useful functions insdie. If followed, you should be able to grasp a basic understadning of PySparks and its common functions.


1. Initialize the Spark Session
We need to begin with initilize the Spark Session. DataFrame can be created and registered as tables. Moreover, SQL tables be executed, tables can be cached, and parquet/json/csv/avro data formatted files can be read.

2. Load Data
You can download the Kaggle dataset, which includes the book title, author, the date of the best seller list, the published date of the list, the book description, the rank (this week and last week), the publisher, number of weeks on the list, and the price Link.

Spark is so Awesome that it supports all different types of data to be read.

DataFrames can be created by reading txt, csv, json and parquet file formats. In our example, we will be using .json formatted file. You can also find and read text, csv and parquet file formats by using the related read functions as shown below.


3. Useful common functions
[1] Remove Duplicate Values
Duplicate values in a table can be eliminated by using dropDuplicates() function
[2] 'Select' Operation
It is possible to obtain columns by column or by indexing (i.e. dataframe[‘author’]).
[3] 'When' Operation
[4] 'isin' Operation
[5] 'Like' Operation
[6] 'Startswith' — 'Endswith' Operation
StartsWith scans from the beginning of word/content with specified criteria in the brackets. In parallel, EndsWith processes the word/content starting from the end. Both of the functions are case sensitive.
[7] 'Substring' Operation
In the following examples, texts are extracted from the index numbers (1, 3), (3, 6) and (1, 6).
[8] Adding Columns
[10] Removing Columns
Removal of a column can be achieved in two ways: \

Adding the list of column names in the drop() function
Specifying columns by pointing in the drop function
