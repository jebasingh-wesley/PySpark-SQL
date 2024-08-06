# PySpark-SQL

## PySpark and SparkSQL Complete Guide

Apache Spark is a cluster computing system that offers comprehensive libraries and APIs for developers. SparkSQL is a module in Apache Spark for processing structured data with the help of the DataFrame API.

In this notebook, we will cover the basics of running Spark jobs with PySpark (Python API) and executing useful functions within it. By following this guide, you should be able to grasp a basic understanding of PySpark and its common functions.

### Table of Contents
1. [Initialize the Spark Session](#initialize-the-spark-session)
2. [Load Data](#load-data)
3. [Useful Common Functions](#useful-common-functions)

### 1. Initialize the Spark Session
To begin, initialize the Spark Session. DataFrames can be created and registered as tables. SQL tables can be executed, cached, and parquet/json/csv/avro data formatted files can be read.

### 2. Load Data
You can download the Kaggle dataset, which includes information such as the book title, author, the date of the best seller list, the published date of the list, the book description, the rank (this week and last week), the publisher, number of weeks on the list, and the price. 

Apache Spark supports reading various types of data. DataFrames can be created by reading txt, csv, json, and parquet file formats. In our example, we will be using a .json formatted file. You can also find and read text, csv, and parquet file formats by using the related read functions as shown below.

### 3. Useful Common Functions
Here are some common functions that are useful when working with PySpark:

#### [1] Remove Duplicate Values
Duplicate values in a table can be eliminated by using the `dropDuplicates()` function.

#### [2] 'Select' Operation
Columns can be obtained by column name or by indexing (i.e., `dataframe['author']`).

#### [3] 'When' Operation
Conditional operations can be performed using the `when` function.

#### [4] 'isin' Operation
Filter rows where column values are in a provided list.

#### [5] 'Like' Operation
Filter rows based on string patterns.

#### [6] 'Startswith' — 'Endswith' Operation
- `StartsWith`: Scans from the beginning of word/content with specified criteria.
- `EndsWith`: Processes the word/content starting from the end. Both functions are case sensitive.

#### [7] 'Substring' Operation
Extracts parts of a string based on specified index ranges. For example:
- `dataframe.column.substr(1, 3)`
- `dataframe.column.substr(3, 6)`
- `dataframe.column.substr(1, 6)`

#### [8] Adding Columns
Add new columns to the DataFrame.

#### [9] Removing Columns
Remove columns in two ways:
- Adding the list of column names in the `drop()` function.
- Specifying columns directly in the `drop()` function.

---

Feel free to explore the code, contribute, and raise issues if you encounter any. Happy coding!
