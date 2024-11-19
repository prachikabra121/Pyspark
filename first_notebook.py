# Databricks notebook source
# MAGIC %md
# MAGIC ### Reading csv files
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - spark.read is a data  frame api in databricks
# MAGIC - option is for infer schema - it automatically infers the schema by having look at some of the records and then it assumes the best fit according to the data types
# MAGIC - format is for file format
# MAGIC

# COMMAND ----------

#to sea the available files in our folder
dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

df=spark.read.format('csv').option("inferSchema",True).option("header",True).load('/FileStore/tables/BigMart_Sales.csv')


# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Reading semistructure **data**

# COMMAND ----------

/FileStore/tables/drivers.json

# COMMAND ----------

df_json=spark.read.format('json')\
      .option("inferSchema",True)\
      .option("header",True)\
      .option("multiline",False)\
      .load('/FileStore/tables/drivers.json')

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###which one is best to read the data in databricks spark or pandas
# MAGIC
# MAGIC The choice between using Spark or Pandas in Databricks depends on the size of your dataset and your computational needs.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. When to Use Spark
# MAGIC Spark is best suited for large-scale data processing and distributed computing. Databricks is optimized for Spark, so it is the ideal choice for most workflows involving big data.
# MAGIC
# MAGIC ####Advantages of Spark in Databricks
# MAGIC - Scalability: Processes data distributed across multiple nodes, making it capable of handling terabytes or petabytes of data.
# MAGIC - Performance: Optimized for parallel computation; handles large datasets efficiently.
# MAGIC - Integration: Works seamlessly with Delta Lake, Databricks tables, and streaming data pipelines.
# MAGIC - SQL-like Interface: Use Spark SQL for complex queries on distributed datasets.
# MAGIC - Fault Tolerance: Automatically handles failures in a distributed environment.
# MAGIC - Supported File Formats: Supports various formats like Parquet, ORC, JSON, Avro, and CSV with efficient I/O.

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. When to Use Pandas
# MAGIC Pandas is better for smaller datasets that fit into memory or for workflows requiring specific Pandas functions.
# MAGIC
# MAGIC ####Advantages of Pandas
# MAGIC - Ease of Use: Intuitive for small-scale data analysis and exploration.
# MAGIC - Rich Functionality: Ideal for tasks like complex statistical analysis and data visualization.
# MAGIC - Interactive Exploration: Useful for prototyping or working with small samples.
# MAGIC ####Limitations of Pandas in Databricks
# MAGIC - Memory Constraints: Limited by the memory of the driver node in Databricks.
# MAGIC - Not Distributed: Operates on a single machine, making it unsuitable for large datasets.
# MAGIC - Performance: Slower than Spark for large-scale computations.

# COMMAND ----------

import pandas as pd

# Reading a CSV file
df = pd.read_json("/FileStore/tables/drivers.json")



# COMMAND ----------

import pandas as pd
from pandas import json_normalize
import json

# File path to the JSON file
json_file_path = "/FileStore/tables/drivers.json"

# Load the JSON data from the file
with open(json_file_path, 'r') as file:
    data = json.load(file)  # This reads the JSON data from the file into a Python object

# Flatten the nested data using json_normalize
df = json_normalize(data, sep='_')

# Display the resulting DataFrame
print(df)



# COMMAND ----------

# Reading data from a Spark DataFrame
spark_df = spark.read.format("json").option("inferSchema",True).option("header", "true").option("multiline",False).load("/FileStore/tables/drivers.json")
pandas_df = spark_df.toPandas()  # Convert to Pandas DataFrame

print(pandas_df.head())



# COMMAND ----------

# MAGIC %md
# MAGIC ###Schema Defination
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

df.PrintSchema()

# COMMAND ----------

spark_df = spark.read.format("json").option("inferSchema",True).option("header", "true").option("multiline",False).load("/FileStore/tables/drivers.json")

# COMMAND ----------

spark_df.printSchema()

# COMMAND ----------

my_ddl_schema = ''' Item_Identifier STRING,
                   Item_Weight STRING,
                   Item_Fat_Content STRING,
                   Item_Visibility DOUBLE,
                   Item_Type STRING,
                   Item_MRP DOUBLE,
                   Outlet_Identifier STRING,
                   Outlet_Establishment_Year INTEGER,
                   Outlet_Size STRING,
                   Outlet_Location_Type STRING,
                   Outlet_Type STRING,
                   Item_Outlet_Sales DOUBLE
                '''


# COMMAND ----------

df.printSchema()

# COMMAND ----------

df=spark.read.format('csv').schema(my_ddl_schema).option("header",True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()


# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### use select statement to select the perticular columns from our data frame

# COMMAND ----------

from pyspark.sql.functions import col

df.select(col('Item_Identifier'), col('Item_Weight'), col('Item_Fat_Content')).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Aliases
# MAGIC

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID'), col('Item_Weight'), col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Filter/WHERE
# MAGIC
# MAGIC 1)Filter the data with fat_content = Regular
# MAGIC
# MAGIC
# MAGIC 2)slice the data with item type = softdrink and weight<10
# MAGIC
# MAGIC
# MAGIC 3)Fetch the data with tier in (tier 1 and tier 2) and oulet size is null

# COMMAND ----------

df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

df.filter((col('Item_Type')=='Soft Drinks') & (col('Item_Weight')<10)).display()

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###withColumnRenamed
# MAGIC
# MAGIC this different with aliases , aliases cghanging the name of column temporarily but withColumnRenamed changes the name on data frame level
# MAGIC

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_WT').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###withColumn
# MAGIC
# MAGIC 1)To add a new column in our data frame we use withColumn
# MAGIC
# MAGIC 2)To modifty the existing column

# COMMAND ----------

from pyspark.sql.functions import lit

df = df.withColumn("Flag", lit("New"))


# COMMAND ----------

df.display()


# COMMAND ----------

df=df.withColumn('Multiply',col('Item_Weight')*col('Item_MRP'))

# COMMAND ----------

df.display()

# COMMAND ----------

#to modify the existing columns
df.WithColumn("Item_Fat_Content",regExp_replace(("Item_Fat_Content"),"Regular","Regl")).display()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

df = df.withColumn("Item_Fat_Content", regexp_replace("Item_Fat_Content", "Regular", "Regl"))\
      .withColumn("Item_Fat_Content", regexp_replace("Item_Fat_Content", "Low Fat", "LF"))
df.display()  # Use .show() in PySpark; use display() if you're in Databricks


# COMMAND ----------

# MAGIC %md
# MAGIC ###Type Casting

# COMMAND ----------

from pyspark.sql.types import StringType
from pyspark.sql.functions import col

df = df.withColumn("Item_Weight", col("Item_Weight").cast("int"))
df.display()  # Use .show() to display the DataFrame


# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sort/order by

# COMMAND ----------

df.sort(col("Item_Weight").desc()).display()


# COMMAND ----------

df.sort(col("Item_Weight").asc()).display()

# COMMAND ----------

##for multiple columns
df.sort(['Item_Weight',"Item_Visibility"],Ascending=[0,0]).display()

# COMMAND ----------

df.sort(['Item_Weight',"Item_Visibility"],Ascending=[0,1]).display()

# COMMAND ----------

df.sort(['Item_Weight',"Item_Visibility"],descending=[1,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Limit

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop

# COMMAND ----------

###drop 1 column
df.drop("Item_Visibility").display()

# COMMAND ----------

df.drop("item_visibility","Flag").display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop_Duplicates

# COMMAND ----------

df.dropDuplicates().display()


# COMMAND ----------

df.dropDuplicates(subset=['Item_Type']).display()

# COMMAND ----------

df.display()


# COMMAND ----------

df.distinct().display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Union and Union By name

# COMMAND ----------

data1 = [('1','kad'),
        ('2','sid')]
schema1 = 'id STRING, name STRING' 

df1 = spark.createDataFrame(data1,schema1)

data2 = [('3','rahul'),
        ('4','jas')]
schema2 = 'id STRING, name STRING' 

df2 = spark.createDataFrame(data2,schema2)


# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------


df1.union(df2).display()

# COMMAND ----------

data1 = [('kad','1'),
        ('sid','2')]
schema1 = 'name STRING,id STRING' 

df1 = spark.createDataFrame(data1,schema1)

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

from pyspark.sql.functions import col

# Align column types for df1
df1 = df1.withColumn("id", col("id").cast("string")).withColumn("name", col("name").cast("string"))

# Align column types for df2
df2 = df2.withColumn("id", col("id").cast("string")).withColumn("name", col("name").cast("string"))

# Perform unionByName
result = df1.unionByName(df2)
display(result)



# COMMAND ----------

# MAGIC %md
# MAGIC ##String Functions
# MAGIC
# MAGIC
# MAGIC ###1)INITCAP()
# MAGIC ###2)UPPER()
# MAGIC ###3)LOWER()

# COMMAND ----------

from pyspark.sql.functions import initcap


df.select(initcap("Item_Type").alias("Item_Type_Capitalized")).display()  



# COMMAND ----------

from pyspark.sql.functions import lower

# Assuming df has a column named 'Item_Type'
df.select(lower("Item_Type").alias("Item_Type_Lowercase")).display()
 

# COMMAND ----------

from pyspark.sql.functions import upper

# Assuming df has a column named 'Item_Type'
df.select(upper("Item_Type").alias("Item_Type_upercase")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Date Functions
# MAGIC
# MAGIC ###1)Current_Date()
# MAGIC ###2)Date_add()
# MAGIC ###3)Date_Sub()

# COMMAND ----------

from pyspark.sql.functions import current_date

df = df.withColumn("curr_date", current_date())


# COMMAND ----------



# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import date_add

df = df.withColumn("date_add", date_add("curr_date",7))

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import date_sub
df.withColumn("sub_date",date_sub('curr_date',7)).display()

# COMMAND ----------

from pyspark.sql.functions import date_add

df = df.withColumn("new_date", date_add("curr_date",-7))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Date_Diff

# COMMAND ----------

from pyspark.sql.functions import datediff

df = df.withColumn("date_diff", datediff("curr_date","new_date"))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###date_format
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import date_format

df = df.withColumn("formatted_date",date_format('curr_date','dd-MM-yyyy'))

# COMMAND ----------

df.display()1111

# COMMAND ----------

# MAGIC %md
# MAGIC ###handling null

# COMMAND ----------

# MAGIC %md
# MAGIC ###1)dropping Nulls

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Filling Nulls

# COMMAND ----------

df.fillna('NotAvailable').display()

# COMMAND ----------

df.fillna('NotAvailable',subset=['Outlet_Size','Item_Weight']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###split and indexing

# COMMAND ----------

from pyspark.sql.functions import *
df.withColumn("Outlet_Type",split('Outlet_Type',' ')).display()

# COMMAND ----------

df.withColumn("Outlet_Type",split('Outlet_Type',' ')[0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### explode

# COMMAND ----------

df_exp=df.withColumn("Outlet_Type",split('Outlet_Type',' '))

# COMMAND ----------

df_exp.display()

# COMMAND ----------

df_exp.withColumn('Outlet_Type',explode('Outlet_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Array_contains
# MAGIC

# COMMAND ----------

df_exp.withColumn('Type1_flag',array_contains('Outlet_Type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###GroupBy
# MAGIC

# COMMAND ----------

df.groupby('Item_type').agg(sum('Item_MRP')).display()

# COMMAND ----------

df.groupby('Item_type').agg(avg('Item_MRP')).display()

# COMMAND ----------

df.groupby('Item_type','Outlet_Size').agg(avg('Item_MRP').alias('Total_MRP')).display()

# COMMAND ----------

df.groupby('Item_type','Outlet_Size').agg(avg('Item_MRP').alias('Total_MRP'),avg('Item_MRP').alias('Average_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Collect_List

# COMMAND ----------

data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]

schema = 'user string, book string'

df_book = spark.createDataFrame(data,schema)

df_book.display()

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book')).display()

# COMMAND ----------


df.select('Item_Type','Outlet_Size','Item_MRP').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###PIVOT

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###WHEN-OTHERWISE

# COMMAND ----------

df = df.withColumn('veg_flag',when(col('Item_Type')=='Meat','Non-Veg').otherwise('Veg'))

# COMMAND ----------

df.display()

# COMMAND ----------



# COMMAND ----------

df.withColumn('veg_exp_flag',when(((col('veg_flag')=='Veg') & (col('Item_MRP')<100)),'Veg_Inexpensive')\
                            .when((col('veg_flag')=='Veg') & (col('Item_MRP')>100),'Veg_Expensive')\
                            .otherwise('Non_Veg')).display() 

# COMMAND ----------

# MAGIC %md
# MAGIC ###JOINS

# COMMAND ----------

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inner Join
# MAGIC

# COMMAND ----------

df1.join(df2, df1['dept_id']==df2['dept_id'],'inner').display()
     

# COMMAND ----------

# MAGIC %md
# MAGIC ### Left Join

# COMMAND ----------

df1.join(df2, df1['dept_id']==df2['dept_id'],'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Right JOIN

# COMMAND ----------

df1.join(df2, df1['dept_id']==df2['dept_id'],'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ANTI JOIN

# COMMAND ----------


df1.join(df2,df1['dept_id']==df2['dept_id'],'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Window Functions

# COMMAND ----------



from pyspark.sql.window import Window

# COMMAND ----------

from pyspark.sql.functions import row_number,rank,col,dense_rank
from pyspark.sql.window import Window


df.withColumn('rowCol',row_number().over(Window.orderBy('Item_Identifier'))).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### RANK VS DENSE RANK

# COMMAND ----------

df.withColumn('rank',rank().over(Window.orderBy(col('Item_Identifier').desc())))\
        .withColumn('denseRank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc())))\
        .withColumn('row_number',row_number().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cumulative Sum

# COMMAND ----------

from pyspark.sql.functions import sum
from pyspark.sql.window import Window
from pyspark.sql.functions import col

# Ensure 'Item_MRP' is numeric
df = df.withColumn("Item_MRP", col("Item_MRP").cast("double"))

# Define the window specification
window_spec = Window.orderBy("Item_Type").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Add the cumulative sum column
df = df.withColumn('cumsum', sum("Item_MRP").over(window_spec))

# Display the resulting DataFrame
df.display()

     

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()
     

# COMMAND ----------

df.withColumn('totalsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()
     
