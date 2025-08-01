# Databricks notebook source
import sys
sys.path.append('/Workspace/Users/kaique@gracchusconsulting.com/RHSC/util')

# COMMAND ----------

### Import necessary libraries
import json
from pyspark.sql import functions as f
from utility import *

# COMMAND ----------

### Create Parameters
dbutils.widgets.text('args','')
args = json.loads(dbutils.widgets.get('args'))


'''
Args Exemple:
  [
    {
      "read_format":"xlsx",
      "read_path":"/Workspace/Users/kaique@gracchusconsulting.com/RHSC/Data/GFPVAN/RHSC_VAN_master data_countrybucketsettings_2025-05-15.xlsx", 
      "table":"countrybucketsettings",
      "write_mode":"overwrite",
      "partition":"",

    },
    {
      "read_format":"xlsx",
      "read_path":"/Workspace/Users/kaique@gracchusconsulting.com/RHSC/Data/GFPVAN/RHSC_VAN_master data_v_hub_2025-05-15.xlsx", 
      "table":"v_hub",
      "write_mode":"overwrite",
      "partition":""
    },
    {
      "read_format":"xlsx",
      "read_path":"/Workspace/Users/kaique@gracchusconsulting.com/RHSC/Data/GFPVAN/RHSC_VAN_master data_v_products_2025-07-07.xlsx", 
      "table":"v_products",
      "write_mode":"overwrite",
      "partition":""
    },
    {
      "read_format":"xlsx",
      "read_path":"/Workspace/Users/kaique@gracchusconsulting.com/RHSC/Data/GFPVAN/RHSC_VAN_master_data_v_tradeitems_2025-05-15.xlsx", 
      "table":"v_tradeitems",
      "write_mode":"overwrite",
      "partition":""
    },
    {
      "read_format":"xlsx",
      "read_path":"/Workspace/Users/kaique@gracchusconsulting.com/RHSC/Data/GFPVAN/RHSC_VAN_transactional_v_ros_2025-05-15.xlsx", 
      "table":"transactional_v_ros",
      "write_mode":"overwrite",
      "partition":""
    }
  ]

'''

# COMMAND ----------

### Read File or Table

if args['read_format'] == 'xlsx':
  try:
    df = read_excel_with_spark(spark,path=args['read_path'])
    df.printSchema()
  except:
    df = read_excel_with_pandas(spark,path=args['read_path'])
    df.printSchema()

elif args['read_format'] == 'table':
  df = spark.read.table(args['read_table'])

elif args['read_format'] == 'csv':
  df = spark.read.format(args['read_format']).option("header", "true").load(args['read_path'])

# COMMAND ----------

### Create Control Columns
df = df.withColumn('ingest_timestamp', f.current_date())

# COMMAND ----------

df = rename_cols(df)

# COMMAND ----------

df_write = (
  df.write
  .mode(args['write_mode'])
  .format('delta')
  .option("partitionoverwritemode", "dynamic")
)

if 'partition' in args and args['partition']!='':
  df_write = df_write.partitionBy(args['partition'])
else:
  df_write = df_write.partitionBy('ingest_timestamp')

df_write.saveAsTable(f"default.bronze_{args['table']}")