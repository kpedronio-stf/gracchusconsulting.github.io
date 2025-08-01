# Databricks notebook source
### Import necessary libraries
import sys,importlib.util,os
sys.path.append('/Workspace/Users/kaique@gracchusconsulting.com/RHSC/util')
sys.path.append('/Workspace/Users/kaique@gracchusconsulting.com/RHSC/Transformations')

import json
from pyspark.sql import functions as f
import os
from utility import *


# COMMAND ----------

### Create Parameters
dbutils.widgets.text('args','')
args = json.loads(dbutils.widgets.get('args'))

# COMMAND ----------

module_name = args['table']
module_file = f"/Workspace/Users/kaique@gracchusconsulting.com/RHSC/Transformations/{module_name}.py"
if os.path.exists(module_file):
    spec = importlib.util.spec_from_file_location(module_name, module_file)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    print(f"✅ Imported '{module_name}' as module.")
    
    # You can now use module.some_function()
    # e.g., result = module.get_product_list()
else:
    print(f"❌ File '{module_file}' not found.")

# COMMAND ----------

### Read File or Table

if 'silver_file' in args:
  if args['read_format'] == 'xlsx':
    try:
      df = read_excel_with_spark(spark,path=args['read_path'])
      df.printSchema()
    except:
      df = read_excel_with_pandas(spark,path=args['read_path'])
      df.printSchema()
  elif args['read_format'] == 'csv':
    df = spark.read.format(args['read_format']).option("header", "true").load(args['read_path'])
else:
  df = spark.read.table(f"bronze_{args['table']}")



# COMMAND ----------

### Create Control Columns
df = df.withColumn('ingest_timestamp', f.current_date())

# COMMAND ----------

### Apply lower case on columns name
df = columns_name_lower(df)

# COMMAND ----------

if hasattr(module, args['table']):
    print("✅ 'v_products' class exists in the module.")
    df = getattr(module, args['table'])(df,args['table']).transform()
else:
    print("❌ 'v_products' class does not exist in the module.")

# COMMAND ----------

df_write = (
  df.write
  .mode(args['write_mode'])
  .format('delta')
)

if 'partition' in args and args['partition']!='':
  df_write = df_write.partitionBy(args['partition'])

df_write.saveAsTable(args['table'])