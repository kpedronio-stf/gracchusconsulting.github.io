# Databricks notebook source
import importlib.util
import subprocess
import sys
import logging
import re

packages_install = ["openpyxl"]
for pckg in packages_install:
  try:
      if importlib.util.find_spec(pckg) is None:
          subprocess.check_call([sys.executable, "-m", "pip", "install", pckg])
  except:
    logging.warning(f"Package {pckg} not installed")


# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as f,DataFrame,SparkSession
import logging

# COMMAND ----------

import logging

def to_timestamp(df, mapDatetime):
    if mapDatetime:
        logging.info("Starting to convert columns to timestamp")

        date_formats = [
            "yyyy-MM-dd HH:mm:ssXXX",
            "M/dd/yyyy h:mm:ss a",
            "d-MMM-yyyy h:mm:ss a",
            "M-dd-yyyy h:mm:ss a",
            "MM-dd-yyyy HH:mm:ss",
            "dd/MM/yyyy HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss",
            "M/d/yyyy, h:mm a",
            "M-d-yyyy, h:mm a",
            "M/d/yyyy H:mm",
            "MM/dd/yy HH:mm",
            "M-d-yyyy H:mm",
            "MM-dd-yy HH:mm",
            "MM/dd/yyyy hh:mm:ss a",
            "dd-MM-yyyy HH:mm:ss",
            "MM-dd-yyyy HH:mm:ss",
            "MM-dd-yyyy"
        ]

        dfCols = [c.lower() for c in df.columns]
        for c in mapDatetime:
            if c.lower() in dfCols:
                logging.debug(f"Converting column {c} with inferred format")

                timestamp_exprs = [f.expr(f"try_to_timestamp({c}, '{fmt}')") for fmt in date_formats]
                df = df.withColumn(c, f.coalesce(*timestamp_exprs))

                logging.debug(f"Column {c} converted to timestamp")
            else:
                logging.warning(f"Column {c} not found in DataFrame")

        logging.info("Completed converting columns to timestamp")
        return df
    else:
        logging.warning("No datetime columns provided, returning original DataFrame")
        return df
    

def remove_special_char(df,columns=None):
  columns = columns if columns else df.columns
  for c in columns:
    df = df.withColumn("c",f.regexp_replace(f.trim(f.col("c")), "[^a-zA-Z0-9 ]", ""))
  return df


def read_excel_with_spark(spark, path: str, sheet_name: str = 'Sheet1'):
    df = (
        spark.read.format("com.crealytics.spark.excel")
        .option("dataAddress", f"'{sheet_name}'!A1")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .load(path)
    )
    return df

def read_excel_with_pandas(spark: SparkSession, path: str, sheet_name: str = None) -> DataFrame:

    excel_file = pd.ExcelFile(path, engine="openpyxl")
    
    selected_sheet = sheet_name or excel_file.sheet_names[0]
    
    df_pd = pd.read_excel(excel_file, sheet_name=selected_sheet, engine="openpyxl")
    
    df_spark = spark.createDataFrame(df_pd)
    return df_spark

def columns_name_lower(df):
    for c in df.columns:
        df = df.withColumnRenamed(c,c.lower())
    return df

import os

def get_class(folder_path: str, filename: str):
    full_path = os.path.join(folder_path, filename)

    if os.path.exists(full_path):
        print(f" Found {filename}, executing...")
        with open(full_path, 'r') as f:
            code = f.read()
        exec(code)
    else:
        print(f" File '{filename}' not found in '{folder_path}'")

def rename_cols(df):
    for c in df.columns:
        # Replace any non-alphanumeric character with underscore
        new_col = re.sub(r'\W+', '_', c)
        df = df.withColumnRenamed(c, new_col)
    return df



