# Databricks notebook source
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *
import calendar
from datetime import datetime, timedelta
import fnmatch
import pandas as pd
from datetime import datetime 
from datetime import date 
import pytz
import calendar
import json
import uuid
import re
from pyspark.sql import SparkSession

spark.sql("use catalog poc_stg;")
spark.sql("use schema poc;")

spark.conf.set("spark.sql.ansi.enabled", False)
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


##-- set batch process success --##
def set_process_success():
    return json.dumps({
    "status": "Success",
    "message": None
    })

##-- set batch process failed  --##
def set_process_failed(_error_msg):  
    return json.dumps({
    "status": "Failed",
    "message": "{}".format(_error_msg)
    })

##-- get batch process status  --##
def get_process_status(_json_status):
    result =json.loads(_json_status)
    ### -- assign execute status
    status=result['status']
    message=result['message']
    return status,message


def convert_empty_to_none(_param):
    if str(_param).strip()=="":
        return None
    return _param

def convert_none_to_empty(_param):
    if _param is None:
        return ''
    return _param.strip()

def convert_null_to_empty(_param):
    if _param is None or _param.lower()=='null':
        return ''
    return _param.strip()

# COMMAND ----------


##-- inbound file
def get_file_schema(_source_file):
    if _source_file == "RATEREF":
        return StructType([
                    StructField("RATE_SHD_CODE", StringType(), True, {"length": 8}),
                    StructField("EFF_DT", StringType(), True, {"length": 8}),
                    StructField("TERM_CTF_DYS", StringType(), True, {"length": 5}),
                    StructField("MN_BAL_IN_TERM", StringType(), True, {"length": 12}),
                    StructField("RATE_ASSC_MN_BAL", StringType(), True, {"length": 8}),
                    StructField("BTCH_DT", StringType(), True, {"length": 8}),
                    StructField("SRC_STM_ID", StringType(), True, {"length": 3}),
                    StructField("FILLER", StringType(), True, {"length": 22})
                ])
    elif _source_file == "iris.parquet":
        return [
    "cast(`sepal.length` as decimal(10, 1)) as `sepal.length`",
    "cast(`sepal.width` as decimal(10, 1)) as `sepal.width`",
    "cast(`petal.length` as decimal(10, 1)) as `petal.length`",
    "cast(`petal.width` as decimal(10, 1)) as `petal.width`",
    "cast(`variety` as string) as `variety`"
    ]
    
    else:
        return None
