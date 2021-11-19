import os
import logging

from datetime import date
from airflow.hooks.base_hook import BaseHook
from pyspark.sql import SparkSession
from pyspark.sql import *

import pyspark.sql.functions as F

def API_to_GP(**kwargs):
    
    logging.info(f"Loading table API to GP")

    

    
    spark = SparkSession.builder\
        .master('local')\
        .appName('HT_FINAL_API_to_GP')\
        .getOrCreate()
        
    
    logging.info(f"Loading table API from silver")  
    table_df = spark.read\
        .option('header',True)\
        .option('inferSchema',True)\
        .csv(os.path.join('/','datalake','silver','api_data'))
    logging.info(f"Table {table} processing. Load to GP")
    
   
    #В GP до этого создана таблица out_of_stock c компрессией и хранением по колонкам
    
    gp_conn = BaseHook.get_connection('rd_greenplum')
    gp_url= f'jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}'
    gp_creds = {'user':gp_conn.login,'password':gp_conn.password}
    logging.info(f"Table out_of_stock writing to silver")
    table_df.write.jdbc(gp_url, table='out_of_stock',properties = gp_creds)
       
        
        
    
    logging.info(f"Loading table out_of_stock to GP Completed")
    