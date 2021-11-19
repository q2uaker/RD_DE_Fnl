import os
import logging

from datetime import date
from airflow.hooks.base_hook import BaseHook
from pyspark.sql import SparkSession
from pyspark.sql import *

import pyspark.sql.functions as F

def tables_to_GP(table, **kwargs):
    
    logging.info(f"Loading table {table} to GP")

    

    
    spark = SparkSession.builder\
        .master('local')\
        .appName('HT_FINAL_tables_to_GP')\
        .getOrCreate()
        
    
    logging.info(f"Loading table {table} from silver")  
    table_df = spark.read\
        .option('header',True)\
        .option('inferSchema',True)\
        .csv(os.path.join('/','datalake','silver','dshop',table))
    logging.info(f"Table {table} processing. Load to GP")
    
   
    #Таблицы до этого созданы в GP c компрессией и хранением по колонкам
    
    gp_conn = BaseHook.get_connection('rd_greenplum')
    gp_url= f'jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}'
    gp_creds = {'user':gp_conn.login,'password':gp_conn.password}
    logging.info(f"Table {table} writing to silver")
    table_df.write.jdbc(gp_url, table=table,properties = gp_creds)
       
        
        
    
    logging.info(f"Loading table {table} to GP Completed")
    