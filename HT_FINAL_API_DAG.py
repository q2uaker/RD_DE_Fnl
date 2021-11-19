from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator

import sys
sys.path.append('./HT_FINAL')
import json
import os

from HT_FINAL.complex_http_operator import ComplexHttpOperator
from HT_FINAL.api_to_silver import api_to_silver


dag = DAG(
    dag_id="HT_FINAL_dag_API",
    description="Final HT dag API",
    start_date = datetime(2021,6,19,12,00),
    schedule_interval = '@daily',
    user_defined_macros={
        'json': json
        }
    )

           

api_load_to_bronze_task = ComplexHttpOperator(
                       task_id = "robodreams_api",
                       dag = dag,
                       http_conn_id="rd_api",
                       method="GET",
                       headers={},
                       endpoint="out_of_stock",
                       data = {"date": str("{{ds}}")},
                       )

load_to_silver_task =   PythonOperator(
                 task_id = "move_to_silver",
                 dag = dag,
                 python_callable = api_to_silver,
                 op_kwargs={},
                 provide_context = True
                )
api_load_to_bronze_task >> load_to_silver_task