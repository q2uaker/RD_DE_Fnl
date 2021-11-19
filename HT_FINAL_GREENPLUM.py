from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


import sys
sys.path.append('./HT_FINAL')
import json
import os

from HT_FINAL.tables_to_GP import tables_to_GP
from HT_FINAL.API_to_GP import API_to_GP
from airflow.operators.dummy_operator import DummyOperator


dag = DAG(
    dag_id="HT_FINAL_dag_GP",
    description="Final HT dag Greenplum",
    start_date = datetime(2021,6,19,15,00),
    schedule_interval = '@daily',

    )

dummy1 = DummyOperator(task_id="start_dag", dag=dag)
dummy2 = DummyOperator(task_id="end_dag", dag=dag)
tables=['clients','orders','products','aisles','departments']
tables_GP=[]          



                
for tbl in tables:
    pyop = PythonOperator(
                 task_id = f"GP_table_{tbl}",
                 dag = dag,
                 python_callable = tables_to_GP,
                 op_kwargs={'table':tbl},
                 provide_context = True
                )
    tables_GP.append(pyop) 
load_API_to_GP =   PythonOperator(
    task_id = "move_API_GP",
    dag = dag,
    python_callable = API_to_GP,
    provide_context = True
    )
tables_GP.append(load_API_to_GP) 
        
    

dummy1 >> tables_GP >> dummy2