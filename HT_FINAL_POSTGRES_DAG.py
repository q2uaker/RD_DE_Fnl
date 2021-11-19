from datetime import datetime
from airflow import DAG



import sys
sys.path.append('./HT_FINAL')
import json
import os


from HT_FINAL.tables_to_bronze import tables_to_bronze
from HT_FINAL.tables_to_silver import tables_to_silver
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator


dag = DAG(
    dag_id="HT_FINAL_dag_db",
    description="Final Hometask Postgres to bronze",
    start_date = datetime(2021,11,7,12,00),
    schedule_interval = '@daily',
    )

dummy1 = DummyOperator(task_id="start_dag", dag=dag)
dummy2 = DummyOperator(task_id="end_dag", dag=dag)
tables=['clients','orders','products','aisles','departments']
tables_bronze_dags=[]
tables_silver_dags=[]



for tbl in tables:
    pyop = PythonOperator(
                 task_id = f"save_table_{tbl}",
                 dag = dag,
                 python_callable = tables_to_bronze,
                 op_kwargs={'table':tbl},
                 provide_context = True
                ) 
    load_to_silver =   PythonOperator(
                 task_id = f"move_to_silver_{tbl}",
                 dag = dag,
                 python_callable = tables_to_silver,
                 op_kwargs={'table':tbl},
                 provide_context = True
                )
    pyop >> load_to_silver >> dummy2
    tables_bronze_dags.append(pyop)  
        
    

dummy1 >> tables_bronze_dags