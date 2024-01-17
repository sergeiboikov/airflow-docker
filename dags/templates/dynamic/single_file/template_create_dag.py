import os
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

# ================== Initialization ===================== #
DOC_MD_DAG = """
DAG template with create_dag function

@Author: Sergei Boikov

@Description:
  DAG template with create_dag function

"""

class ConstDag:
    dag_start_date = days_ago(1)
    dag_schedule = None
    tags = ["create_dag", "template", "dynamic"]

TABLE_NAMES = ["table_1",
               "table_2",
               "table_3"]

# ================== DAG creation ===================== #
def create_dag(dag_id: str, start_date: datetime, schedule: str,
               tags: list, doc_md: str, table_name: str) -> DAG:
    generated_dag = DAG(
        dag_id=dag_id,
        start_date=start_date,
        schedule=schedule,
        tags=tags,
        doc_md=doc_md
    )
    with generated_dag:

        # ================== Operator callbacks ===================== #
        def callback_process_table(table_name: str):
            print(f"Process {table_name}...")

        # ================== General operators ===================== #
        operator_process_table = PythonOperator(
            task_id="process_table",
            python_callable=callback_process_table,
            op_kwargs={"table_name": table_name}
        )

        # ================== Dependencies ===================== #
        operator_process_table

    return generated_dag

for table_name in TABLE_NAMES:
    dag_id = f"dag_process_{table_name}"

    globals()[dag_id] = create_dag(dag_id=dag_id,
                                   start_date=ConstDag.dag_start_date,
                                   schedule=ConstDag.dag_schedule,
                                   tags=ConstDag.tags,
                                   doc_md=DOC_MD_DAG,
                                   table_name=table_name)
