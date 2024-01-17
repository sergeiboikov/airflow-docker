import os
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

# ================== Initialization ===================== #
DOC_MD_DAG = """
DAG template with create_dag function from config file

@Author: Sergei Boikov

@Description:
  DAG template with create_dag function from config file

"""

class ConstDag:
    dag_start_date = days_ago(1)
    dag_schedule = None
    tags = ["create_dag_from_config", "template", "dynamic"]


# ================== Functions ===================== #
def get_config_names(folder_path: str) -> list:
    """
    Get config names in format 'Example.yml' from folder

    :param folder_path: Path to folder with config files
    :type folder_path: str
    :return: List with config file names
    :rtype: list
    """
    files = [f for f in os.listdir(folder_path)]
    return files


# ================== DAG creation ===================== #
def create_dag(dag_id: str, start_date: datetime, schedule: str,
               tags: list, doc_md: str, file_name: str) -> DAG:
    generated_dag = DAG(
        dag_id=dag_id,
        start_date=start_date,
        schedule=schedule,
        tags=tags,
        doc_md=doc_md
    )
    with generated_dag:

        # ================== Operator callbacks ===================== #
        def callback_validate_file(file_name: str):
            print(f"Validate {file_name}...")

        def callback_transform_file(file_name: str):
            print(f"Transform {file_name}...")

        # ================== General operators ===================== #
        operator_validate_file = PythonOperator(
            task_id="validate_file",
            python_callable=callback_validate_file,
            op_kwargs={"file_name": file_name}
        )

        operator_transform_file = PythonOperator(
            task_id="transform_file",
            python_callable=callback_transform_file,
            op_kwargs={"file_name": file_name}
        )

        # ================== Dependencies ===================== #
        operator_validate_file >> operator_transform_file

    return generated_dag

config_names = get_config_names(r"dags/templates/dynamic/single_file/config")

for config_name in config_names:
    file_name = config_name.lower().split(".")[0]
    dag_id = f"dag_process_{file_name}"

    globals()[dag_id] = create_dag(dag_id=dag_id,
                                   start_date=ConstDag.dag_start_date,
                                   schedule=ConstDag.dag_schedule,
                                   tags=ConstDag.tags,
                                   doc_md=DOC_MD_DAG,
                                   file_name=file_name)
