import os

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.decorators import task_group, task
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

# ================== Initialization ===================== #
DOC_MD_DAG = """
Example implementation for loading files

@Author: Sergei Boikov

@Description:
  Example implementation for loading files by using dynamic mapping tasks
"""


class ConstDag:
    dag_id = os.path.basename(__file__).replace('.pyc', '').replace('.py', '')
    dag_start_date = days_ago(1)
    dag_schedule = None
    tags = ["mapping_task", "template", "dynamic"]
    configs_path = "/opt/airflow/dags/templates/dynamic/mapping_tasks/rules/"


# ================== DAG creation ===================== #
dag = DAG(
    dag_id=ConstDag.dag_id,
    start_date=ConstDag.dag_start_date,
    schedule=ConstDag.dag_schedule,
    tags=ConstDag.tags,
    doc_md=DOC_MD_DAG
)


def get_config_names(configs_path: str) -> list:
    config_names = [[os.path.basename(f)] for f in os.listdir(configs_path) if f.endswith(".yml")]
    return config_names


def is_file_modified(config_name: str):
    file_name = config_name.split(".")[0]
    if file_name == "file_1":
        return False
    else:
        return True


def validate_file(file_name: str, is_file_for_validation: bool):
    if is_file_for_validation:
        print(f"Validating the file {file_name}...")
    else:
        raise AirflowSkipException(f"Skipping {file_name}...")


def transform_file(file_name: str, is_file_for_transformation: bool):
    if is_file_for_transformation:
        print(f"Transform the file {file_name}...")
    else:
        raise AirflowSkipException(f"Skipping {file_name}...")


# ================== General operators ===================== #
with dag:

    operator_get_configs = PythonOperator(
        task_id="get_configs",
        python_callable=get_config_names,
        op_kwargs={"configs_path": ConstDag.configs_path}
    )

    operator_is_file_modified = PythonOperator.partial(
        task_id="is_file_modified",
        python_callable=is_file_modified,
    ).expand(op_args=operator_get_configs.output)

    files_for_loading = operator_get_configs.output.zip(operator_is_file_modified.output)

    operator_validate_file = PythonOperator.partial(
        task_id="validate_file",
        python_callable=validate_file
    ).expand(op_args=files_for_loading)

    operator_transform_file = PythonOperator.partial(
        task_id="transform_file",
        python_callable=transform_file
    ).expand(op_args=files_for_loading)


# ================== Dependencies ===================== #
operator_get_configs >> operator_is_file_modified >> operator_validate_file >> operator_transform_file
