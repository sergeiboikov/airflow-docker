from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

# ================== Initialization ===================== #
DOC_MD_DAG = """
DAG template with generating DAG files from JSON config file

@Author: Sergei Boikov

@Description:
  DAG template with generating DAG files from JSON config file

"""

class ConstDag:
    dag_id = "dag_id_to_replace"
    dag_start_date = days_ago(1)
    dag_schedule = None
    dag_catchup = False
    tags = ["generate_dag_files_from_config", "template", "dynamic"]


# ================== DAG creation ===================== #

dag = DAG(
    dag_id=ConstDag.dag_id,
    start_date=ConstDag.dag_start_date,
    schedule=ConstDag.dag_schedule,
    catchup=ConstDag.dag_catchup,
    tags=ConstDag.tags,
    doc_md=DOC_MD_DAG
)


# ================== Operator callbacks ===================== #

def callback_operator_process_file(src_file_name: str, tgt_table_name: str) -> None:
    print(f"Loading data from the file: '{src_file_name}' to the table: '{tgt_table_name}'...")


# ================== General operators ===================== #

with dag:
    operator_process_file = PythonOperator(
        task_id="process_file",
        python_callable=callback_operator_process_file,
        op_kwargs={"src_file_name": "src_file_name_to_replace",
                   "tgt_table_name": "tgt_table_name_to_replace"}
    )


# ================== Dependencies ===================== #
operator_process_file