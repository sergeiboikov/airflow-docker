import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

# ================== Initialization ===================== #
DOC_MD_DAG = """
Simple DAG template with sensor and operator

@Author: Sergei Boikov

@Description:
  Simple DAG template with sensor and operator

"""

class ConstDag:
    dag_id = os.path.basename(__file__).replace('.pyc', '').replace('.py', '')
    dag_start_date = days_ago(1)
    dag_schedule = None
    dag_catchup = False
    tags = ["sensor", "operator" "template", "simple"]


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

    sensor_waiting_for_file = FileSensor(
        task_id="waiting_for_file",
        filepath=r"C:/test.csv",
        poke_interval=30,
        timeout=60 * 5
    )

    operator_process_file = PythonOperator(
        task_id="process_file",
        python_callable=callback_operator_process_file,
        op_kwargs={"src_file_name": "file_1",
                   "tgt_table_name": "table_1"}
    )

    operator_send_email = EmailOperator(
        task_id="send_email",
        to="1@1.ru",
        subject="Email Subject",
        html_content="<h1>Header</h1>"
    )


# ================== Dependencies ===================== #

sensor_waiting_for_file >> operator_process_file >> operator_send_email