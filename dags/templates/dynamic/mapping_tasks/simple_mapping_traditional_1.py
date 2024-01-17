import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

# ================== Initialization ===================== #
DOC_MD_DAG = """
DAG template with simple mapping task (traditional syntax)

@Author: Sergei Boikov

@Description:
  DAG reproduce simple mapping task  approach (traditional syntax)

"""

class ConstDag:
    dag_id = os.path.basename(__file__).replace('.pyc', '').replace('.py', '')
    dag_start_date = days_ago(1)
    dag_schedule = None
    tags = ["mapping_task", "template", "dynamic"]


# ================== DAG creation ===================== #
dag = DAG(
    dag_id=ConstDag.dag_id,
    start_date=ConstDag.dag_start_date,
    schedule=ConstDag.dag_schedule,
    tags=ConstDag.tags,
    doc_md=DOC_MD_DAG
)


# ================== Operator callbacks ===================== #
def add_function(x: int, y: int):
    return x + y


# ================== General operators ===================== #
with dag:

    operator_added_values = PythonOperator.partial(
        task_id="add",
        python_callable=add_function,
        op_kwargs={"y": 10}
    ).expand(op_args=[[1],[2],[3]])


# ================== Dependencies ===================== #
operator_added_values