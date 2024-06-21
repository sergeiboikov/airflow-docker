import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

# ================== Initialization ===================== #
DOC_MD_DAG = """
Repeated mapping

@Author: Sergei Boikov

@Description:
  DAG reproduce repeated mapping
  https://www.astronomer.io/docs/learn/dynamic-tasks#repeated-mapping

  You can dynamically map an Airflow task over the output of another dynamically mapped task.
  This results in one mapped task instance for every mapped task instance of the upstream task.
  The following example shows three dynamically mapped tasks.
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
def multiply_by_2_func(num):
    return [num * 2]


def add_10_func(num):
    return [num + 10]


def multiply_by_100_func(num):
    return num * 100


# ================== General operators ===================== #
with dag:

    multiply_by_2 = PythonOperator.partial(
        task_id="multiply_by_2",
        python_callable=multiply_by_2_func
    ).expand(op_args=[[1], [2], [3]])

    add_10 = PythonOperator.partial(
        task_id="add_10",
        python_callable=add_10_func
    ).expand(op_args=multiply_by_2.output)

    multiply_by_100 = PythonOperator.partial(
        task_id="multiply_by_100",
        python_callable=multiply_by_100_func
    ).expand(op_args=add_10.output)

# ================== Dependencies ===================== #

multiply_by_2 >> add_10 >> multiply_by_100
