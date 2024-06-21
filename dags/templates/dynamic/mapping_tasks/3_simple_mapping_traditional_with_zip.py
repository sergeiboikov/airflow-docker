import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

# ================== Initialization ===================== #
DOC_MD_DAG = """
Provide positional arguments with the built-in Python zip()

@Author: Sergei Boikov

@Description:
  DAG reproduce providing positional arguments with the built-in Python zip()
  https://www.astronomer.io/docs/learn/dynamic-tasks#provide-positional-arguments-with-the-built-in-python-zip

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
# use the zip function to create three-tuples out of three lists
zipped_arguments = list(zip([1, 2, 3], [10, 20, 30], [100, 200, 300]))
# zipped_arguments contains: [(1,10,100), (2,20,200), (3,30,300)]


# function for the PythonOperator
def add_numbers_function(x, y, z):
    return x + y + z


# ================== General operators ===================== #
with dag:

    # dynamically mapped PythonOperator
    add_numbers = PythonOperator.partial(
        task_id="add_numbers",
        python_callable=add_numbers_function,
    ).expand(op_args=zipped_arguments)


# ================== Dependencies ===================== #
