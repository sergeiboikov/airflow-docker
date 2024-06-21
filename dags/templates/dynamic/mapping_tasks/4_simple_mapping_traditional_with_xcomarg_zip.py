import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

# ================== Initialization ===================== #
DOC_MD_DAG = """
Provide positional arguments with XComArg.zip()

@Author: Sergei Boikov

@Description:
  DAG reproduce providing positional arguments with XComArg.zip()
  https://www.astronomer.io/docs/learn/dynamic-tasks#provide-positional-arguments-with-xcomargzip

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
def one_two_three_function():
    return [1, 2]


def ten_twenty_thirty_function():
    return [10]


def one_two_three_hundred_function():
    return [100, 200, 300]


# ================== General operators ===================== #
with dag:

    one_two_three = PythonOperator(
        task_id="one_two_three", python_callable=one_two_three_function
    )

    ten_twenty_thirty = PythonOperator(
        task_id="ten_twenty_thirty", python_callable=ten_twenty_thirty_function
    )

    one_two_three_hundred = PythonOperator(
        task_id="one_two_three_hundred", python_callable=one_two_three_hundred_function
    )

    zipped_arguments = one_two_three.output.zip(
        ten_twenty_thirty.output, one_two_three_hundred.output, fillvalue=1000
    )
    # zipped_arguments contains [(1, 10, 100), (2, 1000, 200), (1000, 1000, 300)]

    # function that will be used in the dynamically mapped PythonOperator
    def add_nums_function(x, y, z):
        return x + y + z

    add_nums = PythonOperator.partial(
        task_id="add_nums", python_callable=add_nums_function
    ).expand(op_args=zipped_arguments)

# ================== Dependencies ===================== #
