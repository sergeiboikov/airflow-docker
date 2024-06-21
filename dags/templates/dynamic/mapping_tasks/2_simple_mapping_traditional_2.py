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
def callback_one_two_three():
    # this adjustment is due to op_args expecting each argument as a list
    return [[1], [2], [3]]


def callback_plus_10(x: int):
    return x + 10


# ================== General operators ===================== #
with dag:

    operator_one_two_three = PythonOperator(
        task_id="one_two_three",
        python_callable=callback_one_two_three
    )

    operator_plus_10 = PythonOperator.partial(
        task_id="plus_10",
        python_callable=callback_plus_10
    ).expand(op_args=operator_one_two_three.output)


# ================== Dependencies ===================== #
operator_one_two_three >> operator_plus_10