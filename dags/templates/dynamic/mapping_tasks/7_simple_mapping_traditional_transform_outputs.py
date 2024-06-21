import os

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.decorators import task_group, task
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

# ================== Initialization ===================== #
DOC_MD_DAG = """
Transform outputs with .map

@Author: Sergei Boikov

@Description:
  DAG reproduce transforming outputs with .map
  https://www.astronomer.io/docs/learn/dynamic-tasks#transform-outputs-with-map

  There are use cases where you want to transform the output of an upstream task
  before another task dynamically maps over it.
  For example, if the upstream traditional operator returns its output in a fixed format
  or if you want to skip certain mapped task instances based on a logical condition.
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


# an upstream task returns a list of outputs in a fixed format
def list_strings():
    return ["skip_hello", "hi", "skip_hallo", "hola", "hey"]


# the function used to transform the upstream output before
# a downstream task is dynamically mapped over it
def skip_strings_starting_with_skip(string):
    if len(string) < 4:
        return [string + "!"]
    elif string[:4] == "skip":
        raise AirflowSkipException(f"Skipping {string}; as I was told!")
    else:
        return [string + "!"]


# function to use in the dynamically mapped PythonOperator
def mapped_printing_function(string):
    return "Say " + string


with dag:
    listed_strings = PythonOperator(
        task_id="list_strings",
        python_callable=list_strings,
    )

    # transforming the output of the first task with the map function.
    # since `op_args` expects a list of lists it is important
    # each element of the list is wrapped in a list in the map function.
    transformed_list = listed_strings.output.map(skip_strings_starting_with_skip)

    mapped_printing = PythonOperator.partial(
        task_id="mapped_printing",
        python_callable=mapped_printing_function,
    ).expand(op_args=transformed_list)
