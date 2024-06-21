import os

from airflow import DAG
from airflow.decorators import task_group, task
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

# ================== Initialization ===================== #
DOC_MD_DAG = """
Mapping over task groups

@Author: Sergei Boikov

@Description:
  DAG reproduce mapping over task groups
  https://www.astronomer.io/docs/learn/dynamic-tasks#mapping-over-task-groups

  Task groups defined with the @task_group decorator can be dynamically mapped as well.
  The syntax for dynamically mapping over a task group is the same as dynamically mapping
  over a single task.
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

with dag:
    # creating a task group using the decorator with the dynamic input my_num
    @task_group(group_id="group1")
    def tg1(my_num):
        @task
        def print_num(my_num):
            return my_num

        @task
        def add_42(my_num):
            return my_num + 42

        print_num(my_num) >> add_42(my_num)

    # creating 6 mapped task group instances of the task group group1
    tg1_object = tg1.expand(my_num=[19, 23, 42, 8, 7, 108])
