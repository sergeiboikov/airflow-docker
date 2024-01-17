from datetime import datetime

from airflow import DAG, settings
from airflow.models import Connection
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

# ================== Initialization ===================== #
DOC_MD_DAG = """
DAG template with create_dag function from connections

@Author: Sergei Boikov

@Description:
  DAG template with create_dag function from connections

"""

class ConstDag:
    dag_start_date = days_ago(1)
    dag_schedule = None
    tags = ["create_dag_from_connections", "template", "dynamic"]


# adjust the filter criteria to filter which of your connections to use
# to generated your DAGs
session = settings.Session()
conns = (
    session.query(Connection.conn_id)
    .filter(Connection.conn_id.ilike("%conn_table%"))
    .all()
)

# ================== DAG creation ===================== #
def create_dag(dag_id: str, start_date: datetime, schedule: str,
               tags: list, doc_md: str, conn_name: str) -> DAG:
    generated_dag = DAG(
        dag_id=dag_id,
        start_date=start_date,
        schedule=schedule,
        tags=tags,
        doc_md=doc_md
    )
    with generated_dag:

        # ================== Operator callbacks ===================== #
        def callback_process_table(conn_name: str):
            print(f"Process for {conn_name}...")

        # ================== General operators ===================== #
        operator_process_table = PythonOperator(
            task_id="process_table",
            python_callable=callback_process_table,
            op_kwargs={"conn_name": conn_name}
        )

        # ================== Dependencies ===================== #
        operator_process_table

    return generated_dag

for conn in conns:
    dag_id = f"dag_process_for_{conn[0]}"

    globals()[dag_id] = create_dag(dag_id=dag_id,
                                   start_date=ConstDag.dag_start_date,
                                   schedule=ConstDag.dag_schedule,
                                   tags=ConstDag.tags,
                                   doc_md=DOC_MD_DAG,
                                   conn_name=conn[0])
