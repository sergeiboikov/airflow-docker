import os

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from dtp.contrib.libs.operators.custom_operators import DTPHttpToS3Operator
import dtp.constants as consts
import pathlib

# ================== Initialization ===================== #
DOC_MD_DAG = """
DAG for loading files from website to S3 bucket

@Author: Sergei Boikov

@Description:
  DAG loads files from website: https://dtp-stat.ru/opendata to S3 bucket

"""


class ConstDag:
    dag_id = os.path.basename(__file__).replace('.pyc', '').replace('.py', '')
    dag_start_date = consts.AIRFLOW_DAG_START_DATE
    dag_schedule = None
    tags = ["dtp", "web", "s3"]
    dtp_regions = consts.DTPPRJ_V_DAG__DTPPRJ__LOAD_WEB_S3_DTP__REGIONS


# ================== DAG creation ===================== #
dag = DAG(
    dag_id=ConstDag.dag_id,
    start_date=ConstDag.dag_start_date,
    schedule=ConstDag.dag_schedule,
    tags=ConstDag.tags,
    doc_md=DOC_MD_DAG
)


# ================== Operator callbacks ===================== #
def get_url_for_region(region_name: str) -> list:
    return f"https://cms.dtp-stat.ru/media/opendata/{region_name}.geojson"


def get_urls_for_regions(regions: tuple) -> list:
    print(pathlib.Path(__file__).parent.resolve())
    return [[get_url_for_region(region)] for region in regions]


# ================== General operators ===================== #
with dag:
    with TaskGroup("tasks_load_files_web_s3") as tasks_load_files_web_s3:
        for region in ConstDag.dtp_regions:
            url = get_url_for_region(region)
            # operator_load_file_web_s3 = DTPDownloadFileOperator(
            #     task_id=f"load_file__web_s3_{region}",
            #     url=url,
            #     output_path=f"/opt/airflow/dags/dtp/source_files/{region}.geojson"
            # )

            operator_load_file_web_s3 = DTPHttpToS3Operator(
                task_id=f"load_file__web_s3_{region}",
                http_conn_id=consts.WEB_DTP_CONN_ID,
                endpoint=f"/{region}.geojson",
                s3_bucket=consts.S3_BUCKET,
                s3_key=f"dtp/files/{region}.geojson",
                replace=True,
            )

            operator_convert_geojson_to_csv = DTPPythonOperator(
                task_id=f"load_file__web_s3_{region}",
                http_conn_id=consts.WEB_DTP_CONN_ID,
                endpoint=f"/{region}.geojson",
                s3_bucket=consts.S3_BUCKET,
                s3_key=f"dtp/files/{region}.geojson",
                replace=True,
            )

# ================== Dependencies ===================== #
tasks_load_files_web_s3
