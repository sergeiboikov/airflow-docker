"""
Operators and sensors for using in DAGs

@Author: Sergei Boikov
@Description:
    Module for operators and sensors for using in DAGs directly
"""

from dtp.contrib.libs.operators.web.custom_base_operators_web import DTPBaseDownloadFileOperator, DTPBaseHttpToS3Operator, DTPBasePythonOperator


class DTPDownloadFileOperator(DTPBaseDownloadFileOperator):
    pass


class DTPHttpToS3Operator(DTPBaseHttpToS3Operator):
    pass


class DTPPythonOperator(DTPBasePythonOperator):
    pass