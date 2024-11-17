"""
Base operators and sensors for WEB for using as base for overriding

@Author: Sergei Boikov
@Description:
    Module for base operators and sensors for WEB. Please do not use it in DAGs
    It is only for overriding in custom_operators module
"""
from __future__ import annotations
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator
from airflow.utils.decorators import apply_defaults
from time import sleep
from urllib.request import urlretrieve
import dtp.constants as consts

if TYPE_CHECKING:
    from requests.auth import AuthBase


class DTPBaseDownloadFileOperator(BaseOperator):
    @apply_defaults
    def __init__(self, url, output_path, retries, retry_delay_sec, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url
        self.output_path = output_path
        self.retries = retries if retries is not None else consts.WEB_RETRIES
        self.retry_delay_sec = timedelta(
            seconds=int(retry_delay_sec if retry_delay_sec is not None else consts.WEB_RETRY_DELAY_SEC)
        )

    def execute(self, context):
        self.log.info(f"Download file: '{self.url}' -> '{self.output_path}'")
        for i in range(self.retries):
            try:
                urlretrieve(self.url, self.output_path)
                break
            except Exception as e:
                if i < self.retries - 1:
                    self.log.warning(f"Failed to download file. Retrying in {self.retry_delay_sec} seconds.")
                    sleep(self.retry_delay_sec)
                else:
                    raise AirflowException(f"Failed to download file. Max retries exceeded. {str(e)}")


class DTPBaseHttpToS3Operator(HttpToS3Operator):
    def __init__(
            self,
            *,
            endpoint: str | None = None,
            method: str = "GET",
            data: Any = None,
            headers: dict[str, str] | None = None,
            extra_options: dict[str, Any] | None = None,
            http_conn_id: str = None,
            log_response: bool = False,
            auth_type: type[AuthBase] | None = None,
            tcp_keep_alive: bool = True,
            tcp_keep_alive_idle: int = 120,
            tcp_keep_alive_count: int = 20,
            tcp_keep_alive_interval: int = 30,
            s3_bucket: str | None = None,
            s3_key: str,
            replace: bool = False,
            encrypt: bool = False,
            acl_policy: str | None = None,
            aws_conn_id: str | None = None,
            verify: str | bool | None = None,
            **kwargs
    ):
        super(DTPBaseHttpToS3Operator, self).__init__(
            http_conn_id=http_conn_id,
            method=method,
            endpoint=endpoint,
            headers=headers or {},
            data=data or {},
            extra_options=extra_options or {},
            log_response=log_response,
            auth_type=auth_type,
            tcp_keep_alive=tcp_keep_alive,
            tcp_keep_alive_idle=tcp_keep_alive_idle,
            tcp_keep_alive_count=tcp_keep_alive_count,
            tcp_keep_alive_interval=tcp_keep_alive_interval,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            replace=replace,
            encrypt=encrypt,
            acl_policy=acl_policy,
            aws_conn_id=aws_conn_id if aws_conn_id is not None else consts.S3_CONN_ID,
            verify=verify,
            **kwargs
        )
