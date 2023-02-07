#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This module contains operators to replicate records from
DynamoDB table to S3 with point in time recovory.
"""

from datetime import datetime
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DynamoDBToS3PITROperator(BaseOperator):
    """
    This code created based on boto3 python clien below.
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.export_table_to_point_in_time
    :param TableArn: 
    :param ExportTime: 
    :param ClientToken: 
    :param S3Bucket:
    :param S3BucketOwner:
    :param S3Prefix:
    :param S3SseAlgorithm:
    :param S3SseKmsKeyId:
    :param ExportFormat:
    :param exportid:
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    """
    def __init__(
        self,
        *,
        TableArn: str,
        ExportTime: datetime,
        ClientToken: str,
        S3Bucket: str,
        S3BucketOwner: str,
        S3Prefix: str,
        S3SseAlgorithm: str,
        S3SseKmsKeyId: str,
        ExportFormat: str,
        exportid: str,
        aws_conn_id: str = "aws_default",
        region_name: str = "us-east-1",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_arn = TableArn
        self.export_time = ExportTime
        self.client_token = ClientToken
        self.s3_bucket = S3Bucket
        self.s3_owner = S3BucketOwner
        self.s3_prefix = S3Prefix
        self.s3_algo = S3SseAlgorithm
        self.s3_kmskey = S3SseKmsKeyId
        self.export_format = ExportFormat
        self.export_id = exportid
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

    def execute(self, context: Context):
        hook = DynamoDBHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
        client = hook.conn.meta.client
        response = client.export_table_to_point_in_time(
            TableArn=self.table_arn,
            ExportTime=self.export_time,
            ClientToken=self.client_token,
            S3Bucket=self.s3_bucket,
            S3BucketOwner=self.s3_owner,
            S3Prefix=self.s3_prefix,
            S3SseAlgorithm=self.s3_algo,
            S3SseKmsKeyId=self.s3_kmskey,
            ExportFormat=self.export_id
        )
        self.log.info("Export DynamoDB table to S3 using point in time recovery")

        return response
