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


@dataclass(kw_only=True)
class DynamoDBToS3PITROperator(BaseOperator):
    """
    :param
    :param
    :param
    """
    TableArn: str
    ExportTime: datetime
    ClientToken: str
    S3Bucket: str
    S3BucketOwner: str
    S3Prefix: str
    S3SseAlgorithm: str
    S3SseKmsKeyId: str
    ExportFormat: str
    exportid: str
    hook: DynamoDBHook | None = None
    meta: field(default_factory=dict)

    def __post_init__(self):
        super().__init__(self.meta)

    def execute(self, context: Context):
        self.hook = DynamoDBHook(aws_conn_id=self.aws_conn_id)
        client = self.hook.conn.meta.client
        client.export_table_to_point_in_time(
            TableArn=self.TableArn,
            ExportTime=self.ExportTime,
            ClientToken=self.ClientToken,
            S3Bucket='string',
            S3BucketOwner='string',
            S3Prefix='string',
            S3SseAlgorithm='AES256' | 'KMS',
            S3SseKmsKeyId='string',
            ExportFormat='DYNAMODB_JSON' | 'ION'
        )
        self.log.info("Export DynamoDB table to S3 using point in time recovery")
