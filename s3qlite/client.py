# -*- coding: utf-8 -*-
import os
import sqlite3
import uuid
import re

import boto3


class Client:
    def __init__(self, **kwargs):
        self.boto_session = boto3.session.Session()
        self.boto_client = self.boto_session.client('s3', **kwargs)
        self.bucket = None

    def connect(self, bucket):
        self.bucket = bucket

    def execute(self, query, **kwargs):
        filter_func = kwargs.pop('filter', None)

        results = {}
        if filter_func is None:
            def filter_func(obj):
                return re.compile('^.*\.(db|sqlite|sqlite3)$').match(obj['Key'])

        objects = self.boto_client.list_objects(Bucket=self.bucket, **kwargs)['Contents']

        for obj in objects:
            if not filter_func(obj):
                continue

            results[obj['Key']] = self._execute_single_obj(obj['Key'], query)
        return results

    def _execute_single_obj(self, key, query):
        dest = os.path.join(str(uuid.uuid4()))
        self.boto_client.download_file(self.bucket, key, dest)

        conn = sqlite3.connect(dest)
        results = []
        for row in conn.execute(query):
            results.append(row)

        os.remove(dest)
        return results


