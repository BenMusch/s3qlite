# -*- coding: utf-8 -*-
import pprint
import os
import sqlite3
import uuid
import threading
import re

import boto3


class resultSingleton:
    def __init__(self):
        self.val = {}

    def insert(self, key, val):
        self.val[key] = val

    def get(self):
        return self.val


class queryThread(threading.Thread):
    def __init__(self, threadID, boto_client, bucket, keys, query, result):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.boto_client = boto_client
        self.bucket = bucket
        self.keys = keys
        self.query = query
        self.result = result

    def run(self):
        self._execute_batch()

    def _execute_batch(self):
        for key in self.keys:
            self.result.insert(key, self._execute_single_obj(key))

    def _execute_single_obj(self, key):
        dest = os.path.join(str(uuid.uuid4()))
        self.boto_client.download_file(self.bucket, key, dest)

        try:
            conn = sqlite3.connect(dest)
            results = []
            for row in conn.execute(self.query):
                results.append(row)
        except Exception as e:
            os.remove(dest)
            raise e

        os.remove(dest)
        return results


class Client:
    def __init__(self, **kwargs):
        self.boto_session = boto3.session.Session()
        self.boto_client = self.boto_session.client('s3', **kwargs)
        self.bucket = None

    def connect(self, bucket):
        self.bucket = bucket

    def execute(self, query, **kwargs):
        num_threads = kwargs.pop('threads', 8)
        filter_func = kwargs.pop('filter', None)

        if filter_func is None:
            def filter_func(obj):
                return re.compile('^.*\.(db|sqlite|sqlite3)$').match(obj['Key'])

        objects = self.boto_client.list_objects(Bucket=self.bucket, **kwargs)['Contents']

        batches = [None] * num_threads
        threads = [None] * num_threads
        result = resultSingleton()
        i = 0

        for obj in objects:
            if not filter_func(obj):
                continue

            if i < num_threads:
                # if i < num_threads, batches[i] is not initialized
                batches[i] = [obj['Key']]
            else:
                batches[i % num_threads].append(obj['Key'])
            i += 1

        for i in range(num_threads):
            threads[i] = queryThread(i, self.boto_client, self.bucket, batches[i], query, result)

        [ thread.start() for thread in threads ]
        [ thread.join() for thread in threads ]

        return result.get()

