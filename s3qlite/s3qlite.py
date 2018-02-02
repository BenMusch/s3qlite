# -*- coding: utf-8 -*-

"""Main module."""
import sqlite3
import os
import uuid
import re

import boto3
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

session = boto3.session.Session()
client = session.client(
        's3',
        region_name='nyc3',
        endpoint_url='https://nyc3.digitaloceanspaces.com',
        aws_access_key_id=os.environ['DIGITALOCEAN_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['DIGITALOCEAN_SECRET_ACCESS_KEY'],
        )


def run_query_bucket(bucket, query, prefix='', filter_func=None):
    if filter_func is None:
        def filter_func(filename):
            return re.compile('^.*\.(db|sqlite|sqlite3)$').match(filename)

    objects = client.list_objects(Bucket=bucket, Prefix=prefix)['Contents']
    results = {}

    for obj in objects:
        if not filter_func(obj['Key']):
            continue

        results[obj['Key']] = run_query_single_object(bucket, obj['Key'], query)
    return results

def run_query_single_object(bucket, key, query):
    dest = os.path.join(str(uuid.uuid4()))
    client.download_file(bucket, key, dest)

    conn = sqlite3.connect(dest)
    results = []
    for row in conn.execute(query):
        results.append(row)

    os.remove(dest)
    return results
