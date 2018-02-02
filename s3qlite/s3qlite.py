# -*- coding: utf-8 -*-

"""Main module."""
import pprint
import os

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

response = client.list_buckets()

if __name__ == '__main__':
    pprint.pprint(response)
