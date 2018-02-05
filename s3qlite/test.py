import os
import pprint

from dotenv import find_dotenv, load_dotenv

import s3qlite

load_dotenv(find_dotenv())

client = s3qlite.client(
        region_name='nyc3',
        endpoint_url='https://nyc3.digitaloceanspaces.com',
        aws_access_key_id=os.environ['DIGITALOCEAN_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['DIGITALOCEAN_SECRET_ACCESS_KEY'])
client.connect('mittab-backups')

if __name__ == '__main__':
    print(client.execute('select * from tab_debater'))
