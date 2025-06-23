from time import sleep
from typing import List
from urllib.parse import urlparse
from io import BytesIO
import pdb
import boto3 as boto3
import pandas as pd
from botocore.config import Config

boto3.set_stream_logger(name='botocore')

class S3Url(object):

    def __init__(self, url):
        self._parsed = urlparse(url, allow_fragments=False)

    @property
    def bucket(self):
        return self._parsed.netloc

    @property
    def key(self):
        if self._parsed.query:
            return self._parsed.path.lstrip('/') + '?' + self._parsed.query
        else:
            return self._parsed.path.lstrip('/')

    @property
    def url(self):
        return self._parsed.geturl()
        
class FileReader:

    @staticmethod
    def read(uri: str):
        origin = urlparse(uri, allow_fragments=False)
        if origin.scheme in ('s3', 's3a'):
            config = Config(
                connect_timeout=1, read_timeout=1,
                retries={'max_attempts': 1})
            s3 = boto3.client("s3", config=config)
            #session = boto3.session.Session()
            #s3 = session.client('s3')
            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
            sleep(3)
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data
        else:
            with open(uri) as f:
                return f.read()

class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        file = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_csv(file,dtype=str)
        return df
