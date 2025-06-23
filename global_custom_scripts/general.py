from typing import List
from urllib.parse import urlparse
from io import BytesIO
import pdb
import boto3 as boto3
import pandas as pd
import pytz
from datetime import datetime
from botocore.config import Config

CONFIG_GENERAL = Config(connect_timeout=120, read_timeout=120, retries={'max_attempts': 3})


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
        

def FileReader(uri: str, encoding = "utf-8"):
    origin = urlparse(uri, allow_fragments=False)
    if origin.scheme in ('s3', 's3a'):
        session = boto3.session.Session(region_name='us-east-1')
        s3 = session.client('s3', config=CONFIG_GENERAL)
            
        s3_url = S3Url(uri)

        obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
        lm = obj['LastModified']
        obj = obj['Body'].read().decode(encoding)
        return obj,lm
    else:
        with open(uri) as f:
            return f.read().decode(encoding), datetime.now()
