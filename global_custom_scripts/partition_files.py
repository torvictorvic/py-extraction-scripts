import pytz
import boto3
import pandas as pd
from io import BytesIO,StringIO
from urllib.parse import urlparse
from io import BytesIO
from datetime import datetime

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

            session = boto3.session.Session()
            s3 = session.client('s3')

            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            binary_data = obj['Body']
            #binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri, "rb") as f:
                return f,datetime.now()

class Extractor():
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')

        session = boto3.session.Session()
        s3 = session.client('s3')

        csv_buffer = BytesIO()

        
        bucket_dest = "global-sp-sources-test"
        route = "SFTP/CIRCULARIZACION/TEMPORAL_PRISMA/"
        
        df = pd.read_csv(BytesIO(file.read()),chunksize=70000, sep="\t", iterator=True)
        for i in range(1000000):
            try:
                df.get_chunk().to_csv(csv_buffer, index=False)
                s3.upload_fileobj(csv_buffer,Bucket=bucket_dest,Key=route + "chunk{x}".format(x=i))
            except:
                break