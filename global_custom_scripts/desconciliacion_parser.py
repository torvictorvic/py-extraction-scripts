import boto3
import base64
import pandas as pd

from io import BytesIO
from datetime import datetime
from urllib.parse import urlparse

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

            # session = boto3.Session(profile_name='sts')
            # s3 = session.client(
            #     's3'
            # )
            session = boto3.session.Session(region_name='us-east-1')
            s3 = session.client('s3')
            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)['Body'].read()
            obj = BytesIO(obj)
            return obj
        else:
            with open(uri) as f:
                return f.read()


class ExtractorNormal:
    @staticmethod
    def run(filename, **kwargs):

        file = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        #a= decode_base(file.encode())

        df = pd.read_csv(file,dtype=str,header=None)
        df.columns = ['ID_INPUT']
        for idx,valor in df.iterrows():
            mensaje = decode_base(valor['ID_INPUT'])
            df.loc[idx, 'ID_INPUT_CLEAN'] = mensaje
        for idx,valor in df.iterrows():
            id_input_clean = valor['ID_INPUT_CLEAN'].split('_')[-2]
            df.loc[idx, 'SKT_ID_A'] = id_input_clean
            id_input_clean = valor['ID_INPUT_CLEAN'].split('_')[-1]
            df.loc[idx, 'SKT_ID_B'] = id_input_clean
            report_name = ('_').join(valor['ID_INPUT_CLEAN'].split('_')[0:-2])
            df.loc[idx, 'REPORT_NAME'] = report_name
        return df


class ExtractorBolsa:
    @staticmethod
    def run(filename, **kwargs):

        file = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        #a= decode_base(file.encode())

        df = pd.read_csv(file,dtype=str,header=None)
        df.columns = ['ID_INPUT']
        for idx,valor in df.iterrows():
            mensaje = decode_base(valor['ID_INPUT'])
            df.loc[idx, 'ID_INPUT_CLEAN'] = mensaje
        for idx,valor in df.iterrows():
            id_input_clean = valor['ID_INPUT_CLEAN'].split('_')[-1]
            df.loc[idx, 'BATCH_ID'] = id_input_clean
            report_name = ('_').join(valor['ID_INPUT_CLEAN'].split('_')[0:-1])
            df.loc[idx, 'REPORT_NAME'] = report_name
        print(df['REPORT_NAME'])
        return df

def decode_base(base64_message):
    base64_bytes = base64_message.encode('ascii')
    message_bytes = base64.b64decode(base64_bytes)
    message = message_bytes.decode('ascii')
    return message