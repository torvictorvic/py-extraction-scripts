import pytz
import boto3
import pandas as pd

from io import BytesIO
from urllib.parse import urlparse
from datetime import date, timedelta, datetime

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
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri) as f:
                return uri,datetime.today()


def is_conciliable(df):
  return df["conciliable"].str[-3:].eq("00").all()


class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        file,lm = FileReader.read(filename)
        colspecs = [
            (9,11),
            (9,11),
            (28,36),
            (28,36),
            (13,28),
            (135,166),
            (277,279)
            ]

        df = pd.read_fwf(file,dtype=object,colspecs=colspecs,header=None)
        df=df[1:-1]
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        columns = ["tipo_operacion","codigo_operacion","Fecha de recep archivo","Fecha de Compra","Importe","Reference ID", "conciliable"]
        df.columns = columns
        df["tipo_operacion"] = df["tipo_operacion"].replace('30', "C")
        df["tipo_operacion"] = df["tipo_operacion"].replace(['31', '34'], "D")

        df["codigo_operacion"] = df["codigo_operacion"].replace('30', "C")
        df["codigo_operacion"] = df["codigo_operacion"].replace(['31', '34'], "D")

        df['Site'] = "MLM"
        df['Acquirer'] = "Domiciliacion"
        df['Payment Method'] = "account_money"


        df.reset_index(drop=True)

        df = df[df["conciliable"].str[-3:] == "00"]
        df = df.drop(columns=["conciliable"])
        return df
      
filname='APLP.00000778.008.TG-0002.F230802.0030785.pgp'
df=Extractor.run(filname)
df