import pytz
import boto3
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
            session = boto3.session.Session()
            s3 = session.client('s3')
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri,"rb") as f:
                return BytesIO(f.read()),datetime.today()

class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,dtype=str,sep="|")
            df.columns = ["FECHA","HORA","CUENTA","SUBCTA","COD_MOV","DESCRIPCION_MOV","REFERENCIA_MONI","IMPORTE","DEBITO_CREDITO","SALDO", "NUM_CUENTA_ORIGEN", "CVU_Origen", "CUIT_ORIGINANTE", "RZN_ORIGINANTE", "CUIT_BENEFICIARIO", "RZN_BENEFICIARIO", "COMPROBANTE_IB", "NSBT"]
            df['UPLOAD_DATE'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['REPORT_DATE'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            #Eliminacion del simbolo $
            df['IMPORTE'] = df['IMPORTE'].str.replace('$','', regex=True)
            df['SALDO'] = df['SALDO'].str.replace('$','', regex=True)
            out = filename.split('/')[-1]
            df['FILE_NAME'] = out
            df.reset_index(drop=True)
            df['SKT_EXTRACTION_RN'] = df.index.values
             
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)