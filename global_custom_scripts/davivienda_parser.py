import os
import pytz
import boto3
import pandas as pd

from io import BytesIO
from zipfile import ZipFile
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
        if ".zip" in filename:
            s3_url = S3Url(filename)
            session = boto3.session.Session()
            s3 = session.client('s3')
            ambiente= os.environ.get('ENVIRONMENT')
            key_f = "CO/DAVIVIENDA/"
            if "test" in ambiente:
                bucket_dest = "global-in-sources-xxxxxxxxxx"
            else:
                bucket_dest = "global-in-sources-xxxxxxxxxx"
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            datos = BytesIO(obj['Body'].read())
            zip_file = ZipFile(datos)
            zip_file.setpassword(b"3198")
            ls = ["A425MERCAPAGDICJDHEHA", "A425MERCAPAGDICJHC03A"]   
            for text_file in zip_file.namelist():
                if any(subs in text_file for subs in ls):
                    datos = BytesIO(zip_file.open(text_file).read())
                    s3.put_object(Body=datos.getvalue(),Bucket=bucket_dest,Key=key_f + text_file)
            df = None
            return df
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df = pd.read_csv(file,sep=';',index_col=False,dtype=str)
        columns = list(filter(lambda x: "Unnamed" not in str(x) ,df.columns))
        df = df[columns]
        df.columns = ["trn","comercio","franquicia","cuenta","f_vale","f_proceso","f_abono","tarjeta","hora_trn","comprobante","autorizacion","terminal","vlr_compra","vlr_iva","vlr_propina","vlr_total","vlr_comision","vlr_rete_iva","vlr_rete_ica","vlr_rete_fuente","vlr_abono","t_tarjeta"]
        numericas = ["vlr_compra","vlr_iva","vlr_propina","vlr_total","vlr_comision","vlr_rete_iva","vlr_rete_ica","vlr_rete_fuente","vlr_abono"]
        for n in numericas: df[n] = df[n].apply(lambda x: x.replace(',','.'))
        df = df[df.columns[0:]].replace(',', '', regex=True)
        df = df.astype(str)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1]).replace('.TXT','.csv')
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df

