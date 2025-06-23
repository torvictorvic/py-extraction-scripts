import pytz
import boto3
import pandas as pd
from io import BytesIO
from urllib.parse import urlparse
from io import BytesIO
import re
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

            session = boto3.session.Session(region_name='us-east-1')
            s3 = session.client('s3')

            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri, "rb") as f:
                return BytesIO(f.read()),datetime.now()

class Extractor():
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_csv(file, sep=";", dtype=str)
        df.columns = [i.strip() for i in df.columns]

        if "NRO. LIQ." in df.columns:

            df["autor"] = ""
            df = df[['FEC.OPER', 'FEC.PRES', 'FEC.PAGO', 'COMERCIO', 'CUPON', "autor", 'MOV',
                     'IMPORTE CON DTO', 'CP', 'CU', 'DESC.TIPO FINANCIACION',
                     'TARJETA', 'PRODUCTO', 'NRO. LIQ.']]
        
        else:

            df["num_liq"] = ""

        df.columns = ['fe_oper',
                      'fe_pres',
                      'fe_pago',
                      'comercio',
                      'cupon',
                      'autor',
                      'mov',
                      'importe',
                      'cp',
                      'cu',
                      'desc_tipo_financiacion',
                      'tarjeta',
                      'producto',
                      'num_liq']

            

               
                           
        signo = [i[0] for i in df.importe]
        df["importe"] =  [i[1:].lstrip("0") for i in df.importe]        
        df["importe"] = [signo[i]+df.importe.iloc[i] for i in range(len(df.importe))]

        df.fe_oper = df.fe_oper.str.strip()
        df.fe_pago = df.fe_pago.str.strip()
        df.fe_pres = df.fe_pres.str.strip()

        if len(df.fe_oper[0])==6:
            df.fe_oper = [i.strftime("%Y-%m-%d") for i in pd.to_datetime(df.fe_oper,format="%y%m%d")]
            df.fe_pago = [i.strftime("%Y-%m-%d") for i in pd.to_datetime(df.fe_pago,format="%y%m%d")]
            df.fe_pres = [i.strftime("%Y-%m-%d") for i in pd.to_datetime(df.fe_pres,format="%y%m%d")]
        
        elif len(df.fe_oper[0])==8:
            df.fe_oper = [i.strftime("%Y-%m-%d") for i in pd.to_datetime(df.fe_oper,format="%Y%m%d")]
            df.fe_pago = [i.strftime("%Y-%m-%d") for i in pd.to_datetime(df.fe_pago,format="%Y%m%d")]
            df.fe_pres = [i.strftime("%Y-%m-%d") for i in pd.to_datetime(df.fe_pres,format="%Y%m%d")]
        
        elif len(df.fe_oper[0])==10:

            df.fe_oper = df.fe_oper.str.replace(".","")
            df.fe_pago = df.fe_pago.str.replace(".","")
            df.fe_pres = df.fe_pres.str.replace(".","")

            df.fe_oper = [i.strftime("%Y-%m-%d") for i in pd.to_datetime(df.fe_oper,format="%d%m%Y")]
            df.fe_pago = [i.strftime("%Y-%m-%d") for i in pd.to_datetime(df.fe_pago,format="%d%m%Y")]
            df.fe_pres = [i.strftime("%Y-%m-%d") for i in pd.to_datetime(df.fe_pres,format="%d%m%Y")]


        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        upload_date = lm.astimezone(new_timezone)
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df