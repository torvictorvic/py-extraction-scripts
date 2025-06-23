import pytz
import boto3
import pandas as pd

from io import StringIO, BytesIO
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

            #session = boto3.Session(profile_name="sts")
            #s3 = session.client('s3')
            session = boto3.session.Session()
            s3 = session.client('s3')

            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri) as f:
                return uri,datetime.today()

class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        #df = pd.read_excel(file,dtype=object,skiprows=3)
        
        df = pd.read_csv(file,dtype=object,sep='\t',encoding='utf-16')
        df['fecha_inicial'] = df.iloc[6,1]
        df['fecha_final'] = df.iloc[6,5]
        df = df.iloc[9:]
        #df = df.drop('Unnamed: 2', axis=1)
        #df = df.drop('Unnamed: 6', axis=1)
        #df = df.drop('Unnamed: 8', axis=1)
        df.columns = ['cip','ord_comercio','s','monto','estado','fec_emision','fec_cancelacion','ec_anulada','fecha_expirar','fec_eliminado','servicio','cliente_nombre','cliente_apellidos','cliente_email','tipo_doc','nro_documento','cliente_alias','cliente_telefono','concepto_pago', 'datos_adicionales', 'canal','fecha_inicial','fecha_final']
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        df['monto'] = df['monto'].apply(lambda x: str(x).replace(',','.') if x else x)
        df = df[~df['cip'].isnull()]
        return df

class ExtractorExcel:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_excel(file,dtype=object,skiprows=3)
        fecha_inicial = df.iloc[2,7]
        fecha_final = df.iloc[2,13]
        df2 = df = pd.read_excel(file,dtype=object,skiprows=12)
        df2['moneda'] = df2["Unnamed: 14"]
        df2.rename({'Nro': 'numero', 'CIP': 'cip', 'IdComercio': 'id_comercio','Servicio': 'servicio','Comisión': 'comision','Total': 'total','Estado': 'estado','Banco': 'banco','Fecha Emisión': 'fecha_emision','Fecha Cancelación': 'fecha_cancelado','Archivo de Conciliación': 'archiv_conciliacion','Referencia': 'referencia','Concepto Pago': 'concepto_pago'}, axis=1, inplace=True)
        columns = list(filter(lambda x: "Unnamed" not in str(x) ,df2.columns))
        df2 = df2[columns]
        formato =  ['numero','cip','id_comercio','servicio','moneda','comision','total','estado','banco','fecha_emision','fecha_cancelado','concepto_pago']
        df2 = df2[formato]
        df2['fecha_inicial'] = fecha_inicial
        df2['fecha_final'] = fecha_final
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df2['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df2['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df2['file_name'] = out
        df2.reset_index(drop=True)
        df2['skt_extraction_rn'] = df2.index.values
        return df2 

