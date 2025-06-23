import io
import boto3
import pytz
import numpy as np
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

            session = boto3.session.Session(region_name='us-east-1')
            s3 = session.client('s3')
   
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            #obj = obj['Body'].read().decode("ISO-8859-1") code line to read hmtl
            obj = BytesIO(obj['Body'].read())
            return obj,lm
        else:
            with open(uri,"rb") as f:
                return BytesIO(f.read()),datetime.today()

class FileReader_2:
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

class StringConverter(dict):
    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return str

    def get(self, default=None):
        return str

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
        columns = ["drop_1","protocolo","data","datos_usuario","transaccion","pagamento","status","valor","autenticaciom","aute_externa","terminal_externo","ip_origen_cpf_cnpj","protocolo_contracargo","drop_2","drop_3"]
        df = pd.read_html(file, skiprows=1)[0]
        try:
          df.columns = columns
          for column in df.columns:
            if 'drop' in column:
              df.drop(column, axis=1,inplace=True)
          df.drop(df.tail(1).index,inplace=True)          
        except:
          columns = ["protocolo","data","datos_usuario","transaccion","pagamento","status","valor","autenticaciom","aute_externa","terminal_externo","ip_origen_cpf_cnpj","protocolo_contracargo"]
          df.columns = columns
        df['valor'] = df['valor'].astype('str')
        df['valor'] = df['valor'].str.replace(',','.').str.replace('R','').str.replace('$','')
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df

class Extractor_Excel:
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
        columns = ["protocolo","data","datos_usuario","transaccion","pagamento","status","valor","terminal_externo","aute_externa","ip_origen_cpf_cnpj","protocolo_contracargo","status_compensacion"]
        df = pd.read_excel(file, dtype=str)
        
        df.columns = columns
        df = df[["protocolo","data","transaccion","pagamento","status","valor","terminal_externo","aute_externa"]]
        #df["datos_usuario"] = [df["datos_usuario"].iloc[i]+" "+df["datos_usuario"].iloc[i+1]+" "+df["datos_usuario"].iloc[i+2] for i in range(len(df)) if i < len(df)-2]+[""]+[""]
        df.dropna(thresh=5, inplace=True)
        #df = df.iloc[:,:-1]  
        
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True, inplace=True)
        df['skt_extraction_rn'] = df.index.values
        return df

class Extractor_txt:
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
            colspec = [(1, 4), (5, 9), (9, 17), (17, 19),(19,22),(22,34),(36,37),(37,45),
                                    (45,51),(51,91),(91,101),(101,111),(111,161),(161,181),(181,183),
                                    (183,185),(185,187),(187,214),(214,262),(262,264),(264,268)]
            df = pd.read_fwf(file,header=None,colspecs=colspec,dtype ='str')
            df.columns = ['CODIGO_CORPORACAO', 'NUMERO_LOTE', 'ENVIO_DO_ARQUIVO', 'ZEROS','DESCRIPCIO_OPERACION',
                    'MONTO_BRUTO', 'CODIGO_TRANSACAO', 'FECHA_Y_HORA', 'HORA_TRANSACAO', 'DESCRICAO_TRANSACAO',
                    'NUMERO_AUTORIZACION', 'TRANSACAO_DO_USUARIO', 'EXTERNAL_ID', 'PAYMENT_ID', 'TIPO_OPERACION',
                    'CARTAO_EFETUOU_TRANSACAO', 'QUANTIDADE_PARCELAS', 'NSU_AUTORIZACAO_PAGAMENTO_CARTAO',
                    'CODIGO_BARRAS', 'BRANCOS', 'REGISTRO_DO_ARQUIVO']
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df

        except Exception as e:
            print("Error al subir la fuente: ",e)       