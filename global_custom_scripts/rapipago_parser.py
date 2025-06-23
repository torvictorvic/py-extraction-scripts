import copy
import pytz
import boto3
import pandas as pd

from datetime import datetime
from io import StringIO, BytesIO
from urllib.parse import urlparse
import logging  

boto3.set_stream_logger('', logging.DEBUG)

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
            #session = boto3.session.Session()
            session = boto3.session.Session(region_name='us-east-1')
            s3 = session.client('s3')
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            logger = logging.getLogger('custom-script')
            logger.info("RequestId: %s "+ s3_url.bucket + s3_url.key, obj['ResponseMetadata']['RequestId'])
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
            df = pd.read_excel(file,dtype=object)
            df = df.dropna(axis=1, how='all')
            if df.empty:
                columns = ["reference_id","token","identification_type","identification_number","op_type","payment_date","amount"]
                df = pd.DataFrame(columns = columns)
                df = df.append(pd.Series(), ignore_index=True)
            columns = ["reference_id","token","identification_type","identification_number","op_type","payment_date","amount"]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            df['identification_number'] = df['identification_number'].apply(lambda x:  str(x).replace('#ERROR','0') if x == '#ERROR' else x)
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorPagos:
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
            df = pd.read_excel(file,dtype=object,skiprows=3)
            df = df.dropna(axis=1, how='all')
            if df.empty:
                columns = ['desc_empresa_central','desc_empresa_puesto',"fecha_caja","importe","dato_adicional","barra"]
                df = pd.DataFrame(columns = columns)
                df = df.append(pd.Series(), ignore_index=True)
                vacio = True
            else:
                vacio = False
            df.columns = ['desc_empresa_central','desc_empresa_puesto',"fecha_caja","importe","dato_adicional","barra"]
            if not vacio:
                df['fecha_caja'] = pd.to_datetime(df['fecha_caja'], yearfirst=True).dt.strftime('%Y-%m-%d')
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorCISFTP:
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
        file_B = copy.deepcopy(file)
        try:
            df = pd.read_csv(file_B,dtype=str, index_col=False, sep=",", header=None, skiprows=1)
            if len(df.columns) == 8:
                columns = ["reference_id","token","identification_type","identification_number","op_type","payment_date","amount","traceability_metadata"]
                df.columns = columns
            else:
                columns = ["reference_id","token","identification_type","identification_number","op_type","payment_date","amount","transaction_date","external_id","branch_id","terminal_id"]
                df.columns = columns
            
            characters = [' ','/']
            for x in characters:
                df['reference_id'] = df['reference_id'].str.replace(x,'')
            
            print("se corrigieron los valores")
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            print('Se cargó a raw')
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorPagosSFTP:
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
            df = pd.read_csv(file,dtype=object)
            if df.empty:
                columns = ["payment_date","reference_id","amount","transaction_date","external_id","branch_id","terminal_id"]
                df = pd.DataFrame(columns = columns)
                df = df.append(pd.Series(), ignore_index=True)
            columns = ["payment_date","reference_id","amount","transaction_date","external_id","branch_id","terminal_id"]
            df.columns = columns
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorImpuestos:
    @staticmethod
    def run(filename):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        file_str = StringIO(file.read().decode())
        df = pd.read_csv(file_str,dtype=str)
        if df.empty:
            columns = ["orden_pago","codigo_retencion","nro_comprobante","codigo_impuesto","fecha_pago","importe_pago","importe_retencion","fecha","importe_bruto","importe_neto","importe_impuesto","proveedor"]
            df = pd.DataFrame(columns=columns)
            df = df.append(pd.Series(),ignore_index=True)
        else:
            columns = ["orden_pago","codigo_retencion","nro_comprobante","codigo_impuesto","fecha_pago","importe_pago","importe_retencion","fecha","importe_bruto","importe_neto","importe_impuesto","proveedor"]
            df.columns = columns
        df = df.reset_index(drop=True)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df
