import io
import boto3
import pytz
import numpy as np
import pandas as pd
from io import StringIO, BytesIO
import gzip
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

            #session = boto3.session.Session()
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
        file = gzip.open(file,mode="rb").read()
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            columns = ["id_transaccion_organismo","load_date_time","load_retry_counter","load_ack_error","status","provider","tipo_tarjeta","numero_serie_hex","fecha_hora_transaccion","linea","estacion","autobus","ruta","equipo","tipo_equipo","location_id","tipo_transaccion","saldo_antes_transaccion","monto_transaccion","saldo_despues_transaccion","perfil1","perfil2","perfil3","sam_serial_hex_ultima_recarga","sam_serial_hex","contador_recargas","contador_validaciones","event_log","load_log","purchase_log","mac","counter_value","counter_amount","sam_counter","environment","environment_issuer_id","contract","contract_tariff","contract_sale_same","contract_restrict_time","contract_validity_start_date","contract_validity_duration","load_date_time_hex"]
            # last column will be read it but dropped later, this is do it to not alter the snowflake table
            df = pd.read_csv(BytesIO(file),dtype=object, sep=";", names=columns, skiprows=1, index_col=False)
            df = df[df.columns[:-1]]
            if df.empty:
                columns = ["id_transaccion_organismo","load_date_time","load_retry_counter","load_ack_error","status","provider","tipo_tarjeta","numero_serie_hex","fecha_hora_transaccion","linea","estacion","autobus","ruta","equipo","tipo_equipo","location_id","tipo_transaccion","saldo_antes_transaccion","monto_transaccion","saldo_despues_transaccion","perfil1","perfil2","perfil3","sam_serial_hex_ultima_recarga","sam_serial_hex","contador_recargas","contador_validaciones","event_log","load_log","purchase_log","mac","counter_value","counter_amount","sam_counter","environment","environment_issuer_id","contract","contract_tariff","contract_sale_same","contract_restrict_time","contract_validity_start_date","contract_validity_duration","load_date_time_hex"]
                df = pd.DataFrame(columns = columns)
                df = df.append(pd.Series(), ignore_index=True)
                df = df[df.columns[:-1]]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)
