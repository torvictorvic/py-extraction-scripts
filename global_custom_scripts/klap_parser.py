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

class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        file,lm = FileReader.read(filename)
        df = pd.read_csv(file,encoding='latin1',dtype=str,sep=";")
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone  
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        columns = ["fecha_trx","hora_trx","codigo_mc","sucursal","terminal","canal","tipo_tx","tipo_tarjeta","tipo_cuotas","cuotas","cuota_liquidada", "monto_venta","monto_venta_neto","iva_venta","propina","vuelto","comision_bruta","comision_neta","iva_comision","comision_operador","comision_transaccion","monto_pagado_bruto","monto_pagado_neto","md","mdr","iva","codigo_autorizacion","marca","emisor","bin","ultimos_cuatro_digitos","consumer_transaction_id","codigo_mc_Transaccion_original","consumer_transaction_id_original","codigo_subcomercio","mcc","soft_descriptor","numero_liquidacion","numero_proceso","fecha_Liquidacion","fecha_abono","banco_abono","cuenta_corriente_abono"]
        df.columns = columns
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df