import pytz
import boto3
import pandas as pd

from enum import Enum
from io import BytesIO
from urllib.parse import urlparse
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

            #session = boto3.session.Session()
            session = boto3.session.Session(region_name='us-east-1')
            s3 = session.client('s3')

            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            return obj,lm
        else:
            with open(uri) as f:
                return uri,datetime.today()

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
        if "txt" in filename:
            buffer_trx = BytesIO(file)
            df = pd.read_csv(buffer_trx,dtype=str,encoding="windows-1252",sep="\t")
            df.columns = ["numero_secuencial","status_recarga","importe_recarga","numero_linea","numero_cabecera","transaccion_id","fecha_recarga","hora_recarga","codigo_banco","status_atm","id_subred","id_pdv_tp","id_pdv_may","saldo","id_tipo_terminal","marca_comision","archivo_nrol","fe_proceso","hora_proceso_archivo","interl_cial","descripcion","numero_corrida","pedido_diario","status","proforma","n_documento","documento_venta","doc_facturacion","doc_comp","referencia","grupo_clientes","cobranza"]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        else:
            df =None
            return df
