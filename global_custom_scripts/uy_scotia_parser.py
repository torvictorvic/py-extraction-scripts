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
            colspec = [(0,8),(8,18),(18,28),(28,43),(43,53),(53,61),(61,69),(69,72),(72,75),(75,81),(81,83),(83,98),(98,99),(99,114),(114,129),(129,144),(144,159),(159,174),(174,183),(183,189),(189,197),(197,205),(205,208),(208,228),(228,243),(243,251),(251,253),(253,255),(255,270),(270,285),(285,300),(300,315),(315,350),(350,362),(362,374),(374,400)]
            df = pd.read_fwf(file,header=None,colspecs=colspec,dtype ='str')
            df.columns = ['FECHA_CIERRE', 'NUMERO_LIQUIDACION', 'NUMERO_COMERCIO', 'PUNTO_DE_VENTA', 'LOTE', 'FECHA_PRESENTACION', 'FECHA_PAGO', 'MONEDA', 'GRUPO_MOVIMIENTO', 'CODIGO_OPERACION', 'CUOTAS', 'IMPORTE', 'SG', 'IMPORTE_ADICIONAL', 'IMPORTE_COSTO', 'IMPORTE_ARANCEL', 'TIPO_DE_CAMBIO', 'TARJETA', 'COMPROBANTE', 'AUTORIZACION', 'TERMINAL', 'FECHA_ORIGINAL', 'ORIGEN', 'SUBCOMERCIO', 'IDENTIFICADOR_CDC', 'FECHA_LIQUIDACION', 'TIPO_TARJETA', 'CUOTA_LIQUIDADA', 'IMPORTE_NETO', 'IVA_SOBRE_ARANCEL', 'RETENCION', 'DEVOLUCION_IVA', 'TRX_ID', 'PAYMENT_ID', 'REFUND_ID', 'FILLER']
            df['UPLOAD_DATE'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['REPORT_DATE'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['FILE_NAME'] = out
            df.reset_index(drop=True)
            df['SKT_EXTRACTION_RN'] = df.index.values
             
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)