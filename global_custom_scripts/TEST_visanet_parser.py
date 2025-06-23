
import pytz
import boto3
import pandas as pd

from io import StringIO, BytesIO
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
            obj = obj['Body'].read().decode()
            return obj,lm
        else:
            with open(uri) as f:
                return uri,datetime.today()
            
class ExtractorVisanet:
    @staticmethod
    def run(filename,**kwargs):
        print('entro al run')
        try: 
            print("entro al try")
            file,lm = FileReader.read(filename)
            print("entra a filereader")
            my_timestamp = datetime.utcnow()
            old_timezone = pytz.timezone("UTC")
            new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
            # returns datetime in the new timezone
            arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
            upload_date = lm.astimezone(new_timezone)
            print('--*--'*10)
            print(f'Uploading {filename} . . .')
            body_df = StringIO(file)
            
            #se conoce la codificación del archivo
            print('se procede a leer el csv') 
            
            df = pd.read_csv(body_df,dtype=object, sep = ',')
            columns = ['SUCURSAL', 'FECHA_CUPON', 'FECHA_LIQUIDACION', 'FECHA_PROCESO',
                'PROCESO', 'SOBRE', 'CUPON', 'MONEDA', 'IMPORTE_CUPON', 'TARJETA',
                'PLAN_VENTA_I', 'AUTORI', 'NRO_PEDIDO', 'NRO_FACTURA', 'PROPINA',
                'PROPINA_EXCEDENTE', 'CASHBACK', 'ID_LIQUIDACION_VISA', 'TERMINAL',
                'LOTETERM', 'BOCA_ENTRADA', 'ARANCEL', 'IVA_ARANCEL', 'RETENCION_LEYES',
                'RET_LEYES_EQUIV', 'PORC_BENEF_LEYES', 'FORMA_PAGO', 'FECHA_PAGO',
                'ADQUIRENTE', 'PLAN_VENTA_II']
            df.columns = columns
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df['skt_extraction_rn'] = df.index.values    
            print("se cargo el df")
            return df
        except Exception as e:
            print("Error visanet: ",e)