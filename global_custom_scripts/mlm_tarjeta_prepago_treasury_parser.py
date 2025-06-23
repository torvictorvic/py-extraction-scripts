import pytz
import boto3
import pandas as pd



from urllib.parse import urlparse
from io import BytesIO
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

            session = boto3.session.Session()
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
        df = pd.read_csv(file, encoding='latin-1',sep=";", dtype=str)
        df.columns= ['nro_de_tarjeta',
                    'codigo_tipo_de_transaccion',
                    'tipo_de_transaccion',
                    'fee_debacred',
                    'mti',
                    'function_code',
                    'reason_code',
                    'mcc',
                    'codigo_de_la_institucion_adquirente',
                    'processing_code',
                    'codigo_de_respuesta',
                    'codigo_de_autorizacion',
                    'fecha_de_transaccion',
                    'fecha_de_presentacion_de_la_transaccion',
                    'moneda_original',
                    'importe_total_original',
                    'fecha_de_liquidacion',
                    'moneda_de_liquidacion',
                    'importe_total_liquidacion',
                    'moneda_tasa_de_intercambio_marca',
                    'tasa_de_intercambio_marca',
                    'tasa_de_intercambio_extendida',
                    'moneda_tasa_de_intercambio_adquirente',
                    'tasa_de_intercambio_adquirente',
                    'moneda_iva_tasa_de_intercambio',
                    'iva_de_la_tasa_de_intercambio',
                    'arn',
                    'feecollection_cuota_inversa',
                    'alcance_de_transaccion',
                    'tipo_de_tarjeta',
                    'nombre_del_comercio',
                    'importe_moneda_emisor'
                    ]       
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
