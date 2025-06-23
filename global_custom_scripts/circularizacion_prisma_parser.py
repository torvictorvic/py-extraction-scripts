import pytz
import boto3
import pandas as pd
from io import BytesIO
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
        df = pd.read_csv(file, dtype=str)
        del file
        if len(df.columns) == 16:
            df["marca_pex"] = ""
            df["marca_cuota_cuota"] = ""
            df["marca_cobro_anticipado"] = ""
            df["monto_restante"] = ""    
            df.columns = ['cuit',  'nro_establecimiento',  'nro_comprobante',  'fecha_movimiento',  'monto_movimiento',  'nro_tarjeta', 'cod_banco_pagador',
                        'nro_caja',  'nro_lote',  'fecha_presentacion',  'fecha_pago',  'nro_autorizacion',  'cantidad_cuotas_total',  'nro_cuota','cod_tipo_operacion', 'dev_cco','marca_pex',
                        'marca_cuota_cuota','marca_cobro_anticipado', "monto_restante"]
            df = df[['cuit',  'nro_establecimiento',  'nro_comprobante',  'fecha_movimiento',  'monto_movimiento',  'nro_tarjeta', 'cod_banco_pagador',
                        'nro_caja',  'nro_lote',  'fecha_presentacion',  'fecha_pago',  'nro_autorizacion',  'cantidad_cuotas_total',  'nro_cuota','marca_pex',
                        'marca_cuota_cuota','marca_cobro_anticipado',"monto_restante",'cod_tipo_operacion', 'dev_cco']]
        elif len(df.columns)==17:
            df["cod_tipo_operacion"] = ""
            df["dev_cco"] = ""
            df["monto_restante"] = ""
            df.columns = ['cuit',  'nro_establecimiento',  'nro_comprobante',  'fecha_movimiento',  'monto_movimiento',  'nro_tarjeta', 'cod_banco_pagador',
                        'nro_caja',  'nro_lote',  'fecha_presentacion',  'fecha_pago',  'nro_autorizacion',  'cantidad_cuotas_total',  'nro_cuota','marca_pex',
                        'marca_cuota_cuota','marca_cobro_anticipado','monto_restante','cod_tipo_operacion', 'dev_cco',]
            df = df[['cuit',  'nro_establecimiento',  'nro_comprobante',  'fecha_movimiento',  'monto_movimiento',  'nro_tarjeta', 'cod_banco_pagador',
                        'nro_caja',  'nro_lote',  'fecha_presentacion',  'fecha_pago',  'nro_autorizacion',  'cantidad_cuotas_total',  'nro_cuota','marca_pex',
                        'marca_cuota_cuota','marca_cobro_anticipado','monto_restante','cod_tipo_operacion', 'dev_cco']]
        elif len(df.columns)==18:

            df["cod_tipo_operacion"] = ""
            df["dev_cco"] = ""
            df.columns = ['cuit',  'nro_establecimiento',  'nro_comprobante',  'fecha_movimiento',  'monto_movimiento',  'nro_tarjeta', 'cod_banco_pagador',
                        'nro_caja',  'nro_lote',  'fecha_presentacion',  'fecha_pago',  'nro_autorizacion',  'cantidad_cuotas_total',  'nro_cuota','marca_pex',
                        'marca_cobro_anticipado','marca_cuota_cuota','monto_restante','cod_tipo_operacion', 'dev_cco',]
            df = df[['cuit',  'nro_establecimiento',  'nro_comprobante',  'fecha_movimiento',  'monto_movimiento',  'nro_tarjeta', 'cod_banco_pagador',
                        'nro_caja',  'nro_lote',  'fecha_presentacion',  'fecha_pago',  'nro_autorizacion',  'cantidad_cuotas_total',  'nro_cuota','marca_pex',
                        'marca_cuota_cuota','marca_cobro_anticipado','monto_restante','cod_tipo_operacion', 'dev_cco']]

            
                      
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