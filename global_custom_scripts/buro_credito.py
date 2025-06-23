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
            binary_data = obj
            return binary_data,lm
        else:
            with open(uri, "rb") as f:
                return f.read(),datetime.now()

class Extractor():
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        cols = ['bc_clave_actual_otorgante', 'bc_nombre_otorgante', 'bc_cuenta_actual', 'bc_tipo_cuenta', 'bc_tipo_contrato', 'bc_clave_unidad_monetaria', 'bc_frecuencia_pagos',
                'bc_monto_pagar', 'bc_fecha_apertura_cuenta', 'bc_fecha_ultimo_pago', 'bc_fecha_ultima_compra', 'bc_fecha_cierre_cuenta', 'bc_fecha_corte', 'bc_credito_maximo',
                'bc_saldo_actual', 'bc_limite_credito', 'bc_saldo_vencido', 'bc_pago_actual', 'bc_historico_pagos', 'bc_total_pagos_reportados', 'bc_fecha_primer_incumplimiento',
                'bc_saldo_insoluto', 'bc_monto_ultimo_pago', 'bc_fecha_ingreso_cartera_vencida', 'bc_monto_correspondiente_intereses', 'bc_forma_pago_actual_intereses',
                'bc_dias_vencimiento', 'bc_plazo_meses', 'bc_monto_credito_originacion'
                ]
        try:
            h = pd.read_csv(BytesIO(file), nrows=1)
            df = pd.read_csv(BytesIO(file), dtype=str, header=None, skiprows=1)

            df.columns = cols
        except:
            h = pd.read_csv(BytesIO(file), nrows=1)
            df = pd.read_csv(BytesIO(file), dtype=str, header=None, skiprows=1, encoding="utf-16")
        
        df.columns = cols #Asignación de nombre columnas al df

        #Asignación de valores para las primeras 5 columnas, 4 del head y 1 del tail
        df.insert(loc=0,column = 'bc_clave_otorgante', value = h.columns[0])
        df.insert(loc=1,column = 'bc_nombre_otorgante_header',value = h.columns[1])
        df.insert(loc=2,column = 'bc_fecha_extraccion',value = h.columns[2])
        df.insert(loc=3,column = 'bc_version',value = h.columns[3])
        df.insert(loc=4,column = 'bc_domicilio_devolución',value = df.iloc[-1].values[7])
        
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
        df.drop(df.tail(1).index,inplace=True) #Elimincación de la última fila
        return df