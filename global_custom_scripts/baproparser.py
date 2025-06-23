from typing import List
from urllib.parse import urlparse
from io import BytesIO,StringIO
import pdb
import boto3 as boto3
import pandas as pd
import pandas as pd
import glob
from datetime import datetime
import pytz

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
            # session = boto3.Session(profile_name="sts")
            # s3 = session.client('s3')
                #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)['Body'].read().decode()
            binary_data = obj
            return binary_data,lm

        else:
            with open(uri) as f:
                return f.read(),datetime.today()

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
            binary_df2 = StringIO(file)
            binary_df = StringIO(file)
            header_df = StringIO(file)
            header_specification = [(0, 8), (8, 21), (21, 25), (25, 30),(30,72),(72,82),(82,92),(92,122)]
            cols_header = ['fecha_header','titulo_header','num_ente_header','num_comprobantes_cobrados_header',
            'relleno_2_header','importe_general_cobrado_header',
            'importe_total_pesos_header','relleno_3_header']

            header = pd.read_fwf(header_df, colspecs=header_specification, nrows=1, header=None, dtype=object)
            header.columns= cols_header

            col_specification = [(0, 4), (4, 14), (14, 21), (21, 27),(27,30),(30,60),(60, 68), (68, 72), (72, 82),(82,92),(92,122)]
            cols = ['entidad','referencia','monto','fecha_pago','digito_verificador',
            'vacio','fecha_cobro','sucursal','importe_total_pagado',
            'importe_parcial_pesos','relleno_1']
            df = pd.read_fwf(binary_df, colspecs=col_specification, skiprows=1, header=None, dtype=object)
            df.columns = cols
            df = df[df['vacio'].isna()]
            col_specification2 = [(0, 4), (4, 16), (16, 25), (25, 31),(31,34),(34,60),(60, 68), (68, 72), (72, 82),(82,92),(92,122)]
            cols = ['entidad','referencia','monto','fecha_pago','digito_verificador',
            'vacio','fecha_cobro','sucursal','importe_total_pagado',
            'importe_parcial_pesos','relleno_1']
            df2 = pd.read_fwf(binary_df2, colspecs=col_specification2, skiprows=1, header=None, dtype=object)
            df2.columns = cols
            df2 = df2[~df2['digito_verificador'].isna()]
            df = pd.concat([df, df2])
            df['fecha_header'] =header.loc[0,'fecha_header'] 
            df['titulo_header'] =header.loc[0,'titulo_header'] 
            df['num_ente_header'] =header.loc[0,'num_ente_header'] 
            df['num_comprobantes_cobrados_header'] =header.loc[0,'num_comprobantes_cobrados_header'] 
            df['relleno_2_header'] =header.loc[0,'relleno_2_header'] 
            df['importe_general_cobrado_header'] =header.loc[0,'importe_general_cobrado_header'] 
            df['importe_total_pesos_header'] =header.loc[0,'importe_total_pesos_header'] 
            df['relleno_3_header'] =header.loc[0,'relleno_3_header'] 

            df.importe_total_pagado = df.importe_total_pagado.astype('float')
            df.importe_parcial_pesos = df.importe_parcial_pesos.astype('float')
            df.importe_general_cobrado_header = df.importe_general_cobrado_header.astype('float')
            df.importe_total_pesos_header = df.importe_total_pesos_header.astype('float')
            df.relleno_1= df.relleno_1.astype('object')
            df.num_ente_header = df.num_ente_header.astype('int')
            df.num_comprobantes_cobrados_header = df.num_comprobantes_cobrados_header.astype('int')
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df

        except pd.io.common.EmptyDataError:                
            columns = ['entidad','referencia','monto','fecha_pago','digito_verificador','vacio','fecha_cobro','sucursal','importe_total_pagado','importe_parcial_pesos','relleno_1','fecha_header','titulo_header','num_ente_header','num_comprobantes_cobrados_header','relleno_2_header','importe_general_cobrado_header','importe_total_pesos_header','relleno_3_header']
            df = pd.DataFrame(columns = columns)
            df = df.append(pd.Series(), ignore_index=True)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df

        except Exception as e:
            print("Error al subir la fuente: ",e)
