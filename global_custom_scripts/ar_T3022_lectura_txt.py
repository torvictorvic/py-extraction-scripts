import boto3
import numpy
import pandas as pd
import io
from io import StringIO, BytesIO
from datetime import date, timedelta, datetime
import glob
import os
import os.path
import sys
import pytz
import time
import pandas as pd
from pandas import DataFrame
from enum import Enum
import math
from urllib.parse import urlparse
import os
from rarfile import RarFile


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

            # session = boto3.Session(profile_name="default")
            # s3 = session.client('s3')

            session = boto3.session.Session(region_name='us-east-1')
            s3 = session.client('s3')

            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            _bytes = BytesIO(obj)
            return _bytes,lm
        else:
            with open(uri) as f:
                return uri,datetime.today()

class StringConverter(dict):
    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return str

    def get(self, default=None):
        return str

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
        #rar_file = RarFile(file)
        df =None
        widths = [1,1,5,5,5,5,2,10,8,10,15,5,1,8,8,8,5,12,5,3,100,3,6,69,500,500]
        widths_footer = [1,5,8,9,16,16]
        try:
            df = pd.read_fwf(file,dtype=object,widths=widths,skiprows=1,header=None)
            formato = ["tipo_de_registro","tipo_de_operacion","entidad_emisora","sucursal_de_la_entidad_emisora","entidad_cobradora","sucursal_de_la_entidad_cobradora","codigo_de_moneda","comprobante","fecha_de_movimiento","numero_de_cuenta","importe","concepto_de_ajuste","debito_o_credito","fecha_de_presentacion","fecha_de_aplicacion","fecha_de_cierre_cta_corriente","origen_de_la_cobranza_ajuste","nro_envi","grupocierrecod","codigo_de_erro","descripcion_del_erro","codigo_de_product","nro_de_caj","fille","cuenta_extern","request_i"]
            df.columns = formato
            df = df.iloc[:-1,:]
            # Extract and append footer
            df_footer = pd.read_fwf(file,dtype=object,widths=widths_footer,header=None)
            df_footer = df_footer.iloc[-1,:]
            df_footer = df_footer.to_frame()
            df_footer = df_footer.transpose()
            formato_footer = ["tipo_de_registro_f","entidad_emisora_f","código_de_identificación_del_archivo_f","cant_de_registros_f","sumatoria_de_importe_en_pesos_f","sumatoria_de_importe_en_moneda_extranjera_f"]            
            df_footer.columns = formato_footer
            df_footer = df_footer.reset_index(drop=True)
            for column in df_footer.columns:
                df[column] = df_footer.loc[0,column]
        except pd.io.common.EmptyDataError:
            formato = ["tipo_de_registro","tipo_de_operacion","entidad_emisora","sucursal_de_la_entidad_emisora","entidad_cobradora","sucursal_de_la_entidad_cobradora","codigo_de_moneda","comprobante","fecha_de_movimiento","numero_de_cuenta","importe","concepto_de_ajuste","debito_o_credito","fecha_de_presentacion","fecha_de_aplicacion","fecha_de_cierre_cta_corriente","origen_de_la_cobranza_ajuste","nro_envi","grupocierrecod","codigo_de_erro","descripcion_del_erro","codigo_de_product","nro_de_caj","fille","cuenta_extern","request_i","tipo_de_registro_f","entidad_emisora_f","código_de_identificación_del_archivo_f","cant_de_registros_f","sumatoria_de_importe_en_pesos_f","sumatoria_de_importe_en_moneda_extranjera_f"]
            df = pd.DataFrame(columns = formato)
            df = df.append(pd.Series(), ignore_index=True)
        if df is not None:
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
        return df