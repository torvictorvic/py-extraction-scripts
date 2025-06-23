import boto3
import numpy
import pandas as pd
import io
from io import StringIO, BytesIO
from datetime import date, timedelta, datetime
import zipfile
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
            bytes_io = BytesIO(obj)
            return bytes_io,lm
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
        rar_file = RarFile(file)
        df =None
        #for text_file in rar_file.infolist():
        widths = [1,10,4,16,20,24,3,3,18,3,10,1,1,1,15,40,9,2,2,80,11,4,1,10,50,2,16,143,500,500]
        widths_footer = [1,8,9,9,9,9]
        #raw_data = rar_file.open(text_file.filename).read().decode()
        #body_df = StringIO(raw_data)
        #footer_df = StringIO(raw_data)
        try:
            df = pd.read_fwf(file,dtype=object,widths=widths,skiprows=1,header=None)
            formato = ["tipo_de_registro","nro_de_cuenta","adicional_n","n__de_tarjeta","fecha___hora_min_seg","importe","tipo_de_moneda","automoneoriiso","plan","cuotas","codigo_de_autorizacion","autoforzada","autoreverflag","autoautodebi","n__de_comercio","nombre_del_comercio","estado","relacionada","origen","rechazo","ica","mcc","tcc","codigo_regla_de_fraude","descripcion_regla_de_fraude","modeo_de_entrada","terminal_pos","filler","cuenta_externa","request_id"]            
            df.columns = formato
            df = df.iloc[:-1,:]
            # Extract and append footer
            df_footer = pd.read_fwf(file,dtype=object,widths=widths_footer,header=None)
            df_footer = df_footer.iloc[-1,:]
            df_footer = df_footer.to_frame()
            df_footer = df_footer.transpose()
            formato_footer = ["tipo_de_registro","fecha_de_generacion","cantde_solic_autorizaciones_anuladas","cant_de_solic_autorizaciones_aprobadas","cant_de_solic_autorizaciones_rechaz","cantidad_de_registros_de_detalle"]            
            df_footer.columns = formato_footer
            df_footer = df_footer.reset_index()
            for column in df_footer.columns:
                df[column] = df_footer.loc[0,column]
        except pd.io.common.EmptyDataError as e:
            formato = ['tipo_de_registro','nro_de_cuenta','adicional_n','n__de_tarjeta','fecha___hora_min_seg','importe','tipo_de_moneda','automoneoriiso','plan','cuotas','codigo_de_autorizacion','autoforzada','autoreverflag','autoautodebi','n__de_comercio','nombre_del_comercio','estado','relacionada','origen','rechazo','ica','mcc','tcc','codigo_regla_de_fraude','descripcion_regla_de_fraude','modeo_de_entrada','terminal_pos','filler','cuenta_externa','request_id','index','fecha_de_generacion','cantde_solic_autorizaciones_anuladas','cant_de_solic_autorizaciones_aprobadas','cant_de_solic_autorizaciones_rechaz','cantidad_de_registros_de_detalle']
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
