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

            session = boto3.session.Session(region_name='us-east-1')
            s3 = session.client('s3')
     
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            _bytes = BytesIO(obj)
            return _bytes,lm
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
        rar_file = RarFile(file) # functional on AWS not functional in local
        df =None
        widths = [4,19,12,3,12,5,10,5,5,23,10,10,70,6,40,25,6,8,3,1,10,7,6]
        for text_file in rar_file.infolist():
            raw_data = rar_file.open(text_file.filename).read().decode()
            body_df = StringIO(raw_data)
        df = pd.read_csv(body_df, names=['abc'], sep=";" )
        df['cod_novedad'] = df['abc'].str[0:4]
        df['tarjeta'] = df['abc'].str[4:23]
        df['importe'] = df['abc'].str[23:35]
        df['moneda'] = df['abc'].str[35:38]
        df['fecha_hora'] = df['abc'].str[38:50]
        df['cod_func'] = df['abc'].str[50:55]
        df['nro_chgbk'] = df['abc'].str[55:65]
        df['motivo_mensaje'] = df['abc'].str[65:70]
        df['mcc'] = df['abc'].str[70:75]
        df['cod_ref_aquir'] = df['abc'].str[75:98]
        df['cod_autoriz'] = df['abc'].str[98:108]
        df['cod_comerc'] = df['abc'].str[108:118]
        df['nombre_comerc'] = df['abc'].str[118:188]
        df['fecha_clearing'] = df['abc'].str[188:194]
        df['observacion'] = df['abc'].str[194:234]
        df['id_archivo'] = df['abc'].str[234:259]
        df['ica_emisor'] = df['abc'].str[259:265]
        df['nro_msje'] = df['abc'].str[265:273]
        df['producto'] = df['abc'].str[273:276]
        df['signo'] = df['abc'].str[276:277]
        df['cuenta'] = df['abc'].str[277:287]
        df['arancel'] = df['abc'].str[287:294]
        df['iva_arancel'] = df['abc'].str[294:300]
        df = df.drop("abc", axis=1)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df