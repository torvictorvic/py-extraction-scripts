import pandas as pd
from pandas import DataFrame
from enum import Enum
from urllib.parse import urlparse
from io import BytesIO,StringIO,TextIOWrapper
import boto3 
import pytz
from datetime import datetime
import os
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
                #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            bytes_io = BytesIO(obj)
            text_io = TextIOWrapper(bytes_io)
            return text_io, lm
        else:
            with open(uri) as f:
                return f.readlines(), datetime.today()

class StringConverter(dict):
    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return str

    def get(self, default=None):
        return str


class DinersParser:

    io: str

    def __init__(self, io):
        self.io = io

    def parse(self):
        data = []
        for line in self.io:
            item = {}
            item['codigo_comercio'] = line[0:10]
            item['tipo_transaccion'] = line[10:14]
            item['tarjeta'] = line[14:33]
            item['tipo_tarjeta'] = line[33:36]
            item['fecha_transaccion'] = line[36:44]
            item['fecha_de_proceso'] = line[44:52]
            item['fecha_de_abono'] = line[52:60]
            item['importe_de_la_transaccion'] = line[60:74]
            item['comision_total'] = line[74:88]
            item['comision_operador'] = line[88:102]
            item['IGV'] = line[102:116]
            item['importe_neto'] = line[116:130]
            item['tipo_de_captura'] = line[130:131]
            item['estado_del_abono'] = line[131:132]
            item['cuenta_bancaria'] = line[132:153]
            item['numero_de_autorizacion'] = line[153:163]
            item['numero_de_ticket'] = line[163:175]
            item['moneda'] = line[175:178]
            item['plan'] = line[178:188]
            item['cuota'] = line[188:191]            
            data.append(item)
        return data

    def run(self) -> DataFrame:
        data = self.parse()
        # Construct dataframe
        return pd.DataFrame(data)

class Extractor:

    @staticmethod
    def run(filename, **kwargs ):    
        file_,lm = FileReader.read(filename)
        raw_data = file_
        df = DinersParser(raw_data).run()
        # Append Report Date and Filename
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df
        