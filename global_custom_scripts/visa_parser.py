import os
import pytz
import boto3 
import pandas as pd

from enum import Enum
from pandas import DataFrame
from datetime import datetime
from urllib.parse import urlparse
from io import BytesIO,StringIO,TextIOWrapper

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


class EfectyParser:
    io: str
    def __init__(self, io):
        self.io = io

    def parse(self):
        data = []
        for line in self.io:
            item = {}
            item['codigo_comercio'] = line[0:9]
            item['referencia'] = line[9:21]
            item['numero_lote'] = line[21:33]
            item['codigo_mensaje'] = line[33:37]
            item['numerotarjeta'] = line[37:56]
            item['tipo_tarjeta'] = line[56:59]
            item['fecha_transaccion'] = line[59:73]
            item['fecha_proceso'] = line[73:81]
            item['fecha_abono'] = line[81:89]
            item['importe_transaccion'] = line[89:101]
            item['descuento_movimiento'] = line[101:113]
            item['comision_visa'] = line[113:125]
            item['comision_igv'] = line[125:137]
            item['importe_neto'] = line[137:149]
            item['tipo_captura'] = line[149:150]
            item['estado_abono'] = line[150:151]
            item['cuenta_bancaria'] = line[151:171]
            item['codigo_autorizacion'] = line[171:177]
            item['id_unico'] = line[177:192]
            item['numero_serie_terminal'] = line[192:203]
            item['numero_cuota'] = line[203:205]
            item['importe_cashback'] = line[205:215]
            item['tipo_transaccion'] = line[215:240]
            item['tipo_retencion'] = line[240:250]
            item['fecha_liberacion'] = line[250:258]
            item['info_adicional'] = line[258:268]            
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
        df = EfectyParser(raw_data).run()
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
        
class EfectyParserPrueba:
    io: str
    def __init__(self, io):
        self.io = io

    def parse(self):
        data = []
        for line in self.io:
            item = {}
            item['codigo_comercio'] = line[0:9]
            item['referencia'] = line[9:21]
            item['numero_lote'] = line[21:33]
            item['codigo_mensaje'] = line[33:37]
            item['numerotarjeta'] = line[37:56]
            item['tipo_tarjeta'] = line[56:59]
            item['fecha_transaccion'] = line[59:73]
            item['fecha_proceso'] = line[73:81]
            item['fecha_abono'] = line[81:89]
            item['importe_transaccion'] = line[89:101]
            item['descuento_movimiento'] = line[101:113]
            item['comision_visa'] = line[113:125]
            item['comision_igv'] = line[125:137]
            item['importe_neto'] = line[137:149]
            item['tipo_captura'] = line[149:150]
            item['estado_abono'] = line[150:151]
            item['cuenta_bancaria'] = line[151:171]
            item['codigo_autorizacion'] = line[171:177]
            item['id_unico'] = line[177:192]
            item['numero_serie_terminal'] = line[192:203]
            item['numero_cuota'] = line[203:205]
            item['importe_cashback'] = line[205:215]
            item['tipo_transaccion'] = line[215:240]
            item['tipo_retencion'] = line[240:250]
            item['fecha_liberacion'] = line[250:258]
            item['info_adicional'] = line[258:395]
            item['indicador_trx_intereses'] = line[395:399]
            item['importe_descontado'] = line[399:411]
            item['codigo_programa'] = line[411:415]
            data.append(item)
        return data

    def run(self) -> DataFrame:
        data = self.parse()
        # Construct dataframe
        return pd.DataFrame(data)


class ExtractorPrueba:
    @staticmethod
    def run(filename, **kwargs ):    
        file_,lm = FileReader.read(filename)
        raw_data = file_
        df = EfectyParserPrueba(raw_data).run()
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