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
from zipfile import ZipFile
import json
import xmltodict
import traceback


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

            # session = boto3.Session(profile_name="sts")
            # s3 = session.client('s3')
            session = boto3.session.Session(region_name='us-east-1')
            s3 = session.client('s3')

            # aws_access_key_id = AWS_ACCESS_KEY,
            # aws_secret_access_key = AWS_SECTRET_KEY  
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data, lm
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


class ExtractorFinancial:
    @staticmethod
    def run(filename, **kwargs):
        file, lm = FileReader.read(filename)
        print(f'Uploading {filename} . . .')
        try:
            lines = [bytes(x.decode().replace('=3D', '=').replace('=C3=AD', 'í'), encoding='utf8') for x in file]
            myio = io.BytesIO()
            for line in lines:
                myio.write(line)
            myio.seek(0)
            df = pd.read_html(myio)
            df = df[0]
            file_dates = df[df[0] == 'Periodo de Envío:'][4].iloc[0]
            list_file_dates = file_dates.split('al')
            initial_date = list_file_dates[0].strip()
            final_date = list_file_dates[1].strip()
            for data in range(len(df)):
                try:
                    if 'Cr' in df.iloc[data, 0]:
                        df = df.iloc[data:, :]
                except:
                    pass
            for data in range(len(df)):
                try:
                    if 'Total Servicio' in df.iloc[data, 0]:
                        df = df.iloc[:data - 1, :]
                except:
                    pass
            df.columns = df.iloc[0]
            df.drop((df.index[0]), inplace=True)
            df = df.loc[:, df.columns.notnull()]
            df = df.iloc[:, ~df.columns.duplicated()]
            df.columns = ['cr', 'descripcion', 'recibos_plaza', 'valor_recibos', 'comision', 'recibos_t_credito',
                          'comision_x_mpe_credito', 'recibos_t_debito', 'comision_x_mpe_debito', 'valor_neto']
            df['fecha_inicio'] = initial_date
            df['fecha_final'] = final_date
            for column in df.columns:
                if column == 'descripcion':
                    df[column] = df[column].apply(lambda x: str(x).replace('=', '').replace('</span>', '').strip())
                else:
                    df[column] = df[column].apply(
                        lambda x: str(x).replace(' ', '').replace('=', '').replace('</span>', '').replace('span>', ''))
        except ValueError:
            columns = ['cr', 'descripcion', 'recibos_plaza', 'valor_recibos', 'comision', 'recibos_t_credito',
                       'comision_x_mpe_credito', 'recibos_t_debito', 'comision_x_mpe_debito',
                       'valor_neto,fecha_inicio,fecha_final']
            df = pd.DataFrame(columns=columns)
            df = df.append(pd.Series(), ignore_index=True)

        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df


class ExtractorSettlement:
    @staticmethod
    def run(filename, **kwargs):
        file, lm = FileReader.read(filename)
        print(f'Uploading {filename} . . .')
        df = pd.read_csv(file, encoding='latin1', header=None)
        df.columns = ["nombre_plaza", "tienda", "fecha_movimiento", "hora", "referencia_1", "referencia_2",
                      "importe"]
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df


class ExtractorPagos:
    @staticmethod
    def run(filename, **kwargs):
        print(f'Uploading {filename} . . .')
        file, lm = FileReader.read(filename)
        zip_file = ZipFile(file)
        for text_file in zip_file.infolist():
            if 'xml' in text_file.filename:
                xml = zip_file.open(text_file.filename).read()
                json_final = json.dumps(xmltodict.parse(xml), indent=4)
                loaded_json = json.loads(json_final)
                df = pd.DataFrame(loaded_json)
                df1_transposed = df.T
                df1_transposed['descripcion'] = df1_transposed["cfdi:Conceptos"].values[0]['cfdi:Concepto']['@Descripcion']
                df1_transposed['impuestos'] = df1_transposed['cfdi:Impuestos'].values[0]['@TotalImpuestosTrasladados']
                df1_transposed = df1_transposed[['@Fecha', '@Folio', 'impuestos', '@Total', '@SubTotal','descripcion']]
                df1_transposed.columns = ["fecha", "folio", "impuestos", "total", "subtotal", "descripcion"]
                my_timestamp = datetime.utcnow()
                old_timezone = pytz.timezone("UTC")
                new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
                # returns datetime in the new timezone
                arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
                upload_date = lm.astimezone(new_timezone)
                df1_transposed['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
                df1_transposed['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
                out = (filename.split('/')[-1])
                df1_transposed['file_name'] = out
                df1_transposed['skt_extraction_rn'] = 0
                return df1_transposed
