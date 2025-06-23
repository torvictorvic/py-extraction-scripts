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
import openpyxl


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

            session = boto3.session.Session()
            s3 = session.client('s3')

            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data, lm
        else:
            return uri, datetime.today()


class StringConverter(dict):
    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return str

    def get(self, default=None):
        return str


class Extractor_FN_Bogota:
    @staticmethod
    def run(filename, **kwargs):
        print('--*--' * 10)
        print(f'Uploading {filename} . . .')
        file_, lm = FileReader.read(filename)
        df = pd.read_excel(file_, sheet_name="APLICA", dtype=str)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone  
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df.columns = ["fecha_comp", "idcl", "bin", "renta", "producto", "valida_bin", "valor", "validar_valor", "plazo",
                  "validar_plazo", "porcentaje","porcentaje_aplicar","estab", "oficina", "tc", "autor_num", "codin", "mt_desc"]
        df = df[df.columns[0:]].replace(',', '.', regex=True)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df


class Extractor_FN_Popular:
    @staticmethod
    def run(filename, **kwargs):
        print('--*--' * 10)
        print(f'Uploading {filename} . . .')
        file_, lm = FileReader.read(filename)
        df = pd.read_excel(file_, dtype=str)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone  
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df.columns = ["bin", "ultimos_4_dig", "fec_fac", "numaut", "cod_com", "nomcomred", "tot_cuotas", "impfac",
                      "mes", "porcentaje_a_cobrar", "valor_a_cobrar", "observaciones"]
        df = df[df.columns[0:]].replace(',', '.', regex=True)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df


class Extractor_FN_Occidente:
    @staticmethod
    def run(filename, **kwargs):
        print('--*--' * 10)
        print(f'Uploading {filename} . . .')
        file_, lm = FileReader.read(filename)
        df = pd.read_excel(file_, dtype=str)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone  
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df.columns = ["fec_txs", "codigo_autorizacion", "codigo_comercio", "monto", "cuotas", "bin",
                      "comision_cobrada_a_mp", "ultimos_4_numeros_de_la_tarjeta"]
        df = df[df.columns[0:]].replace(',', '.', regex=True)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df


class Extractor_FN_Itau:
    @staticmethod
    def run(filename, **kwargs):
        print('--*--' * 10)
        print(f'Uploading {filename} . . .')
        file_, lm = FileReader.read(filename)
        df = pd.read_excel(file_, dtype=str)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone  
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df.columns = ["fecha_txs", "codigo_autorizacion", "codigo_comercio", "monto", "n_cuotas", "bin",
                      "comision_cobrada_a_mp", "ultimos_cuatro_numeros_de_la_tarjeta"]
        df = df[df.columns[0:]].replace(',', '.', regex=True)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df


class Extractor_FN_Davivienda:
    @staticmethod
    def run(filename, **kwargs):
        print('--*--' * 10)
        print(f'Uploading {filename} . . .')
        file_, lm = FileReader.read(filename)
        df = pd.read_excel(file_, dtype=str)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone  
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df.columns = ["fec_incorporacion", "nro_autorizacion", "franquicia", "codigo_establecimiento",
                      "valor_transaccion", "cuotas_diferidas", "ultimos_4", "bin", "porcentaje", "cobro"]
        df = df[df.columns[0:]].replace(',', '.', regex=True)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df


class Extractor_FN_Bancolombia:
    @staticmethod
    def run(filename, **kwargs):
        print('--*--' * 10)
        print(f'Uploading {filename} . . .')
        file_, lm = FileReader.read(filename)
        df = pd.read_excel(file_, dtype=str)
        df = df.iloc[:,0:12]
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone  
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df.columns = ["fec_venta", "nro_autorizacion", "codigo_establecimiento", "nombre_establecimiento",
                    "valor_transaccion", "cuotas_diferidas", "franquicia", "identi", "bin", "ultimos_4_dig",
                    "porcentaje", "cobro"]
        df = df[df.columns[0:]].replace(',', '.', regex=True)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df


class Extractor_FN_BBVA:
    @staticmethod
    def run(filename, **kwargs):
        print('--*--' * 10)
        print(f'Uploading {filename} . . .')
        file_, lm = FileReader.read(filename)
        df = pd.read_excel(file_, dtype=str)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone  
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df.columns = ["fecha", "cod_unico", "bin", "ultimos_4", "cuotas", "monto_facturacion", "comision",
                      "cod_autorizacion", "aplicacion"]
        df = df[df.columns[0:]].replace(',', '.', regex=True)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df


class Extractor_FN_AvVillas:
    @staticmethod
    def run(filename, **kwargs):
        print('--*--' * 10)
        print(f'Uploading {filename} . . .')
        file_, lm = FileReader.read(filename)
        df = pd.read_excel(file_, dtype=str)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone  
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df.columns = ["fec_txs", "codigo_autorizacion", "codigo_comercio", "monto", "n_cuotas", "bin",
                      "comision_cobrada_a_mp", "ultimos_cuatro_numeros_de_la_tarjeta"]
        df = df[df.columns[0:]].replace(',', '.', regex=True)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df


class Extractor_FN_Site_Teradata:
    @staticmethod
    def run(filename, **kwargs):
        print('--*--' * 10)
        print(f'Uploading {filename} . . .')
        file_, lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df = pd.read_csv(file_, dtype=str, sep=",")
        if df.empty:
            columns = ["pay_payment_id", "gtw_merchant_number", "tpv_segment_id", "pay_ccd_first_six_digits",
                       "pay_ccd_last_four_digits", "pay_status_id", "pay_ccd_authorization_code", "pay_created_dt",
                       "pay_move_date", "pay_combo_id", "cus_cust_id_sel", "pay_differential_pricing_id",
                       "pay_deduction_schema", "pay_status", "pay_ccd_installments_qty", "pay_total_paid_amt"]
            df = pd.DataFrame(columns=columns)
            df = df.append(pd.Series(), ignore_index=True)
        else:
            columns = ["pay_payment_id", "gtw_merchant_number", "tpv_segment_id", "pay_ccd_first_six_digits",
                       "pay_ccd_last_four_digits", "pay_status_id", "pay_ccd_authorization_code", "pay_created_dt",
                       "pay_move_date", "pay_combo_id", "cus_cust_id_sel", "pay_differential_pricing_id",
                       "pay_deduction_schema", "pay_status", "pay_ccd_installments_qty", "pay_total_paid_amt"]
            df.columns = columns
        df = df.reset_index(drop=True)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df
