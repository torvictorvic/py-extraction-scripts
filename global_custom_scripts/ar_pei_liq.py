import io
import os
import sys
import pytz
import time
import glob
import math
import numpy
import boto3
import gnupg
import zipfile
import os.path
import pandas as pd

from enum import Enum
from zipfile import ZipFile
from pandas import DataFrame
from io import StringIO, BytesIO
from urllib.parse import urlparse
from datetime import date, timedelta, datetime

def private_key():
    llave ="""-----BEGIN PGP PRIVATE KEY BLOCK-----
            ** FAKE ** TEST ** ** FAKE ** TEST ** 
            -----END PGP PRIVATE KEY BLOCK-----
    """
    return llave

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
            text_file = open("/tmp/pk_pei_asc", "w")
            text_file.write(private_key())
            text_file.close()
            gpg = gnupg.GPG()
            with open('/tmp/pk_pei_asc') as f:
                key_data = f.read()
            import_result = gpg.import_keys(key_data)
            print(import_result)
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            print(obj)
            d_data = gpg.decrypt(
                message = obj,
                passphrase='mlibre'
            )
            d_obj = d_data.data
            return d_obj, lm
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

class ExtractorLiquidaciones:
    @staticmethod
    def run(filename):

        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        print(file)
        try:
            try:
                try:
                    df = pd.read_excel(file,dtype=str)
                    if df.empty:
                        columns = ["fecha","hs_movimiento","cuenta","subcuenta","op_bancaria","desc_movi","referencia","importe","debito_credito","saldo","num_cuenta_origen","cvu_origen","cuit_originante","razon_social_originante","cuit_beneficiario","razon_social_beneficiario","comprobante_interbanking","nsbt"]
                        df = pd.DataFrame(columns=columns)
                        df = df.append(pd.Series(),ignore_index=True)
                    else:
                        columns = ["drop_0", "fecha", "drop_1", "cuenta", "drop_2", "subcuenta", "drop_3", "op_bancaria", "drop_4", "desc_movi", "drop_5", "drop_6", "referencia", "importe", "debito_credito","saldo", "cuit_originante", "razon_social_originante", "cuit_beneficiario", "razon_social_beneficiario", "comprobante_interbanking", "nsbt"]
                        df.columns = columns
                    for column in df.columns:
                        if 'drop_' in column:
                            df.drop(column, axis=1,inplace=True)
                    df = df[~df['fecha'].isin(['Fecha','FILTROS','MOVIMIENTOS TOTALES'])]
                    df = df[df['fecha'].notnull()]
                    df = df.reset_index(drop=True)
                    df['hs_movimiento'] = ""
                    df['num_cuenta_origen'] = ""
                    df['cvu_origen'] = ""
                    df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
                    df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
                    out = (filename.split('/')[-1])
                    df['file_name'] = out
                    df.reset_index(drop=True)
                    df['skt_extraction_rn'] = df.index.values
                    return df
                except:
                    df = pd.read_excel(file,dtype=str)
                    if df.empty:
                        columns = ["fecha","hs_movimiento","cuenta","subcuenta","op_bancaria","desc_movi","referencia","importe","debito_credito","saldo","num_cuenta_origen","cvu_origen","cuit_originante","razon_social_originante","cuit_beneficiario","razon_social_beneficiario","comprobante_interbanking","nsbt"]
                        df = pd.DataFrame(columns=columns)
                        df = df.append(pd.Series(),ignore_index=True)
                    else:
                        columns = ["drop_0", "fecha", "drop_1","hs_movimiento","drop_11", "cuenta", "drop_2", "subcuenta", "drop_3", "op_bancaria", "drop_4","drop_44", "desc_movi","referencia","importe","debito_credito","saldo","num_cuenta_origen","cuit_originante", "razon_social_originante", "cuit_beneficiario", "razon_social_beneficiario", "comprobante_interbanking", "nsbt"]
                        df.columns = columns
                    for column in df.columns:
                        if 'drop_' in column:
                            df.drop(column, axis=1,inplace=True)
                    df = df[~df['fecha'].isin(['Fecha','FILTROS','MOVIMIENTOS TOTALES'])]
                    df = df[df['fecha'].notnull()]
                    df = df.reset_index(drop=True)
                    df['cvu_origen'] = ""
                    df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
                    df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
                    out = (filename.split('/')[-1])
                    df['file_name'] = out
                    df.reset_index(drop=True)
                    df['skt_extraction_rn'] = df.index.values
                    return df
            except:
                df = pd.read_excel(file,dtype=str)
                if df.empty:
                    columns = ["fecha","hs_movimiento","cuenta","subcuenta","op_bancaria","desc_movi","referencia","importe","debito_credito","saldo","num_cuenta_origen","cvu_origen","cuit_originante","razon_social_originante","cuit_beneficiario","razon_social_beneficiario","comprobante_interbanking","nsbt"]
                    df = pd.DataFrame(columns=columns)
                    df = df.append(pd.Series(),ignore_index=True)
                else:
                    columns = ["drop_0", "fecha", "drop_1","hs_movimiento","drop_11", "cuenta", "drop_2", "subcuenta", "drop_3", "op_bancaria", "drop_4","drop_44", "desc_movi","referencia","importe","debito_credito","saldo","num_cuenta_origen","cvu_origen","cuit_originante", "razon_social_originante", "cuit_beneficiario", "razon_social_beneficiario", "comprobante_interbanking", "nsbt"]
                    df.columns = columns
                for column in df.columns:
                    if 'drop_' in column:
                        df.drop(column, axis=1,inplace=True)
                df = df[~df['fecha'].isin(['Fecha','FILTROS','MOVIMIENTOS TOTALES'])]
                df = df[df['fecha'].notnull()]
                df = df.reset_index(drop=True)
                df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
                df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
                out = (filename.split('/')[-1])
                df['file_name'] = out
                df.reset_index(drop=True)
                df['skt_extraction_rn'] = df.index.values
                return df
        except:
            df = pd.read_csv(io.BytesIO(file),dtype=str,sep="|")
            if df.empty:
                columns = ["fecha","hs_movimiento","cuenta","subcuenta","op_bancaria","desc_movi","referencia","importe","debito_credito","saldo","num_cuenta_origen","cvu_origen","cuit_originante","razon_social_originante","cuit_beneficiario","razon_social_beneficiario","comprobante_interbanking","nsbt"]
                df = pd.DataFrame(columns=columns)
                df = df.append(pd.Series(),ignore_index=True)
            else:
                columns = ["fecha","hs_movimiento","cuenta","subcuenta","op_bancaria","desc_movi","referencia","importe","debito_credito","saldo","num_cuenta_origen","cvu_origen","cuit_originante","razon_social_originante","cuit_beneficiario","razon_social_beneficiario","comprobante_interbanking","nsbt"]
                df.columns = columns
            df = df.reset_index(drop=True)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
