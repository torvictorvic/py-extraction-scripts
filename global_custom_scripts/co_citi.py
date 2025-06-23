import io
import os
import sys
import pytz
import time
import glob
import math
import boto3
import numpy
import zipfile
import os.path
import msoffcrypto
import pandas as pd

import warnings
warnings.filterwarnings('ignore')

from enum import Enum
from zipfile import ZipFile
from pandas import DataFrame
from io import StringIO, BytesIO
from urllib.parse import urlparse
from datetime import date, timedelta, datetime

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
            binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri) as f:
                return uri, datetime.today()

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
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        try:
            df = pd.read_excel(file, dtype=str)
        except:
            file = msoffcrypto.OfficeFile(file)
            file.load_key(password="MercadoPago-2020")
            file.decrypt(open("/tmp/decrypted.xlsx", "wb"))
            df = pd.read_excel("/tmp/decrypted.xlsx",dtype=str)
        try:
            df = df.drop(['Unnamed: 18','Unnamed: 19'], axis=1)
            cliente_ls = df.index[df['Unnamed: 0'] == "Nombre del cliente:"].tolist()
            trx_ls = df.index[df['Unnamed: 0'] == "Número de transacciones:"].tolist()
            d = {}
            df_f = {}
            df_final = pd.DataFrame()
            for n in range(len(cliente_ls)):
                d[n] = pd.DataFrame(df.iloc[cliente_ls[n]:trx_ls[n]+1])
                d[n] = d[n][d[n]['Unnamed: 0'].notna()]
                df_f[n] = d[n][d[n]['Unnamed: 17'].notna()]
                col = ["terminal","codigo_tr","referencia","no_tarjeta","fecha_transaccion","comp","codigo_autorizacion","valor_compra","valor_propina","valor_total","valor_descuento","valor_intercambio","valor_iva","valor_imp_consumo","rte_sobre_compra","rte_sobre_iva","rte_ica","vlr_dep_neto"]
                df_f[n].columns = col
                df_f[n]['nombre_cliente'] = d[n]['Unnamed: 1'].iloc[0]
                df_f[n]['cod_establecimiento'] = d[n]['Unnamed: 1'].iloc[1]
                df_f[n]['direccion'] = d[n]['Unnamed: 1'].iloc[2]
                df_f[n]['ciudad'] = d[n]['Unnamed: 1'].iloc[3]
                df_f[n]['fecha_reporte'] = d[n]['Unnamed: 1'].iloc[4]
                df_f[n]['fecha_recaudo'] = d[n]['Unnamed: 0'].iloc[5].split(":")[-1]
                df_f[n]['numero_trx'] = d[n]['Unnamed: 3'].iloc[-1]
                df_f[n]['porcentaje_retencion'] = d[n]['Unnamed: 3'].iloc[-2]
                df_f[n]['base_retencion'] = d[n]['Unnamed: 3'].iloc[-3]
                df_f[n]['valor_recaudado'] = d[n]['Unnamed: 3'].iloc[-4]
                df_final = df_final.append(df_f[n])
            terminal_ls = df_final.index[df_final['terminal'] == "Terminal"].tolist()
            df_final = df_final.drop(terminal_ls)
            out = filename.split('/')[-1]
            if "MC" in out:
                df_final['proc'] = "Mastercard"
            else:
                df_final['proc'] = "Visa"
            df_final = df_final.dropna(subset=["referencia"])
            df_final['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df_final['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            df_final['file_name'] = out
            df_final.reset_index(drop=True, inplace=True)
            df_final['skt_extraction_rn'] = df_final.index.values
            return df_final
        except:
            cliente_ls = df.index[df['Unnamed: 0'] == "Nombre del cliente:"].tolist()
            trx_ls = (df.index[df['Unnamed: 0'].str.contains("Terminal", na=False)] + 1).tolist()
            totales_rec = (df.index[df['Unnamed: 0'].str.contains("Totales por fecha de recaudo", na=False)] + 1).tolist()
            proc = df['Unnamed: 1'][0]
            d = {}
            df_f = {}
            df_final = pd.DataFrame()
            for n in range(len(cliente_ls)):
                try:
                    d[n] = pd.DataFrame(df.iloc[cliente_ls[n]:totales_rec[n]])
                except:
                    d[n] = pd.DataFrame(df.iloc[cliente_ls[n]:trx_ls[n]+10])
                d[n] = d[n][d[n]['Unnamed: 0'].notna()]
                df_f[n] = d[n][d[n]['Unnamed: 17'].notna()]
                col = ["terminal","codigo_tr","referencia","no_tarjeta","fecha_transaccion","comp","codigo_autorizacion","valor_compra","valor_propina","valor_total","valor_descuento","valor_intercambio","valor_iva","valor_imp_consumo","rte_sobre_compra","rte_sobre_iva","rte_ica","vlr_dep_neto"]
                df_f[n].columns = col
                df_f[n]['nombre_cliente'] = d[n]['Unnamed: 1'].iloc[0]
                df_f[n]['cod_establecimiento'] = d[n]['Unnamed: 1'].iloc[1]
                df_f[n]['direccion'] = d[n]['Unnamed: 1'].iloc[2]
                df_f[n]['ciudad'] = d[n]['Unnamed: 1'].iloc[3]
                df_f[n]['fecha_reporte'] = d[n]['Unnamed: 1'].iloc[4]
                df_f[n]['fecha_recaudo'] = d[n]['Unnamed: 0'].iloc[5].split(":")[-1]
                df_final = df_final.append(df_f[n])
            terminal_ls = df_final.index[df_final['terminal'] == "Terminal"].tolist()
            df_final = df_final.drop(terminal_ls)
            out = filename.split('/')[-1]
            if "MASTERCARD" in proc:
                df_final['proc'] = "Mastercard"
            else:
                df_final['proc'] = "Visa"
            df_final['numero_trx'] = None
            df_final['porcentaje_retencion'] = None
            df_final['base_retencion'] = None
            df_final['valor_recaudado'] = None
            df_final = df_final.dropna(subset=["referencia"])
            df_final['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df_final['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            df_final['file_name'] = out
            df_final.reset_index(drop=True, inplace=True)
            df_final['skt_extraction_rn'] = df_final.index.values
            return df_final
