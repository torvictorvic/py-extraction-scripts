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
            session = boto3.session.Session()
            s3 = session.client('s3')

            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read().decode()
            return obj,lm
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
        tipo_tabla = kwargs['tipo_tabla']
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')

        widths_registro_2 = [8,2,7,3,3,7,1,3,2,15,1,1,8,7,14,6,1,11,3,4,26,6,1,10,6,5,6,39,18,8,14,6,1,3,24,11,1,1,9,9,8,1,3,1,8,10,5,1,11,1,6,1,9,1,7,10]
        widths_registro_5 = [8,2,7,3,14,2,3,19,26,8,2,13,13]
        widths_registro_6 = [8,2,7,3,14,2,13,23,8,104,13,12,18]

        try:
            registro_2 = StringIO(file)
            registro_5 = StringIO(file)
            registro_6 = StringIO(file)

            # Registro 2
            df1 = pd.read_fwf(registro_2,widths=widths_registro_2,dtype=object,header=None)
            formato = ["fecha_liq","drop_1","num_comercio","sucursal","drop_2","campo_1","drop_3","moneda","tipo_registro","nro_renglon_liq","tipo_de_registro_2","drop_4","fecha_presentacion","sobre","campo_3","nro_comprobantes","drop_5","campo_4","drop","plan_cuotas","drop_6","alicuotas_com","drop_7","campo_5","comision","drop_8","iva_com","drop_9","tarjeta","fecha_compra","drop_10","cod_auto","drop_11","cod_rechazo","drop_12","importe_bruto","signo","drop_13","nro_lote","drop_14","campo_6","drop_15","off","drop_16","fecha_pago_total_plan","campo_7","porc_iva_no_cobrado","drop_17","imp_iva_no_cobrado","drop_118","auto_alfa","aplica_iva","importe_devolucion","drop_18","nor_factura","importe_imponible"]
            df1.columns = formato
            for column in df1.columns:
                if 'drop' in column:
                    df1.drop(column, axis=1,inplace=True)
            df1 = df1.copy().loc[df1["tipo_registro"] == "02"]
            formato = ["fecha_liq","num_comercio","sucursal","campo_1","moneda","tipo_registro","nro_renglon_liq","tipo_de_registro_2","fecha_presentacion","sobre","campo_3","nro_comprobantes","campo_4","plan_cuotas","alicuotas_com","campo_5","comision","iva_com","tarjeta","fecha_compra","cod_auto","cod_rechazo","importe_bruto","signo","nro_lote","campo_6","off","fecha_pago_total_plan","campo_7","porc_iva_no_cobrado","imp_iva_no_cobrado","auto_alfa","aplica_iva","importe_devolucion","nor_factura","importe_imponible"]
            df1.columns = formato
            df1['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df1['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df1['file_name'] = out
            df1.reset_index(drop=True)
            df1['skt_extraction_rn'] = df1.index.values

            # Registro 5
            df2 = pd.read_fwf(registro_5,widths=widths_registro_5,dtype=object,header=None)
            formato = ["fecha_liq" ,"drop_1","num_comercio","sucursal","drop_2","tipo_registro","drop_3","codigo","operacion","fecha_mov","drop_4","importe","iva"]
            df2.columns = formato
            for column in df2.columns:
                if 'drop' in column:
                    df2.drop(column, axis=1,inplace=True)
            df2 = df2.copy().loc[df2["tipo_registro"] == "05"]
            df2['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df2['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df2['file_name'] = out
            df2.reset_index(drop=True)
            df2['skt_extraction_rn'] = df2.index.values

            # Registro 6:
            df3 = pd.read_fwf(registro_6,widths=widths_registro_6,dtype=object,header=None)
            formato = ["fecha_liq","drop_1","num_comercio","sucursal","drop_2","tipo_registro","drop_3","plan","fecha_pago","drop_4","importe_pagar","drop_5","cta"]
            df3.columns = formato
            for column in df3.columns:
                if 'drop' in column:
                    df3.drop(column, axis=1,inplace=True)
            df3 = df3.copy().loc[df3["tipo_registro"] == "06"]

            df3['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df3['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df3['file_name'] = out
            df3.reset_index(drop=True)
            df3['skt_extraction_rn'] = df3.index.values

            if tipo_tabla == 'pagos':
                return df1

            if tipo_tabla == 'cbk':
                return df2

            if tipo_tabla == 'cobranza':
                return df3

        except pd.io.common.EmptyDataError as e:

            formato = ["fecha_liq","num_comercio","sucursal","campo_1","moneda","tipo_registro","nro_renglon_liq","tipo_de_registro_2","campo_2","campo_3","nro_comprobantes","campo_4","plan_cuotas","alicuotas_com","campo_5","comision","iva_com","tarjeta","fecha_compra","cod_auto","cod_rechazo","importe_bruto","signo","nro_lote","campo_6","off","fecha_pago_total_plan","campo_7","porc_iva_no_cobrado","imp_iva_no_cobrado","auto_alfa","aplica_iva","importe_devolucion","nor_factura","importe_imponible"]
            df1 = pd.DataFrame(columns = formato)
            df1 = df1.append(pd.Series(), ignore_index=True)
            df1['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df1['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df1['file_name'] = out
            df1.reset_index(drop=True)
            df1['skt_extraction_rn'] = df1.index.values

            formato_1 = ["fecha_liq","num_comercio","sucursal","tipo_registro","codigo","operacion","fecha_mov","importe","iva"]
            df2 = pd.DataFrame(columns = formato_1)
            df2 = df2.append(pd.Series(), ignore_index=True)
            df2['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df2['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df2['file_name'] = out
            df2.reset_index(drop=True)
            df2['skt_extraction_rn'] = df2.index.values

            formato_2 = ['num_comercio','sucursal','tipo_registro','plan','fecha_pago','importe_pagar','cta']
            df3 = pd.DataFrame(columns = formato_2)
            df3 = df3.append(pd.Series(), ignore_index=True)
            df3['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df3['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df3['file_name'] = out
            df3.reset_index(drop=True)
            df3['skt_extraction_rn'] = df3.index.values


            if tipo_tabla == 'pagos':
                return df1

            if tipo_tabla == 'cbk':
                return df2
            
            if tipo_tabla == 'cobranza':
                return df3
            
        except Exception as e:
            print("Error al subir la fuente: ",e)

