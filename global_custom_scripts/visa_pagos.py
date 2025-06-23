import io
import os
import sys
import glob
import pytz
import time
import math
import numpy
import boto3
import zipfile
import os.path
import numpy as np
import pandas as pd

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
            obj = obj['Body'].read().decode()
            return obj, lm, origin
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
    def run(filename):
        file,lm,origin = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        registros_1 = StringIO(file)
        registros_2 = StringIO(file)
        widths_registro_d = [8,9,3,10,4,17,1,1,8,1,6,14,6,14,5,27,5,1,6,10,1,1,9,1,34,4,1,16,1,8,14,6,1,3,22,13,1,12,6,1,8,1,3,1,8]
        # Registro TRx
        df4 = pd.read_fwf(registros_1,widths=widths_registro_d,dtype=object,header=None)
        formato = ["fecha_liquidacion","comercio","sucursal","n_a_1","moneda","n_a_2","registro","n_a_3","fecha_presentacion","n_a_4","sobre","n_a_5","cant_transacciones","n_a_6","plan","plan_descripcion","comision","comision_signo","n_a_7","comision_total_dia_plan","signo_comision_total","n_a_8","iva_total","signo_iva","n_a_9","nro_cupon","n_a_10","tarjeta","n_a_11","fecha_compra","n_a_12","nro_autorizacion","n_a_13","cod_rechazo","n_a_14","importe_bruto","signo_importe_bruto","n_a_15","nro_cierre","n_a_16","nro_pos","n_a_17","marca","n_a_18","fecha_vto"]
        df4.columns = formato
        for column in df4.columns:
            if 'n_a' in column:
                df4.drop(column, axis=1,inplace=True)
        df4['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df4['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df4['file_name'] = out
        df4.reset_index(drop=True)
        df4['skt_extraction_rn'] = df4.index.values
        return df4

class ExtractorFooter:
    @staticmethod
    def run(filename):
        file,lm,origin = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        
        registros_f_A = StringIO(file)
        
        # Footer A
        widths_registro_A = [226]
        df = pd.read_fwf(registros_f_A,dtype=object,header=None, widths=widths_registro_A)
        form = ["abc"]
        df.columns = form
        df_A = df[df.abc.str.len() == 215] 
        df = df[df.abc.str.len() == 226] 
        df['last'] = df['abc'].str[-10:]
        df = df.loc[df["last"].str.contains("^(\d{10})$", regex=True), :]
        df = df.drop(['last'], axis=1)
        df['fecha_liquidacion'] = df['abc'].astype(str).str[0:8]
        df['comercio'] = df['abc'].astype(str).str[8:17]
        df['sucursal'] = df['abc'].astype(str).str[17:20]
        df['moneda'] = df['abc'].astype(str).str[31:35]
        df['descripcion'] = df['abc'].astype(str).str[51:72]
        df['fecha_pago'] = df['abc'].astype(str).str[72:80]
        df['importe_bruto'] = df['abc'].astype(str).str[80:92]
        df['s_importe_bruto'] = df['abc'].astype(str).str[92:93]
        df['importe_comision'] = df['abc'].astype(str).str[93:105]
        df['s_importe_comision'] = df['abc'].astype(str).str[105:106]
        df['importe_comision_iva'] = df['abc'].astype(str).str[106:118]
        df['s_importe_comision_iva'] = df['abc'].astype(str).str[118:119]
        df['deducciones'] = df['abc'].astype(str).str[119:131]
        df['s_deducciones'] = df['abc'].astype(str).str[131:132]
        df['monto_a'] = df['abc'].astype(str).str[132:144]
        df['s_monto_a'] = df['abc'].astype(str).str[144:145]
        df['monto_b'] = df['abc'].astype(str).str[145:157]
        df['s_monto_b'] = df['abc'].astype(str).str[157:158]
        df['monto_c'] = df['abc'].astype(str).str[158:170]
        df['s_monto_c'] = df['abc'].astype(str).str[170:171]
        df['monto_d'] = df['abc'].astype(str).str[171:183]
        df['s_monto_d'] = df['abc'].astype(str).str[183:184]
        df['neto'] = df['abc'].astype(str).str[184:197]
        df['s_neto'] = df['abc'].astype(str).str[197:198]
        df['campo_a'] = df['abc'].astype(str).str[198:204]
        df['campo_b'] = df['abc'].astype(str).str[204:208]
        df['campo_c'] = df['abc'].astype(str).str[208:211]
        df['campo_d'] = df['abc'].astype(str).str[211:215]
        df['campo_e'] = df['abc'].astype(str).str[215:226]
        df = df.drop(['abc'], axis=1)
        df_A['fecha_liquidacion'] = df_A['abc'].astype(str).str[0:8]
        df_A['comercio'] = df_A['abc'].astype(str).str[8:17]
        df_A['sucursal'] = df_A['abc'].astype(str).str[17:20]
        df_A['moneda'] = df_A['abc'].astype(str).str[31:35]
        df_A['descripcion'] = df_A['abc'].astype(str).str[51:72]
        df_A['fecha_pago'] = df_A['abc'].astype(str).str[72:80]
        df_A['importe_bruto'] = df_A['abc'].astype(str).str[80:92]
        df_A['s_importe_bruto'] = df_A['abc'].astype(str).str[92:93]
        df_A['importe_comision'] = df_A['abc'].astype(str).str[93:105]
        df_A['s_importe_comision'] = df_A['abc'].astype(str).str[105:106]
        df_A['importe_comision_iva'] = df_A['abc'].astype(str).str[106:118]
        df_A['s_importe_comision_iva'] = df_A['abc'].astype(str).str[118:119]
        df_A['deducciones'] = df_A['abc'].astype(str).str[119:131]
        df_A['s_deducciones'] = df_A['abc'].astype(str).str[131:132]
        df_A['monto_a'] = df_A['abc'].astype(str).str[132:144]
        df_A['s_monto_a'] = df_A['abc'].astype(str).str[144:145]
        df_A['monto_b'] = df_A['abc'].astype(str).str[145:157]
        df_A['s_monto_b'] = df_A['abc'].astype(str).str[157:158]
        df_A['monto_c'] = df_A['abc'].astype(str).str[158:170]
        df_A['s_monto_c'] = df_A['abc'].astype(str).str[170:171]
        df_A['monto_d'] = df_A['abc'].astype(str).str[171:183]
        df_A['s_monto_d'] = df_A['abc'].astype(str).str[183:184]
        df_A['neto'] = df_A['abc'].astype(str).str[184:197]
        df_A['s_neto'] = df_A['abc'].astype(str).str[197:198]
        df_A['campo_a'] = df_A['abc'].astype(str).str[198:204]
        df_A['campo_b'] = df_A['abc'].astype(str).str[204:208]
        df_A['campo_c'] = df_A['abc'].astype(str).str[208:211]
        df_A['campo_d'] = df_A['abc'].astype(str).str[211:215]
        df_A = df_A.drop(['abc'], axis=1)
        df = pd.concat([df_A, df])
        df['indice'] = df.index.values
        df = df.sort_index()
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True, inplace=True)
        df['skt_extraction_rn'] = df.index.values
        return df

class ExtractorSplit:
    @staticmethod
    def run(filename):
        file,lm,origin = FileReader.read(filename)

        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        print('--*--'*10)
        print(f'Revisando {filename} . . .')

        ambiente= os.environ.get('ENVIRONMENT')
        if "test" in ambiente:
            bucket_dest = "global-in-sources-xxxxxxxxxx"
            key_f = "UY/VISA_FNOP_PRUEBA/OUTPUT/"
        else:
            bucket_dest = "global-in-sources-xxxxxxxxxx"
            key_f = "UY/VISA_FNOP/VISA_OUTPUT/"

        s3_client = boto3.client('s3')
        #lectura archivo
        registros = StringIO(file)
        widths = [500]
        df = pd.read_fwf(registros,dtype=object,header=None, widths=widths)
        form = ["abc"]
        df.columns = form
        df_split = df[df['abc'].str.contains("000000000----------------------")]
        out = origin.path.split("/")[-1]
        if out[0] == 'm':
            resultado = out[20:]
        else:
            resultado = out
        name = resultado

        if len(df_split.index) > 1:
            index = df_split.index
            index = index[0]
            df_1 = df.iloc[:index+1]
            df_2 = df.iloc[index+1:]
            key_1 = key_f + name + "_part_1"
            key_2 = key_f + name + "_part_2"
            csv_buffer_1 = StringIO()
            csv_buffer_2 = StringIO()
            df_1.to_csv(csv_buffer_1, header=False, index=False)
            df_2.to_csv(csv_buffer_2, header=False, index=False)
            s3_client.put_object(Body=csv_buffer_1.getvalue(),Bucket=bucket_dest,Key=key_1)
            s3_client.put_object(Body=csv_buffer_2.getvalue(),Bucket=bucket_dest,Key=key_2)
            print('Subiendo al bucket {} a la ruta {} '.format(bucket_dest, key_1))
            print('Subiendo al bucket {} a la ruta {} '.format(bucket_dest, key_2))
        else:
            key = origin.path[1:]
            key_dest = key_f + name
            obj = s3_client.get_object(Bucket=bucket_dest, Key=key)['Body'].read()
            binary_data = BytesIO(obj)
            s3_client.put_object(Body=binary_data.getvalue(),Bucket=bucket_dest,Key=key_dest)
            print('Subiendo al bucket {} a la ruta {} '.format(bucket_dest, key))

        df = None
        return df
    

class ExtractorVisanet:
    @staticmethod
    def run(filename):
        print('entro al run')
        try:  
            file,lm,origin = FileReader.read(filename)
            my_timestamp = datetime.utcnow()
            old_timezone = pytz.timezone("UTC")
            new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
            # returns datetime in the new timezone
            arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
            upload_date = lm.astimezone(new_timezone)
            print('--*--'*10)
            print(f'Uploading {filename} . . .')
            body = StringIO(file)
            df = pd.read_excel(body,dtype=str)
            columns = ['SUCURSAL', 'FECHA_CUPON', 'FECHA_LIQUIDACION', 'FECHA_PROCESO',
                'PROCESO', 'SOBRE', 'CUPON', 'MONEDA', 'IMPORTE_CUPON', 'TARJETA',
                'PLAN_VENTA_0', 'AUTORI', 'NRO_PEDIDO', 'NRO_FACTURA', 'PROPINA',
                'PROPINA_EXCEDENTE', 'CASHBACK', 'ID_LIQUIDACION_VISA', 'TERMINAL',
                'LOTETERM', 'BOCA_ENTRADA', 'ARANCEL', 'IVA_ARANCEL', 'RETENCION_LEYES',
                'RET_LEYES_EQUIV', 'PORC_BENEF_LEYES', 'FORMA_PAGO', 'FECHA_PAGO',
                'ADQUIRENTE', 'PLAN_VENTA']
            df.columns = columns
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['filename'] = out
            df['skt_extraction_rn'] = df.index.values    
            print("se cargo el df")
            return df
        except Exception as e:
            print("Error visanet: ",e)
            
            
        
