from urllib.parse import urlparse
from io import BytesIO,StringIO,TextIOWrapper
import boto3
import numpy as np
import pandas as pd
from datetime import date, timedelta, datetime
import os
import os.path
import pytz

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
            binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri) as f:
                return uri,datetime.today()

class ExtractorCad:
    
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('Contenido ', lm)
        print(f'Uploading {filename} . . .')
        try:
            print('Ingreso al try')

            # Se crean columnas de dataframe
            cols_1 = ['prefijo','fechacompra','lote','tposnet','nrocupon','importe1','ocr','estado','aut','nrorecap','moneda','plan',
            'dni','fechapago','porcarancel','arancel','aliintplanz','intplanz','alictienda','inttienda','alicpl','intplanpl','fechapresentacion',
            'nrocuota','tipomovimiento','tipocd','descripcionmov','identifica_visa','porcentaje_bonificacion', 'importe_bonificacion',
            'numero_debito']

            # Lectura de dataframe
            col_list = ['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30', '31','32']
            df_test = pd.read_csv(file, encoding='utf-8', sep=';', header=None, names=col_list, engine='python')
            
            print('Se leyo el archivo')

            # Separar dataframe de transacciones
            df = df_test[df_test['1'] == 2]
            df = df.iloc[:,:31]
            df.set_axis(cols_1, axis='columns', inplace=True)

            print('Se creo el df de transacciones')

            # Concatenacion de valores del header
            df['constante'] = df_test.iloc[0,0]
            df['comercod'] =  df_test.iloc[0,1]
            df['comercuit'] = df_test.iloc[0,2]
            df['fechaalta'] = df_test.iloc[0,3]
            df['tipoliq'] =   df_test.iloc[0,4]
            df['importe'] =   df_test.iloc[0,5]
            
            print('Se agregaron los campos del header')

            # Adicion de trazabilidad
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True, inplace=True)
            df.index = df.index + 1
            df['skt_extraction_rn'] = df.index.values
            print('Cargo el df')
            
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)
        print('Se retorna df')

class ExtractorCadFooter:
    
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('Contenido ', lm)
        print(f'Uploading {filename} . . .')
        try:
            print('Ingreso al try')

            # Se crean columnas de dataframe
            cols_1 = ['prefijo2','subitem_prefijo','detalle','signo','importe2']

            # Lectura de dataframe
            col_list = ['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30', '31','32']
            df_test = pd.read_csv(file, encoding='utf-8', sep=';', header=None, names=col_list, engine='python')
            
            print('Se leyo el archivo')

            # Separar dataframe de transacciones
            df = df_test[df_test['1'] >= 3]
            df = df.iloc[:,:5]
            df.set_axis(cols_1, axis='columns', inplace=True)

            print('Se creo el df footer')

            # Adicion de trazabilidad
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True, inplace=True)
            df.index = df.index + 1
            df['skt_extraction_rn'] = df.index.values
            print('Cargó el df')
            
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)
        print('Se retorna df')


class ExtractorPag:
    
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        # print(type(file))
        print('Contenido ', lm)
        print(f'Uploading {filename} . . .')
        try:
            print('Ingreso al try')

            # Se crean columnas de dataframe
            cols_1 = ['prefijo','fechacompra','lote','tposnet','nrocupon','importe1','ocr','estado','aut','nrorecap','moneda','plan',
            'dni','fechapago','porcarancel','arancel','aliintplanz','intplanz','alictienda','inttienda','alicpl','intplanpl','fechapresentacion',
            'nrocuota','tipomovimiento','tipocd','descripcionmov','identifica_visa','porcentaje_bonificacion', 'importe_bonificacion',
            'numero_debito']

            # Lectura de dataframe
            col_list = ['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30', '31','32','33','34','35','36']
            df_test = pd.read_csv(file, encoding='latin-1', sep=';', header=None, names=col_list, engine='python', index_col=False)
            
            print('Se leyo el archivo')

            # Separar dataframe de transacciones
            df = df_test[df_test['1'] == 2]
            df = df.iloc[:,:31]
            df.set_axis(cols_1, axis='columns', inplace=True)

            print('Se creo el df de transacciones')

            # Concatenacion de valores del header
            df['constante'] = df_test.iloc[0,0]
            df['comercod'] = df_test.iloc[0,1]
            df['comercuit'] = df_test.iloc[0,2]
            df['fechaliq'] = df_test.iloc[0,3]
            df['tipoliq'] = df_test.iloc[0,4]
            df['numliq'] = df_test.iloc[0,5]
            df['importe'] = df_test.iloc[0,6]
            
            print('Se agregaron los campos del header')

            # Adicion de trazabilidad
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True, inplace=True)
            df.index = df.index + 1
            df['skt_extraction_rn'] = df.index.values
            print('Cargó el df')
            
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)
        print('Se retorna df')


class ExtractorPagFooter:
    
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        # print(type(file))
        print('Contenido ', lm)
        print(f'Uploading {filename} . . .')
        try:
            print('Ingreso al try')

            # Se crean columnas de dataframe
            cols_1 = ['prefijo2','subitem_prefijo','detalle','signo','importe2','porcentaje_impuesto','base_imponible']

            # Lectura de dataframe
            col_list = ['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30', '31','32','33','34','35','36']
            df_test = pd.read_csv(file, encoding='latin-1', sep=';', header=None, names=col_list, engine='python', index_col=False)
            
            print('Se leyo el archivo')

            # Separar dataframe de transacciones
            df = df_test[df_test['1'] >= 3]
            df = df.iloc[:,:7]
            df.set_axis(cols_1, axis='columns', inplace=True)

            print('Se creo el df footer')

            # Adicion de trazabilidad
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True, inplace=True)
            df.index = df.index + 1
            df['skt_extraction_rn'] = df.index.values
            print('Cargó el df')
            
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)
        print('Se retorna df')