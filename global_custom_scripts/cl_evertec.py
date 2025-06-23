import io
import boto3
import pytz
import numpy as np
import pandas as pd
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
            with open(uri,"rb") as f:
                return BytesIO(f.read()),datetime.today()

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
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        
        try:
            columns = ["numero_tarjeta","codigo_transaccion","tipo_transaccion","fee_debito_credito","mti","function_code","reason_code","mcc","codigo_institucion_adquirente","processing_code","codigo_respuesta","codigo_autorizacion","fecha_transaccion","fecha_presentacion_transaccion","moneda_original","importe_total_original","fecha_de_liquidacion","moneda_liquidacion","importe_total_liquidacion","moneda_tasa_intercambio","tasa_intercambio","tasa_intercambio_extendida","moneda_tasa_de_intercambio_adquirente","tasa_de_intercambio_adquirente","moneda_iva_tasa_intercambio","iva_tasa_intercambio","rna","feecollection_cuota_inversa","alcance_de_transaccion","tipo_de_tarjeta","nombre_del_comercio"]
            df = pd.read_csv(file,dtype=object, sep=";", names=columns, skiprows=1, index_col=False, encoding="latin-1")
            if df.empty:
                columns = ["numero_tarjeta","codigo_transaccion","tipo_transaccion","fee_debito_credito","mti","function_code","reason_code","mcc","codigo_institucion_adquirente","processing_code","codigo_respuesta","codigo_autorizacion","fecha_transaccion","fecha_presentacion_transaccion","moneda_original","importe_total_original","fecha_de_liquidacion","moneda_liquidacion","importe_total_liquidacion","moneda_tasa_intercambio","tasa_intercambio","tasa_intercambio_extendida","moneda_tasa_de_intercambio_adquirente","tasa_de_intercambio_adquirente","moneda_iva_tasa_intercambio","iva_tasa_intercambio","rna","feecollection_cuota_inversa","alcance_de_transaccion","tipo_de_tarjeta","nombre_del_comercio"]
                df = pd.DataFrame(columns = columns)
                df = df.append(pd.Series(), ignore_index=True)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)


class ExtractorLiquidacion:
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
        
        try:
            col = ["numero_tarjeta","codigo_tipo_transaccion","tipo_transaccion","mti","processing_code","codigo_respuesta","codigo_autorizacion","codigo_modo_entrada","modo_entrada","plan_venta","mcc","marca","codigo_producto","fecha_transaccion","moneda_original","importe_total_original","moneda_de_liquidacion","importe_total_liquidacion","importe_original_cashback","importe_liquidacion_cashback","importe_original_propina","importe_liquidacion_propina","codigo_pais","codigo_institucion_adquirente","terminal","codigo_motivo_ajuste","motivo_ajuste","codigo_motivo_honra","motivo_honra","moneda_ajuste","importe_ajuste","transaction_identifier","acquirer_reference_number","comercio"]
            a=[]
            df = pd.read_csv(file,dtype=object, sep=",",on_bad_lines=lambda li: a.append(li[:34]),engine='python',header=None)
            if len(df.columns)<34:
                df['comercio']=''
                df.columns=col
            else:
                df=df.iloc[:,:34] 
                df.columns=col
                dfnew=pd.DataFrame(a, columns =col)
                df = pd.concat([df, dfnew], ignore_index=True)
            if df.empty:
                col = ["numero_tarjeta","codigo_tipo_transaccion","tipo_transaccion","mti","processing_code","codigo_respuesta","codigo_autorizacion","codigo_modo_entrada","modo_entrada","plan_venta","mcc","marca","codigo_producto","fecha_transaccion","moneda_original","importe_total_original","moneda_de_liquidacion","importe_total_liquidacion","importe_original_cashback","importe_liquidacion_cashback","importe_original_propina","importe_liquidacion_propina","codigo_pais","codigo_institucion_adquirente","terminal","codigo_motivo_ajuste","motivo_ajuste","codigo_motivo_honra","motivo_honra","moneda_ajuste","importe_ajuste","transaction_identifier","acquirer_reference_number","comercio"]
                df = pd.DataFrame(columns = columns)
                df = df.append(pd.Series(), ignore_index=True)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)

            
class ExtractorControlBancos:
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
        
        try:
            columns = ["mov_type_id","mov_detail","mov_financial_entity_id","mov_label","mov_amount","mov_created_dt"]
            df = pd.read_csv(file,dtype=object, names=columns, skiprows=1,index_col=False)
            if df.empty:
                columns = ["mov_type_id","mov_detail","mov_financial_entity_id","mov_label","mov_amount","mov_created_dt"]
                df = pd.DataFrame(columns = columns)
                df = df.append(pd.Series(), ignore_index=True)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)