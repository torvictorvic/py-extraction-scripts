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

            #session = boto3.Session(profile_name="sts")
            #s3 = session.client('s3')

            session = boto3.session.Session()
            s3 = session.client('s3')

            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
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
        file_,lm = FileReader.read(filename)
        formato_header = ["tipo_columna2", "tipo_moneda2", "monto_total_depositado", "comisiones_pagadas",
                            "comisiones_cobradas", "numero_transacciones", "referencia", "fecha_deposito",
                            "numero_cuenta",
                            "nombre_banco", "fecha_efectiva", "numero_archivo_conciliacion", "impuestos_pagados",
                            "impuestos_cobrados", "filler1"]
        # Body format
        formato_adquirencia = ["tipo_columna", "id_comercio", "id_terminal", "tarjeta", "monto_depositado",
                                "comisiones",
                                "fecha_trx", "importe_bruto", "tipo_moneda", "tipo_trx", "impuestos",
                                "impuestos_pagos_diferidos", "iva_pagado_cashback", "iva_cobrado_cashback",
                                "comisiones_pagadas_cashback", "comisiones_cobradas_cashback",
                                "comisiones_pagos_diferidos",
                                "monto_cashback", "numero_pagos", "diferimiento_inicial", "tipo_plan",
                                "cod_autorizacion",
                                "eci", "referencia_interbancaria", "referencia_payworks", "customer_ref1",
                                "customer_ref2",
                                "customer_ref3", "num_control", "tipo_tarjeta", "fuente_transaccion",
                                "ref_lote_prosa",
                                "comision_adquirente", "iva_adquirente", "indicador_dcc",
                                "comision_tarifa_adquirente", "iva_tarifa_adiquirente", "filler"]
        df = pd.read_csv(file_, header = None, sep = ',', dtype = object, names = [i for i in range(38)])
        if df.empty:
            columnas = [formato_adquirencia,formato_header]
            cols = []
            for column in columnas:
                for col in column:
                    cols.append(col)
            columns = cols
            df = pd.DataFrame(columns = columns)
            df = df.append(pd.Series(), ignore_index=True)
        else:
            df_header = pd.DataFrame(df.iloc[:1, :15])
            # Create a dataframe from body without header
            df = df.iloc[1:, :]
            df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            df.columns = formato_adquirencia
            dtype_body = {"tipo_columna": object, "id_comercio": object, "id_terminal": object,
                            "tarjeta": object, "monto_depositado": float,
                            "comisiones": float,
                            "fecha_trx": object, "importe_bruto": float, "tipo_moneda": object,
                            "tipo_trx": object, "impuestos": float,
                            "impuestos_pagos_diferidos": float, "iva_pagado_cashback": float,
                            "iva_cobrado_cashback": float,
                            "comisiones_pagadas_cashback": float, "comisiones_cobradas_cashback": float,
                            "comisiones_pagos_diferidos": float,
                            "monto_cashback": float, "numero_pagos": float, "diferimiento_inicial": float,
                            "tipo_plan": float,
                            "cod_autorizacion": object,
                            "eci": object, "referencia_interbancaria": object,
                            "referencia_payworks": object, "customer_ref1": object,
                            "customer_ref2": object,
                            "customer_ref3": object, "num_control": object, "tipo_tarjeta": object,
                            "fuente_transaccion": object,
                            "ref_lote_prosa": object,
                            "comision_adquirente": float, "iva_adquirente": float, "indicador_dcc": object,
                            "comision_tarifa_adquirente": float, "iva_tarifa_adiquirente": float, "filler": object}
            df = df.astype(dtype_body)

            # df_header = pd.read_csv(data, nrows=1, header=None, sep=',', dtype=object)
            df_header.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            df_header.columns = formato_header
            dtype_header = {"tipo_columna2": object, "tipo_moneda2": object, "monto_total_depositado": float,
                            "comisiones_pagadas": float,
                            "comisiones_cobradas": float, "numero_transacciones": float, "referencia": object,
                            "fecha_deposito": object,
                            "numero_cuenta": object,
                            "nombre_banco": object, "fecha_efectiva": object, "numero_archivo_conciliacion": object,
                            "impuestos_pagados": float,
                            "impuestos_cobrados": float, "filler1": object}
            df_header = df_header.astype(dtype_header)
            # Make a column in body for every entry in header
            for column in df_header.columns:
                df[column] = df_header.loc[0, column]

            numeric_cols = ["monto_total_depositado", "comisiones_pagadas", "comisiones_cobradas",
                            "impuestos_pagados",
                            "impuestos_cobrados", "monto_depositado", "comisiones", "importe_bruto", "impuestos",
                            "impuestos_pagos_diferidos", "iva_pagado_cashback", "iva_cobrado_cashback",
                            "comisiones_pagadas_cashback", "comisiones_cobradas_cashback",
                            "comisiones_pagos_diferidos",
                            "monto_cashback", "comision_adquirente", "iva_adquirente", "comision_tarifa_adquirente",
                            "iva_tarifa_adiquirente"]
            # Nmeric columns come without deicmals
            for col in numeric_cols:
                df[col] = df[col] / 100
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df

