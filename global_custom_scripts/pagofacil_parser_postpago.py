import boto3
import numpy
import pandas as pd
import io
from io import StringIO, BytesIO
import zipfile
import glob
import os
import os.path
from datetime import date, timedelta, datetime
import sys
import pytz
import time
import pandas as pd
from pandas import DataFrame
from enum import Enum
import math
from urllib.parse import urlparse
from zipfile import ZipFile
import csv

def select_columns(key):
    positions = None
    if key == '01':
        positions = [(0, 2), (2, 12), (12, 20), (20, 60), (60, 132)]
        cols = ["record_type","process_date_1","agent_number_1","agent_name_1","filler"]
    elif key == '05':
        positions = [(0, 2), (2, 8), (8, 38), (38, 132)]
        cols = ["record_type","bacth_number_5","group_id_5","filler"]
    elif key == '09':
        positions = [(0, 2), (2, 8), (8, 14), (14, 22), (22, 23), (23, 33), (33, 41), (41, 45), (45, 49), (49, 52), (52, 73), (73, 88), (88, 96), (96, 102), (102, 113), (113, 123), (123, 132)]
        cols = ["record_type","terminal_id","cashier","box","tx_type","tx_date","tx_time","lot_number","seq_number","currency_code","account_number","payment_amount","electronic_sign","pay_code_id","unique_tx_id","fecha_batch","filler"]
    elif key == '10':
        positions = [(0, 2), (2, 82), (82, 90), (90, 130), (130, 132)]
        cols =["record_type","tx_bar_code_10","utility_number_10","utility_name_10","filler"]
    elif key == '11':
        positions = [(0, 2), (2, 5), (5, 8), (8, 9), (9, 24), (24, 39), (39, 74), (74, 95), (95, 132)]
        cols=["record_type","secondary_sequence_11","payment_currency_11","payment_instrument_11","payment_amount_11","exchanger_rate_11","payment_barcode_11","payment_id_11","filler"]
    elif key == '15':
        positions = [(0, 2), (2, 8), (8, 23), (23, 38), (38, 132)]
        cols = ["record_type","bacth_number_15","total_payments_15","total_amount_15","filler"]
    elif key == '19':
        positions = [(0, 2), (2, 8), (8, 23), (23, 38), (38, 132)]
        cols = ["record_type","total_batch_19","total_payments_19","total_amount19","filler"]
    return positions,cols


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
            #session = boto3.session.Session()
            session = boto3.session.Session(region_name='us-east-1')
            s3 = session.client('s3')
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read().decode('latin1')

            return obj,lm
        else:
            with open(uri,'rb') as f:
                return f.read().decode('latin1'),datetime.today()

class ExtractorPospago:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')

        valid_rows = [i for i in file.splitlines() if i[0:2] in ["09","10","11"]]
        alarm = len(valid_rows)%3
        if alarm == 0:
            alarm = "0"
        else:
            alarm = "1"


        try:
            lineas = {}
            for linea in file.splitlines():
                if linea[0:2] not in lineas:
                    lineas[linea[0:2]] =[]
                    lineas[linea[0:2]].append(linea)
                else:
                    lineas[linea[0:2]].append(linea)
            dframes ={}
            for key in lineas.keys():
                col_specification,cols = select_columns(key)
                df = pd.read_fwf(StringIO(file), colspecs=col_specification, header=None, dtype=object)
                df.columns = cols
                df = df[df["record_type"]==key]
                dframes[key] = df
            df_final = pd.DataFrame()
            i = 0
            for key in dframes.keys():
                if key in ['09','10','11']:
                    if i ==0:
                        df_final = dframes[key].reset_index()
                    else:
                        df_final = df_final.merge(dframes[key].reset_index(), left_index=True, right_index=True)
                    i = i+1
                else:
                    for column in dframes[key].columns:
                        df_final[column] = dframes[key].reset_index().loc[0,column]
            columns = list(filter(lambda x: "record_type" not in str(x) ,df_final.columns))
            columns = list(filter(lambda x: "index" not in str(x) ,columns))
            columns = list(filter(lambda x: "filler" not in str(x) ,columns))
            df_final = df_final[columns]
            df = df_final
            df["alarm"] = alarm
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            
            return df

        except ValueError:
            #Archivo vacio, lógica para agregar.
            columns = ['record_code_6','record_sequence','transaction_code','work_date','transfer_date','account_number','payment_code','amount','terminal_id','payment_date','payment_time','terminal_sequence_number','payment_order','record_code_7','bar_code','filler','record_code_1','create_date_file_header','origin_name_header','client_number_header','client_name_header','record_code_5','create_date_lot_header','batch_number_lot_header','description_lot_header','record_code_8','create_date_lot_footer','batch_number_lot_footer','batch_payment_count_lot_footer','batch_payment_amount_lot_footer','batch_count_lot_footer','record_code_9','create_date_file_footer','total_batches_file_footer','file_payment_count_file_footer','file_payment_amount_file_footer','file_count_file_footer']
            df = pd.DataFrame(columns = columns)
            df = df.append(pd.Series(), ignore_index=True)
            df["alarm"] = ""
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            
            return df

        except Exception as e:
            print("Error al subir la fuente: ",e)
