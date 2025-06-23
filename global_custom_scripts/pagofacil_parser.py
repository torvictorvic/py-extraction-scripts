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


def read_file(txt_file):
    try:
        try:
            lines = txt_file
            header_l = lines[:2]
            footer_l = lines[-2:]
            header = [head.strip() for head in header_l]
            footer = [foot.strip() for foot in footer_l]
        except Exception as e:
            print(e)
        return header, footer, lines[2:-2]
    except(FileNotFoundError):
        print("No file")

def process_header_1(header):
    n = [1, 8, 25, 9, 35, 20]
    res=[]
    for split in n:
        temp=header[:split]
        header=header[split:]
        res.append(temp) 
    return res

def process_header_2(header):
    n = [1, 8, 6, 35, 48]
    res=[]
    for split in n:
        temp=header[:split]
        header=header[split:]
        res.append(temp) 
    return res
    
def process_footer_1(footer):
    n = [1, 8, 6, 7, 12, 38, 5, 21]
    res=[]
    for split in n:
        temp=footer[:split]
        footer=footer[split:]
        res.append(temp) 
    return res
    
def process_footer_2(footer):
    n = [1, 8, 6, 7, 12, 38, 7, 19]
    res=[]
    for split in n:
        temp=footer[:split]
        footer=footer[split:]
        res.append(temp) 
    return res

def process_information(line):
    n = [1, 5, 2, 8, 8, 21, 1, 10, 6, 8, 4, 4, 20, 1, 60, 37]
    res=[]
    for split in n:
        temp=line[:split]
        line=line[split:]
        res.append(temp) 
    return res


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
            obj = obj['Body'].read().decode('utf-8').splitlines()
            return obj,lm
        else:
            with open(uri) as f:
                return f.read().decode('utf-8').splitlines(),datetime.now()



class FileReaderPagos:

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
            return binary_data,lm
        else:
            with open(uri) as f:
                return uri,datetime.now()

class StringConverter(dict):
    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return str

    def get(self, default=None):
        return str

class ExtractorMP:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        try:
            print('--*--'*10)
            print(f'Uploading {filename} . . .')
            header, footer, lines = read_file(file)
            lines = [line.strip() for line in lines]
            lines_joined = [''.join([x, y]) for x, y in zip(lines[::2], lines[1::2])]
            processed_list = [process_information(line) for line in lines_joined]
            cols = ['record_code_6', 'record_sequence', 'transaction_code',
                'work_date', 'transfer_date', 'account_number', 'payment_code',
                'amount', 'terminal_id', 'payment_date', 'payment_time',
                    'terminal_sequence_number', 'payment_order', 'record_code_7', 
                    'bar_code', 'filler']
            df = pd.DataFrame(processed_list)
            df.columns = cols
            df['record_code_1'] = process_header_1(header[0])[0]
            df['create_date_file_header'] = process_header_1(header[0])[1]
            df['origin_name_header'] = process_header_1(header[0])[2].strip()
            df['client_number_header'] = process_header_1(header[0])[3]
            df['client_name_header'] = process_header_1(header[0])[4].strip()

            df['record_code_5'] = process_header_2(header[1])[0]
            df['create_date_lot_header'] = process_header_2(header[1])[1]
            df['batch_number_lot_header'] = process_header_2(header[1])[2]
            df['description_lot_header'] = process_header_2(header[1])[3].strip()

            df['record_code_8'] = process_footer_1(footer[0])[0]
            df['create_date_lot_footer'] = process_footer_1(footer[0])[1]
            df['batch_number_lot_footer'] = process_footer_1(footer[0])[2]
            df['batch_payment_count_lot_footer'] = process_footer_1(footer[0])[3]
            df['batch_payment_amount_lot_footer'] = process_footer_1(footer[0])[4]
            df['batch_count_lot_footer'] = process_footer_1(footer[0])[6]

            df['record_code_9'] = process_footer_2(footer[1])[0]
            df['create_date_file_footer'] = process_footer_2(footer[1])[1]
            df['total_batches_file_footer'] = process_footer_2(footer[1])[2]
            df['file_payment_count_file_footer'] = process_footer_2(footer[1])[3]
            df['file_payment_amount_file_footer'] = process_footer_2(footer[1])[4]
            df['file_count_file_footer'] = process_footer_2(footer[1])[6]

            df.client_number_header = df.client_number_header.astype('int')
            df.batch_number_lot_header = df.batch_number_lot_header.astype('int')
            df.batch_number_lot_footer = df.batch_number_lot_footer.astype('int')
            df.batch_payment_count_lot_footer = df.batch_payment_count_lot_footer.astype('int')
            df.batch_payment_amount_lot_footer = df.batch_payment_amount_lot_footer.astype('float')
            df.batch_count_lot_footer = df.batch_count_lot_footer.astype('int')
            df.total_batches_file_footer = df.total_batches_file_footer.astype('int')
            df.file_payment_count_file_footer = df.file_payment_count_file_footer.astype('int')
            df.file_payment_amount_file_footer = df.file_payment_amount_file_footer.astype('float')
            df.file_count_file_footer = df.file_count_file_footer.astype('int')
            df.record_sequence = df.record_sequence.astype('int')
            df.transaction_code = df.transaction_code.astype('int')
            df.transaction_code = df.transaction_code.astype('int')
            df.amount = df.amount.astype('float')
            df.terminal_sequence_number = df.terminal_sequence_number.astype('int')
            df.record_code_6 = df.record_code_6.astype('int')
            df.record_code_7 = df.record_code_7
            df.record_code_1 = df.record_code_1.astype('int')
            df.record_code_5 = df.record_code_5.astype('int')
            df.record_code_8 = df.record_code_8.astype('int')
            df.record_code_9 = df.record_code_9.astype('int')
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
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df

        except Exception as e:
            print("Error al subir la fuente: ",e)


class ExtractorML:
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
        try:
            header, footer, lines = read_file(file)
            lines = [line.strip() for line in lines]
            lines_joined = [''.join([x, y]) for x, y in zip(lines[::2], lines[1::2])]
            processed_list = [process_information(line) for line in lines_joined]
            cols = ['record_code_6', 'record_sequence', 'transaction_code',
                'work_date', 'transfer_date', 'account_number', 'payment_code',
                'amount', 'terminal_id', 'payment_date', 'payment_time',
                    'terminal_sequence_number', 'payment_order', 'record_code_7', 
                    'bar_code', 'filler']
            df = pd.DataFrame(processed_list)
            df.columns = cols
            df['record_code_1'] = process_header_1(header[0])[0]
            df['create_date_file_header'] = process_header_1(header[0])[1]
            df['origin_name_header'] = process_header_1(header[0])[2].strip()
            df['client_number_header'] = process_header_1(header[0])[3]
            df['client_name_header'] = process_header_1(header[0])[4].strip()

            df['record_code_5'] = process_header_2(header[1])[0]
            df['create_date_lot_header'] = process_header_2(header[1])[1]
            df['batch_number_lot_header'] = process_header_2(header[1])[2]
            df['description_lot_header'] = process_header_2(header[1])[3].strip()

            df['record_code_8'] = process_footer_1(footer[0])[0]
            df['create_date_lot_footer'] = process_footer_1(footer[0])[1]
            df['batch_number_lot_footer'] = process_footer_1(footer[0])[2]
            df['batch_payment_count_lot_footer'] = process_footer_1(footer[0])[3]
            df['batch_payment_amount_lot_footer'] = process_footer_1(footer[0])[4]
            df['batch_count_lot_footer'] = process_footer_1(footer[0])[6]

            df['record_code_9'] = process_footer_2(footer[1])[0]
            df['create_date_file_footer'] = process_footer_2(footer[1])[1]
            df['total_batches_file_footer'] = process_footer_2(footer[1])[2]
            df['file_payment_count_file_footer'] = process_footer_2(footer[1])[3]
            df['file_payment_amount_file_footer'] = process_footer_2(footer[1])[4]
            df['file_count_file_footer'] = process_footer_2(footer[1])[6]

            df.client_number_header = df.client_number_header.astype('int')
            df.batch_number_lot_header = df.batch_number_lot_header.astype('int')
            df.batch_number_lot_footer = df.batch_number_lot_footer.astype('int')
            df.batch_payment_count_lot_footer = df.batch_payment_count_lot_footer.astype('int')
            df.batch_payment_amount_lot_footer = df.batch_payment_amount_lot_footer.astype('float')
            df.batch_count_lot_footer = df.batch_count_lot_footer.astype('int')
            df.total_batches_file_footer = df.total_batches_file_footer.astype('int')
            df.file_payment_count_file_footer = df.file_payment_count_file_footer.astype('int')
            df.file_payment_amount_file_footer = df.file_payment_amount_file_footer.astype('float')
            df.file_count_file_footer = df.file_count_file_footer.astype('int')
            df.record_sequence = df.record_sequence.astype('int')
            df.transaction_code = df.transaction_code.astype('int')
            df.transaction_code = df.transaction_code.astype('int')
            df.amount = df.amount.astype('float')
            df.terminal_sequence_number = df.terminal_sequence_number.astype('int')
            df.record_code_6 = df.record_code_6.astype('int')
            df.record_code_7 = df.record_code_7.astype('int')
            df.record_code_1 = df.record_code_1.astype('int')
            df.record_code_5 = df.record_code_5.astype('int')
            df.record_code_8 = df.record_code_8.astype('int')
            df.record_code_9 = df.record_code_9.astype('int')
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
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df

        except Exception as e:
            print("Error al subir la fuente: ",e)




class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReaderPagos.read(filename)
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,dtype=str)
            df.columns = ["payment_date","reference_id","amount","transaction_date","external_id","branch_id","terminal_id"]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df

        except pd.io.common.EmptyDataError:
            #Archivo vacio, lógica para agregar.
            columns = ["payment_date","reference_id","amount","transaction_date","external_id","branch_id","terminal_id"]
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



class ExtractorCirculares:
    @staticmethod
    def run(filename, **kwargs):
        file_, lm = FileReaderPagos.read(filename)
        df = pd.read_excel(file_,dtype=str)
        df.columns = ["py_uki","py_ltn","py_slt","pycrcd","pypyin","monto","py_ucm","pyzona","py_aky","pya010","py_bdt","py_tdt","py_ttr","pydct","pydoc"]
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(
            my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df
