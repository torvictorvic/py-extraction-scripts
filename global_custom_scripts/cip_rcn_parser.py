import os
import pandas as pd
import re
import boto3
import traceback
from datetime import date, datetime
import pytz
from urllib.parse import urlparse
from io import BytesIO, StringIO


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
            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data, lm
        else:
            with open(uri) as f:
                return uri, datetime.today()

class RcnParser:
    def __init__(self, io):
        self.io = io
    def run(self):
        df = pd.read_csv(self.io, header=None, dtype=str, sep='#')


        a, b = df.shape
        for x in range(0, a):
            for y in range(b):
                if 'Pag' in str(df.iloc[x, y]):
                    Pag_remessa = str(df.iloc[x, y]).split(' ')[-1]
                elif 'Nossa Remessa de' in str(df.iloc[x, y]):
                    nossa_remessa = str(df.iloc[x, y]).split(' ')[-1]
                elif 'Resultado para Liquidacao no SILOC' in str(df.iloc[x, y]):
                    resultado = str(df.iloc[x, y]).split(' ')[-1]
                elif 'TOTAL GERAL VALORES' in str(df.iloc[x, y]):
                    total_general = str(df.iloc[x, y]).split(' ')[-1]
                elif 'Total 015' in str(df.iloc[x, y]):
                    Total_015 = str(df.iloc[x, y]).split(' ')[-1]
                elif 'Total 040' in str(df.iloc[x, y]):
                    Total_040 = str(df.iloc[x, y]).split(' ')[-1]
                elif 'Total 093' in str(df.iloc[x, y]):
                    Total_093 = str(df.iloc[x, y]).split(' ')[-1]
                elif 'Total 041' in str(df.iloc[x, y]):
                    Total_041 = str(df.iloc[x, y]).split(' ')[-1]
                elif 'Total 058' in str(df.iloc[x, y]):
                    Total_058 = str(df.iloc[x, y]).split(' ')[-1]



        a,b = df.shape
        b = []
        c = []
        for i in range(a):
            if 'NOSSA REMESSA' in df.iloc[i,0]:
                str(df.iloc[i+1,0]).split('*')
                b.append(i+1)

                for j in range(a-i):
                    if '-------------' in df.iloc[i+j,0]:
                        str(df.iloc[i:j+i,0]).split('*')
                        c.append(j+i)
                        if j > 2:
                            break
        nossa = []
        for i in range(len(b)):
            nossa.append(df.iloc[b[i]+2:c[i+1],0])

        lista_1 = []


        for i in nossa[0]:
            a = list(i)
            a = i.split()
            if len(a) == 9:      
                k = a[2] + ' ' + a[3] 
                a[2] = k
                a.pop(3)
                lista_1.append(a)
            elif len(a) > 9:    
                k = a[2] + ' ' + a[3] + ' ' + a[4]
                a[2] = k
                a.pop(3)
                a.pop(3)
                lista_1.append(a)


        df = pd.DataFrame(lista_1)
        df = df.reset_index(drop=True)
        df['Pag'] = Pag_remessa
        df['Nossa_Remessa'] = nossa_remessa
        df['resultado'] = resultado
        df['total_general'] = total_general
        df['total_015'] = Total_015
        df['total_040'] = Total_040
        #df['total_093'] = Total_093
        #df['total_041'] = Total_041
        #df['total_058'] = Total_058

        return df


class Extractor:
    @ staticmethod
    def run(filename, **kwargs):
        file_, lm = FileReader.read(filename)
        df = RcnParser(file_).run()
        df.columns = ['bco', 'ispbp', 'codigo_operacao','nossa_quantidade','nossa_valor','sua_quantidade','sua_valor','total','Pag','nossa_remessa','resultado_liquidacion','total_general','total_015','total_040']
        #df = pd.DataFrame(df.columns)
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
        df['skt_extraction_rn'] = range(0, len(df))
        return df