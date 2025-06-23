import pytz
import boto3
import pandas as pd
import re
import numpy as np

from urllib.parse import urlparse
from io import BytesIO
from datetime import datetime

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
            binary_data = obj['Body'].read().decode("utf-8")
            #binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri, "rb") as f:
                return f.read().decode("utf-8"),datetime.now()
                

class MyError(Exception):
    def __init__(self, message):
        self.message = message
        


class Extractor():
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        file = file.splitlines()
        print(f'Uploading {filename} . . .')
        fecha_v, entrada_v, atm_v, suma, international_v = None, None, None, None, None
        for i,j in enumerate(file):
            if "REPORTING FOR:" in j and "9000529707 TOTAL PAGO BR" in j \
            and "BRAZIL CIP NNSS SERVICE" in j:
                fecha_v = j.split()[-2]
                fecha_v = datetime.strptime(fecha_v, '%d%b%y').strftime('%Y-%m-%d')
                linea = i
                for i,j in enumerate(file):
                    if i >= linea and i <= 85 and 'NET SETTLEMENT AMOUNT' in j:
                        entrada_v = j.split()[-2]
                        break
            if "REPORTING FOR:" in j and "9000529706 TOTAL PAGO BR" in j \
            and "BRAZIL CASH DISB NATL NET SERVICE" in j:
                linea = i
                for i,j in enumerate(file):
                    if i >= linea and i <= 61 and 'NET SETTLEMENT AMOUNT' in j:
                        atm_v = j.split()[-2]
                        international_v = False
                        break
            if "REPORTING FOR:" and "9000529705 TOTAL PAGO BR" in j \
            and "INTERNATIONAL SETTLEMENT SERVICE" in j:
                fecha_v = j.split()[-2]
                fecha_v = datetime.strptime(fecha_v, '%d%b%y').strftime('%Y-%m-%d')
                linea = i
                for i,j in enumerate(file):
                    if i >= linea and i <= 85 and 'NET SETTLEMENT AMOUNT' in j:
                        entrada_v = j.split()[-2]
                        international_v = True
                        suma =  entrada_v  
                        break
                     
                        

        if entrada_v and atm_v is not None:
            valor1 = entrada_v.replace(',', '').replace('DB', '')
            valor2 = atm_v.replace(',', '').replace('DB', '')
            suma = float(valor1)+float(valor2)
            suma = '{:,.2f}'.format(suma)

        # if entrada_v is None:
        #     raise MyError("El archivo no contiene cifra para NET SETTLEMENT AMOUNT")
            
        
                
                
        df = pd.DataFrame({'Fecha': [fecha_v], 'Entradas_VSS_110': [entrada_v]\
                          ,'ATM_VSS_110':[atm_v], 'Suma_VSS_110':[suma], 'International':[international_v]})
        df = df.replace({None: 'null'})
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        upload_date = lm.astimezone(new_timezone)
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df

        
        
        