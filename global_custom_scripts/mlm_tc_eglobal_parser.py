import boto3
import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime
import os
import os.path
import sys
import pytz
import pandas as pd
import re
from urllib.parse import urlparse


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
            binary_data = obj
            return binary_data,lm
        else:
            with open(uri,'rb') as f:
                return f.read(),datetime.today()

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
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        file = file.decode().lower()
        try:
            if len(re.findall("\d\d\d\d-\d\d-\d\d",file))>=1:
                fecha = re.findall("\d\d\d\d-\d\d-\d\d",file)[0]
            else:
                print("else en función")
                fecha = re.findall("\d\d\s.+",file.splitlines()[0])[0]
                # setting up a dictionary
                d = {"enero":"01","febrero":"02","marzo":"03","abril":"04","mayo":"05","junio":"06","julio":"07","agosto":"08","septiembre":"09","octubre":"10","noviembre":"11","diciembre":"12"}
                # taking advantage of join method in orde to insert alway in the middle of the splitted word the correspondet value of the dictionary
                for key,value in d.items():
                    y = fecha.split(key)
                    fecha = value.join(y)
                fecha = "-".join(fecha.split(" ")[::-1])
        except:
            print("El archivo no contiene una fecha valida para ser leida")
        try:
            valor_list = re.findall(".+\.\d+",file)[0].split('$')
        except:
            print("El archivo no contiene una cifra válida para leer, esta debe ser una cifra con decimales separados por punto(.)")

        df =  pd.DataFrame([[fecha,valor_list[-1]]], columns=["fecha","valor"])

        linea_comp = re.findall(r"\btotal.+\b",file)[0]
        if "eglobal" in str(linea_comp):
                valor= re.sub(",", "",valor_list[-1])
                df["valor"]=str(float(valor)*-1)

        if "ATM" in filename.split('/')[-1]:
            df["tipo"] = "ATM"
        elif "POS" in filename.split('/')[-1]:
            df["tipo"] = "POS"
        else:
            df["tipo"] = ""

        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df
        

        