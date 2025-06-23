import pytz
import boto3
import pandas as pd

from enum import Enum
from pandas import DataFrame
from datetime import datetime
from io import StringIO, BytesIO
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
            binary_data = BytesIO(obj)
            return binary_data,lm
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
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            positions = [0,44,46,49,50,56,60,67,70,78,84,96,103,113,131,139,147,150,160]
            col_specification =[]
            for i in range(0,len(positions) -1):
                cordenate = (positions[i],positions[i+1] )
                col_specification.append(cordenate)
            binary_df = StringIO(file.read().decode().strip().strip('\x00')) 
            cols = ["codigo_de_barras", "filler_1", "filler_2", "tipo_de_captura", "filler_3", "agencia_remetente", "n_do_lote", "sequencial_do_lote", "data_do_movimento", "centro_processador", "valor_liquido_do_titulo", "versao_do_arquivo", "sequencial_do_arquivo_de_troca", "filler_4", "ispb_if_recebedor", "ispb_if_favorecida", "tipo_de_documento","sequencial_de_arquivo"]   
            df = pd.read_fwf(binary_df, colspecs=col_specification, dtype=object)
            df.columns = cols
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
            df.drop(df.index[len(df)-1])
            return df

        
        except pd.io.common.EmptyDataError:
            columns = ["codigo_de_barras", "filler_1", "filler_2", "tipo_de_captura", "Motivo_de_devolucao", "filler_3", "agencia_remetente", "n_do_lote", "sequencial_do_lote", "data_do_movimento", "centro_processador", "valor_liquido_do_titulo", "versao_do_arquivo", "sequencial_do_arquivo_de_troca", "filler_4", "ispb_if_recebedora", "ispb_if_favorecida", "tipo_de_documento","sequencial_de_arquivo"]
            df = pd.DataFrame(columns = columns)
    
            df = df.append(pd.Series(), ignore_index=True)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
            df.drop(df.index[len(df)-1])
            return df

        except Exception as e:
            print("Error al subir la fuente: ",e)


            
