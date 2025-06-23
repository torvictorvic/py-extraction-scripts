import os
import pytz
import boto3
import pandas as pd
from datetime import datetime
from urllib.parse import urlparse
from io import BytesIO,StringIO,TextIOWrapper
import re

class Extractor:

    def run(self, filename, **kwargs):
        file= self.file.body
        lm= self.file.last_modified
        filename=self.file.key
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')

        columns = ['idpessoa','razaosocial','nomefantasia','cnpj','idoperador','login','nomeoperador'
                   ,'dataoperacao','valoroperacao','desctipocredito','numerocarteira','codigoexterno'
                   ,'nomeusuario','descestadotransacao','identificadorrecarga']

        try:
            df = pd.read_csv(BytesIO(file), encoding="ISO-8859-1",dtype=str,skiprows=1)
            df.columns=[re.sub(r'\W+', '', col.lower()) for col in df.columns]
            df.columns = columns
            df = df[df['razaosocial'].notna()]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            df['file_name'] = filename.split('/')[-1]
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            print("Parseo exitoso")
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)