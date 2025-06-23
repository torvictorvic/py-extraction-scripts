import pytz
import boto3
import pandas as pd
import logging
from enum import Enum
from datetime import datetime
from io import StringIO, BytesIO
from urllib.parse import urlparse


class Extractor:

    def run(self, filename, **kwargs):
        
        file=self.file.body
        lm= self.file.last_modified
        filename=self.file.key
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')

        cols = ["mercadopago","id_item","transaction_id_conectcar","transaction_id","operation_type","value","plate","status","reason","transaction_date"]

        try:    
            df = pd.read_csv(BytesIO(file),dtype=str,sep="|",encoding='latin-1')
            out = (filename.split('/')[-1])
 
            df.columns = cols
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)