import pytz
import boto3
import pandas as pd
import logging
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse
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

        columns=['reason_code','ica','reversal_indicator','mti','function_code','message_reason_code','acq_country'
                 ,'tx_amount_tc','currency_code_tx','tx_amount_rc','currency_code_rc','reconciliation_date',
                 'settlement_date','value_date','is_vat','tax_amount','international']

        try:
            df = pd.read_csv(BytesIO(file), dtype=str)
            df.columns=[re.sub(r'\W+', '', col.lower()) for col in df.columns]
            df.columns = columns
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            df['file_name'] = filename.split('/')[-1]
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            print("Parseo exitoso")
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)