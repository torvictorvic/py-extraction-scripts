import pytz
import boto3
import pandas as pd

from io import BytesIO
from urllib.parse import urlparse
from datetime import date, timedelta, datetime

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
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri) as f:
                return uri,datetime.today()

class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        file,lm = FileReader.read(filename)
        df = pd.read_excel(file, sheet_name = 1, dtype=str)
        df = df.drop([0], axis = 0)
        filas_nan = df.isnull().sum(axis = 1)
        df[filas_nan>38].index
        df = df.drop(df[filas_nan>38].index)
        df.reset_index(drop=True, inplace=True)
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone  
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        columns = ["payment_date","created","merchant_id","merchant_name","created_TS","transaction_type","transaction_status","ticket_number","sale_ticket_number","card_type","card_brand", "bin_card","last_four_digits","approval_code","country","processor_type","payment_method","currency_code","approved_transaction_amount","subtotal_iva","subtotal_iva0","iva_value","variable_fee","variable_percentage","static_amount","kushki_commission","iva_kushki_commission","fraud_retention","kushki_amount","ret_fue","ret_iva","partial_disperse_amount","credit_type","number_of_months","grace_months","release","accounts_payable","adjustment","disperse_amount","metadata"]
        df.columns = columns
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df