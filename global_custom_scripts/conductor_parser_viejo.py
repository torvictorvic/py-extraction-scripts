from typing import List
from urllib.parse import urlparse
from io import BytesIO,StringIO
import pdb
import boto3 as boto3
import pandas as pd
import pandas as pd
import glob
from datetime import datetime
import pytz
import pandas as pd
import json
from pandas.io.json import json_normalize  


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
            # session = boto3.Session(profile_name="sts")
            # s3 = session.client('s3')
                #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)['Body'].read().decode()
            binary_data = obj
            return binary_data,lm

        else:
            with open(uri) as f:
                return f.read(),datetime.today()

class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)

        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        data = json.loads(file) 
        jsons = data['CONTENT']
        df = json_normalize(data, 'CONTENT', [], 
                            record_prefix='content_')
        df_original=pd.read_json(file)
        df_original = df_original.iloc[:,:-1]
        df_final = pd.concat([df_original, df], axis=1)
        columns = list(map(lambda x: str(x).lower().replace('.','_'),df_final.columns))
        df_final.columns = columns
        formato = ['file_id','issuer_id','id_subemissor','brand','filename_base2','filename','sequence','reference_date','records_total','credit_total','credit_amnt','debit_total','debit_amnt','expired_total','content_id','content_record_code','content_transaction_id','content_transaction_external_id','content_transaction_version','content_transaction_pan','content_transaction_card_id','content_transaction_authorization','content_transaction_local_date','content_transaction_gmt_date','content_transaction_installment_nbr','content_transaction_mcc','content_transaction_source_currency','content_transaction_source_value','content_transaction_dest_currency','content_transaction_dest_value','content_transaction_purchase_value','content_transaction_installment_value_1','content_transaction_installment_value_n','content_transaction_boarding_fee','content_transaction_merchant','content_transaction_entry_mode','content_transaction_authorization_date','content_transaction_status','content_transaction_operation_type','content_transaction_product_code','content_clearing_version','content_clearing_installment','content_clearing_currency','content_clearing_value','content_clearing_boarding_fee','content_clearing_commission','content_clearing_settlement_date','content_clearing_presentation','content_clearing_action_code','content_clearing_cancel','content_clearing_confirm','content_clearing_add','content_clearing_credit','content_clearing_debit','content_clearing_error_code']
        df_final = df_final[formato]
        df_final['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df_final['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df_final['file_name'] = out
        df_final.reset_index(drop=True)
        df_final['skt_extraction_rn'] = df_final.index.values
        df_final['raw_data'] = jsons
        df_final['raw_data']= df_final['raw_data'].astype(str)
        return df_final