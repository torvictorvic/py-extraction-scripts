import boto3
import numpy
import pandas as pd
import pytz
from io import StringIO, BytesIO
from datetime import date, timedelta, datetime
from urllib.parse import urlparse

class Extractor:
   
    def run(self,filename):
        file,lm = self.file.body, self.file.last_modified
        filename = self.file.key
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        registro = StringIO(file.decode())
        df = pd.read_csv(registro,dtype=str,skiprows=1,skipfooter=1,sep=",",header=None)
        if df.empty:
            columns = ["record_id","merchant_id","merchant_name","store_id","terminal_id","clerk_id","card_issuer_id","card_issuer_processor_id","acquirer_id","acquired_transaction_date","acquired_transaction_time","gift_card_number","product_id","pos_transaction_date","pos_transaction_time","transaction_type","system_trace_audit_number","product_item_price","currency_code","merchant_transaction_id","bhn_transaction_id","auth_response_code","approval_code","reserval_saf_type_code","transaction_amount","consumer_fee_amount","commission_amount","total_tax_on_tx_amount","total_tax_on_commission_amount","total_tax_on_fees_amount"]
            df = pd.DataFrame(columns=columns)
            df = df.append(pd.Series(),ignore_index=True)
        else:
            columns = ["record_id","merchant_id","merchant_name","store_id","terminal_id","clerk_id","card_issuer_id","card_issuer_processor_id","acquirer_id","acquired_transaction_date","acquired_transaction_time","gift_card_number","product_id","pos_transaction_date","pos_transaction_time","transaction_type","system_trace_audit_number","product_item_price","currency_code","merchant_transaction_id","bhn_transaction_id","auth_response_code","approval_code","reserval_saf_type_code","transaction_amount","consumer_fee_amount","commission_amount","total_tax_on_tx_amount","total_tax_on_commission_amount","total_tax_on_fees_amount"]
            df.columns = columns
        df = df.reset_index(drop=True)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df


