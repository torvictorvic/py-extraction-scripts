from typing import List
from urllib.parse import urlparse
from io import BytesIO,StringIO
import logging
import pdb
import boto3 as boto3
from botocore.config import Config
import pandas as pd
import glob
from datetime import datetime
import pytz
import json


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

        data = json.loads(file.decode()) 
        jsons = data['CONTENT']
        df = pd.io.json.json_normalize(data, 'CONTENT', [], record_prefix='content_')

        try:
            df_original=pd.read_json(file.decode())
        except:
            df_original=pd.read_json(StringIO(file.decode()))
        formato = ['file_id','issuer_id','id_subemissor','brand','filename_base2','filename'
            ,'sequence','file_number','total_files','reference_date','records_total','records_amnt'
            ,'credit_total','credit_amnt','debit_total','debit_amnt','unknown_total','unknown_amnt'
            ,'rejected_total','rejected_amnt','occurrence_total','occurrence_amnt','expired_total'
            ,'content_id','content_record_code','content_transaction_card_number_id'
            ,'content_transaction_arn','content_transaction_external_id','content_transaction_version'
            ,'content_transaction_pan','content_transaction_bin_card','content_transaction_card_id'
            ,'content_transaction_authorization','content_transaction_local_date'
            ,'content_transaction_gmt_date','content_transaction_installment_nbr','content_transaction_mcc'
            ,'content_transaction_source_currency','content_transaction_source_value'
            ,'content_transaction_dest_currency','content_transaction_dest_value'
            ,'content_transaction_purchase_value','content_transaction_installment_value_1'
            ,'content_transaction_installment_value_n','content_transaction_boarding_fee'
            ,'content_transaction_merchant','content_transaction_entry_mode'
            ,'content_transaction_authorization_date','content_transaction_status'
            ,'content_transaction_transaction_qualifier','content_transaction_operation_type'
            ,'content_transaction_issuer_exchange_rate','content_transaction_cdt_amount'
            ,'content_transaction_product_code','content_transaction_reason_code','content_transaction_uuid'
            ,'content_transaction_operation_code','content_transaction_agency'
            ,'content_transaction_account_number','content_transaction_late_presentation'
            ,'content_transaction_error_code','content_transaction_received_change'
            ,'content_clearing_version','content_clearing_installment','content_clearing_currency'
            ,'content_clearing_value','content_clearing_value_date'
            ,'content_clearing_boarding_fee','content_clearing_commission'
            ,'content_clearing_interchange_fee_sign','content_clearing_settlement_date'
            ,'content_clearing_is_international'
            ,'content_clearing_presentation','content_clearing_action_code'
            ,'content_clearing_total_partial_transaction','content_clearing_flag_partial_settlement'
            ,'content_clearing_cancel','content_clearing_confirm','content_clearing_add'
            ,'content_clearing_credit','content_clearing_debit']

        try:
            df_original=df_original.drop(['CONTENT'],axis=1)
            df_final = pd.concat([df_original, df], axis=1)
            columns = list(map(lambda x: str(x).lower().replace('.','_'),df_final.columns))
            df_final.columns = columns
            df_final = df_final[formato]
            df_final = df_final.rename(columns={'content_transaction_arn': 'content_transaction_id'})
            df_final['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df_final['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df_final['file_name'] = out
            df_final.reset_index(drop=True)
            df_final['skt_extraction_rn'] = df_final.index.values
            df_final['raw_data'] = jsons
            df_final['raw_data']= df_final['raw_data'].astype(str)
            return df_final
        except Exception as e:
            print("Error al subir la fuente: ",e)
