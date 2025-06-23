import pytz
import boto3
import pandas as pd

from io import BytesIO
from datetime import datetime
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

            session = boto3.session.Session()
            s3 = session.client('s3')

            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            print('Leído')
            return binary_data,lm
        else:
            with open(uri,"rb") as f:
                return BytesIO(f.read()),datetime.today()

class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_csv(file,dtype=str)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        col = ["site_skt_id","pay_site","site_procesadora","pay_payment_id","site_gtwt_transaction_id","gtwt_transaction_id","mov_id","operation","gtwr_refund_type","site_merchant","pay_flag_carrito","mov_creation_date","gtwr_creation_date","fecha_esperada_pago","fecha_esperada_conci","mov_amount","gtwt_card_number","gtwt_installments","gtwc_authorization_code","gtwo_authorization_code","gtwr_authorization_code","gtwa_authorization_code","num_installment","mov_amount_install","gtwr_refund_id","site_bandera","marca_comprada","contraparte","fecha_de_presentacion","fecha_de_pago","numero_de_lote","tasa_base","spread","tasa_efectiva","monto_intereses","conceptos_impositivos","monto_a_liquidar"]
        df.columns = col
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df

class ExtractorArg:
    @staticmethod
    def run(filename, **kwargs):
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        del my_timestamp
        del old_timezone
        file,lm = FileReader.read(filename)
        upload_date = lm.astimezone(new_timezone)
        del lm
        del new_timezone
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_csv(file,dtype=str)
        file = None
        # returns datetime in the new timezone
        col = ["site_skt_id","pay_site","site_procesadora","pay_payment_id","site_gtwt_transaction_id","gtwt_transaction_id","mov_id","operation","gtwr_refund_type","site_merchant","pay_flag_carrito","mov_creation_date","gtwr_creation_date","fecha_esperada_pago","fecha_esperada_conci","mov_amount","entidad_emisora","gtwt_card_number","gtwt_installments","gtwc_authorization_code","gtwo_authorization_code","gtwr_authorization_code","gtwa_authorization_code","num_installment","mov_amount_install","gtwr_refund_id","site_bandera","marca_comprada","contraparte","fecha_de_presentacion","fecha_de_pago","numero_de_lote","tasa_base","spread","tasa_efectiva","monto_intereses","iva","sellos","isib_percepcion_caba","monto_a_liquidar"]
        df.columns = col
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df

class ExtractorVenta:
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
        df = pd.read_csv(file,dtype=str)
        df.columns = ['site_skt_id_original','site_skt_id','pay_site','site_procesadora','pay_payment_id','mov_id','operation','site_merchant','mov_creation_date','fecha_esperada_pago','fecha_esperada_conci','mov_amount','gtwt_card_number','gtwt_installments','num_installment','mov_amount_install','site_bandera','fecha_de_presentacion','fecha_de_pago']
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df
    
class Extractor_genova:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_csv(file,dtype=str,sep=";",encoding='latin1')
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        col = ["unique_id","parent_acq_tx_id","merchant_operation_reference","auth_id","brand","country","merchant_id","installment_financing","installment_number","installments","process_date","transaction_datetime","capture_datetime","reconciliation_date","original_payment_date","currency_code","tx_amount","total_amount","acquirer_id","issuer_id","trunc_card_number","transaction_card_type","calculated_sign","mdr","mdr_vat","mdr_rate","mdr_vat_rate","installment_issuer_interest_tc","installment_issuer_interest_vat_tc","installment_issuer_interest_vat_rate","installment_risk_tc","installment_risk_vat_tc","installment_risk_vat_rate","advance_amount","advance_financing_entity_id","advanced_payment_date","advanced_qty_days","base_rate_advanced","effective_rate_advanced","spread_advanced","interest_amount","tax_concept_amount","payment_amount"]
        df.columns = col
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df