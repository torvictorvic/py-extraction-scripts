import boto3
import numpy
import pandas as pd
import io
from io import StringIO, BytesIO
from datetime import date, timedelta, datetime
import zipfile
import glob
import os
import os.path
import sys
import pytz
import time
import pandas as pd
from pandas import DataFrame
from enum import Enum
import math
from urllib.parse import urlparse
from zipfile import ZipFile
from decimal import Decimal
import numpy as np

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

            # session = boto3.Session(profile_name="default")
            # s3 = session.client('s3')
            session = boto3.session.Session(region_name='us-east-1')
            s3 = session.client('s3')

            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            return obj,lm
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

def decimal_from_value(value):
    return Decimal(value)

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
        obj_inter= [
            {
                "columna_file": "REVERSO DE SAQUE",
                "columna_df": "comision_reverso_saque_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "SAQUE",
                "columna_df": "comision_saque_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "ATM BALANCE INQUIRY",
                "columna_df": "comision_total_atm_balance_inquiry_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "ATM CASH ADJUSTMENTS",
                "columna_df": "comision_total_atm_cash_adjustments_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "ATM DECLINE",
                "columna_df": "comision_total_atm_decline_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "CHARGEBACK DE COMPRA",
                "columna_df": "comision_total_chargeback_de_compra_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "CHARGEBACK DE QUASI-CASH",
                "columna_df": "comision_total_chargeback_de_quasi_cash_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "CHARGEBACK DE SAQUE",
                "columna_df": "comision_total_chargeback_de_saque_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "COMPRA",
                "columna_df": "comision_total_compra_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "CREDITO VOUCHER",
                "columna_df": "comision_total_credito_voucher_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "FEE COLLECT RC=0140",
                "columna_df": "comision_total_fee_collect_rc_0140_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "FEE COLLECT RC=0350",
                "columna_df": "comision_total_fee_collect_rc_0350_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "FEE COLLECT RC=5040",
                "columna_df": "comision_total_fee_collect_rc_5040_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "FUNDS DISBURSE RC=0350",
                "columna_df": "comision_total_funds_disburse_rc_0350_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "FUNDS DISBURSE RC=5040",
                "columna_df": "comision_total_funds_disburse_rc_5040_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "ORIGINAL CREDIT",
                "columna_df": "comision_total_original_credit_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "QUASI-CASH",
                "columna_df": "comision_total_quasi_cash_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "QUASI-CASH CREDIT",
                "columna_df": "comision_total_quasi_cash_credit_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REAPRESENTAÇÃO",
                "columna_df": "comision_total_reapresentacao_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REAPRESENTAÇÃO DE COMPRA",
                "columna_df": "comision_total_reapresentacao_de_compra_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REAPRESENTAÇÃO DE SAQUE",
                "columna_df": "comision_total_reapresentacao_de_saque_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REVERSO DE CHARGEBACK",
                "columna_df": "comision_total_reverso_de_chargeback_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REVERSO DE COMPRA",
                "columna_df": "comision_total_reverso_de_compra_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REVERSO DE QUASI-CASH",
                "columna_df": "comision_total_reverso_de_quasi_cash_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REVERSO DE REAPRESENTAÇÃO",
                "columna_df": "comision_total_reverso_de_reapresentacao_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "LIQUIDAÇÕES DO DIA",
                "columna_df": "liquidacoes_do_dia_int",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "ATM BALANCE INQUIRY",
                "columna_df": "atm_balance_inquiry_int",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "ATM CASH ADJUSTMENTS",
                "columna_df": "atm_cash_adjustments_int",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "ATM DECLINE",
                "columna_df": "atm_decline_int",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "CHARGEBACK DE COMPRA",
                "columna_df": "chargeback_de_compra_int",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            
            {
                "columna_file": "CHARGEBACK DE QUASI-CASH",
                "columna_df": "chargeback_de_quasi_cash_int",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
                            
            {
                "columna_file": "CHARGEBACK DE SAQUE",
                "columna_df": "chargeback_de_saque_int",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "COMPRA",
                "columna_df": "compra_int",
                "position": 3,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "COMPRA",
                "columna_df": "compra_int_cantidad",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "CREDITO VOUCHER",
                "columna_df": "credito_voucher_int",
                "position": 4,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "CREDITO VOUCHER",
                "columna_df": "credito_voucher_int_cantidad",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "FEE COLLECT RC=0140",
                "columna_df": "fee_collect_rc_0140_int",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "FEE COLLECT RC=0350",
                "columna_df": "fee_collect_rc_0350_int",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "FEE COLLECT RC=5040",
                "columna_df": "fee_collect_rc_5040_int",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "FUNDS DISBURSE RC=0350",
                "columna_df": "funds_disburse_rc_0350_int",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "FUNDS DISBURSE RC=5040",
                "columna_df": "funds_disburse_rc_5040_int",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "ORIGINAL CREDIT",
                "columna_df": "original_credit_int",
                "position": 4,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "ORIGINAL CREDIT",
                "columna_df": "original_credit_int_cantidad",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "QUASI-CASH",
                "columna_df": "quasi_cash_int",
                "position": 3,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "QUASI-CASH",
                "columna_df": "quasi_cash_int_cantidad",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "QUASI-CASH CREDIT",
                "columna_df": "quasi_cash_credit_int",
                "position": 4,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "QUASI-CASH CREDIT",
                "columna_df": "quasi_cash_credit_int_cantidad",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REAPRESENTAÇÃO",
                "columna_df": "reapresentacao_int_cantidad",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REAPRESENTAÇÃO",
                "columna_df": "reapresentacao_int",
                "position": 3,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REAPRESENTAÇÃO DE COMPRA",
                "columna_df": "reapresentacao_de_compra_int",
                "position": 3,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REAPRESENTAÇÃO DE COMPRA",
                "columna_df": "reapresentacao_de_compra_int_cantidad",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REAPRESENTAÇÃO DE SAQUE",
                "columna_df": "reapresentacao_de_saque_int",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REVERSO DE CHARGEBACK",
                "columna_df": "reverso_de_chargeback_int",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REVERSO DE COMPRA",
                "columna_df": "reverso_de_compra_int",
                "position": 4,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REVERSO DE COMPRA",
                "columna_df": "reverso_de_compra_int_cantidad",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REVERSO DE QUASI-CASH",
                "columna_df": "reverso_de_quasi_cash_int",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REVERSO DE REAPRESENTAÇÃO",
                "columna_df": "reverso_de_reapresentacao_int",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REVERSO DE SAQUE",
                "columna_df": "reverso_de_saque_int",
                "position": 4,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REVERSO DE SAQUE",
                "columna_df": "reverso_de_saque_int_cantidad",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "SAQUE",
                "columna_df": "saque_int",
                "position": 3,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "SAQUE",
                "columna_df": "saque_int_cantidad",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_df": "comision_total_manual_cash_int",
                "columna_file": "MANUAL CASH",
                "position": 8
            },
            {
                "columna_df": "comision_total_reverso_manual_cash_int",
                "columna_file": "REVERSO DE MANUAL CASH",
                "position": 8
            },{
                "columna_df": "manual_cash_int",
                "columna_file": "MANUAL CASH",
                "position": 2
            },
            {
                "columna_df": "reverso_manual_cash_int",
                "columna_file": "REVERSO DE MANUAL CASH",
                "position": 2
            },{   
                "columna_file": "REVERSO DE SAQUE",
                "columna_df": "final_reverso_saque_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "SAQUE",
                "columna_df": "final_saque_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "ATM BALANCE INQUIRY",
                "columna_df": "final_total_atm_balance_inquiry_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "ATM CASH ADJUSTMENTS",
                "columna_df": "final_total_atm_cash_adjustments_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "ATM DECLINE",
                "columna_df": "final_total_atm_decline_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "CHARGEBACK DE COMPRA",
                "columna_df": "final_total_chargeback_de_compra_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            
            {
                "columna_file": "CHARGEBACK DE QUASI-CASH",
                "columna_df": "final_total_chargeback_de_quasi_cash_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            
            {
                "columna_file": "CHARGEBACK DE SAQUE",
                "columna_df": "final_total_chargeback_de_saque_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "COMPRA",
                "columna_df": "final_total_compra_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "CREDITO VOUCHER",
                "columna_df": "final_total_credito_voucher_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "FEE COLLECT RC=0140",
                "columna_df": "final_total_fee_collect_rc_0140_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "FEE COLLECT RC=0350",
                "columna_df": "final_total_fee_collect_rc_0350_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "FEE COLLECT RC=5040",
                "columna_df": "final_total_fee_collect_rc_5040_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "FUNDS DISBURSE RC=0350",
                "columna_df": "final_total_funds_disburse_rc_0350_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "FUNDS DISBURSE RC=5040",
                "columna_df": "final_total_funds_disburse_rc_5040_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "ORIGINAL CREDIT",
                "columna_df": "final_total_original_credit_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "QUASI-CASH",
                "columna_df": "final_total_quasi_cash_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "QUASI-CASH CREDIT",
                "columna_df": "final_total_quasi_cash_credit_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REAPRESENTAÇÃO",
                "columna_df": "final_total_reapresentacao_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REAPRESENTAÇÃO DE COMPRA",
                "columna_df": "final_total_reapresentacao_de_compra_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REAPRESENTAÇÃO DE SAQUE",
                "columna_df": "final_total_reapresentacao_de_saque_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REVERSO DE CHARGEBACK",
                "columna_df": "final_total_reverso_de_chargeback_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REVERSO DE COMPRA",
                "columna_df": "final_total_reverso_de_compra_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REVERSO DE QUASI-CASH",
                "columna_df": "final_total_reverso_de_quasi_cash_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "REVERSO DE REAPRESENTAÇÃO",
                "columna_df": "final_total_reverso_de_reapresentacao_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },{
                "columna_df": "final_total_manual_cash_int",
                "columna_file": "MANUAL CASH",
                "position": 5
            },
            {
                "columna_df": "final_total_reverso_manual_cash_int",
                "columna_file": "REVERSO DE MANUAL CASH",
                "position": 5
            },
            {
                "columna_file": "FEE COLLECT RC=0240",
                "columna_df": "fee_collect_rc_0240_int",
                "position": 2,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "FEE COLLECT RC=0240",
                "columna_df": "final_total_fee_collect_rc_0240_int",
                "position": 5,
                "sheet": "Agenda Internacional"
            },
            {
                "columna_file": "FEE COLLECT RC=0240",
                "columna_df": "comision_total_fee_collect_rc_0240_int",
                "position": 8,
                "sheet": "Agenda Internacional"
            }
            
        ]
        obj_nac = [

            {
                "columna_df": "comision_reverso_saque_nac",
                "columna_file": "REVERSO DE SAQUE",
                "position": 8
            },
            {
                "columna_df": "comision_saque_nac",
                "columna_file": "SAQUE",
                "position": 8
            },
            {
                "columna_df": "comision_total_atm_balance_inquiry_nac",
                "columna_file": "ATM BALANCE INQUIRY",
                "position": 8
            },
            {
                "columna_df": "comision_total_atm_cash_adjustments_nac",
                "columna_file": "ATM CASH ADJUSTMENTS",
                "position": 8
            },
            {
                "columna_df": "comision_total_atm_decline_nac",
                "columna_file": "ATM DECLINE",
                "position": 8
            },
            {
                "columna_df": "comision_total_chargeback_de_compra_nac",
                "columna_file": "CHARGEBACK DE COMPRA",
                "position": 8
            },
            {
                "columna_df": "comision_total_chargeback_de_quasi_cash_nac",
                "columna_file": "CHARGEBACK DE QUASI-CASH",
                "position": 8
            },
            {
                "columna_df": "comision_total_chargeback_de_saque_nac",
                "columna_file": "CHARGEBACK DE SAQUE",
                "position": 8
            },
            {
                "columna_df": "comision_total_compra_nac",
                "columna_file": "COMPRA",
                "position": 8
            },
            {
                "columna_df": "comision_total_credito_voucher_nac",
                "columna_file": "CREDITO VOUCHER",
                "position": 8
            },
            {
                "columna_df": "comision_total_fee_collect_rc_0140_nac",
                "columna_file": "FEE COLLECT RC=0140",
                "position": 8
            },
            {
                "columna_df": "comision_total_fee_collect_rc_0350_nac",
                "columna_file": "FEE COLLECT RC=0350",
                "position": 8
            },
            {
                "columna_df": "comision_total_fee_collect_rc_5040_nac",
                "columna_file": "FEE COLLECT RC=5040",
                "position": 8
            },
            {
                "columna_df": "comision_total_funds_disburse_rc_0350_nac",
                "columna_file": "FUNDS DISBURSE RC=0350",
                "position": 8
            },
            {
                "columna_df": "comision_total_funds_disburse_rc_5040_nac",
                "columna_file": "FUNDS DISBURSE RC=5040",
                "position": 8
            },
            {
                "columna_df": "comision_total_original_credit_nac",
                "columna_file": "ORIGINAL CREDIT",
                "position": 8
            },
            {
                "columna_df": "comision_total_quasi_cash_nac",
                "columna_file": "QUASI-CASH",
                "position": 8
            },
            {
                "columna_df": "comision_total_quasi_cash_credit_nac",
                "columna_file": "QUASI-CASH CREDIT",
                "position": 8
            },
            {
                "columna_df": "comision_total_reapresentacao_nac",
                "columna_file": "REAPRESENTAÇÃO",
                "position": 8
            },
            {
                "columna_df": "comision_total_reapresentacao_de_compra_nac",
                "columna_file": "REAPRESENTAÇÃO DE COMPRA",
                "position": 8
            },
            {
                "columna_df": "comision_total_reapresentacao_de_saque_nac",
                "columna_file": "REAPRESENTAÇÃO DE SAQUE",
                "position": 8
            },
            {
                "columna_df": "comision_total_reverso_de_chargeback_nac",
                "columna_file": "REVERSO DE CHARGEBACK",
                "position": 8
            },
            {
                "columna_df": "comision_total_reverso_de_compra_nac",
                "columna_file": "REVERSO DE COMPRA",
                "position": 8
            },
            {
                "columna_df": "comision_total_reverso_de_quasi_cash_nac",
                "columna_file": "REVERSO DE QUASI-CASH",
                "position": 8
            },
            {
                "columna_df": "comision_total_reverso_de_reapresentacao_nac",
                "columna_file": "REVERSO DE REAPRESENTAÇÃO",
                "position": 8
            },
            {
                "columna_df": "comision_total_manual_cash_nac",
                "columna_file": "MANUAL CASH",
                "position": 8
            },
            {
                "columna_df": "comision_total_reverso_manual_cash_nac",
                "columna_file": "REVERSO DE MANUAL CASH",
                "position": 8
            },
            {
                "columna_df": "total_de_liquidacao_nac",
                "columna_file": "TOTAL DE LIQUIDAÇÃO",
                "position": 2
            },
            {
                "columna_df": "total_de_liquidacao_de_saque_nac",
                "columna_file": "TOTAL DE LIQUIDAÇÃO DE SAQUE",
                "position": 2
            },
            {
                "columna_df": "atm_balance_inquiry_nac",
                "columna_file": "ATM BALANCE INQUIRY",
                "position": 2
            },
            {
                "columna_df": "atm_cash_adjustments_nac",
                "columna_file": "ATM CASH ADJUSTMENTS",
                "position": 2
            },
            {
                "columna_df": "atm_decline_nac",
                "columna_file": "ATM DECLINE",
                "position": 2
            },
            {
                "columna_df": "chargeback_de_compra_nac",
                "columna_file": "CHARGEBACK DE COMPRA",
                "position": 2
            },
    
            {
                "columna_df": "chargeback_de_quasi_cash_nac",
                "columna_file": "CHARGEBACK DE QUASI-CASH",
                "position": 2
            },
    
            {
                "columna_df": "chargeback_de_saque_nac",
                "columna_file": "CHARGEBACK DE SAQUE",
                "position": 2
            },
            {
                "columna_df": "compra_nac",
                "columna_file": "COMPRA",
                "position": 3
            },
            {
                "columna_df": "compra_nac_cantidad",
                "columna_file": "COMPRA",
                "position": 2
            },
            {
                "columna_df": "credito_voucher_nac",
                "columna_file": "CREDITO VOUCHER",
                "position": 4
            },
            {
                "columna_df": "credito_voucher_nac_cantidad",
                "columna_file": "CREDITO VOUCHER",
                "position": 2
            },
            {
                "columna_df": "fee_collect_rc_0140_nac",
                "columna_file": "FEE COLLECT RC=0140",
                "position": 2
            },
            {
                "columna_df": "fee_collect_rc_0350_nac",
                "columna_file": "FEE COLLECT RC=0350",
                "position": 2
            },
            {
                "columna_df": "fee_collect_rc_5040_nac",
                "columna_file": "FEE COLLECT RC=5040",
                "position": 2
            },
            {
                "columna_df": "funds_disburse_rc_0350_nac",
                "columna_file": "FUNDS DISBURSE RC=0350",
                "position": 2
            },
            {
                "columna_df": "funds_disburse_rc_5040_nac",
                "columna_file": "FUNDS DISBURSE RC=5040",
                "position": 2
            },
            {
                "columna_df": "original_credit_nac",
                "columna_file": "ORIGINAL CREDIT",
                "position": 4
            },
            {
                "columna_df": "original_credit_nac_cantidad",
                "columna_file": "ORIGINAL CREDIT",
                "position": 2
            },
            {
                "columna_df": "quasi_cash_nac",
                "columna_file": "QUASI-CASH",
                "position": 3
            },
            {
                "columna_df": "quasi_cash_credit_nac",
                "columna_file": "QUASI-CASH CREDIT",
                "position": 4
            },
            {
                "columna_df": "quasi_cash_credit_nac_cantidad",
                "columna_file": "QUASI-CASH CREDIT",
                "position": 2
            },
            {
                "columna_df": "quasi_cash_nac_cantidad",
                "columna_file": "QUASI-CASH",
                "position": 2
            },
            {
                "columna_df": "reapresentacao_nac_cantidad",
                "columna_file": "REAPRESENTAÇÃO",
                "position": 2
            },
            {
                "columna_df": "reapresentacao_nac",
                "columna_file": "REAPRESENTAÇÃO",
                "position": 3
            },
            {
                "columna_df": "reapresentacao_de_compra_nac",
                "columna_file": "REAPRESENTAÇÃO DE COMPRA",
                "position": 3
            },
            {
                "columna_df": "reapresentacao_de_compra_nac_cantidad",
                "columna_file": "REAPRESENTAÇÃO DE COMPRA",
                "position": 2
            },
            {
                "columna_df": "reapresentacao_de_saque_nac",
                "columna_file": "REAPRESENTAÇÃO DE SAQUE",
                "position": 2
            },
            {
                "columna_df": "reverso_de_chargeback_nac",
                "columna_file": "REVERSO DE CHARGEBACK",
                "position": 2
            },
            {
                "columna_df": "reverso_de_compra_nac",
                "columna_file": "REVERSO DE COMPRA",
                "position": 4
            },
            {
                "columna_df": "reverso_de_compra_nac_cantidad",
                "columna_file": "REVERSO DE COMPRA",
                "position": 2
            },
            {
                "columna_df": "reverso_de_quasi_cash_nac",
                "columna_file": "REVERSO DE QUASI-CASH",
                "position": 2
            },
            {
                "columna_df": "reverso_de_reapresentacao_nac",
                "columna_file": "REVERSO DE REAPRESENTAÇÃO",
                "position": 2
            },
            {
                "columna_df": "reverso_de_saque_nac",
                "columna_file": "REVERSO DE SAQUE",
                "position": 4
            },
            {
                "columna_df": "reverso_de_saque_nac_cantidad",
                "columna_file": "REVERSO DE SAQUE",
                "position": 2
            },
            {
                "columna_df": "saque_nac",
                "columna_file": "SAQUE",
                "position": 3
            },
            {
                "columna_df": "saque_nac_cantidad",
                "columna_file": "SAQUE",
                "position": 2
            },
            {
                "columna_df": "manual_cash_nac",
                "columna_file": "MANUAL CASH",
                "position": 2
            },
            {
                "columna_df": "reverso_manual_cash_nac",
                "columna_file": "REVERSO DE MANUAL CASH",
                "position": 2
            },
            {
                "columna_df": "final_reverso_saque_nac",
                "columna_file": "REVERSO DE SAQUE",
                "position": 5
            },
            {
                "columna_df": "final_saque_nac",
                "columna_file": "SAQUE",
                "position": 5
            },
            {
                "columna_df": "final_total_atm_balance_inquiry_nac",
                "columna_file": "ATM BALANCE INQUIRY",
                "position": 5
            },
            {
                "columna_df": "final_total_atm_cash_adjustments_nac",
                "columna_file": "ATM CASH ADJUSTMENTS",
                "position": 5
            },
            {
                "columna_df": "final_total_atm_decline_nac",
                "columna_file": "ATM DECLINE",
                "position": 5
            },
            {
                "columna_df": "final_total_chargeback_de_compra_nac",
                "columna_file": "CHARGEBACK DE COMPRA",
                "position": 5
            },
    
            {
                "columna_df": "final_total_chargeback_de_quasi_cash_nac",
                "columna_file": "CHARGEBACK DE QUASI-CASH",
                "position": 5
            },
    
            {
                "columna_df": "final_total_chargeback_de_saque_nac",
                "columna_file": "CHARGEBACK DE SAQUE",
                "position": 5
            },
            {
                "columna_df": "final_total_compra_nac",
                "columna_file": "COMPRA",
                "position": 5
            },
            {
                "columna_df": "final_total_credito_voucher_nac",
                "columna_file": "CREDITO VOUCHER",
                "position": 5
            },
            {
                "columna_df": "final_total_fee_collect_rc_0140_nac",
                "columna_file": "FEE COLLECT RC=0140",
                "position": 5
            },
            {
                "columna_df": "final_total_fee_collect_rc_0350_nac",
                "columna_file": "FEE COLLECT RC=0350",
                "position": 5
            },
            {
                "columna_df": "final_total_fee_collect_rc_5040_nac",
                "columna_file": "FEE COLLECT RC=5040",
                "position": 5
            },
            {
                "columna_df": "final_total_funds_disburse_rc_0350_nac",
                "columna_file": "FUNDS DISBURSE RC=0350",
                "position": 5
            },
            {
                "columna_df": "final_total_funds_disburse_rc_5040_nac",
                "columna_file": "FUNDS DISBURSE RC=5040",
                "position": 5
            },
            {
                "columna_df": "final_total_original_credit_nac",
                "columna_file": "ORIGINAL CREDIT",
                "position": 5
            },
            {
                "columna_df": "final_total_quasi_cash_nac",
                "columna_file": "QUASI-CASH",
                "position": 5
            },
            {
                "columna_df": "final_total_quasi_cash_credit_nac",
                "columna_file": "QUASI-CASH CREDIT",
                "position": 5
            },
            {
                "columna_df": "final_total_reapresentacao_nac",
                "columna_file": "REAPRESENTAÇÃO",
                "position": 5
            },
            {
                "columna_df": "final_total_reapresentacao_de_compra_nac",
                "columna_file": "REAPRESENTAÇÃO DE COMPRA",
                "position": 5
            },
            {
                "columna_df": "final_total_reapresentacao_de_saque_nac",
                "columna_file": "REAPRESENTAÇÃO DE SAQUE",
                "position": 5
            },
            {
                "columna_df": "final_total_reverso_de_chargeback_nac",
                "columna_file": "REVERSO DE CHARGEBACK",
                "position": 5
            },
            {
                "columna_df": "final_total_reverso_de_compra_nac",
                "columna_file": "REVERSO DE COMPRA",
                "position": 5
            },
            {
                "columna_df": "final_total_reverso_de_quasi_cash_nac",
                "columna_file": "REVERSO DE QUASI-CASH",
                "position": 5
            },
            {
                "columna_df": "final_total_reverso_de_reapresentacao_nac",
                "columna_file": "REVERSO DE REAPRESENTAÇÃO",
                "position": 5
            },
            {
                "columna_df": "final_total_manual_cash_nac",
                "columna_file": "MANUAL CASH",
                "position": 5
            },
            {
                "columna_df": "final_total_reverso_manual_cash_nac",
                "columna_file": "REVERSO DE MANUAL CASH",
                "position": 5
            },{
                "columna_df": "comision_total_fee_collect_rc_0240_nac",
                "columna_file": "FEE COLLECT RC=0240",
                "position": 8
            },            {
                "columna_df": "fee_collect_rc_0240_nac",
                "columna_file": "FEE COLLECT RC=0240",
                "position": 2
            },
            {
                "columna_df": "final_total_fee_collect_rc_0240_nac",
                "columna_file": "FEE COLLECT RC=0240",
                "position": 5
            }
        ]




        try:
            nacional = BytesIO(file)
            internacional = BytesIO(file)
            comisiones = BytesIO(file)

            # Solapa Nacional
            
            df_nac = pd.read_excel(nacional,sheet_name='Agenda Nacional',header=None,dtype=str)
            results =[]
            columns = [obj['columna_df'] for obj in obj_nac]
            for _obj in obj_nac:
                try:
                    _obj['columna_df'] = df_nac[_obj['position']][df_nac[_obj['position']][df_nac[1]==_obj['columna_file']].index[0]]
                    results.append(_obj['columna_df'])
                except Exception as e:
                    _obj['columna_df'] = np.nan
                    results.append(_obj['columna_df'])
            df = pd.DataFrame(results).transpose()
            df.columns = columns

            # Solapa Internacional
            df_int = pd.read_excel(internacional,sheet_name='Agenda Internacional',header=None,dtype=str)
            results =[]
            columns = [obj['columna_df'] for obj in obj_inter]
            for _obj in obj_inter:
                try:
                    _obj['columna_df'] = df_int[_obj['position']][df_int[_obj['position']][df_int[1]==_obj['columna_file']].index[0]]
                    results.append(_obj['columna_df'])
                except Exception as e:
                    _obj['columna_df'] = np.nan
                    results.append(_obj['columna_df'])
            df_inter = pd.DataFrame(results).transpose()
            df_inter.columns = columns
            # Interface Complementar

            df_com = pd.read_excel(comisiones,sheet_name=3,header=None,dtype=str)
            try:
                data_lancamento_com = df_com[0][1]
            except KeyError:
                data_lancamento_com = np.nan
            ## Union all
            final = pd.concat([df_inter,df],sort=False,axis=1)
            final['data_lancamento_com'] = data_lancamento_com

            valor_processao_total_nac = round(df_nac[5][2:df_nac[df_nac[1] == 'TOTAL GERAL'].index[0]].astype('float').sum(),2)
            valor_comissao_total_nac = round(df_nac[8][2:df_nac[df_nac[1] == 'TOTAL GERAL'].index[0]].astype('float').sum(),2)
            valor_total_total_nac = round(df_nac[11][2:df_nac[df_nac[1] == 'TOTAL GERAL'].index[0]].astype('float').sum(),2)
            quantidade_total_geral_nac = df_nac[2][2:df_nac[df_nac[1] == 'TOTAL GERAL'].index[0]].astype('int').sum()

            final['valor_processao_total_nac'] = valor_processao_total_nac
            final['valor_comissao_total_nac'] = valor_comissao_total_nac
            final['valor_total_total_nac'] = valor_total_total_nac
            final['quantidade_total_geral_nac'] = quantidade_total_geral_nac

            valor_processao_total_int = round(df_int[5][2:df_int[df_int[1] == 'TOTAL GERAL'].index[0]].astype('float').sum(),2)
            valor_comissao_total_int = round(df_int[8][2:df_int[df_int[1] == 'TOTAL GERAL'].index[0]].astype('float').sum(),2)
            valor_total_total_int = round(df_int[11][2:df_int[df_int[1] == 'TOTAL GERAL'].index[0]].astype('float').sum(),2)
            quantidade_total_geral_int = df_int[2][2:df_int[df_int[1] == 'TOTAL GERAL'].index[0]].astype('int').sum()

            final['valor_processao_total_int'] = valor_processao_total_int
            final['valor_comissao_total_int'] = valor_comissao_total_int
            final['valor_total_total_int'] = valor_total_total_int
            final['quantidade_total_geral_int'] = quantidade_total_geral_int
            #formato = ["valor_processao_total_nac","final_total_chargeback_de_quasi_cash_nac","chargeback_de_quasi_cash_nac","final_total_chargeback_de_quasi_cash_int","chargeback_de_quasi_cash_int","comision_reverso_saque_nac","comision_saque_nac","comision_total_atm_balance_inquiry_nac","comision_total_atm_cash_adjustments_nac","comision_total_atm_decline_nac","comision_total_chargeback_de_compra_nac","comision_total_chargeback_de_quasi_cash_nac","comision_total_chargeback_de_saque_nac","comision_total_compra_nac","comision_total_credito_voucher_nac","comision_total_fee_collect_rc_0140_nac","comision_total_fee_collect_rc_0350_nac","comision_total_fee_collect_rc_5040_nac","comision_total_funds_disburse_rc_0350_nac","comision_total_funds_disburse_rc_5040_nac","comision_total_original_credit_nac","comision_total_quasi_cash_nac","comision_total_quasi_cash_credit_nac","comision_total_reapresentacao_nac","comision_total_reapresentacao_de_compra_nac","comision_total_reapresentacao_de_saque_nac","comision_total_reverso_de_chargeback_nac","comision_total_reverso_de_compra_nac","comision_total_reverso_de_quasi_cash_nac","comision_total_reverso_de_reapresentacao_nac","valor_comissao_total_nac","valor_total_total_nac","total_de_liquidacao_nac","total_de_liquidacao_de_saque_nac","quantidade_total_geral_nac","atm_balance_inquiry_nac","atm_cash_adjustments_nac","atm_decline_nac","chargeback_de_compra_nac","chargeback_de_saque_nac","compra_nac","credito_voucher_nac","fee_collect_rc_0140_nac","fee_collect_rc_0350_nac","fee_collect_rc_5040_nac","funds_disburse_rc_0350_nac","funds_disburse_rc_5040_nac","original_credit_nac","quasi_cash_nac","quasi_cash_credit_nac","reapresentacao_nac","reapresentacao_de_compra_nac","reapresentacao_de_saque_nac","reverso_de_chargeback_nac","reverso_de_compra_nac","reverso_de_quasi_cash_nac","reverso_de_reapresentacao_nac","reverso_de_saque_nac","saque_nac","valor_processao_total_int","comision_reverso_saque_int","comision_saque_int","comision_total_atm_balance_inquiry_int","comision_total_atm_cash_adjustments_int","comision_total_atm_decline_int","comision_total_chargeback_de_compra_int","comision_total_chargeback_de_quasi_cash_int","comision_total_chargeback_de_saque_int","comision_total_compra_int","comision_total_credito_voucher_int","comision_total_fee_collect_rc_0140_int","comision_total_fee_collect_rc_0350_int","comision_total_fee_collect_rc_5040_int","comision_total_funds_disburse_rc_0350_int","comision_total_funds_disburse_rc_5040_int","comision_total_original_credit_int","comision_total_quasi_cash_int","comision_total_quasi_cash_credit_int","comision_total_reapresentacao_int","comision_total_reapresentacao_de_compra_int","comision_total_reapresentacao_de_saque_int","comision_total_reverso_de_chargeback_int","comision_total_reverso_de_compra_int","comision_total_reverso_de_quasi_cash_int","comision_total_reverso_de_reapresentacao_int","valor_comissao_total_int","valor_total_total_int","liquidacoes_do_dia_int","quantidade_total_geral_int","atm_balance_inquiry_int","atm_cash_adjustments_int","atm_decline_int","chargeback_de_compra_int","chargeback_de_saque_int","compra_int","credito_voucher_int","fee_collect_rc_0140_int","fee_collect_rc_0350_int","fee_collect_rc_5040_int","funds_disburse_rc_0350_int","funds_disburse_rc_5040_int","original_credit_int","quasi_cash_int","quasi_cash_credit_int","reapresentacao_int","reapresentacao_de_compra_int","reapresentacao_de_saque_int","reverso_de_chargeback_int","reverso_de_compra_int","reverso_de_quasi_cash_int","reverso_de_reapresentacao_int","reverso_de_saque_int","saque_int","data_lancamento_com","comision_total_manual_cash_nac","comision_total_reverso_manual_cash_nac","manual_cash_nac","reverso_manual_cash_nac","comision_total_manual_cash_int","comision_total_reverso_manual_cash_int","manual_cash_int","reverso_manual_cash_int","final_reverso_saque_int","final_saque_int","final_total_atm_balance_inquiry_int","final_total_atm_cash_adjustments_int","final_total_atm_decline_int","final_total_chargeback_de_compra_int","final_total_chargeback_de_saque_int","final_total_compra_int","final_total_credito_voucher_int","final_total_fee_collect_rc_0140_int","final_total_fee_collect_rc_0350_int","final_total_fee_collect_rc_5040_int","final_total_funds_disburse_rc_0350_int","final_total_funds_disburse_rc_5040_int","final_total_original_credit_int","final_total_quasi_cash_int","final_total_quasi_cash_credit_int","final_total_reapresentacao_int","final_total_reapresentacao_de_compra_int","final_total_reapresentacao_de_saque_int","final_total_reverso_de_chargeback_int","final_total_reverso_de_compra_int","final_total_reverso_de_quasi_cash_int","final_total_reverso_de_reapresentacao_int","final_total_manual_cash_int","final_total_reverso_manual_cash_int","final_reverso_saque_nac","final_saque_nac","final_total_atm_balance_inquiry_nac","final_total_atm_cash_adjustments_nac","final_total_atm_decline_nac","final_total_chargeback_de_compra_nac","final_total_chargeback_de_saque_nac","final_total_compra_nac","final_total_credito_voucher_nac","final_total_fee_collect_rc_0140_nac","final_total_fee_collect_rc_0350_nac","final_total_fee_collect_rc_5040_nac","final_total_funds_disburse_rc_0350_nac","final_total_funds_disburse_rc_5040_nac","final_total_original_credit_nac","final_total_quasi_cash_nac","final_total_quasi_cash_credit_nac","final_total_reapresentacao_nac","final_total_reapresentacao_de_compra_nac","final_total_reapresentacao_de_saque_nac","final_total_reverso_de_chargeback_nac","final_total_reverso_de_compra_nac","final_total_reverso_de_quasi_cash_nac","final_total_reverso_de_reapresentacao_nac","final_total_manual_cash_nac","final_total_reverso_manual_cash_nac","fee_collect_rc_0240_int","final_total_fee_collect_rc_0240_int","comision_total_fee_collect_rc_0240_int","comision_total_fee_collect_rc_0240_nac","fee_collect_rc_0240_nac","final_total_fee_collect_rc_0240_nac"]
            formato = ["valor_processao_total_nac","final_total_chargeback_de_quasi_cash_nac","chargeback_de_quasi_cash_nac","final_total_chargeback_de_quasi_cash_int","chargeback_de_quasi_cash_int","comision_reverso_saque_nac","comision_saque_nac","comision_total_atm_balance_inquiry_nac","comision_total_atm_cash_adjustments_nac","comision_total_atm_decline_nac","comision_total_chargeback_de_compra_nac","comision_total_chargeback_de_quasi_cash_nac","comision_total_chargeback_de_saque_nac","comision_total_compra_nac","comision_total_credito_voucher_nac","comision_total_fee_collect_rc_0140_nac","comision_total_fee_collect_rc_0350_nac","comision_total_fee_collect_rc_5040_nac","comision_total_funds_disburse_rc_0350_nac","comision_total_funds_disburse_rc_5040_nac","comision_total_original_credit_nac","comision_total_quasi_cash_nac","comision_total_quasi_cash_credit_nac","comision_total_reapresentacao_nac","comision_total_reapresentacao_de_compra_nac","comision_total_reapresentacao_de_saque_nac","comision_total_reverso_de_chargeback_nac","comision_total_reverso_de_compra_nac","comision_total_reverso_de_quasi_cash_nac","comision_total_reverso_de_reapresentacao_nac","valor_comissao_total_nac","valor_total_total_nac","total_de_liquidacao_nac","total_de_liquidacao_de_saque_nac","quantidade_total_geral_nac","atm_balance_inquiry_nac","atm_cash_adjustments_nac","atm_decline_nac","chargeback_de_compra_nac","chargeback_de_saque_nac","compra_nac","compra_nac_cantidad","credito_voucher_nac","credito_voucher_nac_cantidad","fee_collect_rc_0140_nac","fee_collect_rc_0350_nac","fee_collect_rc_5040_nac","funds_disburse_rc_0350_nac","funds_disburse_rc_5040_nac","original_credit_nac","original_credit_nac_cantidad","quasi_cash_nac","quasi_cash_nac_cantidad","quasi_cash_credit_nac","quasi_cash_credit_nac_cantidad","reapresentacao_nac_cantidad","reapresentacao_nac","reapresentacao_de_compra_nac","reapresentacao_de_compra_nac_cantidad","reapresentacao_de_saque_nac","reverso_de_chargeback_nac","reverso_de_compra_nac","reverso_de_compra_nac_cantidad","reverso_de_quasi_cash_nac","reverso_de_reapresentacao_nac","reverso_de_saque_nac","reverso_de_saque_nac_cantidad","saque_nac","saque_nac_cantidad","valor_processao_total_int","comision_reverso_saque_int","comision_saque_int","comision_total_atm_balance_inquiry_int","comision_total_atm_cash_adjustments_int","comision_total_atm_decline_int","comision_total_chargeback_de_compra_int","comision_total_chargeback_de_quasi_cash_int","comision_total_chargeback_de_saque_int","comision_total_compra_int","comision_total_credito_voucher_int","comision_total_fee_collect_rc_0140_int","comision_total_fee_collect_rc_0350_int","comision_total_fee_collect_rc_5040_int","comision_total_funds_disburse_rc_0350_int","comision_total_funds_disburse_rc_5040_int","comision_total_original_credit_int","comision_total_quasi_cash_int","comision_total_quasi_cash_credit_int","comision_total_reapresentacao_int","comision_total_reapresentacao_de_compra_int","comision_total_reapresentacao_de_saque_int","comision_total_reverso_de_chargeback_int","comision_total_reverso_de_compra_int","comision_total_reverso_de_quasi_cash_int","comision_total_reverso_de_reapresentacao_int","valor_comissao_total_int","valor_total_total_int","liquidacoes_do_dia_int","quantidade_total_geral_int","atm_balance_inquiry_int","atm_cash_adjustments_int","atm_decline_int","chargeback_de_compra_int","chargeback_de_saque_int","compra_int","compra_int_cantidad","credito_voucher_int","credito_voucher_int_cantidad","fee_collect_rc_0140_int","fee_collect_rc_0350_int","fee_collect_rc_5040_int","funds_disburse_rc_0350_int","funds_disburse_rc_5040_int","original_credit_int","original_credit_int_cantidad","quasi_cash_int","quasi_cash_int_cantidad","quasi_cash_credit_int","quasi_cash_credit_int_cantidad","reapresentacao_int_cantidad","reapresentacao_int","reapresentacao_de_compra_int","reapresentacao_de_compra_int_cantidad","reapresentacao_de_saque_int","reverso_de_chargeback_int","reverso_de_compra_int","reverso_de_compra_int_cantidad","reverso_de_quasi_cash_int","reverso_de_reapresentacao_int","reverso_de_saque_int","reverso_de_saque_int_cantidad","saque_int","saque_int_cantidad","data_lancamento_com","comision_total_manual_cash_nac","comision_total_reverso_manual_cash_nac","manual_cash_nac","reverso_manual_cash_nac","comision_total_manual_cash_int","comision_total_reverso_manual_cash_int","manual_cash_int","reverso_manual_cash_int","final_reverso_saque_int","final_saque_int","final_total_atm_balance_inquiry_int","final_total_atm_cash_adjustments_int","final_total_atm_decline_int","final_total_chargeback_de_compra_int","final_total_chargeback_de_saque_int","final_total_compra_int","final_total_credito_voucher_int","final_total_fee_collect_rc_0140_int","final_total_fee_collect_rc_0350_int","final_total_fee_collect_rc_5040_int","final_total_funds_disburse_rc_0350_int","final_total_funds_disburse_rc_5040_int","final_total_original_credit_int","final_total_quasi_cash_int","final_total_quasi_cash_credit_int","final_total_reapresentacao_int","final_total_reapresentacao_de_compra_int","final_total_reapresentacao_de_saque_int","final_total_reverso_de_chargeback_int","final_total_reverso_de_compra_int","final_total_reverso_de_quasi_cash_int","final_total_reverso_de_reapresentacao_int","final_total_manual_cash_int","final_total_reverso_manual_cash_int","final_reverso_saque_nac","final_saque_nac","final_total_atm_balance_inquiry_nac","final_total_atm_cash_adjustments_nac","final_total_atm_decline_nac","final_total_chargeback_de_compra_nac","final_total_chargeback_de_saque_nac","final_total_compra_nac","final_total_credito_voucher_nac","final_total_fee_collect_rc_0140_nac","final_total_fee_collect_rc_0350_nac","final_total_fee_collect_rc_5040_nac","final_total_funds_disburse_rc_0350_nac","final_total_funds_disburse_rc_5040_nac","final_total_original_credit_nac","final_total_quasi_cash_nac","final_total_quasi_cash_credit_nac","final_total_reapresentacao_nac","final_total_reapresentacao_de_compra_nac","final_total_reapresentacao_de_saque_nac","final_total_reverso_de_chargeback_nac","final_total_reverso_de_compra_nac","final_total_reverso_de_quasi_cash_nac","final_total_reverso_de_reapresentacao_nac","final_total_manual_cash_nac","final_total_reverso_manual_cash_nac","fee_collect_rc_0240_int","final_total_fee_collect_rc_0240_int","comision_total_fee_collect_rc_0240_int","comision_total_fee_collect_rc_0240_nac","fee_collect_rc_0240_nac","final_total_fee_collect_rc_0240_nac"]
            final = final[formato]

            final = final.astype(object)

            final['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            final['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            final['file_name'] = out
            final.reset_index(drop=True)
            final['skt_extraction_rn'] = final.index.values
            return final


        except pd.io.common.EmptyDataError as e:
            formato = ["valor_processao_total_nac","final_total_chargeback_de_quasi_cash_nac","chargeback_de_quasi_cash_nac","final_total_chargeback_de_quasi_cash_int","chargeback_de_quasi_cash_int","comision_reverso_saque_nac","comision_saque_nac","comision_total_atm_balance_inquiry_nac","comision_total_atm_cash_adjustments_nac","comision_total_atm_decline_nac","comision_total_chargeback_de_compra_nac","comision_total_chargeback_de_quasi_cash_nac","comision_total_chargeback_de_saque_nac","comision_total_compra_nac","comision_total_credito_voucher_nac","comision_total_fee_collect_rc_0140_nac","comision_total_fee_collect_rc_0350_nac","comision_total_fee_collect_rc_5040_nac","comision_total_funds_disburse_rc_0350_nac","comision_total_funds_disburse_rc_5040_nac","comision_total_original_credit_nac","comision_total_quasi_cash_nac","comision_total_quasi_cash_credit_nac","comision_total_reapresentacao_nac","comision_total_reapresentacao_de_compra_nac","comision_total_reapresentacao_de_saque_nac","comision_total_reverso_de_chargeback_nac","comision_total_reverso_de_compra_nac","comision_total_reverso_de_quasi_cash_nac","comision_total_reverso_de_reapresentacao_nac","valor_comissao_total_nac","valor_total_total_nac","total_de_liquidacao_nac","total_de_liquidacao_de_saque_nac","quantidade_total_geral_nac","atm_balance_inquiry_nac","atm_cash_adjustments_nac","atm_decline_nac","chargeback_de_compra_nac","chargeback_de_saque_nac","compra_nac","credito_voucher_nac","fee_collect_rc_0140_nac","fee_collect_rc_0350_nac","fee_collect_rc_5040_nac","funds_disburse_rc_0350_nac","funds_disburse_rc_5040_nac","original_credit_nac","quasi_cash_nac","quasi_cash_credit_nac","reapresentacao_nac","reapresentacao_de_compra_nac","reapresentacao_de_saque_nac","reverso_de_chargeback_nac","reverso_de_compra_nac","reverso_de_quasi_cash_nac","reverso_de_reapresentacao_nac","reverso_de_saque_nac","saque_nac","valor_processao_total_int","comision_reverso_saque_int","comision_saque_int","comision_total_atm_balance_inquiry_int","comision_total_atm_cash_adjustments_int","comision_total_atm_decline_int","comision_total_chargeback_de_compra_int","comision_total_chargeback_de_quasi_cash_int","comision_total_chargeback_de_saque_int","comision_total_compra_int","comision_total_credito_voucher_int","comision_total_fee_collect_rc_0140_int","comision_total_fee_collect_rc_0350_int","comision_total_fee_collect_rc_5040_int","comision_total_funds_disburse_rc_0350_int","comision_total_funds_disburse_rc_5040_int","comision_total_original_credit_int","comision_total_quasi_cash_int","comision_total_quasi_cash_credit_int","comision_total_reapresentacao_int","comision_total_reapresentacao_de_compra_int","comision_total_reapresentacao_de_saque_int","comision_total_reverso_de_chargeback_int","comision_total_reverso_de_compra_int","comision_total_reverso_de_quasi_cash_int","comision_total_reverso_de_reapresentacao_int","valor_comissao_total_int","valor_total_total_int","liquidacoes_do_dia_int","quantidade_total_geral_int","atm_balance_inquiry_int","atm_cash_adjustments_int","atm_decline_int","chargeback_de_compra_int","chargeback_de_saque_int","compra_int","credito_voucher_int","fee_collect_rc_0140_int","fee_collect_rc_0350_int","fee_collect_rc_5040_int","funds_disburse_rc_0350_int","funds_disburse_rc_5040_int","original_credit_int","quasi_cash_int","quasi_cash_credit_int","reapresentacao_int","reapresentacao_de_compra_int","reapresentacao_de_saque_int","reverso_de_chargeback_int","reverso_de_compra_int","reverso_de_quasi_cash_int","reverso_de_reapresentacao_int","reverso_de_saque_int","saque_int","data_lancamento_com","comision_total_manual_cash_nac","comision_total_reverso_manual_cash_nac","manual_cash_nac","reverso_manual_cash_nac","comision_total_manual_cash_int","comision_total_reverso_manual_cash_int","manual_cash_int","reverso_manual_cash_int"]
            df = pd.DataFrame(columns = formato)
            df = df.append(pd.Series(), ignore_index=True)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
