
"""
Reads an exls file from S3 and transforms it into a pandas Dataframe with the same schema of the global_statement_citi table
"""

import zipfile_deflate64
import io
from io import BytesIO
import pdb
import boto3 as boto3
import pandas as pd
from openpyxl import load_workbook
from datetime import date, timedelta, datetime
import os
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
try:
    from core.models import ClientSetting
except:
    from utils.database import RDSConnector
    from models.core import ClientSettings
import xlrd
import zipfile 
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse


# ---------------------------------------------------------------
SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD =os.environ.get('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ROLE =os.environ.get('SNOWFLAKE_ROLE')
SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.environ.get('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA =  "INPUTS"#os.environ.get('SNOWFLAKE_SCHEMA')

# --------------------------------------------------------------------------
# GLOBAL UNITARY TEST BUCKET (used only for checking that the script is runing OK.)

# -------------------------------------------------------------------------
# ---------------------------------------------------------------



SWIFT_SCHEMA = {
            'VALUE_DATE': '',
            'BOOKING_DATE': '',
            'MOVEMENT_TYPE': '',
            'CURRENCY_LAST_CHAR': '',
            'AMOUNT': '',
            'TNX_TYPE_CODE': '',
            'CLIENT_REFERENCE': '',
            'BANK_REFERENCE': '',
            'DESCRIPTION': '',
            'FIELD_86': '',
            'H_BANK_STATEMENT_REF': '',
            'H_SWIFT_CODE': '',
            'H_CLIENT_ACCOUNT_NUMBER': '',
            'H_STATEMENT_NUMBER': '',
            'H_PAGE_NUMBER': '',
            'H_MOVEMENT_TYPE': '',
            'H_BOOKING_DATE': '',
            'H_CURRENCY_CODE': '',
            'H_TOTAL_AMOUNT': '',
            'T_TIPO_SALDO': '',
            'T_FECHA_SALDO': '',
            'T_MONEDA': '',
            'T_SALDO_FINAL': '',
}
class Connection:

    def __init__(self):
        self.engine = self.create()

    def create(self):
        engine = create_engine(URL(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            role=SNOWFLAKE_ROLE,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        ))
        return engine

    def query_to_df(self, query) -> pd.DataFrame:
        with self.engine.connect() as conn:
            df = pd.DataFrame()
            for chunk in pd.read_sql_query(query, conn, chunksize=1000, coerce_float=False):
                df = df.append(chunk)
        df = df.reset_index()

        return df

    def query_to_value(self, query):
        with self.engine.connect() as conn:
            value = conn.execute(query)
        return value

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
            print(uri)
            
            if ".zip" in uri:
                
                bucket_dest = Bucket=s3_url.bucket
                obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)['Body'].read()
                zip_file = zipfile.ZipFile(io.BytesIO(obj))
                for fn in zip_file.namelist():
                    print('ingreso al for...')
                    # Now copy the files to the 'unzipped' S3 folder 
                    print(f"Copying file {fn}") 
                    s3.put_object( 
                        Body=zip_file.open(fn).read(),
                        # might need to replace above line with the one
                        # below for windows files 
                        # 
                        # Body=z.open(filename).read().decode("iso-8859-1").encode(encoding='UTF-8'), 
                        Bucket=s3_url.bucket, 
                        Key='AR/BANCOS_MANUAL/'+ fn 
                    )                     
                df = None
                return df   
                       
            else:
                obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)['Body'].read()    
            return obj
        else:
            with open(uri) as f:
                return f.read()

class ExcelMt940Parser:
    
    @staticmethod
    def file_stament_number_default(tabla):

        query = f"""
        select max(ifnull(h_statement_number, 0))::integer as statement_number from 
        inputs.{tabla}
        """
        conn = Connection()
        df = conn.query_to_df(query)
        statement_number = str(int(df.statement_number[0]) + 1)
        return statement_number
    
    @staticmethod
    def file_stament_number(tabla, fecha, account):
        # find the sourondings filenames:
        query = f"""
             select 
             'S' || RIGHT(DATE_PART(year, '{fecha}'::date), 2) || DAYOFYEAR('{fecha}'::date) as h_st
        """

        conn = Connection()
        df = conn.query_to_df(query)

        statement_number = df['h_st'][0]

        return statement_number

    @staticmethod
    def prepare_amount(monto):
        if '.' in monto:
            decimal = monto.split('.')[1]
            entero = monto.split('.')[0]
            if len(decimal) == 1:
                decimal = decimal + '0'
            elif len(decimal) == 0:
                decimal = decimal + '00'
            monto = entero + '.' + decimal
        else:
            monto = monto + '.00'
        monto =  monto[1:] if monto[0]=='-' else monto
        return monto

    @staticmethod
    def headers_and_trailers(sheet):
        end = 16
        i = 0
        header_and_trailer = {
            'fecha': '',
            'currency_code': '',
            'client_account_number': ''
        }
        while i <= end:
            value = sheet.cell_value(i, 0)
            if value == 'Fecha de Estado de Efectivo':
                header_and_trailer['fecha'] = pd.to_datetime(sheet.cell_value(i, 1)).strftime("%y%m%d")
            if value == 'Moneda y Tipo de Cuenta':
                header_and_trailer['currency_code']  = sheet.cell_value(i, 1)
            if value == 'Número y Nombre de la Cuenta':
                header_and_trailer['client_account_number']  = sheet.cell_value(i, 1)
            if value == 'Fecha de Ingreso':
                header_and_trailer['Saldo Inicial']  = ExcelMt940Parser.prepare_amount(str(sheet.cell_value(i-1, 0)))
                header_and_trailer['Saldo Final']  = ExcelMt940Parser.prepare_amount(str(sheet.cell_value(i-1, 2)))
            
                header_and_trailer['Tipo mov inicial'] = 'C' if float(sheet.cell_value(i-1, 0)) >= 0 else 'D'
                header_and_trailer['Tipo mov final'] = 'C' if float(sheet.cell_value(i-1, 2)) >= 0 else 'D'
            i += 1
        return header_and_trailer

    @staticmethod
    def saldo_inicial_y_final(sheet):
        pass
    

    
    header_line_dict = {
            'BRA': 15,
            'COL': 13,
            'UYU': 13,
            'ARG': 13,
            'AR_ONLINE': 13,
            'CLC': 13,
            'CLS': 13,
            'PEC': 13,
            'PEB': 13,
            'MEB': 13,
            'MEX': 13,
            'MES': 13,
            'MEP': 13}
        
    general_description_code_mapping = {
        'BRA':
            {
                'DEBIT TRANSFER' : {'dscr': 'CTC/123/', 'code': 'NTRF'},
                'ENTRY REVERSAL' : {'dscr': 'CTC/013/', 'code': 'NRTI'},
                'TED SENT REVERSAL' : {'dscr': 'CTC/245/', 'code': 'NRTI'},
                'TED SENT' : {'dscr': 'CTC/245/', 'code': 'NCLR'},
                'TRANSFER ACCR TO INST' : {'dscr': 'CTC/041/', 'code': 'NCMZ' },
                'default': {'dscr': 'CTC/041/', 'code': 'NCMZ' }
            },
        'COL':
            {
                'DEFERRED DR' : {'dscr': 'CTC/110/', 'code': 'NCOM'},
                'MISCELLANEOUS' : {'dscr': 'CTC/762/', 'code': 'NMSC'},
                'TRANSFER RJCT O RECVE O' : {'dscr': 'CTC/690/', 'code': 'NTRF'},
                'TRANSFER RECIB O RECHAZ O' : {'dscr': 'CTC/690/', 'code': 'NTRF'},
                'CREDIT CONTRIBUTION' : {'dscr': 'CTC/519/', 'code': 'NTRF'},
                'DEBITO DIFERIDO' : {'dscr': 'CTC/110/', 'code': 'NCOM'},
                'VENTA FX' : {'dscr': 'CTC/082/', 'code': 'NDDT'},
                'CHEQUES DEPOSITADOS' : {'dscr': 'CTC/025/', 'code': 'NTRF'},
                'CITITRANSFER TRANSFER' : {'dscr': 'CTC/017/', 'code': 'NTRF'},
                'TRANSFERENC CITITRANSF' : {'dscr': 'CTC/017/', 'code': 'NTRF'},
                'CONSUMO CITIBANKING' : {'dscr': 'CTC/006/', 'code': 'NCOM'},
                'CHECK DEPOSIT' : {'dscr': 'CTC/025/', 'code': 'NTRF'},
                'TAX ON COMIS' : {'dscr': 'CTC/357/', 'code': 'NTAX'},
                'IVA SOBRE COMISION' : {'dscr': 'CTC/357/', 'code': 'NTAX'},
                'DEBITO POR TRANSFERENCIA' : {'dscr': 'CTC/018/', 'code': 'NTRF'},
                'BANKING CONSUMING' : {'dscr': 'CTC/006/', 'code': 'NCOM'},
                'MISCELLANEOUS' : {'dscr': 'CTC/518/', 'code': 'NMSC'},
                'TRANSFER SENT': {'dscr': 'CTC/091/', 'code': 'NTRF'},
                'CHECK RETURN FOUNDS': {'dscr': 'CTC/514/', 'code': 'NRTI'},
                'TRANSFER ENVIADA': {'dscr': 'CTC/091/', 'code': 'NTRF'},
                'TRANSFERENC CITITRANSF': {'dscr': 'CTC/017/', 'code': 'NTRF'},
                'CITITRANSFER TRANSFER': {'dscr': 'CTC/017/', 'code': 'NTRF'},
                'CREDITO CONTRIBUCION': {'dscr': 'CTC/519/', 'code': 'NTRF'},
                'FX  SALE': {'dscr': 'CTC/082/', 'code': 'NDDT'},
                'DEBITO CONTRIBUCION 2331': {'dscr': 'CTC/019/', 'code': 'NTAX'},
                'DEBIT CONTRIBUTION 2331': {'dscr': 'CTC/019/', 'code': 'NTAX'},
                'DEV. ACH O CHEQUE FOND': {'dscr': 'CTC/514/', 'code': 'NRTI'},
                'default': {'dscr': 'CTC/690/', 'code': 'NTRF' }
            },
        'UYU':
            {
                'PAYLINK - Pago local' : {'dscr': 'CTC/500/', 'code': 'NTRF'},
                'TRF.RECIBIDA SPI' : {'dscr': 'CTC/067/', 'code': 'NTRF'},
                'PAYLINK Pago Cta Cta' : {'dscr': 'CTC/501/', 'code': 'NTRF'},

                'TRF.RECIBIDA SPI' : {'dscr': 'CTC/067/', 'code': 'NTRF'},
                'DEV.CONSUMO.MASTER' : {'dscr': 'CTC/136/', 'code': 'NCOM'},
                'CONCEPTO' : {'dscr': 'CTC/001/', 'code': 'NMSC'},
                'DEP.CHS.CTRA.O.BCOS.' : {'dscr': 'CTC/005/', 'code': 'NCHK'},
                'PAYLINK-COM.CHEQUES' : {'dscr': 'CTC/511/', 'code': 'NCOM'},
                'PAYLINK COM.PAG.LOC.' : {'dscr': 'CTC/508/', 'code': 'NCOM'},
                'LIQ.PAGOS MASTER' : {'dscr': 'CTC/135/', 'code': 'NCOM'},
                'COMIS.TR.TELEG.ENV.' : {'dscr': 'CTC/582/', 'code': 'NCOM'},
                'COMIS.TR.TELEG.REC.' : {'dscr': 'CTC/581/', 'code': 'NCOM'},
                'PAYLINK Pago Cta Cta' : {'dscr': 'CTC/501/', 'code': 'NTRF'},
                'default': {'dscr': 'CTC/500/', 'code': 'NTRF' }
            },
        'ARG':
            {
                'PAYLINK DATANET PROV': {'dscr': 'CTC/221/', 'code': 'NTRF'},
                'OUTGOING CREDIN': {'dscr': 'CTC/651/', 'code': 'NMSC'},
                'CREDIN SALIENTE': {'dscr': 'CTC/651/', 'code': 'NMSC'},
                'SETTINGS': {'dscr': 'CTC/385/', 'code': 'NMSC'},
                'PAYLINK AUTOM.DEBIT': {'dscr': 'CTC/478/', 'code': 'NTRF'},
                'PAYMENT TO SUPPLIERS TRAN': {'dscr': 'CTC/515/', 'code': 'NTRF'},
                'ARGENCARD CREDIT': {'dscr': 'CTC/171/', 'code': 'NTRF'},
                'CR.TRANSFER OWNER': {'dscr': 'CTC/190/', 'code': 'NTRF'},
                'PAYLINK DEBITO AUTOMATICO': {'dscr': 'CTC/478/', 'code': 'NTRF'},
                'TRANSF.PAGO PROVEEDORES': {'dscr': 'CTC/515/', 'code': 'NTRF'},
                'ACREDITACION ARGENCARD': {'dscr': 'CTC/171/', 'code': 'NTRF'},
                'CREDIT MAESTRO CARD': {'dscr': 'CTC/501/', 'code': 'NMSC'},
                'DEBITS/ARGENCARD': {'dscr': 'CTC/331/', 'code': 'NTRF'},
                'CREDIT DINERS FD MERCHANT': {'dscr': 'CTC/032/', 'code': 'NTCK'},
                'TRANSFERENCE INTERBANKING': {'dscr': 'CTC/165/', 'code': 'NTRF'},
                'CR.TRANSFER TITULAR': {'dscr': 'CTC/190/', 'code': 'NTRF'},
                'DEBITOS ARGENCARD': {'dscr': 'CTC/331/', 'code': 'NTRF'},
                'CREDITO TARJETA MAESTRO': {'dscr': 'CTC/501/', 'code': 'NMSC'},
                'RETENTION IMPOSED PROVINC': {'dscr': 'CTC/485/', 'code': 'NCHG'},
                'RETENTION IMPOSED IIBB SI': {'dscr': 'CTC/542/', 'code': 'NCHG'},
                'PAYLINK B2B CTASPROPIA': {'dscr': 'CTC/284/', 'code': 'NMSC'},
                'AJUSTES': {'dscr': 'CTC/385/', 'code': 'NMSC'},
                'CREDITO DINERS FD COMERCI': {'dscr': 'CTC/032/', 'code': 'NTCK'},
                'TRASNFERENCE INTERBANKING': {'dscr': 'CTC/189/', 'code': 'NTRF'},
                'OUTGOING MEP TRANSFER WIT': {'dscr': 'CTC/003/', 'code': 'NTRF'},
                'TRANSFERENCIA INTERBANKIN': {'dscr': 'CTC/165/', 'code': 'NTRF'},
                'CREDIT CITI ACCOUNT SAME': {'dscr': 'CTC/586/', 'code': 'NMSC'},
                'DEP.CASH/CITI CHECK': {'dscr': 'CTC/061/', 'code': 'NCOL'},
                'DEBIT DINERS FD MERCHANTS': {'dscr': 'CTC/030/', 'code': 'NTRF'},
                'DEBIT MAESTRO CARD': {'dscr': 'CTC/500/', 'code': 'NMSC'},
                'VAT 21%': {'dscr': 'CTC/242/', 'code': 'NCHG'},
                'RETENCION IMPUESTO PROVIN': {'dscr': 'CTC/485/', 'code': 'NCHG'},
                'RECAUDACION IIBB SIRCREB.': {'dscr': 'CTC/542/', 'code': 'NCHG'},
                'REVERSAL': {'dscr': 'CTC/287/', 'code': 'NRTI'},
                'DEBITO DINERS FD COMERCIO': {'dscr': 'CTC/030/', 'code': 'NTRF'},
                'IVA 21%': {'dscr': 'CTC/242/', 'code': 'NCHG'},
                'DEBITO TARJETA MAESTRO': {'dscr': 'CTC/500/', 'code': 'NMSC'},
                'TRANSFERENCIA MEP ENVIADA': {'dscr': 'CTC/003/', 'code': 'NTRF'},
                'TRANSFERENCIA INTERBANKIN': {'dscr': 'CTC/189/', 'code': 'NTRF'},
                'RVS.RETENTION IMPOSED IIB': {'dscr': 'CTC/543/', 'code': 'NCHG'},
                'RETURN RETENTION IMPOSED': {'dscr': 'CTC/529/', 'code': 'NCHG'},
                'CR.TRANSFERENCIA TITULAR': {'dscr': 'CTC/586/', 'code': 'NMSC'},
                'ADJUSTMENT': {'dscr': 'CTC/425/', 'code': 'NMSC'},
                'ISSUANCE GUARANTEE / STAN': {'dscr': 'CTC/022/', 'code': 'NCOM'},
                'CHECK DEPOSIT-48 HRS': {'dscr': 'CTC/063/', 'code': 'NCHK'},
                'YOUR INSTRUCTIONS TO DEBI': {'dscr': 'CTC/304/', 'code': 'NSEC'},
                'SERVICES COMMISSION INTER': {'dscr': 'CTC/214/', 'code': 'NCOM'},
                'DEBIT COMISSION OF LOCAL': {'dscr': 'CTC/593/', 'code': 'NCOM'},
                'INCOMING MEP TRANSFER WIT': {'dscr': 'CTC/211/', 'code': 'NTRF'},
                'CASH ADM FEE': {'dscr': 'CTC/228/', 'code': 'NCOM'},
                'INTEREST EARNED': {'dscr': 'CTC/078/', 'code': 'NINT'},
                'AUTOM CREDIT PAYLINK': {'dscr': 'CTC/360/', 'code': 'NTRF'},
                'COMISION SERVICIOS PAGOS': {'dscr': 'CTC/214/', 'code': 'NCOM'},
                'RETENTION': {'dscr': 'CTC/044/', 'code': 'NCHG'},
                'COMISION PAYLINK': {'dscr': 'CTC/219/', 'code': 'NCOM'},
                'RETENCION PERCEPCION IIBB': {'dscr': 'CTC/079/', 'code': 'NCHG'},
                'DEBIN CREDIT': {'dscr': 'CTC/647/', 'code': 'NMSC'},
                'ELECTRONIC TRANSFERS': {'dscr': 'CTC/256/', 'code': 'NTRF'},
                'PAYLINK-FEES': {'dscr': 'CTC/219/', 'code': 'NCOM'},
                'RETENTION TAX SELLOS': {'dscr': 'CTC/628/', 'code': 'NCHG'},
                'RETENTION PER.IIBB CABA': {'dscr': 'CTC/079/', 'code': 'NCHG'},
                'VAT REVERSAL 21%': {'dscr': 'CTC/152/', 'code': 'NCHG'},
                'FEES PREV.CHARGED REFUND': {'dscr': 'CTC/151/', 'code': 'NCOM'},
                'COMISION COPIA CHEQUE': {'dscr': 'CTC/115/', 'code': 'NCOM'},
                'COBRO DE COMISION POR TRA': {'dscr': 'CTC/593/', 'code': 'NCOM'},
                'ADMIN.EFECTIVO COMISION': {'dscr': 'CTC/228/', 'code': 'NCOM'},
                'default' : {'dscr': 'CTC/515/', 'code': 'NTRF'}
            },
        'AR_ONLINE':
            {
                'PAYLINK DATANET PROV': {'dscr': 'CTC/221/', 'code': 'NTRF'},
                'OUTGOING CREDIN': {'dscr': 'CTC/651/', 'code': 'NMSC'},
                'CREDIN SALIENTE': {'dscr': 'CTC/651/', 'code': 'NMSC'},
                'SETTINGS': {'dscr': 'CTC/385/', 'code': 'NMSC'},
                'PAYLINK AUTOM.DEBIT': {'dscr': 'CTC/478/', 'code': 'NTRF'},
                'PAYMENT TO SUPPLIERS TRAN': {'dscr': 'CTC/515/', 'code': 'NTRF'},
                'ARGENCARD CREDIT': {'dscr': 'CTC/171/', 'code': 'NTRF'},
                'CR.TRANSFER OWNER': {'dscr': 'CTC/190/', 'code': 'NTRF'},
                'PAYLINK DEBITO AUTOMATICO': {'dscr': 'CTC/478/', 'code': 'NTRF'},
                'TRANSF.PAGO PROVEEDORES': {'dscr': 'CTC/515/', 'code': 'NTRF'},
                'ACREDITACION ARGENCARD': {'dscr': 'CTC/171/', 'code': 'NTRF'},
                'CREDIT MAESTRO CARD': {'dscr': 'CTC/501/', 'code': 'NMSC'},
                'DEBITS/ARGENCARD': {'dscr': 'CTC/331/', 'code': 'NTRF'},
                'CREDIT DINERS FD MERCHANT': {'dscr': 'CTC/032/', 'code': 'NTCK'},
                'TRANSFERENCE INTERBANKING': {'dscr': 'CTC/165/', 'code': 'NTRF'},
                'CR.TRANSFER TITULAR': {'dscr': 'CTC/190/', 'code': 'NTRF'},
                'DEBITOS ARGENCARD': {'dscr': 'CTC/331/', 'code': 'NTRF'},
                'CREDITO TARJETA MAESTRO': {'dscr': 'CTC/501/', 'code': 'NMSC'},
                'RETENTION IMPOSED PROVINC': {'dscr': 'CTC/485/', 'code': 'NCHG'},
                'RETENTION IMPOSED IIBB SI': {'dscr': 'CTC/542/', 'code': 'NCHG'},
                'PAYLINK B2B CTASPROPIA': {'dscr': 'CTC/284/', 'code': 'NMSC'},
                'AJUSTES': {'dscr': 'CTC/385/', 'code': 'NMSC'},
                'CREDITO DINERS FD COMERCI': {'dscr': 'CTC/032/', 'code': 'NTCK'},
                'TRASNFERENCE INTERBANKING': {'dscr': 'CTC/189/', 'code': 'NTRF'},
                'OUTGOING MEP TRANSFER WIT': {'dscr': 'CTC/003/', 'code': 'NTRF'},
                'TRANSFERENCIA INTERBANKIN': {'dscr': 'CTC/165/', 'code': 'NTRF'},
                'CREDIT CITI ACCOUNT SAME': {'dscr': 'CTC/586/', 'code': 'NMSC'},
                'DEP.CASH/CITI CHECK': {'dscr': 'CTC/061/', 'code': 'NCOL'},
                'DEBIT DINERS FD MERCHANTS': {'dscr': 'CTC/030/', 'code': 'NTRF'},
                'DEBIT MAESTRO CARD': {'dscr': 'CTC/500/', 'code': 'NMSC'},
                'VAT 21%': {'dscr': 'CTC/242/', 'code': 'NCHG'},
                'RETENCION IMPUESTO PROVIN': {'dscr': 'CTC/485/', 'code': 'NCHG'},
                'RECAUDACION IIBB SIRCREB.': {'dscr': 'CTC/542/', 'code': 'NCHG'},
                'REVERSAL': {'dscr': 'CTC/287/', 'code': 'NRTI'},
                'DEBITO DINERS FD COMERCIO': {'dscr': 'CTC/030/', 'code': 'NTRF'},
                'IVA 21%': {'dscr': 'CTC/242/', 'code': 'NCHG'},
                'DEBITO TARJETA MAESTRO': {'dscr': 'CTC/500/', 'code': 'NMSC'},
                'TRANSFERENCIA MEP ENVIADA': {'dscr': 'CTC/003/', 'code': 'NTRF'},
                'TRANSFERENCIA INTERBANKIN': {'dscr': 'CTC/189/', 'code': 'NTRF'},
                'RVS.RETENTION IMPOSED IIB': {'dscr': 'CTC/543/', 'code': 'NCHG'},
                'RETURN RETENTION IMPOSED': {'dscr': 'CTC/529/', 'code': 'NCHG'},
                'CR.TRANSFERENCIA TITULAR': {'dscr': 'CTC/586/', 'code': 'NMSC'},
                'ADJUSTMENT': {'dscr': 'CTC/425/', 'code': 'NMSC'},
                'ISSUANCE GUARANTEE / STAN': {'dscr': 'CTC/022/', 'code': 'NCOM'},
                'CHECK DEPOSIT-48 HRS': {'dscr': 'CTC/063/', 'code': 'NCHK'},
                'YOUR INSTRUCTIONS TO DEBI': {'dscr': 'CTC/304/', 'code': 'NSEC'},
                'SERVICES COMMISSION INTER': {'dscr': 'CTC/214/', 'code': 'NCOM'},
                'DEBIT COMISSION OF LOCAL': {'dscr': 'CTC/593/', 'code': 'NCOM'},
                'INCOMING MEP TRANSFER WIT': {'dscr': 'CTC/211/', 'code': 'NTRF'},
                'CASH ADM FEE': {'dscr': 'CTC/228/', 'code': 'NCOM'},
                'INTEREST EARNED': {'dscr': 'CTC/078/', 'code': 'NINT'},
                'AUTOM CREDIT PAYLINK': {'dscr': 'CTC/360/', 'code': 'NTRF'},
                'COMISION SERVICIOS PAGOS': {'dscr': 'CTC/214/', 'code': 'NCOM'},
                'RETENTION': {'dscr': 'CTC/044/', 'code': 'NCHG'},
                'COMISION PAYLINK': {'dscr': 'CTC/219/', 'code': 'NCOM'},
                'RETENCION PERCEPCION IIBB': {'dscr': 'CTC/079/', 'code': 'NCHG'},
                'DEBIN CREDIT': {'dscr': 'CTC/647/', 'code': 'NMSC'},
                'ELECTRONIC TRANSFERS': {'dscr': 'CTC/256/', 'code': 'NTRF'},
                'PAYLINK-FEES': {'dscr': 'CTC/219/', 'code': 'NCOM'},
                'RETENTION TAX SELLOS': {'dscr': 'CTC/628/', 'code': 'NCHG'},
                'RETENTION PER.IIBB CABA': {'dscr': 'CTC/079/', 'code': 'NCHG'},
                'VAT REVERSAL 21%': {'dscr': 'CTC/152/', 'code': 'NCHG'},
                'FEES PREV.CHARGED REFUND': {'dscr': 'CTC/151/', 'code': 'NCOM'},
                'COMISION COPIA CHEQUE': {'dscr': 'CTC/115/', 'code': 'NCOM'},
                'COBRO DE COMISION POR TRA': {'dscr': 'CTC/593/', 'code': 'NCOM'},
                'ADMIN.EFECTIVO COMISION': {'dscr': 'CTC/228/', 'code': 'NCOM'},

                'REVERSOS CONTABLES' : {'dscr': 'CTC/283/', 'code': 'NTRF'},
                'DEBITO MANUAL OPERATORIA CREDIN DEBIN' : {'dscr': 'CTC/681/', 'code': 'NMSC'},
                'TR.INTERB.PROVEEDORES DOLARES' : {'dscr': 'CTC/088/', 'code': 'NTRF'},

                'CREDITO DEBIN' : {'dscr': 'CTC/647/', 'code': 'NMSC'},
                'CREDITO MANUAL OPERATORIA CREDIN DEBIN' : {'dscr': 'CTC/680/', 'code': 'NMSC'},

                'default' : {'dscr': 'CTC/515/', 'code': 'NTRF'}
            },
        'MEX':
            {
               'DEPOSITO DE': {'dscr': 'CTC/079/', 'code': 'NMSC'} ,
               'DEPOSITO S.B.C.': {'dscr': 'CTC/635/', 'code': 'NCHK'},
               'DEVOL DOCTO S.B.C.': {'dscr': 'CTC/312/', 'code': 'NCHK'} ,
               'default' : {'dscr': 'CTC/342/', 'code': 'NTRF'}
            },
        'MEB':
            {
               'CHEQUE DEVUELTO': {'dscr': 'CTC/MSC/', 'code': 'NCHK', 'field_86': ':86:/PT/FT/PY/CHEQUE DEVUELTO  RPA MANUAL'} ,
               'CB000000000': {'dscr': 'CTC/MSC/', 'code': 'NCOL', 'field_86': ':86:/PT/FT/PY/CB  RPA MANUAL'} ,
               'CI000000000': {'dscr': 'CTC/MSC/', 'code': 'NCOL', 'field_86': ':86:/PT/FT/PY/CI  RPA MANUAL'} ,
               'CE000000000': {'dscr': 'CTC/MSC/', 'code': 'NCOL', 'field_86': ':86:/PT/FT/PY/CC  RPA MANUAL'} ,
               'CC000000000': {'dscr': 'CTC/MSC/', 'code': 'NCOL', 'field_86': ':86:/PT/FT/PY/CC  RPA MANUAL'} ,               
               'default' : {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:RPA MANUAL'}
            },
        'MES':
            {
               'AB TRANSF TEF': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:RPA MANUAL'} ,
               'DEP EN EFECTIV': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:RPA MANUAL'} ,
               'AB TRASP BANCO': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:RPA MANUAL'} ,
               'AB TRANS ELECT': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:RPA MANUAL'} ,
               'DEP EFECT ATM': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:RPA MANUAL'} ,  
               'AB X COBRANZA': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:RPA MANUAL'} ,            
               'DEP CHEQ N CGO': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:RPA MANUAL'} ,               
               'DEP S B COBRO': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:RPA MANUAL'} ,             
               'DEP CBZA INTER': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:RPA MANUAL'} ,             
               'DEV CHEQ SBC': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:RPA MANUAL'} , 
               'AB                TRANS E': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:RPA MANUAL'},
               'DEP                EN EFE': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:RPA MANUAL'},
               'AB                 TRASP': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:RPA MANUAL'},
               'AB                 X COBR': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:RPA MANUAL'},
               'AB X C                OBR': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:RPA MANUAL'},
               'DEP                CHEQ N': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:RPA MANUAL'},
               'DEP CB                ZA': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:RPA MANUAL'},             
             
               'default' : {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:RPA MANUAL'}
            },
        'MEP':
            {
               'DEP MIXTO EFVO/DOCTO': {'dscr': 'CTC/MSC/', 'code': 'NCHK', 'field_86': ':86:422?00DEP MIXTO EFVO/DOCTO?20/EI/NONREF RPA MANUAL'} ,
               'ORDEN DE ABONO': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:364?00ORDEN DE ABONO?20/EI/0000001 RPA MANUAL'} ,
               'DEP CHEQUE BNM': {'dscr': 'CTC/MSC/', 'code': 'NCHK', 'field_86': ':86:902?00DEP CHEQUE BNM?20/EI/NONREF RPA MANUAL'} ,
               'DEPOSITO S.B.C.': {'dscr': 'CTC/MSC/', 'code': 'NCHK', 'field_86': ':86:635?00DEPOSITO S.B.C.?20/EI/651611665167 RPA MANUAL'} ,
               'default' : {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:RPA MANUAL TRASPASO'}
            },
        }


    @staticmethod
    def defaul_code(description, pais):
        description_code_mapping = ExcelMt940Parser.general_description_code_mapping.get(pais)
        code = description_code_mapping.get(description)

        if code == None:
            code = description_code_mapping.get('default').get('code')
        else:
            code = code.get('code')
        return code

    @staticmethod
    def defaul_description(description, pais):
        description_code_mapping = ExcelMt940Parser.general_description_code_mapping.get(pais)
        complete_description = description_code_mapping.get(description)

        if complete_description == None:
            complete_description = description_code_mapping.get('default').get('dscr') + description
        else:
            complete_description = complete_description.get('dscr')  +  description
        return complete_description

    @staticmethod
    def convert_client_reference_bancomer(line):
        if line[:2] in ('CB', 'CI', 'CC', 'CE'):
            return line[:11]
        else:
            return line 

    @staticmethod
    def defaul_field_86(description, pais):
        description_code_mapping = ExcelMt940Parser.general_description_code_mapping.get(pais)
        mapp = description_code_mapping.get(description)

        if mapp == None or (mapp != None and mapp.get('field_86') == None):
            field_86 = ':86:RPA MANUAL'
        else:
            field_86 = mapp.get('field_86')
        return field_86

    @staticmethod
    def parse(df, file_name, headers_and_trailers, pais):
        
        tablas = {'BRA': 'br_citi_swift',
                  'COL': 'co_citi_swift',
                  'UYU': 'uy_citi_swift',
                  'ARG': 'ar_citi_swift',
                  'AR_ONLINE': 'ar_citi_swift',
                  'MEB': 'mx_banco_bancomer_swift',
                  'MEX': 'mx_banco_banamex_swift',
                  'MES': 'mx_banco_santander_swift',
                  'MEP': 'mx_banco_banamex_swift'}
        nombres = {'BRA': 'MPB_102352A_',
                  'COL': 'MPO_102559A_',
                  'UYU': 'DRU_102862A_',
                  'ARG': 'MLA_102216A_',
                  'AR_ONLINE': 'MLA_102253A_',
                  'MEB': 'MLM_102604A_',
                  'MEX': 'MLM_102602A_',
                  'MES': 'MLM_102606A_',
                  'MEP': 'MEP_102020A_'}
        referencias_de_banco = {'BRA': 'MCL1' + pd.Timestamp.now().strftime('%y%m%d') + '0637',
                  'COL': '9400120193250111',
                  'UYU': '9400120193172211',
                  'ARG': '9402619360011221#',
                  'AR_ONLINE': '9404720010005608',
                  'MEB': '026170142275759',
                  'MEX': '9400120201430426',
                  'MES': '655014139/00521',
                  'MEP': '9400120201190426'}
        

        for c in df.columns:
            df[c] = df[c].astype(str).str.replace(',', '.')
            if c == 'Fecha de Valor':
                df[c] = pd.to_datetime(df[c], format='%m/%d/%Y').dt.strftime("%y%m%d")
            if c == 'Fecha de Ingreso':
                df[c] = pd.to_datetime(df[c], format='%m/%d/%Y').dt.strftime("%m%d")
            if c == 'Monto de la Transacción':
                df['movement_type'] = df['Monto de la Transacción'].apply(lambda x: 'D' if x[0]=='-' else 'C')
                df['Monto de la Transacción'] = df['Monto de la Transacción'].apply(lambda x: ExcelMt940Parser.prepare_amount(x))

        freq_date = df['Fecha de Valor'].value_counts().idxmax()
        
        df = df.replace('nan', '', regex = True)
        swift_dict = {
            'VALUE_DATE': df['Fecha de Valor'].tolist(),
            'BOOKING_DATE': df['Fecha de Ingreso'].tolist(),
            'MOVEMENT_TYPE': df['movement_type'].tolist(),
            'CURRENCY_LAST_CHAR': headers_and_trailers.get('currency_code')[-1],
            'AMOUNT': df['Monto de la Transacción'].tolist(),
            'TNX_TYPE_CODE': 
                            df['Descripción de la Transacción'].apply(lambda x: ExcelMt940Parser.defaul_code(x, pais)).tolist() if pais != 'MEB' else 
                            df['Referencia del Cliente'].apply(lambda x: ExcelMt940Parser.defaul_code(x[:11] if x[:2] in ('CB', 'CI', 'CC', 'CE') else x, pais)).tolist()
             ,
            'CLIENT_REFERENCE': df['Referencia del Cliente'].tolist(),
            'BANK_REFERENCE': df['Referencia del Banco'].tolist(),
            'DESCRIPTION': 
                            df['Descripción de la Transacción'].apply(lambda x: ExcelMt940Parser.defaul_description(x, pais)).tolist() if pais != 'MEB' else 
                            df['Referencia del Cliente'].apply(lambda x: ExcelMt940Parser.defaul_description(x[:11] if x[:2] in ('CB', 'CI', 'CC', 'CE') else x, pais)).tolist()
                            ,
            'FIELD_86': 
                            df['Descripción de la Transacción'].apply(lambda x: ExcelMt940Parser.defaul_field_86(x, pais)).tolist() if pais != 'MEB' else 
                            df['Referencia del Cliente'].apply(lambda x: ExcelMt940Parser.defaul_field_86(x[:11] if x[:2] in ('CB', 'CI', 'CC', 'CE') else x, pais)).tolist()
            ,
            'H_BANK_STATEMENT_REF': referencias_de_banco.get(pais),
            'H_SWIFT_CODE': '',
            'H_CLIENT_ACCOUNT_NUMBER': headers_and_trailers.get('client_account_number'),
            'H_STATEMENT_NUMBER': ExcelMt940Parser.file_stament_number(tabla=tablas[pais], \
                fecha=datetime.strptime(freq_date, "%y%m%d").strftime("%Y-%m-%d"), \
                account=nombres[pais]),
            'H_PAGE_NUMBER': '1',
            'H_MOVEMENT_TYPE': headers_and_trailers.get('Tipo mov inicial'),
            'H_BOOKING_DATE': headers_and_trailers.get('fecha'),
            'H_CURRENCY_CODE': headers_and_trailers.get('currency_code'),
            'H_TOTAL_AMOUNT': headers_and_trailers.get('Saldo Inicial'),
            'T_TIPO_SALDO': headers_and_trailers.get('Tipo mov final'),
            'T_FECHA_SALDO': headers_and_trailers.get('fecha'),
            'T_MONEDA': headers_and_trailers.get('currency_code'),
            'T_SALDO_FINAL': headers_and_trailers.get('Saldo Final')
            }
        final_df = pd.DataFrame(swift_dict)
        final_df['REPORT_DATE'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        final_df['FILENAME'] = nombres[pais] + pd.Timestamp.now().strftime('%Y%m%d') + df['Fecha de Ingreso'].tolist()[0] + '_RPA.xls' 
        final_df['RAW_UNIQUENESS'] = final_df.sort_values(['REPORT_DATE'], ascending=[False]) \
             .groupby(['MOVEMENT_TYPE', 'AMOUNT', 'DESCRIPTION', 'FIELD_86', 'VALUE_DATE']) \
             .cumcount() + 1
        final_df['ORIGINAL_FILENAME'] = file_name.split('/')[-1][:-4] + '_' + pd.Timestamp.now().strftime('%Y%m%d')  + df['Fecha de Ingreso'].tolist()[0] + '.XLS'
        final_df = final_df.where((pd.notnull(final_df)), '')

        return final_df

    @staticmethod
    def n_rows(sheet, start):
        end_of_file = False
        n_row = 1
        
        i = start
        while not end_of_file:
            value = sheet.cell_value(i, 0)
            if value == 'Criterios de Selección':
                n_row = i - start - 1
                end_of_file = True
                break
            i += 1
        return n_row

    @staticmethod
    def run(filename, **kwargs):
        print ('Version 1.2 + Bancos MLM MI')
        from xlrd.book import open_workbook_xls  
        #settings = ClientSetting.objects.filter(name='bank_settings').first()
        #FILE_TO_CHOOSE = settings.parameters
        try:
            settings = ClientSetting.objects.filter(name='bank_settings').first()
        except:
            settings = RDSConnector().session.query(ClientSettings).filter(ClientSettings.name == 'bank_settings').first()
        FILE_TO_CHOOSE = settings.parameters if settings else []
        if 'BRA' in filename:
            pais = 'BRA'
        elif 'COL' in filename:
            pais = 'COL'
        elif 'UYU' in filename or 'URU' in filename:
            pais = 'UYU'
        elif 'AR_' in filename:
            pais = 'ARG'
        elif 'ARG_ONLINE' in filename:
            pais = 'AR_ONLINE'
        elif 'CHI_CITI' in filename:
            pais = 'CLC'
        elif 'CHI_SANT' in filename or 'URU' in filename:
            pais = 'CLS'
        elif 'PE_CITI' in filename:
            pais = 'PEC'
        elif 'PE_BBVA' in filename:
            pais = 'PEB'
        elif 'MLM_BANCOMER' in filename:
            pais = 'MEB'
        elif 'MLM_BANAMEX' in filename:
            pais = 'MEX'    
        elif 'MLM_SANTANDER' in filename:
            pais = 'MES' 
        elif 'MEP_BANAMEX' in filename:
            pais = 'MEP' 
        else :
            pais = 'otro'
        
        obj = FileReader.read(filename)
        wb = open_workbook_xls(file_contents=obj)
        xl = pd.ExcelFile(wb)
        sheet = wb.sheet_by_index(0)
        start = ExcelMt940Parser.header_line_dict[pais]
        header_and_trailer = ExcelMt940Parser.headers_and_trailers(sheet)
        n_row = ExcelMt940Parser.n_rows(sheet, start)
        df = pd.read_excel(xl, header=start, nrows=n_row, usecols=[i for i in range(7)])
        trailers_index = df.loc[df['Fecha de Ingreso'] == '= Indica Saldos Calculados', :].index.tolist()
        col_names_index = df.loc[df['Fecha de Ingreso'] == 'Fecha de Ingreso', :].index.tolist()
        index_to_drop = [*trailers_index, *col_names_index]
        df = df.drop(index_to_drop).reset_index()
        df = ExcelMt940Parser.parse(df.copy(), file_name=filename.split('/')[-1], headers_and_trailers=header_and_trailer, pais=pais)    
        today = pd.Timestamp.now().day_name()
        
        for element in FILE_TO_CHOOSE:
            if 'BRA' == pais:
                if element['country'] == "BR" and element['account_number'] == '102352A':
                    for extraction in element['extraction']:
                        if extraction['day'] == today:
                            extraction_method = extraction['method']
                            if extraction_method == 'MANUAL':
                                pass
                            else:
                                df = df.head(0)
                                print('NO SE CARGO EL ARCHIVO POR SER DIA DE SWIFT', filename)

            if element['country'] == "CO" and element['account_number'] == '102559A':
                if 'COL' == pais:
                    for extraction in element['extraction']:
                        if extraction['day'] == today:
                            extraction_method = extraction['method']
                            if extraction_method == 'MANUAL':
                                pass
                            else:
                                df = df.head(0)
                                print('NO SE CARGO EL ARCHIVO POR SER DIA DE SWIFT', filename)

            if element['country'] == "UY" and element['account_number'] == '102862A':
                if 'UYU' == pais:
                    for extraction in element['extraction']:
                        if extraction['day'] == today:
                            extraction_method = extraction['method']
                            if extraction_method == 'MANUAL':
                                pass
                            else:
                                df = df.head(0)
                                print('NO SE CARGO EL ARCHIVO POR SER DIA DE SWIFT', filename)

            if element['country'] == "AR" and element['account_number'] == '102216A':
                if 'ARG' == pais:
                    for extraction in element['extraction']:
                        if extraction['day'] == today:
                            extraction_method = extraction['method']
                            if extraction_method == 'MANUAL':
                                pass
                            else:
                                df = df.head(0)
                                print('NO SE CARGO EL ARCHIVO POR SER DIA DE SWIFT', filename)
            
            if element['country'] == "AR" and element['account_number'] == '102253A':
                if 'AR_ONLINE' == pais:
                    for extraction in element['extraction']:
                        if extraction['day'] == today:
                            extraction_method = extraction['method']
                            if extraction_method == 'MANUAL':
                                pass
                            else:
                                df = df.head(0)
                                print('NO SE CARGO EL ARCHIVO POR SER DIA DE SWIFT', filename)


        
        print('PROCESADO', filename)
        return df


class CSVMt940Parser:

    @staticmethod
    def file_stament_number_default(tabla):

        query = f"""
        select max(ifnull(h_statement_number, 0))::integer as statement_number from 
        inputs.{tabla}
        """
        conn = Connection()
        df = conn.query_to_df(query)
        statement_number = str(int(df.statement_number[0]) + 1)
        return statement_number

    @staticmethod
    def file_stament_number(tabla, fecha, account):
        # find the sourondings filenames:
        query = f"""
             select 
             'S' || RIGHT(DATE_PART(year, '{fecha}'::date), 2) || DAYOFYEAR('{fecha}'::date) as h_st
        """

        conn = Connection()
        df = conn.query_to_df(query)

        statement_number = df['h_st'][0]

        return statement_number

    @staticmethod
    def prepare_amount(monto):
        if '.' in monto:
            decimal = monto.split('.')[-1]
            entero = monto.split('.')[0]
            if len(decimal) == 1:
                decimal = decimal + '0'
            elif len(decimal) == 0:
                decimal = decimal + '00'
            monto = entero + '.' + decimal
        else:
            monto = monto + '.00'
        monto = monto[1:] if monto[0] == '-' else monto
        return monto


    general_description_code_mapping = {
        'ARG':
            {
                'PAYLINK DATANET PROV': {'dscr': 'CTC/221/', 'code': 'NTRF'},
                'OUTGOING CREDIN': {'dscr': 'CTC/651/', 'code': 'NMSC'},
                'CREDIN SALIENTE': {'dscr': 'CTC/651/', 'code': 'NMSC'},
                'SETTINGS': {'dscr': 'CTC/385/', 'code': 'NMSC'},
                'PAYLINK AUTOM.DEBIT': {'dscr': 'CTC/478/', 'code': 'NTRF'},
                'PAYMENT TO SUPPLIERS TRAN': {'dscr': 'CTC/515/', 'code': 'NTRF'},
                'ARGENCARD CREDIT': {'dscr': 'CTC/171/', 'code': 'NTRF'},
                'CR.TRANSFER OWNER': {'dscr': 'CTC/190/', 'code': 'NTRF'},
                'PAYLINK DEBITO AUTOMATICO': {'dscr': 'CTC/478/', 'code': 'NTRF'},
                'TRANSF.PAGO PROVEEDORES': {'dscr': 'CTC/515/', 'code': 'NTRF'},
                'ACREDITACION ARGENCARD': {'dscr': 'CTC/171/', 'code': 'NTRF'},
                'CREDIT MAESTRO CARD': {'dscr': 'CTC/501/', 'code': 'NMSC'},
                'DEBITS/ARGENCARD': {'dscr': 'CTC/331/', 'code': 'NTRF'},
                'CREDIT DINERS FD MERCHANT': {'dscr': 'CTC/032/', 'code': 'NTCK'},
                'TRANSFERENCE INTERBANKING': {'dscr': 'CTC/165/', 'code': 'NTRF'},
                'CR.TRANSFER TITULAR': {'dscr': 'CTC/190/', 'code': 'NTRF'},
                'DEBITOS ARGENCARD': {'dscr': 'CTC/331/', 'code': 'NTRF'},
                'CREDITO TARJETA MAESTRO': {'dscr': 'CTC/501/', 'code': 'NMSC'},
                'RETENTION IMPOSED PROVINC': {'dscr': 'CTC/485/', 'code': 'NCHG'},
                'RETENTION IMPOSED IIBB SI': {'dscr': 'CTC/542/', 'code': 'NCHG'},
                'PAYLINK B2B CTASPROPIA': {'dscr': 'CTC/284/', 'code': 'NMSC'},
                'AJUSTES': {'dscr': 'CTC/385/', 'code': 'NMSC'},
                'CREDITO DINERS FD COMERCI': {'dscr': 'CTC/032/', 'code': 'NTCK'},
                'TRASNFERENCE INTERBANKING': {'dscr': 'CTC/189/', 'code': 'NTRF'},
                'OUTGOING MEP TRANSFER WIT': {'dscr': 'CTC/003/', 'code': 'NTRF'},
                'TRANSFERENCIA INTERBANKIN': {'dscr': 'CTC/165/', 'code': 'NTRF'},
                'CREDIT CITI ACCOUNT SAME': {'dscr': 'CTC/586/', 'code': 'NMSC'},
                'DEP.CASH/CITI CHECK': {'dscr': 'CTC/061/', 'code': 'NCOL'},
                'DEBIT DINERS FD MERCHANTS': {'dscr': 'CTC/030/', 'code': 'NTRF'},
                'DEBIT MAESTRO CARD': {'dscr': 'CTC/500/', 'code': 'NMSC'},
                'VAT 21%': {'dscr': 'CTC/242/', 'code': 'NCHG'},
                'RETENCION IMPUESTO PROVIN': {'dscr': 'CTC/485/', 'code': 'NCHG'},
                'RECAUDACION IIBB SIRCREB.': {'dscr': 'CTC/542/', 'code': 'NCHG'},
                'REVERSAL': {'dscr': 'CTC/287/', 'code': 'NRTI'},
                'DEBITO DINERS FD COMERCIO': {'dscr': 'CTC/030/', 'code': 'NTRF'},
                'IVA 21%': {'dscr': 'CTC/242/', 'code': 'NCHG'},
                'DEBITO TARJETA MAESTRO': {'dscr': 'CTC/500/', 'code': 'NMSC'},
                'TRANSFERENCIA MEP ENVIADA': {'dscr': 'CTC/003/', 'code': 'NTRF'},
                'TRANSFERENCIA INTERBANKIN': {'dscr': 'CTC/189/', 'code': 'NTRF'},
                'RVS.RETENTION IMPOSED IIB': {'dscr': 'CTC/543/', 'code': 'NCHG'},
                'RETURN RETENTION IMPOSED': {'dscr': 'CTC/529/', 'code': 'NCHG'},
                'CR.TRANSFERENCIA TITULAR': {'dscr': 'CTC/586/', 'code': 'NMSC'},
                'ADJUSTMENT': {'dscr': 'CTC/425/', 'code': 'NMSC'},
                'ISSUANCE GUARANTEE / STAN': {'dscr': 'CTC/022/', 'code': 'NCOM'},
                'CHECK DEPOSIT-48 HRS': {'dscr': 'CTC/063/', 'code': 'NCHK'},
                'YOUR INSTRUCTIONS TO DEBI': {'dscr': 'CTC/304/', 'code': 'NSEC'},
                'SERVICES COMMISSION INTER': {'dscr': 'CTC/214/', 'code': 'NCOM'},
                'DEBIT COMISSION OF LOCAL': {'dscr': 'CTC/593/', 'code': 'NCOM'},
                'INCOMING MEP TRANSFER WIT': {'dscr': 'CTC/211/', 'code': 'NTRF'},
                'CASH ADM FEE': {'dscr': 'CTC/228/', 'code': 'NCOM'},
                'INTEREST EARNED': {'dscr': 'CTC/078/', 'code': 'NINT'},
                'AUTOM CREDIT PAYLINK': {'dscr': 'CTC/360/', 'code': 'NTRF'},
                'COMISION SERVICIOS PAGOS': {'dscr': 'CTC/214/', 'code': 'NCOM'},
                'RETENTION': {'dscr': 'CTC/044/', 'code': 'NCHG'},
                'COMISION PAYLINK': {'dscr': 'CTC/219/', 'code': 'NCOM'},
                'RETENCION PERCEPCION IIBB': {'dscr': 'CTC/079/', 'code': 'NCHG'},
                'DEBIN CREDIT': {'dscr': 'CTC/647/', 'code': 'NMSC'},
                'ELECTRONIC TRANSFERS': {'dscr': 'CTC/256/', 'code': 'NTRF'},
                'PAYLINK-FEES': {'dscr': 'CTC/219/', 'code': 'NCOM'},
                'RETENTION TAX SELLOS': {'dscr': 'CTC/628/', 'code': 'NCHG'},
                'RETENTION PER.IIBB CABA': {'dscr': 'CTC/079/', 'code': 'NCHG'},
                'VAT REVERSAL 21%': {'dscr': 'CTC/152/', 'code': 'NCHG'},
                'FEES PREV.CHARGED REFUND': {'dscr': 'CTC/151/', 'code': 'NCOM'},
                'COMISION COPIA CHEQUE': {'dscr': 'CTC/115/', 'code': 'NCOM'},
                'COBRO DE COMISION POR TRA': {'dscr': 'CTC/593/', 'code': 'NCOM'},
                'ADMIN.EFECTIVO COMISION': {'dscr': 'CTC/228/', 'code': 'NCOM'},
                'REVERSOS CONTABLES': {'dscr': 'CTC/238/', 'code': 'NTRF'},
                'DEBITO MANUAL OPERATORIA CREDIN DEBIN': {'dscr': 'CTC/681/', 'code': 'NMSC'},
                'MANUAL DEBIT CREDIN DEBIN PRODUCT': {'dscr': 'CTC/681/', 'code': 'NMSC'},
                'TR.INTERB.PROVEEDORES DOLARES': {'dscr': 'CTC/088/', 'code': 'NTRF'},
                'CREDITO DEBIN': {'dscr': 'CTC/647/', 'code': 'NMSC'},
                'CREDITO MANUAL OPERATORIA CREDIN DEBIN': {'dscr': 'CTC/680/', 'code': 'NMSC'},
                'default': {'dscr': 'CTC/515/', 'code': 'NTRF'}
            },
        'AR_ONLINE':
            {
                'PAYLINK DATANET PROV': {'dscr': 'CTC/221/', 'code': 'NTRF'},
                'OUTGOING CREDIN': {'dscr': 'CTC/651/', 'code': 'NMSC'},
                'CREDIN SALIENTE': {'dscr': 'CTC/651/', 'code': 'NMSC'},
                'SETTINGS': {'dscr': 'CTC/385/', 'code': 'NMSC'},
                'PAYLINK AUTOM.DEBIT': {'dscr': 'CTC/478/', 'code': 'NTRF'},
                'PAYMENT TO SUPPLIERS TRAN': {'dscr': 'CTC/515/', 'code': 'NTRF'},
                'ARGENCARD CREDIT': {'dscr': 'CTC/171/', 'code': 'NTRF'},
                'CR.TRANSFER OWNER': {'dscr': 'CTC/190/', 'code': 'NTRF'},
                'PAYLINK DEBITO AUTOMATICO': {'dscr': 'CTC/478/', 'code': 'NTRF'},
                'TRANSF.PAGO PROVEEDORES': {'dscr': 'CTC/515/', 'code': 'NTRF'},
                'ACREDITACION ARGENCARD': {'dscr': 'CTC/171/', 'code': 'NTRF'},
                'CREDIT MAESTRO CARD': {'dscr': 'CTC/501/', 'code': 'NMSC'},
                'DEBITS/ARGENCARD': {'dscr': 'CTC/331/', 'code': 'NTRF'},
                'CREDIT DINERS FD MERCHANT': {'dscr': 'CTC/032/', 'code': 'NTCK'},
                'TRANSFERENCE INTERBANKING': {'dscr': 'CTC/165/', 'code': 'NTRF'},
                'CR.TRANSFER TITULAR': {'dscr': 'CTC/190/', 'code': 'NTRF'},
                'DEBITOS ARGENCARD': {'dscr': 'CTC/331/', 'code': 'NTRF'},
                'CREDITO TARJETA MAESTRO': {'dscr': 'CTC/501/', 'code': 'NMSC'},
                'RETENTION IMPOSED PROVINC': {'dscr': 'CTC/485/', 'code': 'NCHG'},
                'RETENTION IMPOSED IIBB SI': {'dscr': 'CTC/542/', 'code': 'NCHG'},
                'PAYLINK B2B CTASPROPIA': {'dscr': 'CTC/284/', 'code': 'NMSC'},
                'AJUSTES': {'dscr': 'CTC/385/', 'code': 'NMSC'},
                'CREDITO DINERS FD COMERCI': {'dscr': 'CTC/032/', 'code': 'NTCK'},
                'TRASNFERENCE INTERBANKING': {'dscr': 'CTC/189/', 'code': 'NTRF'},
                'OUTGOING MEP TRANSFER WIT': {'dscr': 'CTC/003/', 'code': 'NTRF'},
                'TRANSFERENCIA INTERBANKIN': {'dscr': 'CTC/165/', 'code': 'NTRF'},
                'CREDIT CITI ACCOUNT SAME': {'dscr': 'CTC/586/', 'code': 'NMSC'},
                'DEP.CASH/CITI CHECK': {'dscr': 'CTC/061/', 'code': 'NCOL'},
                'DEBIT DINERS FD MERCHANTS': {'dscr': 'CTC/030/', 'code': 'NTRF'},
                'DEBIT MAESTRO CARD': {'dscr': 'CTC/500/', 'code': 'NMSC'},
                'VAT 21%': {'dscr': 'CTC/242/', 'code': 'NCHG'},
                'RETENCION IMPUESTO PROVIN': {'dscr': 'CTC/485/', 'code': 'NCHG'},
                'RECAUDACION IIBB SIRCREB.': {'dscr': 'CTC/542/', 'code': 'NCHG'},
                'REVERSAL': {'dscr': 'CTC/287/', 'code': 'NRTI'},
                'DEBITO DINERS FD COMERCIO': {'dscr': 'CTC/030/', 'code': 'NTRF'},
                'IVA 21%': {'dscr': 'CTC/242/', 'code': 'NCHG'},
                'DEBITO TARJETA MAESTRO': {'dscr': 'CTC/500/', 'code': 'NMSC'},
                'TRANSFERENCIA MEP ENVIADA': {'dscr': 'CTC/003/', 'code': 'NTRF'},
                'TRANSFERENCIA INTERBANKIN': {'dscr': 'CTC/189/', 'code': 'NTRF'},
                'RVS.RETENTION IMPOSED IIB': {'dscr': 'CTC/543/', 'code': 'NCHG'},
                'RETURN RETENTION IMPOSED': {'dscr': 'CTC/529/', 'code': 'NCHG'},
                'CR.TRANSFERENCIA TITULAR': {'dscr': 'CTC/586/', 'code': 'NMSC'},
                'ADJUSTMENT': {'dscr': 'CTC/425/', 'code': 'NMSC'},
                'ISSUANCE GUARANTEE / STAN': {'dscr': 'CTC/022/', 'code': 'NCOM'},
                'CHECK DEPOSIT-48 HRS': {'dscr': 'CTC/063/', 'code': 'NCHK'},
                'YOUR INSTRUCTIONS TO DEBI': {'dscr': 'CTC/304/', 'code': 'NSEC'},
                'SERVICES COMMISSION INTER': {'dscr': 'CTC/214/', 'code': 'NCOM'},
                'DEBIT COMISSION OF LOCAL': {'dscr': 'CTC/593/', 'code': 'NCOM'},
                'INCOMING MEP TRANSFER WIT': {'dscr': 'CTC/211/', 'code': 'NTRF'},
                'CASH ADM FEE': {'dscr': 'CTC/228/', 'code': 'NCOM'},
                'INTEREST EARNED': {'dscr': 'CTC/078/', 'code': 'NINT'},
                'AUTOM CREDIT PAYLINK': {'dscr': 'CTC/360/', 'code': 'NTRF'},
                'COMISION SERVICIOS PAGOS': {'dscr': 'CTC/214/', 'code': 'NCOM'},
                'RETENTION': {'dscr': 'CTC/044/', 'code': 'NCHG'},
                'COMISION PAYLINK': {'dscr': 'CTC/219/', 'code': 'NCOM'},
                'RETENCION PERCEPCION IIBB': {'dscr': 'CTC/079/', 'code': 'NCHG'},
                'DEBIN CREDIT': {'dscr': 'CTC/647/', 'code': 'NMSC'},
                'ELECTRONIC TRANSFERS': {'dscr': 'CTC/256/', 'code': 'NTRF'},
                'PAYLINK-FEES': {'dscr': 'CTC/219/', 'code': 'NCOM'},
                'RETENTION TAX SELLOS': {'dscr': 'CTC/628/', 'code': 'NCHG'},
                'RETENTION PER.IIBB CABA': {'dscr': 'CTC/079/', 'code': 'NCHG'},
                'VAT REVERSAL 21%': {'dscr': 'CTC/152/', 'code': 'NCHG'},
                'FEES PREV.CHARGED REFUND': {'dscr': 'CTC/151/', 'code': 'NCOM'},
                'COMISION COPIA CHEQUE': {'dscr': 'CTC/115/', 'code': 'NCOM'},
                'COBRO DE COMISION POR TRA': {'dscr': 'CTC/593/', 'code': 'NCOM'},
                'ADMIN.EFECTIVO COMISION': {'dscr': 'CTC/228/', 'code': 'NCOM'},
                'REVERSOS CONTABLES' : {'dscr': 'CTC/238/', 'code': 'NTRF'},
                'DEBITO MANUAL OPERATORIA CREDIN DEBIN' : {'dscr': 'CTC/681/', 'code': 'NMSC'},
                'DEBITO MANUAL OPERATORIA' : {'dscr': 'CTC/681/', 'code': 'NTRF'},
                'MANUAL DEBIT CREDIN DEBIN PRODUCT': {'dscr': 'CTC/681/', 'code': 'NMSC'},
                'TR.INTERB.PROVEEDORES DOLARES' : {'dscr': 'CTC/088/', 'code': 'NTRF'},
                'CREDITO DEBIN' : {'dscr': 'CTC/647/', 'code': 'NMSC'},
                'CREDITO MANUAL OPERATORIA CREDIN DEBIN' : {'dscr': 'CTC/680/', 'code': 'NMSC'},
                'MANUAL CREDIT CREDIN DEBIN':{'dscr': 'CTC/680/', 'code': 'NMSC'},
                'IMPORT RELATED FREIGHT PAID THRU LO': {'dscr': 'CTC/634/', 'code': 'NMSC'},
                'REVERSAL PAYMENT':{'dscr': 'CTC/283/', 'code': 'NMSC'},
                'default' : {'dscr': 'CTC/515/', 'code': 'NTRF'}
            }


    }

    @staticmethod
    def defaul_code(description, pais):
        description_code_mapping = CSVMt940Parser.general_description_code_mapping.get(pais)
        code = description_code_mapping.get(description)

        if description_code_mapping.get(description) is None:
            code = description_code_mapping.get('default').get('code')
        else:
            code = code.get('code')
        return code

    @staticmethod
    def defaul_description(description, pais):
        description_code_mapping = CSVMt940Parser.general_description_code_mapping.get(pais)
        complete_description = description_code_mapping.get(description)

        if complete_description is None:
            complete_description = description_code_mapping.get('default').get('dscr') + description
        else:
            complete_description = complete_description.get('dscr')  +  description
        return complete_description



    @staticmethod
    def relevamiento(row):

        #print( codigo, xtra_info_data, bank_ref_no_data, ref_no_data)    
        #return

        if row['TRAN_COD']=='647':
            return row.XTRA_INFO[44:64]    
        
        elif row['TRAN_COD']=='680':
            return row.XTRA_INFO[32:54]    
        
        elif row['TRAN_COD']=='681':
            return row.BANK_REF_NO
        
        elif row['TRAN_COD']=='634':
            return row.REF_NO
        
        elif row['TRAN_COD']=='651':
            return row.REF_NO
        
        else:
            return row.REF_NO
         


    @staticmethod
    def parse(df, file_name, pais):
        tablas = {'ARG': 'ar_citi_swift',
                  'AR_ONLINE': 'ar_citi_swift'}
        nombres = {'ARG': 'MLA_102216A_',
                  'AR_ONLINE': 'MLA_102253A_'}
        referencias_de_banco = {'ARG': '9402619360011221',
                  'AR_ONLINE': '9404720010005608'}

        for c in df.columns:
            if 'DT' in c:
                df[c] = pd.to_datetime(df[c]).dt.strftime("%y%m%d")
            if c == 'TRAN_AMT':
                df['Monto_de_la_Transaccion'] = df[c].apply(lambda x: CSVMt940Parser.prepare_amount(x))
            if c=='TRAN_COD':    
                df['withdraw_id'] = df.apply(lambda x: CSVMt940Parser.relevamiento(x), axis=1)
             
        freq_date = df['TRAN_POST_DT'].value_counts().idxmax()

        totalAmount=df['Monto_de_la_Transaccion'].astype(float).sum()
        
        totalAmountDB= df[ df['CR_DR_MRK']=='D' ].Monto_de_la_Transaccion.astype(float).sum()
        totalAmountCR= df[ df.CR_DR_MRK=='C' ].Monto_de_la_Transaccion.astype(float).sum()
        netAmount=totalAmountCR-totalAmountDB

        
        df['COD_DESC']= 'CTC/' + df['TRAN_COD']+'/'+df['TRAN_TYP_DES']
        #print(df.columns)

        #TODO: POR DEFINIR
        if netAmount <= 0:
            header_mov_type = 'D'
        else:
            header_mov_type = 'C'

        df = df.replace('nan', '', regex = True)
        swift_dict = {
            'VALUE_DATE': df['TRAN_VAL_DT'].tolist(),
            'BOOKING_DATE': pd.to_datetime(df['TRAN_POST_DT'],format= '%y%m%d').dt.strftime("%m%d").tolist(),
            'MOVEMENT_TYPE': df['CR_DR_MRK'].tolist(),
            'CURRENCY_LAST_CHAR': df['ISO_CCY_COD'].str[-1].tolist(),
            'AMOUNT': df['Monto_de_la_Transaccion'].tolist(),
            'TNX_TYPE_CODE': df['TRAN_TYP_DES'].apply(lambda x: CSVMt940Parser.defaul_code(x, pais)).tolist(),
            'CLIENT_REFERENCE': df['withdraw_id'].to_list(), #hardLogic MO/MI
            'BANK_REFERENCE': df['BANK_REF_NO'].tolist(),
            'DESCRIPTION': df['COD_DESC'].tolist(),
            'FIELD_86': (':86:' + df['XTRA_INFO']).to_list(),
            'H_BANK_STATEMENT_REF': referencias_de_banco.get(pais),
            'H_SWIFT_CODE': '',
            'H_CLIENT_ACCOUNT_NUMBER': df['ACCT_NO'].tolist(),
            'H_STATEMENT_NUMBER': CSVMt940Parser.file_stament_number(tabla=tablas[pais], \
                fecha=datetime.strptime(freq_date, "%y%m%d").strftime("%Y-%m-%d"), \
                account=nombres[pais]),
            'H_PAGE_NUMBER': '1',
            'H_MOVEMENT_TYPE':  header_mov_type,
            'H_BOOKING_DATE': df['TRAN_VAL_DT'].tolist(),
            'H_CURRENCY_CODE': df['ISO_CCY_COD'].tolist(),
            'H_TOTAL_AMOUNT': '',
            'T_TIPO_SALDO': header_mov_type,
            'T_FECHA_SALDO': df['TRAN_VAL_DT'].tolist(),
            'T_MONEDA': df['ISO_CCY_COD'].tolist(),
            'T_SALDO_FINAL': '',
            }
        final_df = pd.DataFrame(swift_dict)
        final_df['REPORT_DATE'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        final_df['FILENAME'] = nombres[pais] + pd.Timestamp.now().strftime('%Y%m%d') + df['TRAN_VAL_DT'].tolist()[0] + '_RPA.csv'
        final_df['RAW_UNIQUENESS'] = final_df.sort_values(['REPORT_DATE'], ascending=[False]) \
             .groupby(['MOVEMENT_TYPE', 'CLIENT_REFERENCE']) \
             .cumcount() + 1
        final_df['ORIGINAL_FILENAME'] = file_name.split('/')[-1][:-4] + '_' + pd.Timestamp.now().strftime('%Y%m%d')  + df['TRAN_VAL_DT'].tolist()[0] + '.csv'
        final_df = final_df.where((pd.notnull(final_df)), '')
        return final_df

    @staticmethod
    def run(filename, **kwargs):
        if '.csv' in filename.lower():
            try:
                settings = ClientSetting.objects.filter(name='bank_settings').first()
            except:
                settings = RDSConnector().session.query(ClientSettings).filter(ClientSettings.name == 'bank_settings').first()
            FILE_TO_CHOOSE = settings.parameters
            pais=''
            if 'AR_' in filename:
                pais = 'ARG'
            elif 'ARG_ONLINE' in filename:
                pais = 'AR_ONLINE'


            obj = FileReader.read(filename)
            df = pd.read_csv(io.BytesIO(obj),sep=",",dtype = str)
            list_of_columns=['TRAN_AMT',
            'TRAN_COD',
            'TRAN_POST_DT',
            'CR_DR_MRK',
            'TRAN_VAL_DT',
            'ISO_CCY_COD',
            'BANK_REF_NO',
            'TRAN_TYP_DES',
            'ACCT_NO',
            'REF_NO',
            'XTRA_INFO'
            ]
            df=df[list_of_columns]

            df_obj = df.select_dtypes(['object'])
            df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
            df = CSVMt940Parser.parse(df.copy(), file_name=filename.split('\\')[-1].split('/')[-1], pais=pais)

            today = pd.Timestamp.now().day_name()

            for element in FILE_TO_CHOOSE:
                if element['country'] == "AR" and element['account_number'] == '102216A':
                    if 'ARG' == pais:
                        for extraction in element['extraction']:
                            if extraction['day'] == today:
                                extraction_method = extraction['method']
                                if extraction_method == 'MANUAL':
                                    pass
                                else:
                                    df = df.head(0)
                                    print('NO SE CARGO EL ARCHIVO POR SER DIA DE SWIFT', filename)

                if element['country'] == "AR" and element['account_number'] == '102253A':
                    if 'ARG' == pais:
                        for extraction in element['extraction']:
                            if extraction['day'] == today:
                                extraction_method = extraction['method']
                                if extraction_method == 'MANUAL':
                                    pass
                                else:
                                    df = df.head(0)
                                    print('NO SE CARGO EL ARCHIVO POR SER DIA DE SWIFT', filename)

                if element['country'] == "AR" and element['account_number'] == '102203A':
                    if 'AR_SANT' == pais:
                        for extraction in element['extraction']:
                            if extraction['day'] == today:
                                extraction_method = extraction['method']
                                if extraction_method == 'MANUAL':
                                    pass
                                else:
                                    df = df.head(0)
                                    print('NO SE CARGO EL ARCHIVO POR SER DIA DE SWIFT', filename)


            print('PROCESADO', filename)
            return df
        elif'.zip' in filename.lower():
            print('leyendo .zip con solucion deflate64')
            obj = FileReader.read(filename)
            df = None
            return df

        else: 
            raise Exception('MO : El archivo cargado no tiene extension .csv o extensión .zip')

class Mt940ClassSelector:
    @staticmethod
    def run(filename, **kwargs):
        if '.xls' in filename.lower():
            df = ExcelMt940Parser.run(filename=filename)
            return df
        elif '.csv' in filename.lower():
            df = CSVMt940Parser.run(filename=filename)
            return df
        else : 
            raise Exception('Error: Not valid file extension ')
        

if __name__ == '__main__':
    filename = ''
    df = ExcelMt940Parser.run(filename=filename)
    pdb.set_trace()