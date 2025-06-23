"""
Reads a CSV file from S3 and transforms it into a pandas Dataframe with the same schema of the global_statement_citi table
"""
import io
from io import BytesIO
import pdb
import boto3 as boto3
import pandas as pd
from datetime import date, timedelta, datetime
import os
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
try:
    from core.models import ClientSetting
except:
    from utils.database import RDSConnector
    from models.core import ClientSettings

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse


# ---------------------------------------------------------------
SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ROLE = os.environ.get('SNOWFLAKE_ROLE')
SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.environ.get('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = "INPUTS" # os.environ.get('SNOWFLAKE_SCHEMA')

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
            s3 = boto3.client('s3')
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)['Body'].read()

            return obj
        else:
            with open(uri, 'rb') as f:
                return f.read()

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
             SELECT 
             H_STATEMENT_NUMBER AS S_N, APPROX_TOP_K(SKT__VALUE_DATE)[0][0]::VARCHAR::DATE AS MAIN_DATE, CASE WHEN MAIN_DATE <= '{fecha}' 
                THEN 1 ELSE 0 END as TELLER,
             AVG(TELLER) OVER (ORDER BY MAIN_DATE ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS PREVIOUS
             FROM (SELECT * FROM INPUTS.{tabla})
             WHERE FILENAME LIKE '%{account}%'
             GROUP BY 1
             ORDER BY MAIN_DATE DESC
        """

        statement_number = CSVMt940Parser.file_stament_number_default(tabla)
        conn = Connection()
        df = conn.query_to_df(query)

        if df is not None:
            idx = df.loc[df['previous'] == 0.5, :].index.tolist()
            if idx:
                idx = idx[0]
                follow_statement_number = int(df.loc[[idx - 1], 's_n'].values[0])
                prev_statement_number = int(df.loc[[idx], 's_n'].values[0])

                statment_diff = follow_statement_number - prev_statement_number

                if statment_diff > 1:
                    statement_number = prev_statement_number + 1


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




# if __name__ == '__main__':
#     filename = ''
#     df = CSVMt940Parser.run(filename=filename)
#     pdb.set_trace()