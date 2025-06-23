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
#from sqlalchemy import create_engine
#from core.models import ClientSetting

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



HEADER_SCHEMA = {
            'BANK_NAME': '',
            'BASE_CLIENT_NUMBER': '',
            'H_BANK_STATEMENT_REF' : '',
            'CLIENT_NAME': '',
            'BRANCH_NUMBER': '',
            'BRANCH_NAME': '',
            'ACCOUNT_NUMBER': '',
            'IBAN_NUMBER': '',
            'CURRENCY': '',
            'ACCOUNT_TYPE': '',
            'ACCOUNT_NUMBER': '',
            'H_BANK_STATEMENT_REF': '',
            'H_STATEMENT_NUMBER': '',
            'H_PAGE_NUMBER': '',
            'H_MOVEMENT_TYPE': '',
            'H_BOOKING_DATE': '',
            'H_CURRENCY_CODE': '',
            'H_TOTAL_AMOUNT': '',
            'Q_CREDITS' : '',
            'TOTAL_CREDITS' : '',
            'Q_DEBITS': '',
            'TOTAL_DEBITS': '',
            'CASH_STATUS_DATE': '',
            'AVAILABLE_ACCOUNTING_BALANCE': '',
            'FINAL_ACCOUNTING_BALANCE': '',
            'AVAILABLE_OPENING_BALANCE': '',
            'AVAILABLE_FINAL_BALANCE': ''}
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
            with open(uri,'rb') as f:
                # print(f.read())
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
             SELECT 
             H_STATEMENT_NUMBER AS S_N, APPROX_TOP_K(SKT__VALUE_DATE)[0][0]::VARCHAR::DATE AS MAIN_DATE, CASE WHEN MAIN_DATE <= '{fecha}' 
                THEN 1 ELSE 0 END as TELLER,
             AVG(TELLER) OVER (ORDER BY MAIN_DATE ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS PREVIOUS
             FROM (SELECT * FROM INPUTS.{tabla} UNION ALL SELECT * FROM INPUTS.{tabla}_NO_WITHDRAWS)
             WHERE FILENAME LIKE '%{account}%'
             GROUP BY 1
             ORDER BY MAIN_DATE DESC
        """

        statement_number = ExcelMt940Parser.file_stament_number_default(tabla)
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

    @staticmethod
    def prepare_amount_to_float(monto):
        if ',' in monto:
            decimal = monto.split(',')[-1]
            entero = monto.split(',')[0]
            if len(decimal) == 1:
                decimal = decimal + '0'
            elif len(decimal) == 0:
                decimal = decimal + '00'
            entero = entero.replace('.','')
            monto = entero + '.' + decimal
        else:
            monto = monto + '.00'
        monto = monto[1:] if monto[0] == '-' else monto
        return monto

    @staticmethod
    def prepare_amount_to_int(monto):
        if '.' in monto:
            entero = monto.replace('.','')
            monto = entero
        else:
            monto
        return monto


    @staticmethod
    def parse1(df, file_name, pais):

        df.columns = df.columns.str.replace(' ', '_').str.lower()

        tablas = {'BRA': 'br_citi_swift',
                  'ARG': 'ar_citi_swift',
                  'ARG_ONLINE': 'ar_citi_swift'}
        nombres = {'BRA': 'MPB_102352A_',
                  'ARG': 'MLA_102216A_',
                  'ARG_ONLINE': 'MLA_102253A_'}
        referencias_de_banco = {'BRA': 'MCL1' + pd.Timestamp.now().strftime('%y%m%d') + '0637',
                  'ARG': '9402619360011221',
                  'ARG_ONLINE': '9404720010005608'}

        for c in df.columns:
            if 'fecha_de_estado_de_efectivo' in c and 'Fecha_Extraccion' not in c:
                try:
                    df[c] = pd.to_datetime(df[c], format='%d/%m/%Y').dt.strftime("%y%m%d")
                except:
                    df[c] = pd.to_datetime(df[c], format='%d/%m/%Y').dt.strftime("%y%m%d")

        freq_date = df['fecha_de_estado_de_efectivo'].value_counts().idxmax()

        df['CREDIT_AMOUNT'] = df['monto_total_de_crédito'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_float(x))
        df['DEBIT_AMOUNT'] = df['monto_total_de_débito'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_float(x))
        df['AVAILABLE_BALANCE'] = df['saldo_disponible_actual/final'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_float(x))
        df['COUNT_CREDITS'] = df['número_de_créditos'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_int(x))
        df['COUNT_DEBITS'] = df['número_de_débitos'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_int(x))

        df['AVAILABLE_ACCOUNTING_BALANCE_FLOAT'] = df['saldo_contable_disponible'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_float(x))
        df['FINAL_ACCOUNTING_BALANCE_FLOAT'] = df['saldo_contable_actual/final'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_float(x))
        df['AVAILABLE_OPENING_BALANCE_FLOAT'] = df['saldo_inicial_disponible'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_float(x))        
        df['FINAL_BALANCE'] = df['saldo_disponible_actual/final'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_float(x))

        if float(df['CREDIT_AMOUNT'].iloc[-1]) - float(df['DEBIT_AMOUNT'].iloc[-1]) <= 0:
            header_mov_type='D'
        else:
            header_mov_type = 'C'

        if float(df['AVAILABLE_BALANCE'].iloc[-1]) <= 0:
            trailer_mov_type = 'D'
        else:
            trailer_mov_type = 'C'

        df['T_SALDO_FINAL'] = df['saldo_contable_actual/final'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_float(x))
        df['H_TOTAL_AMOUNT'] = df['saldo_disponible_actual/final'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_float(x))
        df['H_TOTAL_AMOUNT'] = df['saldo_disponible_actual/final'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_float(x))



        df = df.replace('nan', '', regex = True)
        swift_dict = {

            'BANK_NAME': df['nombre_del_banco'].tolist(),
            'BASE_CLIENT_NUMBER': df['número_base_del_cliente'].tolist(),
            'CLIENT_NAME': df['nombre_del_cliente'].tolist(),
            'BRANCH_NUMBER': df['número_de_sucursal'].tolist(),
            'BRANCH_NAME': df['nombre_de_la_sucursal'].tolist(),
            'ACCOUNT_NUMBER': df['número_de_cuenta'].tolist(),
            'IBAN_NUMBER': df['número_iban'].tolist(),
            'CURRENCY': df['moneda_de_la_cuenta_deudora'].tolist(),
            'ACCOUNT_TYPE': df['tipo_de_cuenta'].tolist(),
            'ACCOUNT_NUMBER': df['número_de_cuenta'].tolist(),
            'H_BANK_STATEMENT_REF': referencias_de_banco.get(pais),
            'H_STATEMENT_NUMBER': '',
            'H_PAGE_NUMBER': '1',
            'H_MOVEMENT_TYPE': header_mov_type,
            'H_BOOKING_DATE': df['fecha_de_estado_de_efectivo'].tolist(),
            'H_CURRENCY_CODE': df['moneda_de_la_cuenta_deudora'].tolist(),
            'H_TOTAL_AMOUNT': df['H_TOTAL_AMOUNT'].tolist(),
            'Q_CREDITS' : df['COUNT_CREDITS'],
            'TOTAL_CREDITS' : df['CREDIT_AMOUNT'],
            'Q_DEBITS': df['COUNT_DEBITS'],
            'TOTAL_DEBITS': df['DEBIT_AMOUNT'],
            'CASH_STATUS_DATE': df['fecha_de_estado_de_efectivo'].tolist(),
            'AVAILABLE_ACCOUNTING_BALANCE': df['AVAILABLE_ACCOUNTING_BALANCE_FLOAT'].tolist(),
            'FINAL_ACCOUNTING_BALANCE': df['FINAL_ACCOUNTING_BALANCE_FLOAT'].tolist(),
            'AVAILABLE_OPENING_BALANCE': df['AVAILABLE_OPENING_BALANCE_FLOAT'].tolist(),
            'AVAILABLE_FINAL_BALANCE': df['FINAL_BALANCE'].tolist()
            }
        final_df = pd.DataFrame(swift_dict)
        final_df['REPORT_DATE'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        final_df['FILENAME'] = nombres[pais] + pd.Timestamp.now().strftime('%Y%m%d') + '_HEADERS.csv'
        final_df['RAW_UNIQUENESS'] = final_df.sort_values(['REPORT_DATE'], ascending=[False]) \
             .groupby(['H_MOVEMENT_TYPE', 'ACCOUNT_NUMBER']) \
             .cumcount() + 1
        final_df['ORIGINAL_FILENAME'] = file_name.split('/')[-1][:-4] + '_' + pd.Timestamp.now().strftime('%Y%m%d') + '_HEADERS.csv'
        final_df = final_df.where((pd.notnull(final_df)), '')
        return final_df

   
    @staticmethod
    def parse2(df, file_name, pais):

      df.columns = df.columns.str.replace(' ', '_').str.lower()

      tablas = {'BRA': 'br_citi_swift',
                'ARG': 'ar_citi_swift',
                'ARG_ONLINE': 'ar_citi_swift'}
      nombres = {'BRA': 'MPB_102352A_',
                'ARG': 'MLA_102216A_',
                'ARG_ONLINE': 'MLA_102253A_'}
      referencias_de_banco = {'BRA': 'MCL1' + pd.Timestamp.now().strftime('%y%m%d') + '0637',
                'ARG': '9402619360011221',
                'ARG_ONLINE': '9404720010005608'}

      for c in df.columns:
          if 'fecha_del_extracto' in c and 'Fecha_Extraccion' not in c:
              try:
                  df[c] = pd.to_datetime(df[c], format='%d/%m/%Y').dt.strftime("%y%m%d")
              except:
                  df[c] = pd.to_datetime(df[c], format='%d/%m/%Y').dt.strftime("%y%m%d")

      freq_date = df['fecha_del_extracto'].value_counts().idxmax()


      df['CREDIT_AMOUNT'] = df['importe_total_del_haber'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_float(x))
      df['DEBIT_AMOUNT'] = df['importe_total_del_debe'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_float(x))
      df['AVAILABLE_BALANCE'] = df['saldo_disponible_actual_/_de_cierre'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_float(x))
      df['COUNT_CREDITS'] = df['número_de_haber'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_int(x))
      df['COUNT_DEBITS'] = df['número_de_debe'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_int(x))

      df['AVAILABLE_ACCOUNTING_BALANCE_FLOAT'] = df['saldo_contable_inicial'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_float(x))
      df['FINAL_ACCOUNTING_BALANCE_FLOAT'] = df['saldo_contable_actual_/_de_cierre'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_float(x))
      df['AVAILABLE_OPENING_BALANCE_FLOAT'] = df['saldo_disponible_inicial'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_float(x))        
      df['FINAL_BALANCE'] = df['saldo_disponible_actual_/_de_cierre'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_float(x))

      if float(df['CREDIT_AMOUNT'].iloc[-1]) - float(df['DEBIT_AMOUNT'].iloc[-1]) <= 0:
          header_mov_type='D'
      else:
          header_mov_type = 'C'

      if float(df['AVAILABLE_BALANCE'].iloc[-1]) <= 0:
          trailer_mov_type = 'D'
      else:
          trailer_mov_type = 'C'

      df['T_SALDO_FINAL'] = df['saldo_contable_actual_/_de_cierre'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_float(x))
      df['H_TOTAL_AMOUNT'] = df['saldo_disponible_actual_/_de_cierre'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_float(x))
      df['H_TOTAL_AMOUNT'] = df['saldo_disponible_actual_/_de_cierre'].apply(lambda x: ExcelMt940Parser.prepare_amount_to_float(x))



      df = df.replace('nan', '', regex = True)
      swift_dict = {

          'BANK_NAME': df['nombre_del_banco'].tolist(),
          'BASE_CLIENT_NUMBER': df['número_base_del_cliente'].tolist(),
          'CLIENT_NAME': df['nombre_del_cliente'].tolist(),
          'BRANCH_NUMBER': df['número_de_sucursal'].tolist(),
          'BRANCH_NAME': df['nombre_de_la_sucursal'].tolist(),
          'ACCOUNT_NUMBER': df['número_de_cuenta'].tolist(),
          'IBAN_NUMBER': df['número_iban'].tolist(),
          'CURRENCY': df['divisa_de_la_cuenta'].tolist(),
          'ACCOUNT_TYPE': df['tipo_de_cuenta'].tolist(),
          'ACCOUNT_NUMBER': df['número_de_cuenta'].tolist(),
          'H_BANK_STATEMENT_REF': referencias_de_banco.get(pais),
          'H_STATEMENT_NUMBER': '',
          'H_PAGE_NUMBER': '1',
          'H_MOVEMENT_TYPE': header_mov_type,
          'H_BOOKING_DATE': df['fecha_del_extracto'].tolist(),
          'H_CURRENCY_CODE': df['divisa_de_la_cuenta'].tolist(),
          'H_TOTAL_AMOUNT': df['H_TOTAL_AMOUNT'].tolist(),
          'Q_CREDITS' : df['COUNT_CREDITS'],
          'TOTAL_CREDITS' : df['CREDIT_AMOUNT'],
          'Q_DEBITS': df['COUNT_DEBITS'],
          'TOTAL_DEBITS': df['DEBIT_AMOUNT'],
          'CASH_STATUS_DATE': df['fecha_del_extracto'].tolist(),
          'AVAILABLE_ACCOUNTING_BALANCE': df['AVAILABLE_ACCOUNTING_BALANCE_FLOAT'].tolist(),
          'FINAL_ACCOUNTING_BALANCE': df['FINAL_ACCOUNTING_BALANCE_FLOAT'].tolist(),
          'AVAILABLE_OPENING_BALANCE': df['AVAILABLE_OPENING_BALANCE_FLOAT'].tolist(),
          'AVAILABLE_FINAL_BALANCE': df['FINAL_BALANCE'].tolist()
          }
      final_df = pd.DataFrame(swift_dict)
      final_df['REPORT_DATE'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
      final_df['FILENAME'] = nombres[pais] + pd.Timestamp.now().strftime('%Y%m%d') + '_HEADERS.csv'
      final_df['RAW_UNIQUENESS'] = final_df.sort_values(['REPORT_DATE'], ascending=[False]) \
          .groupby(['H_MOVEMENT_TYPE', 'ACCOUNT_NUMBER']) \
          .cumcount() + 1
      final_df['ORIGINAL_FILENAME'] = file_name.split('/')[-1][:-4] + '_' + pd.Timestamp.now().strftime('%Y%m%d') + '_HEADERS.csv'
      final_df = final_df.where((pd.notnull(final_df)), '')
      return final_df

    @staticmethod
    def run(filename, **kwargs):
#        settings = ClientSetting.objects.filter(name='bank_settings').first()
#        FILE_TO_CHOOSE = settings.parameters

        pais=''
        if 'BRA' in filename:
            pais = 'BRA'
        elif 'COL' in filename:
            pais = 'COL'
        elif 'UYU' in filename or 'URU' in filename:
            pais = 'UYU'
        elif 'AR_' in filename:
            pais = 'ARG'
        elif 'ARG_ONLINE' in filename:
            pais = 'ARG_ONLINE'
        elif 'CHI_CITI' in filename:
            pais = 'CLC'
        elif 'CHI_SANT' in filename or 'URU' in filename:
            pais = 'CLS'
        elif 'PE_CITI' in filename:
            pais = 'PEC'
        elif 'PE_BBVA' in filename:
            pais = 'PEB'
        elif 'MLM_BANAMEX' in filename:
            pais = 'MX_BMEX'
        elif 'MLA_SANTANDER' in filename:
            pais = 'AR_SANT'

        obj = FileReader.read(filename)
        try:
          df = pd.read_csv(io.BytesIO(obj),sep=",",encoding='utf-16', dtype = str)
        except:
          df = pd.read_csv(io.BytesIO(obj),sep=",",encoding='utf-16-le', dtype = str)
        
        df_obj = df.select_dtypes(['object'])
        df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
        
        try:
          df = ExcelMt940Parser.parse1(df.copy(), file_name=filename.split('\\')[-1].split('/')[-1], pais=pais)
        except:
          df = ExcelMt940Parser.parse2(df.copy(), file_name=filename.split('\\')[-1].split('/')[-1], pais=pais)

        print('PROCESADO', filename)
        return df


if __name__ == '__main__':
    filename = ''
    df = ExcelMt940Parser.run(filename=filename)