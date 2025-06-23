
"""
Reads an exls file from S3 and transforms it into a pandas Dataframe with the same schema of the global_statement_citi table
"""

from io import BytesIO
import pdb
import boto3 as boto3
import pandas as pd
from openpyxl import load_workbook
from datetime import date, timedelta, datetime
import os
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
#from core.models import ClientSetting
import xlrd
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse
import io
from io import BytesIO
# ---------------------------------------------------------------
SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD =os.environ.get('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ROLE = os.environ.get('SNOWFLAKE_ROLE')
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

            # session = boto3.Session(profile_name="sts")
            # s3 = session.client('s3')
            session = boto3.session.Session()
            s3 = session.client('s3')

            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)['Body'].read()
            
            return obj
        else:
            
            return uri

class InterbankingMt940Parser:
    
    @staticmethod
    def file_stament_number_default(tabla):

        query = f"""
        select ifnull(max(h_statement_number), 0)::integer as statement_number from 
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
        
        statement_number = InterbankingMt940Parser.file_stament_number_default(tabla)
        conn = Connection()
        df = conn.query_to_df(query)
        if df is not None and not df.empty:
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
    def prepare_amount(monto, decimal = '.', miles = ','):
        monto = monto.replace(miles, '')
        monto = monto.replace(decimal, '.')
        monto = '{:.2f}'.format(float(monto))
        monto =  monto[1:] if monto[0]=='-' else monto
        return monto

    @staticmethod
    def headers_and_trailers(sheet, decimal = '.', miles = ',', book = None):
        end = 13
        i = 0
        header_and_trailer = {
            'fecha': '',
            'currency_code': '',
            'client_account_number': ''
        }
        while i <= end:
            value = sheet.cell_value(i, 0)
            if value == 'Fecha Hasta':
                header_and_trailer['fecha'] = sheet.cell_value(i, 1)
            if value == 'Moneda':
                header_and_trailer['currency_code']  = sheet.cell_value(i, 1)
            if value == 'Numero':
                header_and_trailer['client_account_number']  = sheet.cell_value(i, 1)
            if value == 'Banco':
                header_and_trailer['Saldo Inicial']  = InterbankingMt940Parser.prepare_amount(str(sheet.cell_value(i, 4)), decimal, miles)
                header_and_trailer['Tipo mov inicial'] = 'D' if (str(sheet.cell_value(i, 4))[0] == '-')  else 'C'

            if value == 'Tipo':
                header_and_trailer['Saldo Final']  = InterbankingMt940Parser.prepare_amount(str(sheet.cell_value(i, 4)), decimal, miles)
                header_and_trailer['Tipo mov final'] = 'D' if (str(sheet.cell_value(i, 4))[0] == '-')  else 'C'

            i += 1
        return header_and_trailer

    @staticmethod
    def saldo_inicial_y_final(sheet):
        pass
    

    
    header_line_dict = {
            'MLA_HIPOTECARIO': 13,
            'MLA_SANTANDER': 13,
            'MLA_PATAGONIA': 13 ,
            'MLA_CREDICOOP': 13 
            }
        
    general_description_code_mapping = {
        'MLA_HIPOTECARIO':
            {
                'default': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:INTERBANKING/' }
            }
            ,
        'MLA_SANTANDER':
            {
                'default': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:INTERBANKING/' }
            }
            ,
        'MLA_PATAGONIA':
            {
                'default': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:INTERBANKING/' }
            }
            ,
        'MLA_CREDICOOP':
            {
                'default': {'dscr': 'CTC/MSC/', 'code': 'NTRF', 'field_86': ':86:INTERBANKING/' }
            }
        }


    @staticmethod
    def defaul_code(description, pais):
        description_code_mapping = InterbankingMt940Parser.general_description_code_mapping.get(pais)
        code = description_code_mapping.get(description)

        if code == None:
            code = description_code_mapping.get('default').get('code')
        else:
            code = code.get('code')
        return code

    @staticmethod
    def defaul_description(description, pais):
        description_code_mapping = InterbankingMt940Parser.general_description_code_mapping.get(pais)
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
        description_code_mapping = InterbankingMt940Parser.general_description_code_mapping.get(pais)
        mapp = description_code_mapping.get(description)

        if mapp == None:
            field_86 = description_code_mapping.get('default').get('field_86') + description
        elif (mapp != None and mapp.get('field_86') == None):
            field_86 = ':86:RPA MANUAL/' + descritpion
        else:
            field_86 = mapp.get('field_86') + description
        return field_86

    @staticmethod
    def parse(df, file_name, headers_and_trailers, pais, decimal = '.', miles = ','):
        
        tablas = {'MLA_HIPOTECARIO': 'ar_hipotecario_swift',
                 'MLA_SANTANDER': 'ar_santander_swift',
                 'MLA_PATAGONIA': 'ar_patagonia_swift',
                 'MLA_CREDICOOP': 'ar_credicoop_swift'
                 }
        nombres = {'MLA_HIPOTECARIO': 'MLA_102222A_',
                 'MLA_SANTANDER': 'MLA_102201A_',
                 'MLA_PATAGONIA': 'MLA_102204A_',
                 'MLA_CREDICOOP': 'MLA_102249'
        }
        referencias_de_banco = {
            'MLA_HIPOTECARIO':  '0003961611',
            'MLA_SANTANDER':    '0000057945',
            'MLA_PATAGONIA':    '9430096000',
            'MLA_CREDICOOP':    '0000060213'
            }
        
        formato_fechas = {
            'MLA_HIPOTECARIO':  '%Y-%d-%m %H:%M:%S',
            'MLA_SANTANDER':    '%Y-%m-%d %H:%M:%S',
            'MLA_PATAGONIA':    '%Y-%d-%m %H:%M:%S',
            'MLA_CREDICOOP':    '%Y-%d-%m %H:%M:%S'           
        }

        for c in df.columns:
            if c == 'Fecha':
                df['parsed_date'] = pd.to_datetime(df[c], format = formato_fechas.get(pais), errors='coerce')
                mask = df['parsed_date'].isnull()
                df.loc[mask, 'parsed_date'] =  pd.to_datetime(df[mask][c], dayfirst=True, errors='coerce')
                df[c] = df.parsed_date.dt.strftime("%y%m%d") #
            if c == 'Importe':
                df['movement_type'] = df['Importe'].apply(lambda x: 'D' if x[0]=='-' else 'C')
                df['Importe'] = df['Importe'].apply(lambda x: InterbankingMt940Parser.prepare_amount(x, decimal, miles))

        freq_date = df['Fecha'].value_counts().idxmax()
        
        df = df.replace('nan', '', regex = True)
        swift_dict = {
            'VALUE_DATE': df['Fecha'].tolist(),
            'BOOKING_DATE': df['Fecha'].apply(lambda x: x[2:]).tolist(),
            'MOVEMENT_TYPE': df['movement_type'].tolist(),
            'CURRENCY_LAST_CHAR': headers_and_trailers.get('currency_code')[-1],
            'AMOUNT': df['Importe'].tolist(),
            'TNX_TYPE_CODE': df['Descripcion'].apply(lambda x: InterbankingMt940Parser.defaul_code(x, pais)).tolist() 
             ,
            'CLIENT_REFERENCE': df['Ref. Cliente / CUIT'].tolist(),
            'BANK_REFERENCE': df['Cod. Banco'].tolist(),
            'DESCRIPTION': df['Descripcion'].apply(lambda x: InterbankingMt940Parser.defaul_description(str(x), pais)).tolist() 
                            ,
            'FIELD_86': df['Concepto / Cod. Oper.'].apply(lambda x: InterbankingMt940Parser.defaul_field_86(x, pais)).tolist() 
            ,
            'H_BANK_STATEMENT_REF': referencias_de_banco.get(pais),
            'H_SWIFT_CODE': '',
            'H_CLIENT_ACCOUNT_NUMBER': headers_and_trailers.get('client_account_number'),
            'H_STATEMENT_NUMBER': InterbankingMt940Parser.file_stament_number(tabla=tablas[pais], \
                fecha=datetime.strptime(freq_date, "%y%m%d").strftime("%Y-%m-%d"), \
                account=nombres[pais]),
            'H_PAGE_NUMBER': '1',
            'H_MOVEMENT_TYPE': headers_and_trailers.get('Tipo mov inicial'),
            'H_BOOKING_DATE':  freq_date,
            'H_CURRENCY_CODE': headers_and_trailers.get('currency_code'),
            'H_TOTAL_AMOUNT': headers_and_trailers.get('Saldo Inicial'),
            'T_TIPO_SALDO': headers_and_trailers.get('Tipo mov final'),
            'T_FECHA_SALDO':  freq_date,
            'T_MONEDA': headers_and_trailers.get('currency_code'),
            'T_SALDO_FINAL': headers_and_trailers.get('Saldo Final')
            }
        final_df = pd.DataFrame(swift_dict)
        final_df['REPORT_DATE'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        final_df['FILENAME'] = nombres[pais] + pd.Timestamp.now().strftime('%Y%m%d') + freq_date  + '_INTERBANKING.xlsx' 
        final_df['RAW_UNIQUENESS'] = final_df.sort_values(['REPORT_DATE'], ascending=[False]) \
             .groupby(['MOVEMENT_TYPE', 'AMOUNT', 'DESCRIPTION', 'FIELD_86', 'VALUE_DATE']) \
             .cumcount() + 1
        final_df['ORIGINAL_FILENAME'] = file_name.split('/')[-1][:-5] + '_'  +  freq_date + '.XLSX'
        final_df['SKT_EXTRACTION_RN'] = final_df.index
        final_df = final_df.where((pd.notnull(final_df)), '')

        return final_df

    @staticmethod
    def n_rows(sheet, start):
        end_of_file = False
        n_row = 1
        
        i = start
        while not end_of_file:
            value = sheet.cell_value(i, 0)
            if value == '' or value is None:
                n_row = i - start - 1
                end_of_file = True
                break
            i += 1
        return n_row

    @staticmethod
    def run(filename, **kwargs):
        print ('Version 1.2 + Bancos MLM MI')
        #settings = ClientSetting.objects.filter(name='bank_settings').first()
        #FILE_TO_CHOOSE = settings.parameters
        #settings = ClientSetting.objects.filter(name='bank_settings').first()
        #FILE_TO_CHOOSE = settings.parameters if settings else []
        if 'Hipotecario' in filename and 'ARGENTINA' in filename:
            pais = 'MLA_HIPOTECARIO'
            miles = '.'
            decimales = ','
        elif 'Santander' in filename and 'ARGENTINA' in filename:
            pais = 'MLA_SANTANDER'
            miles = ','
            decimales = '.'
        elif 'Patagonia' in filename and 'ARGENTINA' in filename:
            pais = 'MLA_PATAGONIA'
            miles = '.'
            decimales = ','
        elif 'Credicoop' in filename and 'ARGENTINA' in filename:
            pais = 'MLA_CREDICOOP'
            miles = '.'
            decimales = ','
        else :
            pais = 'otro'
            mies = ','
            decimal = '.'
        obj = FileReader.read(filename)
        if type(obj) == str:
            wb = xlrd.open_workbook(filename=obj)
        else:
            wb = xlrd.open_workbook(file_contents=obj)
        xl = pd.ExcelFile(wb)
        sheet = wb.sheet_by_index(0)
        start = InterbankingMt940Parser.header_line_dict[pais]
        header_and_trailer = InterbankingMt940Parser.headers_and_trailers(sheet, decimal = decimales, miles= miles, book = wb)
        #n_row = InterbankingMt940Parser.n_rows(sheet, start)
        df = pd.read_excel(xl, header=start, dtype = str, convert_float = False)
        trailers_index = df.loc[df['Fecha'] == '= Indica Saldos Calculados', :].index.tolist()
        col_names_index = df.loc[df['Fecha'] == 'Fecha', :].index.tolist()
        index_to_drop = [*trailers_index, *col_names_index]
        df = df.drop(index_to_drop).reset_index()
        df = InterbankingMt940Parser.parse(df.copy(), file_name=filename.split('/')[-1], headers_and_trailers=header_and_trailer, pais=pais, decimal=decimales, miles=miles)    
        today = pd.Timestamp.now().day_name()
        print('PROCESADO', filename)
        return df





if __name__ == '__main__':
    import pdb
    filename = 'mi_interbanking/160045983120200915_ARGENTINA_Interbanking_030039430096000_Banco Patagonia.xlsx'
    #'mi_interbanking/160045934720200918_ARGENTINA_Interbanking_030039430096000_Banco Patagonia.xlsx'
    #'mi_interbanking/160046044220200904_ARGENTINA_Interbanking_030039430096000_Banco Patagonia.xlsx'
    # 'mi_interbanking/20200917_ARGENTINA_Interbanking_300000000396161_Banco Bco Hipotecario.xlsx'
    #'mi_interbanking/20200917_ARGENTINA_Interbanking_030039430096000_Banco Patagonia.xlsx' #'mi_interbanking/20200914_ARGENTINA_CITI_20600057945_Santander Rio.xlsx'# 
    df = InterbankingMt940Parser.run(filename=filename)
    pdb.set_trace()