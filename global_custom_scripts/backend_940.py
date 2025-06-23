from typing import List
from urllib.parse import urlparse

import boto3 as boto3
import pandas as pd
import pdb
import json 


import os
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
try:
    from models.core import ClientSetting
except:
    from utils.database import RDSConnector
    from models.core import ClientSettings
from re import split



# Aws credential used for testing the conection and reading of files from s3:
AWS_ACCESS_KEY = ''
AWS_SECTRET_KEY = ''
BUCKET_NAME = ''

SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ROLE = os.environ.get('SNOWFLAKE_ROLE')
SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.environ.get('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.environ.get('SNOWFLAKE_SCHEMA')
# ------------------------------------------------------------------------------------



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
            session = boto3.session.Session(region_name='us-east-1')
            s3 = session.client('s3')
              # ,aws_access_key_id = AWS_ACCESS_KEY,
              #aws_secret_access_key = AWS_SECTRET_KEY)      
            s3_url = S3Url(uri)
            try:
                lines = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)['Body'].read().decode()
            except UnicodeDecodeError:
                lines = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)['Body'].read().decode('latin1')
            return lines
        else:
            with open(uri) as f:
                return f.read()


class SwiftMT940Parser:
    @staticmethod
    def parse_variable_line(line_part, type_, sep=''):

        ret = ''
        i = 0
        if type_ == 'numeric':
            for i, char in enumerate(line_part):
                if char.isnumeric():
                    ret += char
                else:
                    break
        elif type_ == 'apha':
            num = ''
            for i, char in enumerate(line_part):
                if char.isalpha():
                    ret += char
                else:
                    break
        else:
            raise Exception('Invalid character')
        if len(ret) != 0:
            offset = len(sep)
        else:
            offset = 0
        return ret, line_part[i + offset:]

    """
        Field :61:
        Description:
            Transaction line

    """

    @staticmethod
    def parse_transaction(line: str):

        l = line.split(':')[2]

        # AMOUNT variable length handler (all amounts are separated by "," ahd have two decimal places)
        comma_split = l.split(',')

        decimal_part = comma_split[1].split('N')[0]  # comma_split[1][0:2]  # Get       z decimal part
        if decimal_part == '':
            decimal_part = '00'
        #move backwards from the "," until you find a non numeric character 
        #-> this indicates the end of the integer part of the number.
        integer_part = ''  # integer part
        for i in comma_split[0][::-1]:
            if i.isnumeric():
                integer_part = i + integer_part
                # integer_part += i
            else:
                break

        # TRANSACTION TYPE get
        n_part = 'N'.join(comma_split[1].split('N')[1:])
        tx_type = 'N' + n_part[0:3]
        bar_split = n_part[3:].split('//')
        cli = bar_split[0]
        
        if 'NONREF' in bar_split[0]:
          cli = 'NONREF'
          x = bar_split[1].split('/')[1:]
          x[len(x)-1] = bar_split[0].split('NONREF')[1] + x[len(x)-1]
        else:
            if 'NONREF' in bar_split[0]:
                cli = 'NONREF'
                x = bar_split[1].split('/')[1:]
                x[len(x)-1] = bar_split[0].split('NONREF')[1] + x[len(x)-1]
            else:
                if len(split('(\d+)', bar_split[0])) == 1:
                    cli = split('(\d+)', bar_split[0])[0]
                    x = bar_split[1].split('/')[1:]          
                elif len(split('(\d+)', bar_split[0])) == 5:
                    cli=''.join((split('(\d+)', bar_split[0]))[1:4])
                    x = bar_split[1].split('/')[1:]
                    x[len(x)-1] = split('(\d+)', bar_split[0])[4] + x[len(x)-1]
                else:
                    cli = split('(\d+)', bar_split[0])[1]
                    x = bar_split[1].split('/')[1:]
                    #x[len(x)-1] = split('(\d+)', bar_split[0])[2] + x[len(x)-1] 
        
        #some swift does not includes the booking date field, so to avoid misplacing the other fields, i took for reference the 
        # movement type C/D, starting after the value_date l[0,6]
        value_date = l[0:6] 
        for i in range(5):
            if l[6+i] in ['C', 'D']:
                break
             
        booking_date = l[6: 6+i]
        movement_type = l[6+i:7+i]
        currency_last_char = l[7+i:8+i]
        #print(bar_split)
        #pdb.set_trace()
        return {
            'VALUE_DATE': value_date,
            'BOOKING_DATE': booking_date,
            'MOVEMENT_TYPE': movement_type,
            'CURRENCY_LAST_CHAR': currency_last_char,
            'AMOUNT': integer_part + '.' + decimal_part,
            'TNX_TYPE_CODE': tx_type,
            'CLIENT_REFERENCE': cli,
            'BANK_REFERENCE': bar_split[1].split('/')[0].strip(),
            'DESCRIPTION': '/'.join(bar_split[1].split('/')[1:]).strip(),
            'FIELD_86' : ''
        }

    """
        Field :86:
        Description:
            Structured based on transaction details 
            
    """

    @staticmethod
    def parse_transaction_details(line):
        return {'FIELD_86' : line}

    def parse_headers(self, headers: List[str]) -> dict:

        map_ = {'BANK_STATEMENT_REF': ':20:',
                'SWIFT': ':25:',
                'STATEMENT_NUMBER': ':28',
                'BALANCE': ':60',
                'TRAILER': ':62'}
        cols = {}
        head_counter = 0
        for line in headers:
            if line.startswith(map_['BANK_STATEMENT_REF']):
                head_counter += 1
                cols[ 'H_BANK_STATEMENT_REF'] = line[4:].strip()
            elif line.startswith(map_['SWIFT']):
                s = line[4:].split('/')
                if len(s) == 2:
                    cols[ 'H_SWIFT_CODE'] = s[0]
                    cols[ 'H_CLIENT_ACCOUNT_NUMBER'] = s[1]
                elif len(s) == 1:
                    cols[ 'H_SWIFT_CODE'] = ''
                    cols[ 'H_CLIENT_ACCOUNT_NUMBER'] = s[0]
                else :
                    cols[ 'H_SWIFT_CODE'] = ''
                    cols[ 'H_CLIENT_ACCOUNT_NUMBER'] = ''
            elif line.startswith(map_['STATEMENT_NUMBER']):
                line = line.split(':')[-1].split('/')
                cols[ 'H_STATEMENT_NUMBER'] = line[0]
                if len(line) > 1:
                    cols[  'H_PAGE_NUMBER'] = line[1]
                else: cols[  'H_PAGE_NUMBER'] = 0
            elif line.startswith(map_['BALANCE']):
                line = line.split(':')[-1]
                cols[ 'H_MOVEMENT_TYPE'] = line[0:1]
                cols['H_BOOKING_DATE'] = line[1:7]
                cols[ 'H_CURRENCY_CODE'] = line[7:10]
                
                cols[ 'H_TOTAL_AMOUNT'] = line[10:].strip().replace(',', '.') 
                if cols[ 'H_TOTAL_AMOUNT'][-1] == '.':  cols[ 'H_TOTAL_AMOUNT'] = cols[ 'H_TOTAL_AMOUNT'] + '00'
            elif line.startswith(map_['TRAILER']):
                line = line.split(':')[-1]
                cols['T_TIPO_SALDO'] = line[0]
                cols['T_FECHA_SALDO'] = line[1:7]
                cols['T_MONEDA'] = line[7:10]
                cols['T_SALDO_FINAL'] = line[10:].replace(',', '.').strip('-') 
                if cols[ 'T_SALDO_FINAL'][-1] == '.':  cols[ 'T_SALDO_FINAL'] = cols[ 'T_SALDO_FINAL'] + '00'

        if head_counter > 1:
            raise ValueError('Se encontro mas de un header para este archivo')
        return cols


class BanksReportExtractor:

    @staticmethod
    def convert_lines(lines):
        results = {}
        result = []
        n_file = 1
        for line in lines:
            if line != '-':
                if line.startswith(':'):
                    result.append(line)
                else:
                    item = result.pop()
                    item += line
                    result.append(item)
            else:
                results[f'file_{n_file}'] = result
                n_file += 1
                result = []
        return results



    def run(self, path: str) -> pd.DataFrame:

        try:
            settings = ClientSetting.objects.filter(name='bank_settings').first()
        except:
            settings = RDSConnector().session.query(ClientSettings).filter(ClientSettings.name == 'bank_settings').first()
        FILE_TO_CHOOSE = settings.parameters if settings else []

        raw_lines = FileReader.read(path)
        raw_lines = raw_lines.splitlines()
        statements = BanksReportExtractor.convert_lines(raw_lines)


        df_statements = {}
        for nro_extracto, lines in statements.items():
            header_lines = []
            transactions = []
            df_final = pd.DataFrame( columns = [
                'VALUE_DATE',
                'BOOKING_DATE',
                'MOVEMENT_TYPE',
                'CURRENCY_LAST_CHAR',
                'AMOUNT',
                'TNX_TYPE_CODE',
                'CLIENT_REFERENCE',
                'BANK_REFERENCE',
                'DESCRIPTION',
                'FIELD_86',
                'H_BANK_STATEMENT_REF',
                'H_SWIFT_CODE',
                'H_CLIENT_ACCOUNT_NUMBER',
                'H_STATEMENT_NUMBER',
                'H_PAGE_NUMBER',
                'H_MOVEMENT_TYPE',
                'H_BOOKING_DATE',
                'H_CURRENCY_CODE',
                'H_TOTAL_AMOUNT',
                'T_TIPO_SALDO',
                'T_FECHA_SALDO',
                'T_MONEDA',
                'T_SALDO_FINAL',
            ]
            )
            for line in lines:

                if line.startswith(':61:'):
                    transactions.append(SwiftMT940Parser.parse_transaction(line))
                elif line.startswith(':86:'):
                    add = SwiftMT940Parser.parse_transaction_details(line)
                    lst = transactions.pop()
                    lst = {**lst , **add}
                    transactions.append(lst)
                else:
                    header_lines.append(line)
            headers = SwiftMT940Parser().parse_headers(header_lines)
            df = pd.DataFrame(transactions)
            if len(transactions) != 0:
                for key in headers:
                    df[key] = headers[key]
                for col in df.columns:
                    df_final[col] = df[col]
            else:
                    #df = df.append(headers, ignore_index=True)
                df_final = df_final.append(headers, ignore_index=True)

            df_final['REPORT_DATE'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            suffix = '' if len(statements) == 1 else f'_{nro_extracto}'
            df_final['FILENAME'] = path.split('/')[-1][:-4] + suffix + '.txt'
            df_final['SKT_EXTRACTION_RN'] = df_final.index.values
            df_final['RAW_UNIQUENESS'] = df_final.sort_values(['REPORT_DATE'], ascending=[False]) \
                .groupby(['MOVEMENT_TYPE', 'AMOUNT', 'DESCRIPTION', 'FIELD_86', 'VALUE_DATE']) \
                .cumcount() + 1
            df_final = df_final.where((pd.notnull(df_final)), None)
            df_statements[str(nro_extracto)] = df_final
        
        df_return = pd.concat(df_statements, ignore_index=True)
        
        today = pd.Timestamp.now().day_name()
            
        for element in FILE_TO_CHOOSE:
            if 'MPB_102352A' in path:
                if element['country'] == "BR" and element['account_number'] == '102352A':
                    for extraction in element['extraction']:
                        if extraction['day'] == today:
                            extraction_method = extraction['method']
                            if extraction_method == 'SWIFT':
                                pass
                            else:
                                df_return = df_return.head(0)
                                print('NO SE SUBIO EL ARCHIVO POR SER DIA MANUAL', path )
            if 'MPO_102862A' in path:
                if element['country'] == "CO" and element['account_number'] == '102862A':
                    for extraction in element['extraction']:
                        if extraction['day'] == today:
                            extraction_method = extraction['method']
                            if extraction_method == 'SWIFT':
                                pass
                            else:
                                df_return = df_return.head(0)
                                print('NO SE SUBIO EL ARCHIVO POR SER DIA MANUAL', path )
            if 'DRU_102352A' in path:
                if element['country'] == "UY" and element['account_number'] == '102352A':
                    for extraction in element['extraction']:
                        if extraction['day'] == today:
                            extraction_method = extraction['method']
                            if extraction_method == 'SWIFT':
                                pass
                            else:
                                df_return = df_return.head(0)
                                print('NO SE SUBIO EL ARCHIVO POR SER DIA MANUAL', path )

            if 'MLA_102216A' in path:
                if element['country'] == "AR" and element['account_number'] == '102216A':
                    for extraction in element['extraction']:
                        if extraction['day'] == today:
                            extraction_method = extraction['method']
                            if extraction_method == 'SWIFT':
                                pass
                            else:
                                df_return = df_return.head(0)
                                print('NO SE SUBIO EL ARCHIVO POR SER DIA MANUAL', path )

            '''if 'EMP_103181A' in path:
                if element['country'] == "CL" and element['account_number'] == '103181A':
                    for extraction in element['extraction']:
                        if extraction['day'] == today:
                            extraction_method = extraction['method']
                            if extraction_method == 'SWIFT':
                                pass
                            else:
                                df_return = df_return.head(0)
                                print('NO SE SUBIO EL ARCHIVO POR SER DIA MANUAL', path )'''
                
        print('PROCESADO', path.split('/')[-1])
 
        return df_return

if __name__ == '__main__':
    #c = BanksReportExtractor()
    #df = c.run('mi940SourceExamples/MPO_102564A_202008191907.txt')
    #'/mnt/c/Users/sergi/Desktop/MLA_102247A_201910082104.txt'   )#'s3://meli-sl-tests/MPB_102349A_201910012106.txt') #s3://meli-sl-tests/MLC_102407A_201910072105.txt') #
    #pdb.set_trace()
    #print(df.head())
    print('Running ... as main')