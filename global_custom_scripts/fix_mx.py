import os
import pandas as pd
from sqlalchemy import create_engine
import datetime
import pytz
import boto3
from io import BytesIO
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
            session = boto3.session.Session(region_name='us-east-1')
            s3 = session.client('s3')
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data
        else:
            with open(uri) as f:
                return uri


class MyError(Exception):
    def __init__(self, message, ids):
        self.message = message
        self.ids = ids

    def __str__(self):
        return self.message


class ExtractorFiles:
    @staticmethod
    def run(filename, **kwargs):
        bucket_origen = "global-in-manual-reconciliation-production"

        bucket_dest = "global-in-sources-xxxxxxxxxx"

        s3_client= (boto3.client('s3'))

        pref = 'Conciliaciones/'

        try:
            pag = s3_client.get_paginator('list_objects_v2')
            pages = pag.paginate(Bucket=bucket_origen, Prefix=pref)
            key_list = []
            for page in pages:
                for obj in page['Contents']:
                    key_list.append(obj['Key'])
                    
                
            for s in key_list:
                key = s
                file_name = s.split('/')[-1]
                obj = s3_client.get_object(Bucket=bucket_origen, Key=key)['Body'].read()
                binary_data = BytesIO(obj)
                s3_client.put_object(Body=binary_data.getvalue(),Bucket=bucket_dest,Key = 'FIX_MX/' + file_name)

            

        
    
        except Exception as e:
            print("Error: ",e)
        
                
        df = None
        return df
    
class Engine:
    
    def __init__(self, USER, PASSWORD, DB, WH, ROLE,ACCOUNT,SCHEMA = 'OUTPUTS'):
        self.USER = USER
        self.PASSWORD = PASSWORD
        self.DB = DB
        self.WH = WH
        self.ROLE = ROLE
        self.ACCOUNT = ACCOUNT
        self.SCHEMA = SCHEMA
    
    def get_engine(self):
        print('Connecting to Snowflake')
        engine = create_engine(f'snowflake://{self.USER}:{self.PASSWORD}@{self.ACCOUNT}/{self.DB}/{self.SCHEMA}?warehouse={self.WH}&role={self.ROLE}')
        return engine


    
class Extractor:
    @staticmethod
    def read_file(expectd_columns, file_name):
        df = pd.read_csv(file_name, dtype='str', sep=';', header=0)
        file_cols = [x.upper() for x in df.columns]
        if file_cols != expectd_columns:
            raise MyError('Las columnas del archivo no coinciden con las esperadas', None)

        ids = df['ID_TIPO'].unique()
        if len(ids) > 1:
            raise MyError('Se encontró mas de 1 ID_tipo', None)

        df['COMMENTS'] = df['COMMENTS'].fillna('')
        df['COMMENTS'] = df['COMMENTS'] + ' //Registro agregado mediante archivo masivo de nombre ' + str(file_name)

        return df, ids[0]
    
    @staticmethod
    def validate_duplicated_in_file(df, column_name, side=None):
        df_dupl = df[df.duplicated([column_name])]
        if len(df_dupl) > 0:
            ids_str = '-'.join(map(str, df_dupl[column_name].unique()))
            if side is not None:
                msg = f'Al menos un id del lado {side} se encuentra más de una vez en el archivo cuando debe ser único'
            else:
                msg = 'Al menos un id se encuentra más de una vez en el archivo cuando debe ser único'
            raise MyError(msg, ids_str)
    
    
    @staticmethod
    def check_miss(id_series, miss_table, engine, side=None):
        if miss_table == '':
            raise MyError('Vista de pendientes no configurada', None)

        if len(id_series) > 16000:
            raise MyError('El archivo posee mas de 16000 registros a verificar', None)

        query_miss = f'''WITH DATA_IN AS (
            SELECT *
            FROM VALUES
            ({'),('.join(id_series)}) A(ID)
            )
            SELECT D.*
            FROM DATA_IN D
            LEFT JOIN OUTPUTS.{miss_table.upper()} M
            ON D.ID = M.SKT_ID
            WHERE M.SKT_ID IS NULL
        '''
        #print('Validating ids in missing view')
        df_miss = pd.read_sql(query_miss, engine)
        if len(df_miss) > 0:
            ids_str = '-'.join(map(str, df_miss.iloc[:, 0]))
            if side is not None:
                msg = f'Al menos un id del lado {side} no se encuentra en estado pendiente'
            else:
                msg = 'Al menos un id no se encuentra en estado pendiente'
            raise MyError(msg, ids_str)
        
        
    @staticmethod
    def get_table_names(id, engine, side='A', adjust_cbk=False):
        read_query = f'''
        SELECT *
        FROM MOTHER_TABLE.GLOBAL_CONCI_MANUAL_CONFIG
        WHERE ID_TIPO = {id};
        '''
        #print('Reading config from DB')
        df_read = pd.read_sql(read_query, engine)
        if len(df_read) > 1:
            raise MyError('Se encontró mas de una tabla en la cual escribir', None)
        tables = df_read['report_name'].fillna('')
        if side == 'A':
            tables_adj = df_read['adjustment_a'].fillna('')
            miss_a = df_read['missings_a'].fillna('').iloc[0]
            miss_b = df_read['missings_b'].fillna('').iloc[0]
        else:
            tables_adj = df_read['adjustment_b'].fillna('')
            miss_b = df_read['missings_a'].fillna('').iloc[0]
            miss_a = df_read['missings_b'].fillna('').iloc[0]
        site = df_read['site'].fillna('').iloc[0]
        table_conci = tables.iloc[0]
        table_adj = tables_adj.iloc[0]
        table_batch = df_read['report_name_batch'].fillna('').iloc[0]
        if adjust_cbk:
            read_query_cbk = f'''
                SELECT *
                FROM MOTHER_TABLE.GLOBAL_CONCI_MANUAL_CONFIG_CBK
                WHERE SITE = '{site}';
                '''
            print('Reading config from DB CBK')
            df_read_cbk = pd.read_sql(read_query_cbk, engine)
            report_name_cbk = df_read_cbk['report_name'].fillna('').iloc[0]
            site_cbk = df_read_cbk['site'].fillna('').iloc[0]
        else:
            report_name_cbk = ''
            site_cbk = ''
        return table_conci.lower(), site_cbk.lower(), table_adj.lower(), report_name_cbk.lower(), table_batch.lower(), miss_a.lower(), miss_b.lower()
    
    @staticmethod
    def create_df_conci(df):
        date = datetime.datetime.now().astimezone(pytz.timezone("UTC"))
        df_write = pd.DataFrame(
            columns=['SKT_ID_A', 'SKT_ID_B', 'USER_ID', 'COMMENTS', 'CONCILIATION_DATETIME', 'MISSING_A', 'MISSING_B'])
        df_write[['SKT_ID_A', 'SKT_ID_B', 'COMMENTS', 'MISSING_A', 'MISSING_B']] = df[
            ['SKT_ID_A', 'SKT_ID_B', 'COMMENTS', 'SOURCE_A', 'SOURCE_B']]
        df_write['USER_ID'] = 0
        df_write['CONCILIATION_DATETIME'] = date

        return df_write
    
    
    @staticmethod
    def run(filename, **kwargs):
        
        myfile = FileReader.read(filename)
        
        En = Engine(USER = 'SIMETRIK_PROD_IN',PASSWORD = '2FLYceY2pkX0',DB = 'IN_PRODUCTION',WH = 'IN_PRODUCTION_WH',ROLE = 'IN_PRODUCTION',ACCOUNT = 'ik90710.us-east-1',SCHEMA='OUTPUTS')
        engine = En.get_engine()
        
        
        id_tipos = f'''
        SELECT DISTINCT ID_TIPO
        FROM MOTHER_TABLE.GLOBAL_CONCI_MANUAL_CONFIG
        WHERE SITE = 'MLM';
        '''
        df_tipos = pd.read_sql(id_tipos, engine)
        
        list_tipos = df_tipos['id_tipo'].tolist()
        
        try:
            expectd_columns = ['ID_TIPO', 'SKT_ID_A', 'SKT_ID_B', 'COMMENTS', 'SOURCE_A', 'SOURCE_B']
            df, id1 = Extractor.read_file(expectd_columns, myfile)
            
            if int(id1) in list_tipos:
                table, _, _, _, _, missing_a, missing_b = Extractor.get_table_names(id1, engine)
                #print(f'Table: {table}')
                if table == '':
                    raise MyError('Tabla no configurada', None)
                try:
                    df_write = Extractor.create_df_conci(df)
                    Extractor.validate_duplicated_in_file(df_write, 'SKT_ID_A', 'A')
                    Extractor.validate_duplicated_in_file(df_write, 'SKT_ID_B', 'B')
                    Extractor.check_miss(df_write['SKT_ID_A'], missing_a, engine, 'A')
                    Extractor.check_miss(df_write['SKT_ID_B'], missing_b, engine, 'B')
                    #df1 = pd.concat([df1, df_write], ignore_index = True)
                    df_write['REPORT'] = table
                    print('Writing to DB')
                    return df_write
                except:
                    pass
        except:
            pass
        

    