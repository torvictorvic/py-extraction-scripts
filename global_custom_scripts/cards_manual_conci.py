import os
import io
import base64
import pandas as pd
from utils.database import make_engine
from sqlalchemy import create_engine
from datetime import date, timedelta, datetime
import pytz
import boto3
from urllib.parse import unquote_plus
from io import BytesIO
from urllib.parse import urlparse

class StringConverter(dict):
    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return str

    def get(self, default=None):
        return str

class MyError(Exception):
    def __init__(self, message, ids):
        self.message = message
        self.ids = ids
    def __str__(self):
        return self.message


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
            return binary_data,lm
        else:
            with open(uri) as f:
                return uri,datetime.today()

class Engine:
    def get_engine(self,SCHEMA):    
        #read local variables
        USER = os.environ.get('SNOWFLAKE_USER_CARDS')
        PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD_CARDS')
        DB = os.environ.get('SNOWFLAKE_DATABASE_CARDS')
        WH = os.environ.get('SNOWFLAKE_WAREHOUSE_CARDS')
        ROLE = os.environ.get('SNOWFLAKE_ROLE_CARDS')
        ACCOUNT='ik90710'
        REGION='us-east-1'
        # SCHEMA='OUTPUTS'
        print('Connecting to Snowflake')
        params={'user':USER,
                'password':PASSWORD,
                'schema':SCHEMA,
                'warehouse':WH,
                'account':ACCOUNT,
                'region':REGION,
                'database':DB,
                "use_region": True
                }
        engine = make_engine(params)
        #engine = create_engine(f'snowflake://{USER}:{PASSWORD}@{ACCOUNT}/{DB}/{SCHEMA}?warehouse={WH}&role={ROLE}')
        return engine

def write_log(engine,status, file_name, err= False, message= None, ids=None):
    print('Writing log in db')
    date = datetime.datetime.now().astimezone(pytz.timezone("UTC"))
    df_log = pd.DataFrame([[status,file_name,err,message,ids,date]],columns=['estado','file','error','mensaje','skt_id','fecha_log'])
    df_log.to_sql(name='logs_conci_manual_config', con=engine, if_exists='append', index=False, chunksize=10000)
    return df

def read_file(path,expectd_columns,file_name):
    df = pd.read_csv(path, dtype='str', sep=';', header=0)
    file_cols= [x.upper() for x in df.columns]
    if file_cols != expectd_columns:
        raise MyError('Las columnas del archivo no coinciden con las esperadas',None)

    ids = df['ID_TIPO'].unique()
    if len(ids)>1:
        raise MyError('Se encontró mas de 1 ID_tipo',None)

    df['COMMENTS'] = df['COMMENTS'].fillna('')
    df['COMMENTS'] = df['COMMENTS'] + ' //Registro agregado mediante archivo masivo de nombre ' + str(file_name)

    return df, ids[0]


def get_table_names(id,engine,side='A',adjust_cbk=False):
    read_query = f'''
    SELECT *
    FROM MOTHER_TABLE.GLOBAL_CONCI_MANUAL_CONFIG
    WHERE ID_TIPO = {id};
    '''
    print('Reading config from DB')
    df_read = pd.read_sql(read_query, engine)
    if len(df_read) > 1:
        raise MyError('Se encontró mas de una tabla en la cual escribir',None)
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
    # print(report_name_cbk)
    return table_conci.lower(), site_cbk.lower(), table_adj.lower(), report_name_cbk.lower(), table_batch.lower(), miss_a.lower(), miss_b.lower()

def write_log(status, file_name, err= False, message= None, ids=None):
    print('Writing log in db')
    date = datetime.datetime.now().astimezone(pytz.timezone("UTC"))
    df_log = pd.DataFrame([[status,file_name,err,message,ids,date]],columns=['estado','file','error','mensaje','skt_id','fecha_log'])
    df_log.to_sql(name='logs_conci_manual_config', con=engine, if_exists='append', index=False, chunksize=10000)

def validate_duplicated_in_file(df,column_name, side=None):
    df_dupl = df[df.duplicated([column_name])]
    if len(df_dupl)>0:
        ids_str = '-'.join(map(str,df_dupl[column_name].unique()))
        if side is not None:
            msg = f'Al menos un id del lado {side} se encuentra más de una vez en el archivo cuando debe ser único'
        else:
            msg = 'Al menos un id se encuentra más de una vez en el archivo cuando debe ser único'
        raise MyError(msg,ids_str)

def check_miss(id_series, miss_table, engine, side=None):
    if miss_table == '':
        raise MyError('Vista de pendientes no configurada',None)
    if len(id_series)>100000:
        raise MyError('El archivo posee mas de 100000 registros a verificar',None)
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
    print('Validating ids in missing view')
    df_miss = pd.read_sql(query_miss, engine)
    if len(df_miss)>0:
        ids_str = '-'.join(map(str,df_miss.iloc[:,0]))
        if side is not None:
            msg = f'Al menos un id del lado {side} no se encuentra en estado pendiente'
        else:
            msg = 'Al menos un id no se encuentra en estado pendiente'
        raise MyError(msg,ids_str)
def write_adj(df_write,table,engine):
    cols = ",".join(df_write.columns)
    insq_q = f"INSERT INTO OUTPUTS.{table.upper()} ({cols}) "
    with engine.begin() as connection:
        for _,row in df_write.iterrows():
            ins = f"SELECT PARSE_JSON('{row['SKT_ID']}'), '{row['USER_ID']}', '{row['COMMENTS']}','{row['REASON']}',CONVERT_TIMEZONE('UTC',CURRENT_TIMESTAMP)::TIMESTAMP_NTZ,'{row['MISSING']}'"
            connection.execute(insq_q+ins)
def create_df_adj(df,tipo):
    df_write = pd.DataFrame()
    if tipo == 1:
        df = df.astype({'SKT_ID': int})
        validate_duplicated_in_file(df,'SKT_ID')
        df2 = df.groupby(['ID_TIPO', 'COMMENTS', 'REASON', 'SOURCE'])['SKT_ID'].apply(list).reset_index(name='ID_ARR')
        if len(df2) > 1:
            raise MyError('Se encontró mas de un tipo de ajuste',None)
        df_write = pd.DataFrame(columns=['SKT_ID', 'USER_ID', 'COMMENTS', 'REASON', 'CONCILIATION_DATETIME', 'MISSING'])
        df_write[['SKT_ID', 'COMMENTS', 'REASON', 'MISSING']] = df2[['ID_ARR', 'COMMENTS', 'REASON', 'SOURCE']]
        df_write['USER_ID'] = 0
    elif tipo == 2:
        df = df.astype({'SKT_ID': int})
        validate_duplicated_in_file(df,'SKT_ID')
        df2 = df.groupby(['ID_TIPO', 'COMMENTS', 'REASON', 'SOURCE'])['SKT_ID'].apply(list).reset_index(name='ID_ARR')
        if len(df2) > 1:
            raise MyError('Se encontró mas de un tipo de ajuste',None)
        df_write = pd.DataFrame(columns=['SKT_ID', 'USER_ID', 'COMMENTS', 'REASON', 'CONCILIATION_DATETIME', 'MISSING'])
        df_write[['SKT_ID', 'COMMENTS', 'REASON', 'MISSING']] = df2[['ID_ARR', 'COMMENTS', 'REASON', 'SOURCE']]
        df_write['USER_ID'] = 0
    return df_write
def create_df_conci(df):
    date = datetime.now().astimezone(pytz.timezone("UTC"))
    df_write = pd.DataFrame(columns=['SKT_ID_A','SKT_ID_B','USER_ID','COMMENTS','CONCILIATION_DATETIME','MISSING_A','MISSING_B'])
    df_write[['SKT_ID_A','SKT_ID_B','COMMENTS','MISSING_A','MISSING_B']] = df[['SKT_ID_A','SKT_ID_B','COMMENTS','SOURCE_A','SOURCE_B']]
    df_write['USER_ID']=0
    df_write['CONCILIATION_DATETIME']= date
    
    return df_write

def decode_base(base64_message):
    base64_bytes = base64_message.encode('ascii')
    message_bytes = base64.b64decode(base64_bytes)
    message = message_bytes.decode('ascii')
    return message


def main_func(folder,path,file_name,engine):
    
    if folder == 'Conciliaciones':
        expectd_columns = ['ID_TIPO','SKT_ID_A','SKT_ID_B','COMMENTS','SOURCE_A','SOURCE_B']
        df,id = read_file(path,expectd_columns,file_name)
        table, _, _, _, _, missing_a, missing_b = get_table_names(id, engine)
        print(f'Table: {table}')
        if table == '':
            raise MyError('Tabla no configurada',None)
        df_write = create_df_conci(df)
        print(df_write)
        validate_duplicated_in_file(df_write,'SKT_ID_A','A')
        validate_duplicated_in_file(df_write,'SKT_ID_B','B')
        check_miss(df_write['SKT_ID_A'], missing_a, engine, 'A')
        check_miss(df_write['SKT_ID_B'], missing_b, engine, 'B')
        print('Writing to DB')
        df_write.to_sql(name=table, con=engine, if_exists='append', index=False, chunksize=10000)
    elif folder == 'Ajustes':
        expectd_columns = ['ID_TIPO','SKT_ID','A_B','COMMENTS','REASON','SOURCE']
        df,id = read_file(path,expectd_columns,file_name)
        sides = df['A_B'].unique()
        _, _, table, _, _, missing, _ = get_table_names(id, engine, side=sides[0])
        print(f'Table_Adj: {table}')
        if table == '':
            raise MyError('Tabla no configurada',None)
        df_write = create_df_adj(df, tipo=1)
        check_miss(list(map(str,df_write['SKT_ID'][0])), missing, engine)
        print('Writing to DB')
        write_adj(df_write,table,engine)
    elif folder == 'Desconciliaciones':
        
        df = pd.read_csv(path,dtype=str,header=None)
        df.columns = ['ID_INPUT']
        for idx,valor in df.iterrows():
            mensaje = decode_base(valor['ID_INPUT'])
            df.loc[idx, 'ID_INPUT_CLEAN'] = mensaje
        for idx,valor in df.iterrows():
            id_input_clean = valor['ID_INPUT_CLEAN'].split(',')[0]
            if id_input_clean == '':
                df.loc[idx, 'SKT_ID_A'] = None
            else:
                df.loc[idx, 'SKT_ID_A'] = id_input_clean
            id_input_clean = valor['ID_INPUT_CLEAN'].split(',')[1]
            if id_input_clean == '':
                df.loc[idx, 'SKT_ID_B'] = None
            else:
                df.loc[idx, 'SKT_ID_B'] = id_input_clean
            report_name = valor['ID_INPUT_CLEAN'].split(',')[2]
            df.loc[idx, 'REPORT_NAME'] = report_name
        df['CREATED_AT'] = datetime.today()
        print('Writing to DB')
        print(df)
        En = Engine()
        engine = En.get_engine(SCHEMA='_GLOBAL_')
        df.to_sql(name='DESCONCILIACION_CARDS_GENERAL', con=engine, SCHEMA='_GLOBAL_', if_exists='append', index=False, chunksize=10000)
    
    elif folder == 'Desconciliaciones_batch':
        
        df = pd.read_csv(path,dtype=str,header=None)
        df.columns = ['ID_INPUT']
        for idx,valor in df.iterrows():
            mensaje = decode_base(valor['ID_INPUT'])
            df.loc[idx, 'ID_INPUT_CLEAN'] = mensaje
        for idx,valor in df.iterrows():
            id_input_clean = valor['ID_INPUT_CLEAN'].split(',')[1]
            if id_input_clean == '':
                df.loc[idx, 'BATCH_ID'] = None
            else:
                df.loc[idx, 'BATCH_ID'] = id_input_clean
            report_name = valor['ID_INPUT_CLEAN'].split(',')[2]
            df.loc[idx, 'REPORT_NAME'] = report_name
        df['CREATED_AT'] = datetime.today()
        print('Writing to DB')
        print(df)
        En = Engine()
        engine = En.get_engine(SCHEMA='_GLOBAL_')
        df.to_sql(name='DESCONCILIACION_CARDS_GENERAL_BATCH', con=engine, SCHEMA='_GLOBAL_', if_exists='append', index=False, chunksize=10000)
            
    else:
        raise MyError('S3 folder no configurado',None)

#write_log(engine,'Exitoso',file_name)

class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        buffer, lm = FileReader.read(filename)
       

        folder = str(filename).split('/')[4]
        file_name = str(filename).split('/')[5]
        print(f'key: {filename}')
        print(f'folder: {folder}')
        print(f'file_name: {file_name}')

        En = Engine()
        engine = En.get_engine(SCHEMA='OUTPUTS')
        print('starting manual configuration ',filename)
        date = datetime.now().astimezone(pytz.timezone("UTC"))

        
        try:
            main_func(folder,buffer,file_name,engine)
            print('Writing log in db')
            df_log = pd.DataFrame([['Exitoso',str(f'{folder}/{file_name}'),False,None,None,date]],columns=['estado','file','error','mensaje','skt_id','fecha_log'])
            return df_log
        except MyError as me:
            print('Sending to write error')
            # write_log(engine,'Error', str(f'{folder}/{file_name}'), True, message=str(me), ids=me.args[1])
            df_log = pd.DataFrame([['Error',str(f'{folder}/{file_name}'),True,str(me),me.args[1],date]],columns=['estado','file','error','mensaje','skt_id','fecha_log'])

            print('Error: ' + str(me))
            return df_log

        except Exception as e:
            print('Error: ' + str(e))
            # write_log(engine,'Error', str(f'{folder}/{file_name}'), True, message=f'Ocurrió un error indefinido con este mensaje: "{str(e)}"')
            df_log = pd.DataFrame([['Error',str(f'{folder}/{file_name}'),True,f'Ocurrió un error indefinido con este mensaje: "{str(e)}"',None,date]],columns=['estado','file','error','mensaje','skt_id','fecha_log'])
            return df_log




