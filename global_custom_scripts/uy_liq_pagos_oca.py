import pytz
import boto3
import pandas as pd

from io import StringIO
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
            obj = obj['Body'].read().decode()
            return obj,lm
        else:
            with open(uri) as f:
                return uri,datetime.today()

class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        tipo_tabla = kwargs['tipo_tabla']
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')

        rut_periodo_df = StringIO(file)
        body_df = StringIO(file)
        footer_df = StringIO(file)

        # Extract rut y periodo
        df = pd.read_csv(rut_periodo_df,dtype=object,header=None,nrows=1)
        df['rut'] = df[0].str.extract(r'(Rut: \d*)')
        df['rut'] = df['rut'].astype(str).str.replace('Rut: ','')
        df['periodo'] = df[0].str.extract(r'(Periodo:\d*)')
        df['periodo'] = df['periodo'].astype(str).str.replace('Periodo:','')

        # Extract body
        df1 = pd.read_csv(body_df,dtype=object,sep=';',skiprows=2,header=None)
        df1.drop(df1.columns[21],inplace=True,axis=1)
        formato = ["comercio","detalle","cantcupones","fechacupon","tarjeta","moneda","imp_presentado","imp_devuelto","imp_neto_liquidado","arancel","iva_arancel","recargofinanc","iva_recargo_financ","imp_otros","iva_otros","liq_a_cobrar","pos","modo","autorizacion","id_transaccion","id_wt"]
        df1.columns = formato
        for data in range(len(df1)):
            try:
                if 'Comercio' in df1.loc[data,'comercio']:
                    df1 = df1.loc[:data-1,:]
            except:
                pass
        df1['rut'] = df.loc[0,'rut']
        df1['periodo'] = df.loc[0,'periodo']
        formato = ["comercio","detalle","cantcupones","fechacupon","tarjeta","moneda","imp_presentado","imp_devuelto","imp_neto_liquidado","arancel","iva_arancel","recargofinanc","iva_recargo_financ","imp_otros","iva_otros","liq_a_cobrar","pos","modo","autorizacion","id_transaccion","id_wt","rut","periodo"]
        df1.columns = formato
        df1['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df1['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df1['file_name'] = out
        df1.reset_index(drop=True)
        df1['skt_extraction_rn'] = df1.index.values

        # Extract footer
        df2 = pd.read_csv(footer_df,dtype=object,sep=';',skiprows=2,header=None)
        df2.drop(df2.columns[21],inplace=True,axis=1)
        formato = ["comercio","detalle","cantcupones","fechacupon","tarjeta","moneda","imp_presentado","imp_devuelto","imp_neto_liquidado","arancel","iva_arancel","recargofinanc","iva_recargo_financ","imp_otros","iva_otros","liq_a_cobrar","pos","modo","autorizacion","id_transaccion","id_wt"]
        df2.columns = formato
        df2 = df2[["comercio","detalle","cantcupones"]]
        for data in range(len(df2)):
            if 'Comercio' in df2.loc[data,'comercio']:
                df2 = df2.loc[data:,:]
                break
        df2 = df2[df2['comercio'] != "Comercio"]
        formato = ['comercio','vto_cheque','imp_cheque']
        df2.columns = formato
        df2['imp_cheque'] = df2['imp_cheque'].astype(str).str.strip()
        df2['imp_cheque'] = df2['imp_cheque'].str.replace('.','')
        df2['imp_cheque'] = df2['imp_cheque'].str.replace(',','.')
        df2['rut'] = df.loc[0,'rut']
        df2['periodo'] = df.loc[0,'periodo']
        df2['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df2['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df2['file_name'] = out
        df2.reset_index(drop=True, inplace=True)
        df2['skt_extraction_rn'] = df2.index.values

        if tipo_tabla == 'liquidaciones':
            return df1

        if tipo_tabla == 'pagos':
            return df2
        
class Extractor_v2:
    @staticmethod
    def run(filename,**kwargs):  
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        
        body_df = StringIO(file)

        df = pd.read_csv(body_df,dtype=object, sep= ',')
        print("se lee el archivo")
        formato = ['fecha_vencimiento', 'subproducto', 'producto', 'pais_emisor',
            'id_transaccion_tpw', 'tipo_linea', 'rut', 'cod_subproducto',
            'fecha_transaccion', 'modo_entrada_aut',
            'numero_comercio_mayorista_minorista', 'importe_cupon',
            'retencion_del_minorista', 'cod_producto', 'tarjeta', 'id_liquidacion',
            'autorizacion', 'desc_moneda', 'id_cupon',
            'nro_comerciante_mayorista_minorista', 'numero_pos', 'fecha_publicacion',
            'fecha_proceso', 'cod_moneda', 'id_wt', 'nro_comercio',
            'fecha_liquidacion', 'id_sobre', 'plan_venta',
            'nombre_mayorista_minorista']
        df.columns = formato
        print("Se arman las columnas del df")
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df
        
class Extractor_Pagos_v2:
    @staticmethod
    def run(filename,**kwargs): 
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        body_df = StringIO(file)
        df = pd.read_csv(body_df,dtype=object, sep= ',')
        df.columns = df.columns.str.replace(' ', '_')
        df.columns = df.columns.str.replace('.','').str.lower()
        print("Se arman las columnas del df")
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df

        

