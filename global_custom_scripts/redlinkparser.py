from typing import List
from urllib.parse import urlparse
from io import BytesIO,StringIO
import pdb
import boto3 as boto3
import pandas as pd
import pandas as pd
import glob
from datetime import datetime

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
            # session = boto3.Session(profile_name="sts")
            # s3 = session.client('s3')
                #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)['Body'].read().decode()
            binary_df = obj
            return binary_df
        else:
            with open(uri) as f:
                return f.read()

class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        file = FileReader.read(filename)
        
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            binary_df = StringIO(file)
            header_df = StringIO(file)
            footer_df = StringIO(file)
            col_specification = [(0, 1), (1, 4), (4, 15), (15, 27),(27,35),(35,45),(45,47)]
            cols = ['tipo_registro','id_concepto','id_usuario','importe',
                    'fecha_pago','id_ingresado_usuario', 'tipo_terminal']
            df = pd.read_fwf(binary_df, colspecs=col_specification, skiprows=1, skipfoot=1, header=None,dtype=str)
            df.columns = cols
            
            header_specification = [(0, 1), (1, 4), (4, 12), (12, 36)]
            cols_header = ['tipo_registro_header','codigo_ente_header','fecha_proceso_header','relleno_header']
            header = pd.read_fwf(header_df, colspecs=header_specification, nrows=1, header=None,dtype=str)
            header.columns = cols_header
            
            footer_specification = [(0, 1), (1, 7), (7, 23), (23, 36)]
            cols_footer = ['tipo_registro_footer','cantidad_registros_footer','total_importe_footer','filler']
            footer = pd.read_fwf(footer_df, colspecs=footer_specification, header=None,dtype=str)
            footer = footer[-1:]
            footer.columns = cols_footer
            footer = footer.reset_index(drop=True)
            df['tipo_registro_header'] =header.loc[0,'tipo_registro_header'] 
            df['codigo_ente_header'] =header.loc[0,'codigo_ente_header']
            df['fecha_proceso_header'] =header.loc[0,'fecha_proceso_header'] 
            df['relleno_header'] =header.loc[0,'relleno_header'] 
            df['tipo_registro_footer'] =footer.loc[0,'tipo_registro_footer'] 
            df['cantidad_registros_footer'] =footer.loc[0,'cantidad_registros_footer']
            df['total_importe_footer'] =footer.loc[0,'total_importe_footer'] 
            df['filler'] =footer.loc[0,'filler'] 
            df['report_date'] = datetime.today()
            df['file_name'] = filename.split('/')[-1]
            df.cantidad_registros_footer = df.cantidad_registros_footer.astype('int')
            df.total_importe_footer = df.total_importe_footer.astype('float')/100
            df.importe = df.importe.astype('float')/100
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df

        except pd.io.common.EmptyDataError:                
            columns = ['tipo_registro','id_concepto','id_usuario','importe','fecha_pago','id_ingresado_usuario','tipo_terminal','tipo_registro_header','codigo_ente_header','fecha_proceso_header','relleno_header','tipo_registro_footer','cantidad_registros_footer','total_importe_footer','filler']
            df = pd.DataFrame(columns = columns)
            df = df.append(pd.Series(), ignore_index=True)
            df['report_date'] = datetime.today()
            df['file_name'] = filename.split('/')[-1]
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df

        except Exception as e:
            print("Error al subir la fuente: ",e)

