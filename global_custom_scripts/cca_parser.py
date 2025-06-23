import pytz
import boto3
import pandas as pd

from datetime import  datetime
from io import StringIO, BytesIO
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
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        header_positions = [0,1,4,10,14,16,22,28]
        footer_positions =[0,1,7,22]
        positions = [0,1,7,11,17,29,41,53,59,65,71,74,78,98,110,210,214,234,246,346,415,416,446]
        col_specification =[]
        header_specification =[]
        footer_specification =[]
        for i in range(0,len(header_positions) -1):
            cordenate = (header_positions[i],header_positions[i+1] )
            header_specification.append(cordenate)
        for i in range(0,len(footer_positions) -1):
            cordenate = (footer_positions[i],footer_positions[i+1] )
            footer_specification.append(cordenate)
        for i in range(0,len(positions) -1):
            cordenate = (positions[i],positions[i+1] )
            col_specification.append(cordenate)
        header_df = StringIO(file)
        binary_df = StringIO(file)
        footer_df = StringIO(file)
        cols_header= ["tipo_registro_h","id_banco_h","fecha_compensacion_h","hora_compensacion_h","tipo_archivo_h","fecha_archivo_h","hora_archivo_h"]
        cols_footer=["tipo_registro_f","total_registros_f","total_monto_f"]
        cols = ["tipo_registro","n_secuencia","tipo_mensaje","codigo_trx","monto_trx","tms_cca","n_trace","fecha_ifo","hora_ifo","codigo_autorizacion","codigo_respuesta","banco_origen","cuenta_origen","rut_girador","nombre_girador","banco_destino","cuenta_destino","rut_destino","nombre_destino","referencia","indicador_tef","n_operacion"]
        df_header = pd.read_fwf(header_df, colspecs=header_specification, nrows=1, header=None, dtype=object)
        df_header.columns = cols_header
        df = pd.read_fwf(binary_df, colspecs=col_specification, skiprows=1, header=None, dtype=object)
        df.columns = cols
        df = df[:-1]
        df_footer = pd.read_fwf(footer_df, colspecs=footer_specification, skiprows=len(df)+1, header=None, dtype=object)
        df_footer.columns = cols_footer
        for col in df_header:
            df[col] =df_header.loc[0,col] 
        for col in df_footer:
            df[col] =df_footer.loc[0,col] 
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df