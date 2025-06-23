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
            with open(uri, 'rb') as f:
                return BytesIO(f.read())

class Extractor:
    def run(self, filename, **kwargs):
        file = self.file.body
        lm= self.file.last_modified
        filename=self.file.key
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')

        try:
            df = pd.read_excel(file, sheet_name='eg-meli')
            fecha=datetime.strptime(filename.split('.')[0].split('_')[-1], '%d%m%Y').date()
            list1=df.iloc[2:5,1:2].to_numpy().transpose().tolist()[0]
            if sorted(['INTERCAMBIO NACIONAL', 'INTERCAMBIO DE TRANSACCIONES', 'EGLOBAL - MERCADO LIBRE'])==sorted(list1) and fecha:
                if '(+) CONTRACARGOS' in list(df.iloc[22:23,5]):
                    contracargos=list(df.iloc[22:23,8])[0]
                if '(+) RECHAZOS SINTAXIS' in list(df.iloc[24:25,5]):
                    rechazos=list(df.iloc[24:25,8])[0]
                if '(-) COMISION DE SINTAXIS' in list(df.iloc[29:30,5]):
                    comision=list(df.iloc[29:30,8])[0]
                if '(-) I.V.A. SINTAXIS' in list(df.iloc[36:37,5]):
                    iva_sintaxis=list(df.iloc[36:37,8])[0]
            df_final = pd.DataFrame(
                                        {
                                            "fecha_archivo": [fecha],
                                            "contracargos": [contracargos],
                                            "rechazos_sintaxis": [rechazos],
                                            "comision_sintaxis": [comision],
                                            "iva_sintaxis": [iva_sintaxis]
                                        }, dtype=object
                                    )
            df_final['reservado_1'] = None
            df_final['reservado_2'] = None
            df_final['reservado_3'] = None
            df_final['reservado_4'] = None
            df_final['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df_final['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            df_final['file_name'] = filename.split('/')[-1]
            print("Parseo exitoso")
            return df_final
        except Exception as e:
            print("Error al subir la fuente: ",e)

