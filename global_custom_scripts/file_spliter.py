import os
import boto3

from io import StringIO, BytesIO
from urllib.parse import urlparse
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
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            sz = obj['ContentLength']
            obj = obj['Body'].read().decode()
            obj = StringIO(obj)
            return obj,lm,sz
        else:
            with open(uri) as f:
                return uri,datetime.today()

class SpliterRedelcom:
    @staticmethod
    def run(filename, **kwargs):
        file,lm,sz = FileReader.read(filename)
        ambiente= os.environ.get('ENVIRONMENT')
        filename = filename.split('/')[-1]
        if "test" in ambiente:
            bucket_dest = "global-in-sources-xxxxxxxxxx"
        else:
            bucket_dest = "global-in-sources-xxxxxxxxxx"
        route = 'CL/REDELCOM_TRANSBANK/TEST/'
        s3_client= (boto3.client('s3'))
        if sz >= 236000000:
            print ('Particion requerida')
            print('Inicia lectura')
            lines = file.readlines()
            file.close()
            print('Lectura correcta')
            file_header = []
            file_footer = []
            positions = []

            # Create file parts
            file_header = lines[0:1]
            file_footer = lines[-1:]
            last_line = len(lines) - 1
            for i in range(1,last_line, 950000):
                positions.append(i)
            positions.append(last_line)

            # Assemble files
            print('Iniciando ensamble de archivos')
            aux_filename = filename.split('.')
            for i in range(len(positions) - 1):
                split_name = aux_filename[0] + '_part' + str(i+1) + '.' + aux_filename[1]
                f = StringIO()
                f.writelines(file_header)
                f.writelines(lines[positions[i]:positions[i+1]])
                f.writelines(file_footer)
                datos = BytesIO(bytes(f.getvalue(), encoding='utf-8'))
                f.close()
                print('Subiendo: ' + route + split_name)
                s3_client.upload_fileobj(datos,Bucket=bucket_dest,Key=route + split_name)
                datos.close()
            return
        else:
            fl = file.read()
            file.close()
            datos = BytesIO(bytes(fl, encoding='utf-8'))
            print('No requiere partición')
            if sz > 20000000:
                print('Subiendo: ' + route + filename)
                s3_client.upload_fileobj(datos,Bucket=bucket_dest,Key=route + filename)
            else:
                print('Subiendo: ' + route + filename)
                s3_client.put_object(Body=datos.getvalue(),Bucket=bucket_dest,Key=route + filename)

class SpliterCuponesMLA:
    @staticmethod
    def run(filename, **kwargs):
        file,lm,sz = FileReader.read(filename)
        ambiente= os.environ.get('ENVIRONMENT')
        filename = filename.split('/')[-1]
        if "test" in ambiente:
            bucket_dest = "global-in-sources-xxxxxxxxxx"
        else:
            bucket_dest = "global-in-sources-xxxxxxxxxx"
        route = 'AR/CUPONES/'
        s3_client= (boto3.client('s3'))
        if sz >= 280000000:
            print ('Particion requerida')
            print('Inicia lectura')
            lines = file.readlines()
            file.close()
            print('Lectura correcta')
            file_header = []
            positions = []

            # Create file parts
            file_header = lines[0:1]
            last_line = len(lines)
            for i in range(1,last_line, 900000):
                positions.append(i)
            positions.append(last_line)

            # Assemble files
            print('Iniciando ensamble de archivos')
            aux_filename = filename.split('.')
            for i in range(len(positions) - 1):
                split_name = aux_filename[0] + '_part' + str(i+1) + '.' + aux_filename[1]
                f = StringIO()
                f.writelines(file_header)
                f.writelines(lines[positions[i]:positions[i+1]])
                datos = BytesIO(bytes(f.getvalue(), encoding='utf-8'))
                f.close()
                print('Subiendo: ' + route + split_name)
                s3_client.upload_fileobj(datos,Bucket=bucket_dest,Key=route + split_name)
                datos.close()
            return
        else:
            fl = file.read()
            file.close()
            datos = BytesIO(bytes(fl, encoding='utf-8'))
            print('No requiere partición')
            if sz > 20000000:
                print('Subiendo: ' + route + filename)
                s3_client.upload_fileobj(datos,Bucket=bucket_dest,Key=route + filename)
            else:
                print('Subiendo: ' + route + filename)
                s3_client.put_object(Body=datos.getvalue(),Bucket=bucket_dest,Key=route + filename)