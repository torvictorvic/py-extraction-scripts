from datetime import date, timedelta, datetime
from urllib.parse import urlparse
import io
import boto3
import os
import os.path
import pytz
import zipfile


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
            # print(uri)

            if ".zip" in uri:
                Bucket = s3_url.bucket
                obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)['Body'].read()
                zip_file = zipfile.ZipFile(io.BytesIO(obj))
                for fn in zip_file.namelist():
                    # ts = str(os.path.getmtime(obj))
                    # ts = ts.replace('.', '_')
                    if ('Liquidadas' in fn):
                        dest = 'ARG_LIQUIDACIONES_PRISMA/'
                    elif ('Pagos' in fn):
                        dest = 'ARG_PAGOS_PRISMA/'
                    # Now copy the files to the 'unzipped' S3 folder
                    print(f"Copying file {fn}")
                    s3.put_object(Body=zip_file.open(fn).read(), Bucket=Bucket, Key=dest+fn)
                df = None
                return df
        else:
            with open(uri) as f:
                return uri,datetime.today()


class ExtractorPrismaZIP:

    @staticmethod
    def run(filename, **kwargs):
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        # print(type(file))
        print(f'Uploading {filename} . . .')
        try:
            print('Ingreso al try')
            if '.zip' in filename:
                print('leyendo .zip')
                obj = FileReader.read(filename)
                df = None
                return df
            else:
                print("zip files didn't found...")
        except Exception as e:
            print("Error al subir la fuente: ",e)
        print('Se retorna df')
