import pandas as pd
from pandas import DataFrame
from enum import Enum
from urllib.parse import urlparse
from io import BytesIO, StringIO, TextIOWrapper
import boto3
from datetime import datetime
import re
import pytz
import os


def convert_negative(integer):
    if str(integer).endswith('-'):
        return '-' + str(integer).replace('-', '').replace('.', '').replace(',', '.')
    return integer.replace('.', '').replace(',', '.')


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
            obj = obj['Body'].read().decode('utf-8').encode('utf-8')
            bytes_io = BytesIO(obj)
            text_io = TextIOWrapper(bytes_io,encoding='utf-8')
            return text_io, lm
        else:
            with open(uri,encoding='utf-8') as f:
                return f.readlines(), datetime.today()


class StringConverter(dict):
    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return str

    def get(self, default=None):
        return str


class DataParser:

    io: str

    def __init__(self, io):
        self.io = io
        self.manifest = [
            {
                "start_at": 3,
                "ends_at": 39,
                "column_name": "td_concepto"
            },
            {
                "start_at": 42,
                "ends_at": 50,
                "column_name": "fecha"
            },
            {
                "start_at": 52,
                "ends_at": 63,
                "column_name": "numero"
            },
            {
                "start_at": 72,
                "ends_at": 81,
                "column_name": "transacciones"
            },
            {
                "start_at": 87,
                "ends_at": 107,
                "column_name": "bruto"
            },
        ]

    def run(self) -> DataFrame:
        data = []

        pattern = re.compile(r'[0-9]*', re.IGNORECASE)

        n_doc_pago = ''
        fec_pago = ''
        importe = ''
        mercado_tipo = ''
        for line in self.io:
            if 'Fecha pago' in line:
                fec_pago = line.split(' ')[-1]

                continue
            elif 'Nro. Doc. Pago' in line:
                n_doc_pago = line.split(' ')[-1]
                mercado_tipo = [x for x in pattern.findall(line) if x != ''][0]
                continue
            elif 'Importe' in line and not '.... Importe .....' in line:
                importe = line.split(
                    ' ')[-1].replace('.', '').replace(',', '.')
                continue

            item = {}
            for col in self.manifest:
                item[col['column_name']
                     ] = line[col['start_at'] - 1: col['ends_at']].strip()

            data.append(item)

        df = pd.DataFrame(data)
        df['N de Pago'] = n_doc_pago.strip()
        df['Fecha Pago'] = fec_pago.strip()
        df['Importe'] = importe.strip()
        df['Tipo'] = mercado_tipo.strip()
        fecha = ''
        for x in range(len(df)):
            if re.match(r'[0-9]{2,}/[0-9]{2,}/[0-9]{2,}', str(df.loc[x, 'fecha'])):
                fecha = df.loc[x, "fecha"]

        df['fecha'] = fecha

        df = df[(df['bruto'].str.contains('[0-9]')) & ~
                (df['bruto'].str.contains('[a-zA-Z]')) & ~(df['bruto'].str.contains("\*"))]

        df = df[~(df['td_concepto'].str.contains('Total'))
                & (df['numero'] != '')]
        df['bruto'] = df['bruto'].apply(convert_negative)
        return df


class ExtractorPagoFacil:

    @staticmethod
    def run(filename, **kwargs):
        file_, lm = FileReader.read(filename)
        df = DataParser(file_).run()
        # Append Report Date and Filename
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(
            my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df.columns = ['td_concepto', 'fecha', 'numero', 'transacciones',
                      'bruto', 'num_pago', 'fecha_pago', 'importe', 'tipo']
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = range(0, len(df))
        return df

