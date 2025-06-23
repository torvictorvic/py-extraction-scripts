import pandas as pd
import re
import boto3
import traceback
from datetime import date, datetime
import pytz
from urllib.parse import urlparse
from io import BytesIO, StringIO
import pandas as pd
import PyPDF2
import os


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
            #session = boto3.session.Session()
            session = boto3.session.Session(region_name='us-east-1')
            s3 = session.client('s3')
            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data, lm
        else:
            return uri, datetime.today()


class RappiPagoParser:
    def __init__(self, io):
        self.io = io

    def run(self, **kwargs):
        pdfFileObj = self.io
        pdfReader = PyPDF2.PdfFileReader(pdfFileObj)

        data = []
        pags = pdfReader.getNumPages()

        if kwargs['tipo_tabla'] == 'Concepto':

            for x in range(pags):
                pageObj = pdfReader.getPage(x)

                palabras = pageObj.extractText().split('\n')
                # print(palabras)
                if palabras[0] == 'DOCUMENTO':
                    x = -1
                else:
                    x = 0
                next = False

                for ixx, el in enumerate(palabras):

                    if 'ORDEN DE PAGO' == el:
                        fecha_cobro = palabras[ixx+1]

                for ixx, el in enumerate(palabras):

                    if 'DOCUMENTO' == el:
                        inn = palabras.index(el)
                        divisa = palabras[inn+8]
                        if '$' in palabras[inn+8]:
                            divisa = palabras[inn+7]

                    if 'Ingresos brutos' == el:
                        item = {}
                        item[0] = palabras[ixx]
                        item[1] = ''
                        item[2] = divisa
                        item[3] = fecha_cobro
                        item[4] = palabras[ixx+3]
                        x = -1
                        data.append(item)

                    if 'Ganancias' == el:
                        item = {}
                        item[0] = palabras[ixx]
                        item[1] = ''
                        item[2] = divisa
                        item[3] = fecha_cobro
                        item[4] = palabras[ixx+3]
                        x = -1
                        data.append(item)

                    if el == 'CONCEPTO':
                        x = 0

                    if 'BENEFICIARIO:' == el:
                        raz_social = palabras[ixx+2]
                    # print(x)
                    if x == 0:
                        item = {}
                        if el == divisa:
                            item[x] = ''
                            item[x+1] = ''
                            item[x+2] = el
                            x += 3
                        else:
                            item[x] = el
                            x += 1
                    elif x > 0:
                        if x == 1:
                            if el == divisa:
                                item[x] = ''
                                item[x+1] = el
                                x += 1
                        item[x] = el
                        x += 1
                        if x == 5:
                            data.append(item)
                            x = 0

            df = pd.DataFrame(data)

            df.columns = df.iloc[0, :]
            df = df[1:]
            df = df[df['DIVISA'] == divisa]

            df['IMPORTE'] = df['IMPORTE'].str.replace(
                '.', '').str.replace(',', '.').replace('$', '')
            df = df[~df['IMPORTE'].str.contains(r'[a-z A-Z]')]
            df['FECHA_COBRO'] = fecha_cobro
            df = df[df['FECHA'].str.contains(r'[0-9]{2,}/[0-9]{2,}/[0-9]{2,}')]
            df['RAZON_SOCIAL'] = raz_social
            return df

        elif kwargs['tipo_tabla'] == 'Comprobante':

            for x in range(pdfReader.getNumPages()):
                pageObj = pdfReader.getPage(x)

                palabras = pageObj.extractText().split('\n')

                x = -1
                next = False
                for ixx, el in enumerate(palabras):
                    if next:
                        fecha_cobro = el
                        next = False

                    if 'ORDEN DE PAGO' == el:
                        next = True

                    if 'DOCUMENTO' == el:
                        x = 0

                    if el == 'CONCEPTO':
                        x = -1

                    if 'BENEFICIARIO:' == el:
                        raz_social = palabras[ixx+2]

                    if x == 0:
                        item = {}
                        item[x] = el
                        x += 1
                    elif x > 0:
                        if x == 2:
                            if el != 'FECHA' and '/' not in el:
                                item[x] = item[1]
                                item[1] = ''
                                x += 1
                        item[x] = el
                        x += 1
                        if x == 5:
                            data.append(item)
                            x = 0

            df = pd.DataFrame(data)
            df.columns = df.iloc[0, :]
            df = df[1:]

            # df['FECHA COMPROBANTES'] = fec
            # df['DIVISA'] = divisa

            df['IMPORTE'] = df['IMPORTE'].str.replace(
                '$', '').str.replace('.', '').str.replace(',', '.')

            df['FECHA_COBRO'] = fecha_cobro

            df = df[df['FECHA'].str.contains(r'[0-9]{2,}/[0-9]{2,}/[0-9]{2,}')]
            df['RAZON_SOCIAL'] = raz_social

            return df


class Extractor:

    @staticmethod
    def run(filename, **kwargs):
        file_, lm = FileReader.read(filename)
        df = RappiPagoParser(file_).run(**kwargs)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(
            my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df.columns = list(
            map(lambda x: str(x).strip().replace(' ', '_').lower(), df.columns))
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df

class ExtractorCirculares:
    @staticmethod
    def run(filename, **kwargs):
        file_, lm = FileReader.read(filename)
        df = pd.read_excel(file_,dtype=str)
        df.columns = ["fecha_liquidacion","codigo_empresa_central","emp_central","codigo_empresa_puesto","descripcion_emp_puesto","importe","nro_operacion","barra","fecha_rendicion","fecha_recaudacion","fecha_proceso"]
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(
            my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df