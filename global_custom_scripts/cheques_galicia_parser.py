import boto3
import pandas as pd
import fitz  # PyMuPDF
from datetime import datetime
import re
import pytz
from urllib.parse import urlparse
from io import BytesIO


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
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data,lm
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
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        try:
            doc = fitz.open(stream=file.read(), filetype="pdf")
            print('cargo el archivo')
            text = ""
            for page_num in range(doc.page_count):
                page = doc.load_page(page_num)
                text += page.get_text()
            lista = text.splitlines()
            dict={
                'nro_operacion' : lista[5],
                'tipo_venta' : lista[6],
                'fecha_ejecutada' : lista[7],
                'empresa' : lista[9],
                'CUIT_empresa' : lista[11],
                'cuenta_acreditar' : lista[13],
                'monto_neto' : lista[17].replace('.','').replace(',','.').replace('$ ',''),
                'tasa' : lista[18].replace(' %',''),
                'cantidad_cheques' : lista[19],
                'intereses' : lista[23].replace('.','').replace(',','.').replace('$ ',''),
                'iva_intereses' : lista[24].replace('.','').replace(',','.').replace('$ ',''),
                'iva_percepcion' : lista[25].replace('.','').replace(',','.').replace('$ ',''),
                'impuesto_sellos' : lista[29].replace('.','').replace(',','.').replace('$ ',''),
                'ingresos_brutos' : lista[30].replace('.','').replace(',','.').replace('$ ',''),
                'monto_bruto' : lista[31].replace('.','').replace(',','.').replace('$ ','')
                    }

            head_data = pd.DataFrame(dict,index=[0])
            print('cargo el header')
            tabla = lista[37:]
            filas = []
            i = 0
            while i < len(tabla):
                if tabla[i] == 'Banco Galicia' and i + 1 < len(tabla):
                    if i + 4 < len(tabla):
                        sublist = tabla[i:i + 5]
                        filas.append(sublist)
                        i += 5
                    else:
                        break
                else:
                    i += 1

            print('cargo los cheques')
            cheques = pd.DataFrame(filas,columns=['banco','nro_cheque','cuit_librador','vencimiento','monto'])
            cheques['monto'] = cheques['monto'].str.replace('$ ','').str.replace('.','').str.replace(',','.')
            tabla = head_data.merge(cheques, how='cross')
            print('unio cheques con cabecera')
            # Adición de trazabilidad
            tabla['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            tabla['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            tabla['file_name'] = out
            tabla.reset_index(drop=True)
            tabla['skt_extraction_rn'] = tabla.index.values
            print('Cargó el df')
            
            return tabla
        except Exception as e:
            print("Error al subir la fuente: ",e)