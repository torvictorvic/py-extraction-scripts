import pytz
import boto3
import pandas as pd
from io import BytesIO
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
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data, lm
        else:
            with open(uri) as f:
                return uri, datetime.today()


def is_conciliable(df):
    return df["conciliable"].str[-3:].eq("00").all()


class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        print('---*---' * 10)
        print(f'Uploading {filename} . . . . . .')
        file, lm = FileReader.read(filename)
        colspecs = [
            (9, 11),
            (9, 11),
            (28, 36),
            (28, 36),
            (13, 28),  # Modificado para incluir los últimos 2 como decimales
            (142, 169),
            (277, 279),
            (70, 73),
            (215, 230),
            (277, 279),
            (0, 2),
            (2, 9),
            (11, 13),
            (36, 60),
            (60, 62),
            (62, 70),
            (73, 75),
            (75, 95),
            (95, 135),
            (135, 175),
            (175, 215),
            (230, 237),
            (237, 277),
            (279, 300)
        ]

        df = pd.read_fwf(file, dtype=object, colspecs=colspecs, header=None)
        df = df[1:-1]
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        columns = [
            "tipo_operacion", "codigo_operacion", "Fecha_de_recep_archivo", "Fecha_de_Compra", "Importe",
            "Reference_ID", "conciliable", "Banco_Receptor_del_Cliente_Usuario",
            "Importe_del_IVA_de_la_Operacion", "Motivo_de_Devolucion",
            "Tipo_de_Registro",
            "Numero_de_Secuencia",
            "Codigo_de_Divisa",
            "Uso_Futuro_BBVA",
            "Tipo_de_Operacion_Costos",
            "Fecha_de_Vencimiento",
            "Tipo_de_Cuenta_del_Cliente_Usuario",
            "Numero_de_Cuenta_del_Cliente_Usuario",
            "Nombre_del_Cliente_Usuario",
            "Referencia_del_Servicio_Emisor",
            "Nombre_del_Titular_del_Servicio",
            "Referencia_Numerica_del_Emisor",
            "Referencia_Leyenda_del_Emisor",
            "Uso_Futuro"
        ]
        df.columns = columns

        # Convertir la columna 'Importe' a decimales
        df['Importe'] = df['Importe'].astype(float) / 100  # Dividir por 100 para considerar los últimos 2 caracteres como decimales

        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values

        df.reset_index(drop=True)

        return df
