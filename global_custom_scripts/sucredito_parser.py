import pytz
import boto3
import pandas as pd

from io import BytesIO
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
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri) as f:
                return uri,datetime.now()


# Inicio Parser Sucredito
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
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        colspecs = [(0,1),(1,16),(16,23),(23,31),(31,39),(39,47),(47,50),(50,59),(59,64),(64,66),(66,68),(68,81),(81,82),(82,95),(95,96),(96,98),(98,118),(118,121),(121,132),(132,144),(144,156),(156,157),(157,169),(169,170),(170,182),(182,183),(183,195),(195,196),(196,208),(208,209),(209,221),(221,222),(222,226),(226,227),(227,229),(229,240),(240,-1)]
        try:
            df = pd.read_fwf(file, colspecs=colspecs, header=None, encoding='latin1')
            df = df[(df[0] == 1)].reset_index(drop=True)
            df.columns = ['TIPO_REGISTRO','NRO_COMERCIO_LIQUIDADO','NRO_LIQUIDACION','FECHA_PAGO','FECHA_OPERACION','FECHA_PRESENTACION','CODIGO_MOVIMIENTO','CAJA_O_TERMINAL_POSNET','NRO_CUPON','CANTIDAD_CUOTAS','CUOTA_LIQUIDADA','IMPORTE_BRUTO_LIQUIDADO','SIGNO_IMPORTE_BRUTO','IMPORTE_NETO_LIQUIDADO','SIGNO_IMPORTE_NETO','TIPO_PLAN','NRO_PLASTICO_SOCIO','CODIGO_RECHAZO_DEBITO','NRO_AUTORIZACION','PAYMENT_ID','COMISIONES','SIGNO_COMISIONES','IVA_COMISIONES','SIGNO_IVA_COMISIONES','PAGO_VELOZ','SIGNO_PAGO_VELOZ','IVA_PAGO_VELOZ','SIGNO_IVA_PAGO_VELOZ','PAGO_ANTICIPADO','SIGNO_PAGO_ANTICIPADO','IVA_PAGO_ANTICIPADO','SIGNO_IVA_PAGO_ANTICIPADO','TIPO_TARJETA','CUOTAS_LIQUIDADAS','NRO_LOTE','REFUND_ID','NOTA']
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ", e)


class ExtractorTotalesComercioBasico:

    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        colspecs = [(0,1),(1,16),(16,23),(23,31),(31,34),(34,37),(37,40),(40,53),(53,54),(54,67),(67,68),(68,81),(81,82),(82,95),(95,96),(96,109),(109,110),(110,123),(123,124),(124,130)]
        try:
            df = pd.read_fwf(file, colspecs=colspecs, header=None, encoding='latin1')
            df = df[(df[0] == 2)].reset_index(drop=True)
            df.columns = ['TIPO_REGISTRO','NRO_COMERCIO_LIQUIDADO','NRO_LIQUIDACION','FECHA_PAGO','MONEDA','BANCO','SUCURSAL_BANCARIA','IMPORTE_BRUTO_LIQ_COMERCIO','SIGNO_IMPORTE_BRUTO_LIQ_COM','IMPORTE_NETO_LIQ_COMERCIO','SIGNO_IMPORTE_NETO_LIQ_COM','DESCUENTO_COMISION_COMERCIO','SIGNO_DESC_COMISION_COMERCIO','DESCUENTO_PAGO_VELOZ','SIGNO_DESC_PAGO_VELOZ','DESCUENTO_PAGO_ANTICIPADO','SIGNO_DESC_PAGO_ANTICIPADO','OTROS_CREDITOS','SIGNO_OTROS_CREDITOS','CANTIDAD_DETALLES']
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ", e)
    


class ExtractorTotalesComercioImpuestos:

    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        colspecs = [(0,1),(1,16),(16,23),(23,31),(31,44),(44,45),(45,58),(58,59),(59,72),(72,73),(73,86),(86,87),(87,100),(100,101),(101,114),(114,115),(115,128),(128,129),(129,142),(142,143),(143,156),(156,157),(157,170),(170,171),(171,184),(184,185),(185,198),(198,199)]
        try:
            df = pd.read_fwf(file, colspecs=colspecs, header=None, encoding='latin1')
            df = df[(df[0] == 3)].reset_index(drop=True)
            df.columns = ['TIPO_REGISTRO','NRO_COMERCIO_LIQUIDADO','NRO_LIQUIDACION','FECHA_PAGO','IVA_COMISIONES_TOTALES','SIGNO_IVA_COMISIONES_TOTALES','RETENCION_IVA','SIGNO_RETENCION_IVA','PERCEPCION_IVA','SIGNO_PERCEPCION_IVA','RETENCION_IMPUESTO_GANANCIAS','SIGNO_RETENCION_IMPUESTO_GANANCIAS','RETENCION_INGRESOS_BRUTOS','SIGNO_RETENCION_INGRESOS_BRUTOS','PERCEPCION_INGRESOS_BRUTOS','SIGNO_PERCEPCION_INGRESOS_BRUTOS','IMPUESTO_LEY_25063','SIGNO_IMPUESTO_LEY_25063','RETENCION_TEM','SIGNO_RETENCION_TEM','RETENCION_PUBLICIDAD_PROPAGANDA','SIGNO_RETENCION_PUBLICIDAD_PROPAGANDA','IVA_COMISIONES','SIGNO_IVA_COMISIONES','IVA_PAGO_VELOZ','SIGNO_IVA_PAGO_VELOZ','IVA_PAGO_ANTICIPADO','SIGNO_IVA_PAGO_ANTICIPADO']
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ", e)    


class ExtractorPieArchivo:

    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        colspecs = [(0,1), (1,31), (31,40), (40,48), (48,130)]
        try:
            df = pd.read_fwf(file, colspecs=colspecs, header=None, encoding='latin1')
            df = df[(df[0] == 4)].reset_index(drop=True)
            df.columns = ['Tipo_de_registro', 'Nombre_de_archivo', 'Cantidad_de_liquidaciones', 'Fecha_de_proceso','RELLENO']
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ", e)