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


# Inicio Parser CCA
class ExtractorCabecera:

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
        colspecs = [(0,1), (1,2), (2,8), (8,14), (14,18), (18,20), (20,21), (21,22), (22,25), (25,40), (40,90), (90,140), (140,190), (190,240), (240,241), (241,341), (341,371), (371,401), (401,421), (421,436), (436,438), (438,443), (443,453), (453,463), (463,473), (463,473), (473,476), (476,486), (486,501), (501,546), (546,591), (591,602), (602,647), (647,662), (662,677), (677,692), (692,702), (702,717), (717,732), (732,747)]
        try:
            df = pd.read_fwf(file, colspecs=colspecs, header=None, encoding='latin1', dtype=object)
            
            encabezado = list(df.iloc[0][1:6])
            cantidad_mov = (df[1]=='D').sum()

            df = df[(df[0] == '1') & (df[1] == 'C')].reset_index(drop=True)
            df.columns = ['strMarcaC', 'strTipoRegC', 'strCodForm', 'strCodTipPapel', 'strSucursal', 'strTipoBanca', 'strTipoCliente', 'strSexo', 'intEdad', 'strRut', 'strNombre','strApePaterno', 'strApeMaterno', 'strRazonSocial', 'strTipoEnvio', 'strDireccion', 'strComuna', 'strCiudad', 'strNumCuentaC', 'strMoneda', 'strCodProd', 'strCodSubProd', 'intNumFolio', 'dtFechaDesde', 'dtFechaHasta', 'dtFechaEmision', 'strTotPaginas', 'dtFechaCartAnt', 'dtSaldoFinAnt', 'strNomEjecutivo', 'strOficEjecutivo', 'strTelefEjecutivo', 'strEmailEjecutivo', 'intMontoAprob', 'intMontoUtilizado', 'intMontoDisp', 'dtFechaVenc', 'strMonAprobAdd', 'strMonUtilAdd', 'strMonDispAdd']
            
            df['encabezado'] = ''.join(encabezado)
            df['cantidad_movimientos'] = cantidad_mov

            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values

            return df
        except Exception as e:
            print("Error al subir la fuente: ", e)


class ExtractorDetalle:

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
        colspecs = [(0,1), (1,2), (2,22), (22,32), (32,36), (36,41), (41,155), (166,175), (175,176), (176,190), (190,204), (204,219)]
        try:
            df = pd.read_fwf(file, colspecs=colspecs, header=None, encoding='latin1',dtype=object)
            df = df[(df[1] == 'D')].reset_index(drop=True)
            df.columns = ['strMarcaD', 'strTipoRegD', 'strNumCuenta', 'strNumFolio', 'intCorrelativo', 'dtFechaMov', 'strDescGlosas', 'intNumDoc', 'strTipoMov', 'intMontoCargo', 'intMontoAbono', 'intSaldo']
            
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ", e)
    


class ExtractorPiePagina:

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
        colspecs = [(0,1), (1,2), (2,22), (22,32), (32,47), (47,61), (61,75), (75,90), (90,105), (105,120), (120,462), (462,476), (476,490), (490,504), (504,518), (518,532), (532,546), (546,560), (560,574), (574,588), (588,602), (602,616), (616,630)]
        try:
            df = pd.read_fwf(file, colspecs=colspecs, header=None, encoding='latin1',dtype=object)
            df = df[(df[1] == 'R')].reset_index(drop=True)
            df.columns = ['strMarcaR', 'strResumenR', 'strNumCuenta', 'strNumFolio', 'intSaldoInicial', 'intTotalCargo', 'intTotalAbono', 'intSaldoFinal', 'intSaldoDispon', 'intTotalComision', 'strPublic1', 'intComision1', 'intComision2', 'intComision3', 'intComision4', 'intComision5', 'intComision6', 'intComision7', 'intComision8', 'intComision9', 'intComision10', 'intComision11', 'intComision12']
            
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ", e)    

