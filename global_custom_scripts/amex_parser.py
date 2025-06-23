import pytz
import boto3
import pandas as pd

from enum import Enum
from pandas import DataFrame
from datetime import datetime
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
            # session = boto3.Session(profile_name="sts")
            # s3 = session.client('s3')
            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri) as f:
                return uri,datetime.today()

class StringConverter(dict):
    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return str

    def get(self, default=None):
        return str

class AmexRecordType(Enum):
    HEADER = '0'
    PAYMENT = '1'
    TAXES = '2'
    SOC = '3'
    ROC = '4'
    ADJ = '5'
    TRAILER = '9'

class AmexExtractor:

    path: str
    record_type: AmexRecordType

    def __init__(self, path='~/Desktop/amex.csv', record_type='a'):
        self.path = path
        self.record_type = record_type

    @staticmethod
    def split(df: DataFrame, record_type: AmexRecordType) -> DataFrame:
        if record_type is AmexRecordType.HEADER:
            columns = ['AFILIACION', 'CONSTANTE10101', 'CONSTANTE0', 'CONSTANTE01', 'CONSTANTE02', 'CONSTANTE03',
                    'CONSTANTE04',
                    'FECHACREACION', 'HORACREACION', 'IDENTIFICADOR', 'HEADERFILENAME']
            df = df[df['EPA-ADJ-REC-TYPE'] == AmexRecordType.HEADER.value]
            df = df[df.columns[0:11]]
            df.columns = columns
        if record_type is AmexRecordType.TAXES:
            columns = ['AFILIACION',
                    'FECHAPAGO',
                    'BLOQUEREFERENCIA',
                    'AFILIACIONSUBMITE',
                    'SECUENCIA',
                    'TIPOREGISTRO',
                    'TIPOIMPUESTO',
                    'DESCIMPUESTO',
                    'BASEIMPONIBLE',
                    'FECHADEPOSITO',
                    'PORCENTAJEIMPUESTO',
                    'IMPORTEIMPUESTO']
            df = df[df['EPA-ADJ-REC-TYPE'] == AmexRecordType.TAXES.value]
            df = df[df.columns[0:12]]
            df.columns = columns
        elif record_type is AmexRecordType.PAYMENT:

            columns = ['AFILIACION',
                    'FECHADEPOSITO',
                    'BLOQUEREFERENCIA',
                    'AFILIACIONSUBMITE',
                    'SECUENCIA',
                    'TIPOREGISTRO',
                    'TIPOIMPUESTO',
                    'MONTODEPOSITO',
                    'BANCOID',
                    'SUCURSALID',
                    'CUENTABANCARIA',
                    'NOMBREESTABLECIMIENTO',
                    'MONEDAID',
                    'MONTODEBITOPREVIO',
                    'MONTOBRUTO',
                    'TASADESCUENTO',
                    'IVATASADESCUENTO',
                    'SERVICEFEE',
                    'MONTONETO',
                    'TIPODEPAGO']
            df = df[df['EPA-ADJ-REC-TYPE'] == AmexRecordType.PAYMENT.value]
            df = df[df.columns[0:20]]
            df.columns = columns
        elif record_type is AmexRecordType.SOC:
            columns = ['AFILIACION',
                    'FECHADEPOSITO',
                    'BLOQUEREFERENCIA',
                    'AFILIACIONSUBMITE',
                    'SECUENCIASOC',
                    'TIPOREGISTRO',
                    'TIPOIMPUESTO',
                    'FECHAPROCESO',
                    'REFERENCIASOC',
                    'MONTOBRUTODECLARADO',
                    'MONTOBRUTOCALCULADO',
                    'TASADESCUENTO',
                    'IVATASADESCUENTO',
                    'SERVICEFEE',
                    'MONTONETO',
                    'NUMEROTRANSACCIONES',
                    'MONEDAID',
                    'TASACAMBIO',
                    'NUMEROMENSUALIDAD',
                    'NUMEROACELERACION',
                    'FECHAORIGINAL',
                    'FECHAACELERADA',
                    'DIASACELERADOS',
                    'MONTOACELERACIONFEE',
                    'MONTONETOACELERADO',
                    'MONTOBRUTO',
                    'CONSTANTE']
            df = df[df['EPA-ADJ-REC-TYPE'] == AmexRecordType.SOC.value]
            df = df[df.columns[0:27]]
            df.columns = columns
        elif record_type is AmexRecordType.ROC:
            columns = ['AFILIACION',
                    'FECHADEPOSITO',
                    'BLOQUEREFERENCIA',
                    'AFILIACIONSUBMITE',
                    'SECUENCIASOC',
                    'TIPOREGISTRO',
                    'TIPOIMPUESTO',
                    'FECHAPROCESO',
                    'REFERENCIASOC',
                    'CODIGOAUTORIZACION',
                    'NUMEROTARJETA',
                    'MONTOCARGO',
                    'MONTOPRIMERAMENSUALIDAD',
                    'MONTORESTOMENSUALIDADES',
                    'NUMEROMENSUALIDADES',
                    'MENSUALIDAD',
                    'CODIGORECHAZO',
                    'DESCRIPCIONRECHAZO',
                    'REFERENCIAROC',
                    'REFERENCIAROC1',
                    'CONSTANTE',
                    'DESCUENTOTASASERVICIOPLANN',
                    'DESCUENTOTASAACELERACION']
            df = df[df['EPA-ADJ-REC-TYPE'] == AmexRecordType.ROC.value]
            df = df[df.columns[0:23]]
            df.columns = columns
        elif record_type is AmexRecordType.ADJ:
            columns = ['AFILIACION',
                    'FECHADEPOSITO',
                    'BLOQUEREFERENCIA',
                    'AFILIACIONSUBMITE',
                    'SECUENCIASOC',
                    'TIPOREGISTRO',
                    'TIPOIMPUESTO',
                    'CONSTANTE',
                    'MONTOBRUTO',
                    'TASADESCUENTO',
                    'TASAIVA',
                    'SERVICEFEE',
                    'MONTONETO',
                    'TARJETA',
                    'CODIGOAJUSTE',
                    'DESCRIPCIONAJUSTE',
                    'MONEDAID',
                    'NUMEROACELERACION',
                    'CONSTANTE1',
                    'CONSTANTE2',
                    'CONSTANTE3',
                    'CONSTANTE4',
                    'CONSTANTE5',
                    'CONSTANTE6',
                    'CONSTANTE7',
                    'CONSTANTE8',
                    'CONSTANTE9',
                    'CONSTANTE10']
            df = df[df['EPA-ADJ-REC-TYPE'] == AmexRecordType.ADJ.value]
            df = df[df.columns[0:28]]
            df.columns = columns
        elif record_type is AmexRecordType.TRAILER:
            columns = ['AFILIACION',
                    'CONSTANTE1',
                    'CONSTANTE2',
                    'CONSTANTE3',
                    'CONSTANTE4',
                    'TIPOREGISTRO',
                    'CONSTANTE5',
                    'FECHACREACIONARCHIVO',
                    'HORACREACIONARCHIVO',
                    'IDARCHIVO',
                    'CONSTANTE6',
                    'NUMEROREGISTROS']
            df = df[df['EPA-ADJ-REC-TYPE'] == AmexRecordType.TRAILER.value]
            df = df[df.columns[0:12]]
            df.columns = columns
        return df

    def run(self) -> DataFrame:
        initial_columns = ['EPA-ADJ-STTL-SE-NO',
                        'EPA-ADJ-STTL-DT',
                        'EPA-ADJ-STTL-SEQ',
                        'EPA-ADJ-SUBM-SE-NO',
                        'EPA-ADJ-SOC-SEQ',
                        'EPA-ADJ-REC-TYPE',
                        'EPA-ADJ-TAX-TYPE',
                        'EPA-ADJ-MSG-REF-DS',
                        'EPA-ADJ-GROSS-AM',
                        'EAP-ADJ-DISC-AM',
                        'EPA-ADJ-TAX-AM',
                        'EPA-ADJ-SVC-FEE',
                        'EPA-ADJ-NET-AM',
                        'EPA-ADJ-CM-NO',
                        'EPA-ADJ-MSG-CD',
                        'EPA-ADJ-MSG-DS',
                        'EPA-ADJ-SUBM-CURR-CD',
                        'EPA-ADJ-ACCEL-NO',
                        'EPA-ADJ-DB-GROSS-AMOUNT',
                        'EPA-ADJ-CR-GROSS-AMOUNT',
                        'EPA-ADJ-CBK-SUBM-SE-ACCT-NO',
                        'EPA-ADJ-CBK-ROC-CHRG-AMT',
                        'EPA-ADJ-CBK-ROC-CHRG-DT',
                        'EPA-ADJ-CBK-ROC-INV-NO',
                        'EPA-ADJ-CBK-SE-ROC-REF-NO',
                        'EPA-ADJ-CBK-VAT-INV-SEQ-NO',
                        'EPA-ADJ-CBK-ELEC-ROC-REF-NO',
                        'EPA-ADJ-CBK-NO-INSTALL']
        data = pd.read_csv(self.path, sep=',',
                        names=initial_columns,
                        encoding='ISO-8859-1',
                        converters=StringConverter(),
                        engine='c')

        return AmexExtractor.split(data, record_type=self.record_type)


class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        tipo_tabla = kwargs['tipo_tabla']
        for tipo in AmexRecordType:
            if tipo_tabla in str(tipo).split('.')[-1]:
                tipos=tipo
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        emp2 = AmexExtractor(file,tipos)
        df_amex = AmexExtractor.run(emp2)
        if df_amex.empty:
            df_amex = df_amex.append(pd.Series(), ignore_index=True)
        df_amex['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df_amex['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1]).replace('.TXT','.csv')
        df_amex['file_name'] = out
        df_amex.reset_index(drop=True)
        df_amex['skt_extraction_rn'] = df_amex.index.values
        return df_amex


class ExtractorPeru:
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
        try:
            positions = [0,9,21,33,37,56,59,73,81,89,101,113,125,137,149,150,151,171,177,192,203,205,215,250]
            col_specification =[]
            for i in range(0,len(positions) -1):
                cordenate = (positions[i],positions[i+1] )
                col_specification.append(cordenate)
            binary_df = StringIO(file.read().decode().strip().strip('\x00'))
            cols = ["codigo_comercio","referencia","num_lote","tipo_operacion","num_tarjeta","tipo_tarjeta","fecha_transaccion","fecha_proceso","fecha_abono","importe_tx","descuento_movimiento","comision_visa","comision_igv","importe_neto","tipo_captura","estado_abono","cuenta_bancaria","codigo_auth","id_unico_tx","numero_serie_terminal","numero_cuota","importe_cashback","fill"]
            df = pd.read_fwf(binary_df, colspecs=col_specification, header=None, dtype=object)
            df.columns = cols
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except pd.io.common.EmptyDataError:
            columns= ["codigo_comercio","referencia","num_lote","tipo_operacion","num_tarjeta","tipo_tarjeta","fecha_transaccion","fecha_proceso","fecha_abono","importe_tx","descuento_movimiento","comision_visa","comision_igv","importe_neto","tipo_captura","estado_abono","cuenta_bancaria","codigo_auth","id_unico_tx","numero_serie_terminal","numero_cuota","importe_cashback","fill"]
            df = pd.DataFrame(columns = columns)
            df = df.append(pd.Series(), ignore_index=True)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df

        except Exception as e:
            print("Error al subir la fuente: ",e)


class ExtractorCirculares():
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_excel(file,dtype=str)
        ls_columns = ["fecha_transaccion","fecha_liquidacion","no_establecimiento","valor_cargo","id_ubicacion","no_titular_tarjeta","tipo_ajuste","no_referencia_cargo","c_1","c_2","tipo"]
        df.columns = ls_columns
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        upload_date = lm.astimezone(new_timezone)
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        df['valor_cargo'] = df['valor_cargo'].str.replace('.','')
        df['valor_cargo'] = df['valor_cargo'].str.replace(',','.')
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        df['original_filename'] = out
        return df
