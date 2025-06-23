import io
import re
import pytz
import boto3
import gnupg
import zipfile
import pandas as pd

from io import StringIO
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
            return obj,lm
        else:
            with open(uri) as f:
                return uri,datetime.today()

class FileReaderKey:

    @staticmethod
    def read(uri: str):
        origin = urlparse(uri, allow_fragments=False)
        llave ="""-----BEGIN PGP PRIVATE KEY BLOCK-----
            ** FAKE ** TEST ** ** FAKE ** TEST ** 
            -----END PGP PRIVATE KEY BLOCK-----
        """

        if origin.scheme in ('s3', 's3a'):
            session = boto3.session.Session()
            s3 = session.client('s3')
            gpg = gnupg.GPG(gnupghome="/tmp")
            # import
            import_result = gpg.import_keys(llave)
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            dd = gpg.decrypt(
                message = obj,
                passphrase='mlibre'
            )
            d_obj = dd.data
            return d_obj,lm
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

class Extractor:
    @staticmethod
    def run(filename):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')

        if ".zip" in filename:
            zip_file = zipfile.ZipFile(io.BytesIO(file))
            out = zip_file.infolist()[0].filename
            df = pd.read_csv(zip_file.open(zip_file.infolist()[0]),dtype=str,encoding='ISO-8859-1', sep="|")
            zip_file.close()
        elif ".CSV" in filename:
            df = pd.read_csv(io.BytesIO(file),dtype=str,encoding='ISO-8859-1', sep="|")
            out = (filename.split('/')[-1]) 

        formato = ["operacion","referencia","trx_comercio","estado","fecha","tipo","modalidad","concepto","moneda","importe","nro_tarjeta","marca_tarjeta","banco_emisor","codigo_sucursal","descripcion_sucursal","canal","terminal","terminal_desc","referencia_bancaria","estado_conciliacion","tipo_devolucion","motivo_rechazo","fecha_pago_original","nro_referencia_bancaria_original","motivo_devolucion","billetera_qr"]
        df.columns = formato
        df['importe'] = df['importe'].str.replace(',','.')
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df


class ExtractorLiquidaciones:
    @staticmethod
    def run(filename):
        if "csv.gpg" in filename:
            file,lm = FileReaderKey.read(filename)
        else:
            file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            try:
                try:
                    df = pd.read_excel(file,dtype=str)
                    if df.empty:
                        columns = ["fecha","hs_movimiento","cuenta","subcuenta","op_bancaria","desc_movi","referencia","importe","debito_credito","saldo","num_cuenta_origen","cvu_origen","cuit_originante","razon_social_originante","cuit_beneficiario","razon_social_beneficiario","comprobante_interbanking","nsbt"]
                        df = pd.DataFrame(columns=columns)
                        df = df.append(pd.Series(),ignore_index=True)
                    else:
                        columns = ["drop_0", "fecha", "drop_1", "cuenta", "drop_2", "subcuenta", "drop_3", "op_bancaria", "drop_4", "desc_movi", "drop_5", "drop_6", "referencia", "importe", "debito_credito","saldo", "cuit_originante", "razon_social_originante", "cuit_beneficiario", "razon_social_beneficiario", "comprobante_interbanking", "nsbt"]
                        df.columns = columns
                    for column in df.columns:
                        if 'drop_' in column:
                            df.drop(column, axis=1,inplace=True)

                    df = df[~df['fecha'].isin(['Fecha','FILTROS','MOVIMIENTOS TOTALES'])]
                    df = df[df['fecha'].notnull()]
                    df = df.reset_index(drop=True)
                    df['hs_movimiento'] = ""
                    df['num_cuenta_origen'] = ""
                    df['cvu_origen'] = ""
                    df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
                    df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
                    out = (filename.split('/')[-1])
                    df['file_name'] = out
                    df.reset_index(drop=True)
                    df['skt_extraction_rn'] = df.index.values
                    return df
                except:
                    df = pd.read_excel(file,dtype=str)
                    if df.empty:
                        columns = ["fecha","hs_movimiento","cuenta","subcuenta","op_bancaria","desc_movi","referencia","importe","debito_credito","saldo","num_cuenta_origen","cvu_origen","cuit_originante","razon_social_originante","cuit_beneficiario","razon_social_beneficiario","comprobante_interbanking","nsbt"]
                        df = pd.DataFrame(columns=columns)
                        df = df.append(pd.Series(),ignore_index=True)
                    else:
                        columns = ["drop_0", "fecha", "drop_1","hs_movimiento","drop_11", "cuenta", "drop_2", "subcuenta", "drop_3", "op_bancaria", "drop_4","drop_44", "desc_movi","referencia","importe","debito_credito","saldo","num_cuenta_origen","cuit_originante", "razon_social_originante", "cuit_beneficiario", "razon_social_beneficiario", "comprobante_interbanking", "nsbt"]
                        df.columns = columns
                    for column in df.columns:
                        if 'drop_' in column:
                            df.drop(column, axis=1,inplace=True)
                    df = df[~df['fecha'].isin(['Fecha','FILTROS','MOVIMIENTOS TOTALES'])]
                    df = df[df['fecha'].notnull()]
                    df = df.reset_index(drop=True)
                    df['cvu_origen'] = ""
                    df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
                    df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
                    out = (filename.split('/')[-1])
                    df['file_name'] = out
                    df.reset_index(drop=True)
                    df['skt_extraction_rn'] = df.index.values
                    return df
            except:
                df = pd.read_excel(file,dtype=str)
                if df.empty:
                    columns = ["fecha","hs_movimiento","cuenta","subcuenta","op_bancaria","desc_movi","referencia","importe","debito_credito","saldo","num_cuenta_origen","cvu_origen","cuit_originante","razon_social_originante","cuit_beneficiario","razon_social_beneficiario","comprobante_interbanking","nsbt"]
                    df = pd.DataFrame(columns=columns)
                    df = df.append(pd.Series(),ignore_index=True)
                else:
                    columns = ["drop_0", "fecha", "drop_1","hs_movimiento","drop_11", "cuenta", "drop_2", "subcuenta", "drop_3", "op_bancaria", "drop_4","drop_44", "desc_movi","referencia","importe","debito_credito","saldo","num_cuenta_origen","cvu_origen","cuit_originante", "razon_social_originante", "cuit_beneficiario", "razon_social_beneficiario", "comprobante_interbanking", "nsbt"]
                    df.columns = columns
                for column in df.columns:
                    if 'drop_' in column:
                        df.drop(column, axis=1,inplace=True)
                df = df[~df['fecha'].isin(['Fecha','FILTROS','MOVIMIENTOS TOTALES'])]
                df = df[df['fecha'].notnull()]
                df = df.reset_index(drop=True)
                df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
                df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
                out = (filename.split('/')[-1])
                df['file_name'] = out
                df.reset_index(drop=True)
                df['skt_extraction_rn'] = df.index.values
                return df
        except:
            file_A = StringIO(file.decode())
            df = pd.read_csv(file_A,dtype=str,sep="|")
            if df.empty:
                columns = ["fecha","hs_movimiento","cuenta","subcuenta","op_bancaria","desc_movi","referencia","importe","debito_credito","saldo","num_cuenta_origen","cvu_origen","cuit_originante","razon_social_originante","cuit_beneficiario","razon_social_beneficiario","comprobante_interbanking","nsbt"]
                df = pd.DataFrame(columns=columns)
                df = df.append(pd.Series(),ignore_index=True)
            else:
                columns = ["fecha","hs_movimiento","cuenta","subcuenta","op_bancaria","desc_movi","referencia","importe","debito_credito","saldo","num_cuenta_origen","cvu_origen","cuit_originante","razon_social_originante","cuit_beneficiario","razon_social_beneficiario","comprobante_interbanking","nsbt"]
                df.columns = columns
            df.desc_movi = df.desc_movi.replace({re.escape('CrC)dito'):'Crédito'}, regex=True)
            df.desc_movi = df.desc_movi.replace({re.escape("ComisiC3n"):'Comisión'}, regex=True)
            df = df.reset_index(drop=True)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
