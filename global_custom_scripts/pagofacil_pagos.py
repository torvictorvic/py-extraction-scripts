import os
import pandas as pd
import re
import boto3
import traceback
from datetime import date, datetime
import pytz
from urllib.parse import urlparse
from io import BytesIO, StringIO


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
            #session = boto3.session.Session()
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
            with open(uri) as f:
                return uri, datetime.today()

class PagoFacilParser:
    def __init__(self, io):
        self.io = io
    def run(self):
        df = pd.read_csv(self.io, header=None, dtype=str, sep='#')
        try:
            for x in range(len(df)):
                if 'Td' in str(df.iloc[x, 1]) and 'Concepto' in str(df.iloc[x, 1]):
                    df.iloc[x, 1] = re.sub(
                        r"Td\s{3,}Concepto", "Td Concepto", str(df.iloc[x, 1]))

            df = df[1].str.split(r"\s{6,}", expand=True)
        except:
            for x in range(len(df)):
                if 'Td' in str(df.iloc[x, 0]) and 'Concepto' in str(df.iloc[x, 0]):
                    df.iloc[x, 0] = re.sub(
                        r"Td\s{3,}Concepto", "Td Concepto", str(df.iloc[x, 0]))
        if len(df) > 25:
            df = df[0].str.split(r"\s{6,}", expand=True)
        else:
            df = df[0].str.split(r"\s{6,15}", expand=True)
        a, b = df.shape
        for x in range(a):
            if 'RF  Retenci' in df.iloc[x, 0]:
                df.iloc[x, 3] = df.iloc[x, 2]
                df.iloc[x, 2] = df.iloc[x, 1]
                df.iloc[x, 1] = ''
        ret = True
        for x in range(0, a):
            for y in range(b):
                if 'Nro. Doc. Pago ' in str(df.iloc[x, y]):
                    n_doc_pago = str(df.iloc[x, y]).split(' ')[-1]
                elif 'Fecha pago' in str(df.iloc[x, y]):
                    fec_pago = str(df.iloc[x, y]).split(' ')[-1]
                elif 'Importe' == str(df.iloc[x, y]).replace(' ', ''):
                    importe = str(
                        df.iloc[x, y+1]).replace('.', '').replace(',', '.')
                elif 'Retencion IB' in str(df.iloc[x, y]) and ret:
                    cons_ret = str(df.iloc[x, y])
                    for z in range(b):
                        if ',' in df.iloc[x, z]:
                            if '-' in df.iloc[x, z]:
                                retencion = '-' + \
                                    df.iloc[x, z].replace('.', '').replace(
                                        ',', '.').replace('-', '')
                            else:
                                df.iloc[x, z].replace('.', '').replace(
                                    ',', '.').replace('-', '')
                            break
                    ret = False
                elif 'Pagador' in str(df.iloc[x, y]):
                    mercado_tipo = df.iloc[x, y]
        for x in range(0, a):
            if 'Td Concepto' in str(df.iloc[x, 0]) or 'RF  Ventas' in str(df.iloc[x, 0]):
                df.columns = df.iloc[x, :]
                df = df[x+1:]
                break
        df.columns = df.columns.astype(str).str.replace('.', '').str.strip()
        df = df.reset_index(drop=True)
        df['Fecha_copia'] = df['Fecha']
        df['Fecha_copia'].replace({None: ''}, inplace=True)
        df['fecha_liquidacion'] = df['Td Concepto'] + df['Fecha_copia']
        df.drop('Fecha_copia', inplace=True, axis=1)
        index_liq_ls = []
        for n in range(len(df['Td Concepto'])):
            if (len(df['Td Concepto'][n]) == 50):
                index_liq_ls.append(df.index[n])
        for n in range(len(df['Fecha'])):
            try:
                if (len(df['Fecha'][n]) == 8):
                    index_liq_ls.append(df.index[n])
            except:
                pass
        index_ls = []
        fecha_ls = []
        for n in range(len(df['fecha_liquidacion'])):
            if (df.index[n] in index_liq_ls):
                df['fecha_liquidacion'] = df['fecha_liquidacion'].str.replace("-","")
                df['fecha_liquidacion'] = df['fecha_liquidacion'].str.replace(" ","")
                df['fecha_liquidacion'] = df['fecha_liquidacion'].str.replace("[a-zA-Z]","", regex=True)
                index_ls.append(df.index[n])
                fecha_ls.append(df['fecha_liquidacion'][n])
            else:
                df['fecha_liquidacion'][n] = ""
        for n in range(len(fecha_ls)):
            try:
                df.fecha_liquidacion.loc[index_ls[n]:index_ls[n+1]] = fecha_ls[n]
            except:
                df.fecha_liquidacion.loc[index_ls[n]:df.index[-1]] = fecha_ls[n]
        for x in range(len(df)):
            if (str(df.loc[x, 'Td Concepto']).startswith('CYBA') or str(df.loc[x, 'Td Concepto']).startswith('INTE') or ('RF' in str(df.loc[x, 'Td Concepto']) and 'Ventas' in str(df.loc[x, 'Td Concepto']))):
                concepto = df.loc[x, 'Td Concepto']
            if pd.isna(df.loc[x, 'Bruto']):
                df.loc[x, 'Bruto'] = df.loc[x, "tx's"]
                df.loc[x, "tx's"] = ""
            if re.match(r'[0-9]{2,}/[0-9]{2,}/[0-9]{2,}', str(df.loc[x, 'Fecha'])):
                fecha = df.loc[x, "Fecha"]
            else:
                try:
                    if not pd.isna(df.loc[x, 'Fecha']):
                        df.loc[x, 'Td Concepto'] = df.loc[x,
                                                          'Td Concepto'] + ' ' + df.loc[x, 'Fecha']
                        df.loc[x, 'Fecha'] = fecha
                except:
                    pass
        df['N de Pago'] = n_doc_pago
        df['Fecha Pago'] = fec_pago
        df['Importe'] = importe
        df['Tipo'] = mercado_tipo
        for col in ["tx's", 'Bruto']:
            df[col] = df[col].str.replace('.', '').str.replace(',', '.')
        df['Bruto'] = df.Bruto.fillna("")
        df['Numero'] = df.Numero.fillna("")
        if "None" in df.columns:
            df['None'] = df['None'].replace('', None)
            df = df.dropna(axis=1, how='all')
            df['None'] = df['None'].fillna("")
            df['None'] = df['None'].str.replace('.','').str.replace(',','.') 
            for n in range(len(df.Bruto)):
                if (df['Bruto'][n] == "") and ("." in df['None'][n]):
                    df['Bruto'][n] = df['None'][n]
            df = df.drop('None', axis=1)
            for n in range(len(df.Numero)):
                if (df['Numero'][n] == "") and (" " in df["tx's"][n]):
                    df['Numero'][n] = df["tx's"][n].strip()[-12:]
                    df["tx's"][n] = ""
        else:
            df['Numero'] = df['Numero'].str.replace('.','').str.replace(',','.')
            for n in range(len(df.Bruto)):
                if (df['Bruto'][n] == "") and ("." in df['Numero'][n]):
                    df['Bruto'][n] = df['Numero'][n]
            for n in range(len(df.Numero)):
                if ("." in df['Numero'][n]) and (df['Td Concepto'][n][-1].isdigit()):
                    df['Numero'][n] = df['Td Concepto'][n].strip()[-12:]
                    df['Td Concepto'][n] = df['Td Concepto'][n].strip()[:-12]
            for n in range(len(df.Numero)):
                if ("." in df['Numero'][n]):
                    df['Numero'][n] = ""
        df = df[~df['Numero'].astype(str).str.contains(r'-')]
        df = df[~df['Numero'].astype(str).str.contains('[a-z]')]
        df = df.reset_index(drop=True)    
        for x in range(len(df)):
            try:
                if str(df.loc[x, 'Bruto']).endswith('-'):
                    df.loc[x, 'Bruto'] = '-' + \
                        df.loc[x, 'Bruto'].replace('-', '')
            except:
                print(traceback.format_exc())
        try:
            df.loc[len(df)] = [cons_ret, fecha, '', '', retencion,
                               n_doc_pago, fec_pago, importe, mercado_tipo]
        except:
            pass
        df = df[(df['Bruto'] != '') & ~(df['Bruto'].astype(
            str).str.contains('[a-zA-Z]'))]
        try:
            for n in range(len(df.Numero)):
                if (len(df['Fecha'][n]) != 8):
                    df['Fecha'][n] = ""
        except:
            pass
        df = df.dropna(axis=1, how='all')
        return df

class ExtractorPagoFacil:
    @ staticmethod
    def run(filename, **kwargs):
        file_, lm = FileReader.read(filename)
        df = PagoFacilParser(file_).run()
        df.columns = ['td_concepto', 'fecha', 'numero', 'transacciones',
                      'bruto', 'fecha_liq', 'num_pago', 'fecha_pago', 'importe', 'tipo']
        df = df[~df['td_concepto'].astype(str).str.contains('Total')]
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
        df['skt_extraction_rn'] = range(0, len(df))
        return df