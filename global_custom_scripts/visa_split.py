import io
import os
import sys
import time
import glob
import pytz
import math
import boto3
import numpy
import zipfile
import os.path
import numpy as np
import pandas as pd

from enum import Enum
from zipfile import ZipFile
from pandas import DataFrame
from io import StringIO, BytesIO
from urllib.parse import urlparse
from datetime import date, timedelta, datetime

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
            obj = obj['Body'].read().decode()
            return obj,lm
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
    def run(filename, **kwargs):
        tipo_tabla = kwargs['tipo_tabla']
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')

        def explode(df, lst_cols, fill_value='', preserve_index=False):
            # make sure `lst_cols` is list-alike
            if (lst_cols is not None
                and len(lst_cols) > 0
                and not isinstance(lst_cols, (list, tuple, np.ndarray, pd.Series))):
                lst_cols = [lst_cols]
            # all columns except `lst_cols`
            idx_cols = df.columns.difference(lst_cols)
            # calculate lengths of lists
            lens = df[lst_cols[0]].str.len()
            # preserve original index values
            idx = np.repeat(df.index.values, lens)
            # create "exploded" DF
            res = (pd.DataFrame({
                        col:np.repeat(df[col].values, lens)
                        for col in idx_cols},
                        index=idx)
                    .assign(**{col:np.concatenate(df.loc[lens>0, col].values)
                                    for col in lst_cols}))
            # append those rows that have empty lists
            if (lens == 0).any():
                # at least one list in cells is empty
                res = (res.append(df.loc[lens==0, idx_cols], sort=False)
                        .fillna(fill_value))
            # revert the original index order
            res = res.sort_index()
            # reset index if requested
            if not preserve_index:
                res = res.reset_index(drop=True)
            return res
        
        registros_f_A = StringIO(file)
        registros_f_B = StringIO(file)
        registros_f_C = StringIO(file)
        
        # Footer A
        widths_registro_A = [226]
        df = pd.read_fwf(registros_f_A,dtype=object,header=None, widths=widths_registro_A)
        form = ["abc"]
        df.columns = form
        df_A = df[df.abc.str.len() == 215]
        df = df[df.abc.str.len() == 226]
        df['last'] = df['abc'].str[-10:]
        df = df.loc[df["last"].str.contains("^\d{10}$"), :]
        df = df.drop(['last'], axis=1)
        df['fecha_liquidacion'] = df['abc'].astype(str).str[0:8]
        df['comercio'] = df['abc'].astype(str).str[8:17]
        df['sucursal'] = df['abc'].astype(str).str[17:20]
        df['moneda'] = df['abc'].astype(str).str[31:35]
        df['descripcion'] = df['abc'].astype(str).str[51:72]
        df['fecha_pago'] = df['abc'].astype(str).str[72:80]
        df['importe_bruto'] = df['abc'].astype(str).str[80:92]
        df['s_importe_bruto'] = df['abc'].astype(str).str[92:93]
        df['importe_comision'] = df['abc'].astype(str).str[93:105]
        df['s_importe_comision'] = df['abc'].astype(str).str[105:106]
        df['importe_comision_iva'] = df['abc'].astype(str).str[106:118]
        df['s_importe_comision_iva'] = df['abc'].astype(str).str[118:119]
        df['deducciones'] = df['abc'].astype(str).str[119:131]
        df['s_deducciones'] = df['abc'].astype(str).str[131:132]
        df['monto_a'] = df['abc'].astype(str).str[132:144]
        df['s_monto_a'] = df['abc'].astype(str).str[144:145]
        df['monto_b'] = df['abc'].astype(str).str[145:157]
        df['s_monto_b'] = df['abc'].astype(str).str[157:158]
        df['monto_c'] = df['abc'].astype(str).str[158:170]
        df['s_monto_c'] = df['abc'].astype(str).str[170:171]
        df['monto_d'] = df['abc'].astype(str).str[171:183]
        df['s_monto_d'] = df['abc'].astype(str).str[183:184]
        df['neto'] = df['abc'].astype(str).str[184:197]
        df['s_neto'] = df['abc'].astype(str).str[197:198]
        df['campo_a'] = df['abc'].astype(str).str[198:204]
        df['campo_b'] = df['abc'].astype(str).str[204:208]
        df['campo_c'] = df['abc'].astype(str).str[208:211]
        df['campo_d'] = df['abc'].astype(str).str[211:215]
        df['campo_e'] = df['abc'].astype(str).str[215:226]
        df = df.drop(['abc'], axis=1)
        df_A['fecha_liquidacion'] = df_A['abc'].astype(str).str[0:8]
        df_A['comercio'] = df_A['abc'].astype(str).str[8:17]
        df_A['sucursal'] = df_A['abc'].astype(str).str[17:20]
        df_A['moneda'] = df_A['abc'].astype(str).str[31:35]
        df_A['descripcion'] = df_A['abc'].astype(str).str[51:72]
        df_A['fecha_pago'] = df_A['abc'].astype(str).str[72:80]
        df_A['importe_bruto'] = df_A['abc'].astype(str).str[80:92]
        df_A['s_importe_bruto'] = df_A['abc'].astype(str).str[92:93]
        df_A['importe_comision'] = df_A['abc'].astype(str).str[93:105]
        df_A['s_importe_comision'] = df_A['abc'].astype(str).str[105:106]
        df_A['importe_comision_iva'] = df_A['abc'].astype(str).str[106:118]
        df_A['s_importe_comision_iva'] = df_A['abc'].astype(str).str[118:119]
        df_A['deducciones'] = df_A['abc'].astype(str).str[119:131]
        df_A['s_deducciones'] = df_A['abc'].astype(str).str[131:132]
        df_A['monto_a'] = df_A['abc'].astype(str).str[132:144]
        df_A['s_monto_a'] = df_A['abc'].astype(str).str[144:145]
        df_A['monto_b'] = df_A['abc'].astype(str).str[145:157]
        df_A['s_monto_b'] = df_A['abc'].astype(str).str[157:158]
        df_A['monto_c'] = df_A['abc'].astype(str).str[158:170]
        df_A['s_monto_c'] = df_A['abc'].astype(str).str[170:171]
        df_A['monto_d'] = df_A['abc'].astype(str).str[171:183]
        df_A['s_monto_d'] = df_A['abc'].astype(str).str[183:184]
        df_A['neto'] = df_A['abc'].astype(str).str[184:197]
        df_A['s_neto'] = df_A['abc'].astype(str).str[197:198]
        df_A['campo_a'] = df_A['abc'].astype(str).str[198:204]
        df_A['campo_b'] = df_A['abc'].astype(str).str[204:208]
        df_A['campo_c'] = df_A['abc'].astype(str).str[208:211]
        df_A['campo_d'] = df_A['abc'].astype(str).str[211:215]
        df_A = df_A.drop(['abc'], axis=1)
        df = pd.concat([df_A, df])
        df['indice'] = df.index.values
        df = df.sort_index()
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True, inplace=True)
        df['skt_extraction_rn'] = df.index.values

        # Footer B
        widths_registro_B = [200]
        df_B = pd.read_fwf(registros_f_B,dtype=object,header=None, widths=widths_registro_B)
        form = ["abc"]
        df_B.columns = form
        df_B = df_B[df_B.abc.str.len() == 117]
        df_B['fecha_liquidacion'] = df_B['abc'].astype(str).str[0:8]
        df_B['comercio'] = df_B['abc'].astype(str).str[8:17]
        df_B['sucursal'] = df_B['abc'].astype(str).str[17:20]
        df_B['moneda'] = df_B['abc'].astype(str).str[31:35]
        df_B['codigo_op'] = df_B['abc'].astype(str).str[34:36]
        df_B['descripcion'] = df_B['abc'].astype(str).str[57:84]
        df_B['fecha_pago'] = df_B['abc'].astype(str).str[84:92]
        df_B['signo_b'] = df_B['abc'].astype(str).str[93:94]
        df_B['monto_deducciones'] = df_B['abc'].astype(str).str[95:106]
        df_B['monto_a'] = df_B['abc'].astype(str).str[106:117]
        df_B['indice'] = df_B.index.values
        df_B = df_B.drop(['abc'], axis=1)
        df_B = df_B.reset_index(drop=True)
        df_B['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df_B['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df_B['file_name'] = out
        df_B.reset_index(drop=True)
        df_B['skt_extraction_rn'] = df_B.index.values

        # Footer C
        widths_registro_C = [200]
        df_C = pd.read_fwf(registros_f_C,dtype=object,header=None, widths=widths_registro_C)
        form = ["abc"]
        df_C.columns = form
        df_C = df_C[df_C.abc.str.len() == 187]
        df_C['fecha_liquidacion'] = df_C['abc'].astype(str).str[0:8]
        df_C['comercio'] = df_C['abc'].astype(str).str[8:17]
        df_C['sucursal'] = df_C['abc'].astype(str).str[17:20]
        df_C['moneda'] = df_C['abc'].astype(str).str[31:35]
        df_C['importe_bruto'] = df_C['abc'].astype(str).str[58:71]
        df_C['s_importe_bruto'] = df_C['abc'].astype(str).str[71:72]
        df_C['importe_bruto_a'] = df_C['abc'].astype(str).str[79:93]
        df_C['s_importe_bruto_a'] = df_C['abc'].astype(str).str[93:94]
        df_C['importe_comision'] = df_C['abc'].astype(str).str[94:106]
        df_C['s_importe_comision'] = df_C['abc'].astype(str).str[106:107]
        df_C['importe_comision_iva'] = df_C['abc'].astype(str).str[107:119]
        df_C['s_importe_comision_iva'] = df_C['abc'].astype(str).str[119:120]
        df_C['importe_bruto_b'] = df_C['abc'].astype(str).str[120:134]
        df_C['s_importe_bruto_b'] = df_C['abc'].astype(str).str[134:135]
        df_C['deducciones'] = df_C['abc'].astype(str).str[158:171]
        df_C['s_deducciones'] = df_C['abc'].astype(str).str[171:172]
        df_C['monto_a'] = df_C['abc'].astype(str).str[172:186]
        df_C['s_monto_a'] = df_C['abc'].astype(str).str[186:187]
        df_C['indice'] = df_C.index.values
        df_C = df_C.drop(['abc'], axis=1)
        df_C = df_C.reset_index(drop=True)
        df_C['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df_C['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df_C['file_name'] = out
        df_C.reset_index(drop=True)
        df_C['skt_extraction_rn'] = df_C.index.values

        # Footer D
        df_B['monto_deducciones_A'] = df_B['monto_deducciones'].astype("int64")
        df_B['monto_iva'] = df_B['monto_a'].astype("int64")
        df['monto_a'] = df['monto_a'].fillna(0)
        df['monto_a_A'] = df['monto_a'].astype("int64")
        df['importe_bruto_A'] = df['deducciones'].astype("int64")
        df['importe_bruto_B'] = df['importe_bruto_A']# + df['monto_a_A']

        # -------------------------------------------------------
        df_F = df[df['descripcion'].str.contains("CONTAD")]
        df_F_aux = df[df['descripcion'].str.contains("VISA")]
        df_F = pd.concat([df_F, df_F_aux], ignore_index=True)
        # -------------------------------------------------------

        df_B_A = DataFrame({'sum_deducciones': df_B.groupby("descripcion").monto_deducciones_A.sum()}).reset_index()
        df_B_B = DataFrame({'indice': df_B.groupby("descripcion").indice.min()}).reset_index()
        df_B_D = DataFrame({'indice': df_B.groupby(["file_name", "descripcion"]).indice.min()}).reset_index()
        df_B_C = DataFrame({'sum_iva': df_B.groupby("descripcion").monto_iva.sum()}).reset_index()

        df_B_A = df_B_A.merge(df_B_B, how='left', on='descripcion')
        df_B_A = df_B_A.merge(df_B_C, how='left', on='descripcion')
        df_B_A = df_B_A.merge(df_B_D, how='left', on='descripcion')
        del df_B_B
        df_B_A = df_B_A.sort_values(by='indice_y').reset_index(drop=True)

        df_B_A['total'] = df_B_A['sum_deducciones'] + df_B_A['sum_iva']
        df_B_A['descripcion'] = df_B_A['descripcion'].str.lower()
        df_B_A.loc[df_B_A['descripcion'].str.contains("comision"), 'total'] = df_B_A["total"] * -1
        df_B_A.loc[df_B_A['descripcion'].str.contains("anulacion de contracargo"), 'total'] = df_B_A["total"] * -1
        df_B_A.loc[df_B_A['descripcion'].str.contains("dev. ints. financiacion"), 'total'] = df_B_A["total"] * -1
        df_B_A.loc[df_B_A['descripcion'].str.contains("reliquid. de aranceles"), 'total'] = df_B_A["total"] * -1
        df_B_A['codigo_deducciones'] = df_B['codigo_op']
        df_B_A['comision_contracargo'] = df_B_A.apply( lambda row: row['sum_deducciones'] if 'comision contracargo' in row['descripcion'] else 0, axis=1 )
        # listas
        importe_value = df_F['importe_bruto_B'].to_list()
        importe_name = df_F['descripcion'].to_list()
        deducciones_value = df_B_A['total'].to_list()
        deducciones_name = df_B_A['descripcion'].to_list()
        bolsas = list(map(lambda n: dict( total = n , deducciones = [], balance = n) , importe_value))
        deducciones_o = []
        residuo = 0
        
        if (len(importe_value) == 1):
            var_1 = sum(deducciones_value)
            if var_1 == importe_value[0]:
                bolsas[0]['deducciones'].extend(deducciones_value)
                bolsas[0]['balance'] = 0
                deducciones_o.extend(deducciones_value)
            else:
                print("Error: total deducciones es diferente a total importes.")
        else:
            for bolsa in bolsas:
                if residuo != 0:
                    bolsa['balance'] -= residuo
                    bolsa['deducciones'].append(residuo)
                    deducciones_o.append(d)
                    residuo = 0
                while bolsa['balance'] != 0 and len(deducciones_value) > 0:
                    d = deducciones_value[0]
                    if bolsa['balance'] - d < 0:
                        bolsa['deducciones'].append(bolsa['balance'])
                        if bolsa['balance'] < 0:
                            residuo = d + bolsa['balance']
                        else:
                            residuo = d - bolsa['balance']
                        deducciones_value.pop(0)
                        deducciones_o.append(d)
                        bolsa['balance'] -= bolsa['balance']
                    else:
                        bolsa['balance'] -= d
                        bolsa['deducciones'].append(d)
                        deducciones_value.pop(0)
                        deducciones_o.append(d)
                        
        df_D = pd.DataFrame(list(bolsas))
        df_D = df_D.merge(df_F, left_on='total', right_on='importe_bruto_B')
        df_D = pd.DataFrame(list(bolsas))
        df_D = df_D.merge(df_F, left_on='total', right_on='importe_bruto_B')
        df_D = df_D.drop(['monto_a','monto_b','monto_c','monto_d','s_monto_a','s_monto_b','s_monto_c','s_monto_d','fecha_liquidacion','comercio','sucursal','moneda','fecha_pago','importe_bruto','s_importe_bruto','importe_comision','s_importe_comision','importe_comision_iva','s_importe_comision_iva','deducciones_y','s_deducciones','neto','s_neto','campo_a','campo_b','campo_c','campo_d','campo_e','indice','upload_date','report_date','skt_extraction_rn','total'], axis=1)
        df = df.drop(['monto_a_A','importe_bruto_A','importe_bruto_B'], axis=1)

        # ---------------------------------------------------------
        mask = df_D['deducciones_x'].apply(lambda x: len(x) == 0)
        # Eliminar las filas con la máscara
        df_D = df_D[~mask]
        # ---------------------------------------------------------

        df_D = explode(df_D,['deducciones_x'], fill_value="")
        df_D['deducciones_x'] = pd.to_numeric(df_D['deducciones_x'])
        df_D['importes'] = pd.Series(deducciones_o)
        print("348")
        if residuo > 0:
            # ---------------------------------------------------------
            df_l = df_D.iloc[-1,:].to_frame().T
            # ---------------------------------------------------------
            df_l["importes"] = residuo * (-1)
            df_l["deducciones_x"] = residuo * (-1)
            
            # ---------------------------------------------------------
            df_D = pd.concat([df_D, df_l], ignore_index=True)
            # ---------------------------------------------------------
        df_D = pd.merge(df_D, df_B_A, left_on="importes", right_on="total", how="left")
        df_D = df_D.drop(['total','indice_y','file_name_y','indice_x','importe_bruto_A','monto_a_A','importes','comision_contracargo'], axis=1)
        df_D = df_D.fillna(0)
        df_D.columns = ['balance','importe','file_name','importe_valor','deducciones_valor','deducciones','desglose_importe', 'desglose_iva','codigo_deducciones']
        df_D = df_D[['importe','importe_valor','deducciones','deducciones_valor','balance','desglose_importe','desglose_iva','codigo_deducciones','file_name']]
        df_D['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df_D['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        df_D.reset_index(drop=True)
        df_D['skt_extraction_rn'] = df_D.index.values
        df_D = df_D.applymap(str)
        if tipo_tabla == 'D':
            print("...")
            return df_D