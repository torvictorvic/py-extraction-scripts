import boto3
#import numpy
import pandas as pd
#import io
from io import StringIO, BytesIO
from datetime import date, timedelta, datetime
#import zipfile
#import glob
#import os
import os.path
#import sys
import pytz
#import time
import patoolib
#from enum import Enum
#import math
#from rarfile import RarFile

class StringConverter(dict):
    def __contains__(self, item):
        return True
    def __getitem__(self, item):
        return str
    def get(self, default=None):
        return str
class Extractor:
    def run(self,filename, **kwargs):
        file,lm = self.file.body, self.file.last_modified
        filename= self.file.key
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        #rar_file = RarFile(file)
        with open('archivo.rar', 'wb') as f:
                f.write(file)
        rar_file=patoolib.extract_archive("archivo.rar")
        df =None
        with open(rar_file,encoding='latin1') as f_2:
            raw_data = f_2.read()
        os.remove(rar_file)
        os.remove('archivo.rar')
        widths = [1,10,4,16,20,24,3,3,18,3,10,1,1,1,15,40,9,2,2,80,11,4,1,10,50,2,16,143,500,500]
        widths_footer = [1,8,9,9,9,9]
        #raw_data = rar_file.open(text_file.filename).read()
        body_df = StringIO(raw_data)
        footer_df = StringIO(raw_data) 
        try:
            df = pd.read_fwf(body_df,dtype=object,widths=widths,skiprows=1,header=None)
            formato = ["tipo_de_registro","nro_de_cuenta","adicional_n","n__de_tarjeta","fecha___hora_min_seg","importe","tipo_de_moneda","automoneoriiso","plan","cuotas","codigo_de_autorizacion","autoforzada","autoreverflag","autoautodebi","n__de_comercio","nombre_del_comercio","estado","relacionada","origen","rechazo","ica","mcc","tcc","codigo_regla_de_fraude","descripcion_regla_de_fraude","modeo_de_entrada","terminal_pos","filler","cuenta_externa","request_id"]            
            df.columns = formato
            df = df.iloc[:-1,:]
            # Extract and append footer
            df_footer = pd.read_fwf(footer_df,dtype=object,widths=widths_footer,header=None)
            df_footer = df_footer.iloc[-1,:]
            df_footer = df_footer.to_frame()
            df_footer = df_footer.transpose()
            formato_footer = ["tipo_de_registro","fecha_de_generacion","cantde_solic_autorizaciones_anuladas","cant_de_solic_autorizaciones_aprobadas","cant_de_solic_autorizaciones_rechaz","cantidad_de_registros_de_detalle"]            
            df_footer.columns = formato_footer
            df_footer = df_footer.reset_index()
            for column in df_footer.columns:
                df[column] = df_footer.loc[0,column]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)

