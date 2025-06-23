import boto3
import numpy
import pandas as pd
import io
from io import StringIO, BytesIO
from datetime import date, timedelta, datetime
import zipfile
import glob
import os
import os.path
import sys
import pytz
import time
import pandas as pd
from pandas import DataFrame
from enum import Enum
import math
from urllib.parse import urlparse


class Extractor:
    
    def run(self,filename, **kwargs):
        print("Iniciando AFIP")
        file,lm = self.file.body, self.file.last_modified
        filename= self.file.key 
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            positions = [0,6,12,22,27,33,45,54,61,71,85]
            col_specification =[]
            for i in range(0,len(positions) -1):
                cordenate = (positions[i],positions[i+1] )
                col_specification.append(cordenate)
            binary_df = StringIO(BytesIO(file).read().decode().rstrip().strip('\x00'))
            cols = ["banco_pagador","nro_rendicion","fecha_posting","entidad_pago","sucursal","nro_transaccion","fecha_pago","no_se_usa","nro_rend","monto"]
            df = pd.read_fwf(binary_df, colspecs=col_specification, header=None, dtype=object)
            df.columns = cols
            columnas = filter(lambda x: "no_se_usa" not in str(x) ,df.columns)
            df = df[columnas]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            print("Terminando AFIP")
            return df
        
        except Exception as e:
            print("Error al subir la fuente: ",e)