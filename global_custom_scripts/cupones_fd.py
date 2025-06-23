import pytz
import boto3
import pandas as pd
import logging
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse
import re

def Normal(df,lista,tipo):
    if tipo==1:
        normal = lambda x: x.replace('.','-')
        for col in lista:
            df[col]=pd.to_datetime(df[col].apply(normal), format='%Y-%m-%d').astype(str)
    elif tipo==2:
        normal = lambda x: re.sub('[^0-9,]','',str(x)).replace(',','.')
        for col in lista:
            df[col]=df[col].fillna('0')
            df[col]=df[col].apply(normal)

class Extractor:

    def run(self, filename, **kwargs):
       
        file= self.file.body
        lm= self.file.last_modified
        filename=self.file.key
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        columns=['fec_oper','fec_pres','fec_pago','comercio','cupon','mov','importe_con_dto','cp','cu','nro_liq','desc_tipo_financiacion','tarjeta','producto']
        try:
            df = pd.read_excel(BytesIO(file))
            df.columns=columns
            Normal(df,['fec_oper','fec_pres','fec_pago'],1)

            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            df['file_name'] = filename.split('/')[-1]
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values 
            print("Parseo exitoso")
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)