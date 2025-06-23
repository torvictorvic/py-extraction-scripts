import pytz
import boto3
import pandas as pd
import logging
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse


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
        try:
            df=pd.read_excel(file,sheet_name='eg-meli')
            fecha=datetime.strptime(filename.split('.')[0].split('_')[-1], '%d%m%Y').date()
            list1=df.iloc[2:5,1:2].to_numpy().transpose().tolist()[0]
        except:
            fecha=False

        try:
            if sorted(['INTERCAMBIO NACIONAL', 'INTERCAMBIO DE TRANSACCIONES', 'EGLOBAL - MERCADO LIBRE'])==sorted(list1) and fecha:
                if 'FECHA DE PROCESO' in list(df.iloc[5:6,1]):
                    fecha_pro=list(df.iloc[6:7,1])[0]
                    #print(fecha_pro)
                if 'FECHA DE INTERCAMBIO' in list(df.iloc[5:6,7]):
                    fecha_int=list(df.iloc[6:7,7])[0]
                    #print(fecha_int)
                if 'MISCELANEOS:' in list(df.iloc[38:39,1]) and '0112 (20)' in list(df.iloc[63:64,1]):
                    charge=list(df.iloc[64:65,4])[0]
                    #print(charge)
            df_final = pd.DataFrame(
                                        {
                                            "fecha_archivo": [fecha],
                                            #"fecha_proceso": [fecha_pro],
                                            #"fecha_intercambio": [fecha_int],
                                            "charge":[charge]
                                        }
                                    )
            df_final['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df_final['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            df_final['file_name'] = filename.split('/')[-1]
            print("Parseo exitoso")
            return df_final
        except Exception as e:
            print("Error al subir la fuente: ",e)