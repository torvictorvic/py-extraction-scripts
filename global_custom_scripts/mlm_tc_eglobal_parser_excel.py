import pytz
import boto3
import pandas as pd
import logging
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse
import re

def fromato_fecha(Fecha,tipo):
    li_mes = [('ene','01'),('feb','02'),('mar','03'),('abr','04'),('may','05'),('jun','06'),('jul','07'),('ago','08'),('sep','09')
           ,('oct','10'),('nov','11'),('dic','12')]
    if tipo==1:
        dia,mes,anio=Fecha.split(',')[-1].split('de')
    elif tipo==2:
        anio,mes,dia=Fecha.split()
    else:
        return 0
        
    mes=[y for x,y in li_mes if mes.strip().lower()[:3]==x][0]
    return anio.strip()+"-"+mes+"-"+dia.strip()

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
            df = pd.read_excel(BytesIO(file), dtype=str)
            Fecha=df.iloc[0,3]
            #Fecha_nomal=fromato_fecha(Fecha,1)
            colum=['concepto','mercado_libre','eglobal','favor_egloval']
            df_final=pd.DataFrame()
            nor_str=lambda col: str(col).lower().strip() if col is not None else ""
            for x in range(1,5):
                df_final[colum[x-1]]=df.iloc[7:,x].dropna().apply(nor_str)
            df_final['fecha']=Fecha
            #df_final['fecha_normal']=Fecha_nomal
            df_final['fecha_file']=fromato_fecha(filename.split('/')[-1].split('Libre')[-1].split('.')[0].lower().strip(),2)
            df_final['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df_final['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            df_final['file_name'] = filename.split('/')[-1]
            df_final=df_final.reset_index(drop=True)
            df_final['skt_extraction_rn'] = df_final.index.values
            print("Parseo exitoso")
            return df_final
        except Exception as e:
            print("Error al subir la fuente: ",e)