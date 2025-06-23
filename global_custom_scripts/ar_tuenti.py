import boto3
import numpy
import pandas as pd
import pytz
from io import StringIO, BytesIO
from datetime import date, timedelta, datetime
from urllib.parse import urlparse



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
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_excel(file, engine=None)
        columns = ["fecha_hora","num_abonado","cod_tipmov","monto","cod_tipactor","cod_actor","razon_social","plataforma","cod_transac_soap","datos_extra","error_code","error_desc","datos_extra2"]
        #,"id","tipo_de_negocio"
        try:
            df['COD_TRANSAC_SOAP']=df['COD_TRANSAC_SOAP'].apply(lambda x : x[3:] if x[:3]=='999' else x)
            df.columns = columns
            df["id"]=''
            df["tipo_de_negocio"]=''
            df = df.reset_index(drop=True)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df    
        except Exception as e:
            print("Error al subir la fuente: ",e)
