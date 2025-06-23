import pytz
import boto3
import pandas as pd
import logging
from enum import Enum
from datetime import datetime
from io import StringIO, BytesIO
from urllib.parse import urlparse
import zipfile
from zipfile import ZipFile

class Extractor:

    def run(self, filename, **kwargs):
        
        file=self.file.body
        lm= self.file.last_modified
        filename=self.file.key
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')

        cols = ['banco', 'financial_entity', 'flujo', 'tipo_operacion_bancario',
       'tipo_operacion_conciliacion', 'fecha_operacion', 'fecha_pago_bancario','codigo_transaccional', 'descripcion_transaccional',
       'codigo_transaccional_conciliable','descripcion_transaccional_conciliable' ,'importe', 'moneda','id_conciliable_original', 'extra_info_conciliable', 'referencia_no',
       'bank_reference', 'swift_tran_cod', 'creat_methd_type', 'cr_ref_id',
       'fcube_tran_seq_no', 'id_conciliable_refund', 'payment_id','reference_id_1', 'reference_id_2', 'file_name_doc', 'row_number']
            
        try:    
            try: 
                df = pd.read_csv(BytesIO(file), sep=',',encoding='latin-1', dtype=str)
                out = (filename.split('/')[-1])
            except:
                zip_file = zipfile.ZipFile(BytesIO(file))
                out = zip_file.infolist()[0].filename
                df = pd.read_csv(BytesIO(zip_file.open(out).read()), sep=',',encoding='latin-1', dtype=str)
                zip_file.close() 

            df.columns = cols
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)