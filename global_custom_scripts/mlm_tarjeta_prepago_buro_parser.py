import pytz
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime

class Extractor():
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
    
        cols = ['bc_clave_actual_otorgante', 'bc_nombre_otorgante', 'bc_cuenta_actual', 'bc_tipo_cuenta', 'bc_tipo_contrato', 'bc_clave_unidad_monetaria', 'bc_frecuencia_pagos',
                'bc_monto_pagar', 'bc_fecha_apertura_cuenta', 'bc_fecha_ultimo_pago', 'bc_fecha_ultima_compra', 'bc_fecha_cierre_cuenta', 'bc_fecha_corte', 'bc_credito_maximo',
                'bc_saldo_actual', 'bc_limite_credito', 'bc_saldo_vencido', 'bc_pago_actual', 'bc_historico_pagos', 'bc_total_pagos_reportados', 'bc_fecha_primer_incumplimiento',
                'bc_saldo_insoluto', 'bc_monto_ultimo_pago', 'bc_fecha_ingreso_cartera_vencida', 'bc_monto_correspondiente_intereses', 'bc_forma_pago_actual_intereses',
                'bc_dias_vencimiento', 'bc_plazo_meses', 'bc_monto_credito_originacion','bc_forma_pago_mop','bc_disponible_cuenta']
        
        try:
            h = pd.read_csv(BytesIO(file), nrows=1)
            df = pd.read_csv(BytesIO(file), dtype=str, header=None, skiprows=1)

            df.columns = cols
        except:
            h = pd.read_csv(BytesIO(file), nrows=1, dtype=str, encoding="utf-16")
            df = pd.read_csv(BytesIO(file), dtype=str, header=None, skiprows=1, encoding="utf-16")
        
            df.columns = cols
        # creando columnas adicionales a partir del header
        df.bc_cuenta_actual = df.bc_cuenta_actual.str.strip("0") 
        df["bc_clave_otorgante"] = h.columns[0]
        df["bc_nombre_otorgante_header"] = h.columns[1]
        df["bc_fecha_extraccion"] = h.columns[2]
        df["bc_version"] =h.columns[3]
        df["bc_domicilio_devolucion"] = df.iloc[-1][4]
        
        df = df.iloc[:-1,:]
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df