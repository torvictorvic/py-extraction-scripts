import pytz
import boto3
import pandas as pd
from enum import Enum
from datetime import datetime
from io import StringIO, BytesIO
from urllib.parse import urlparse


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
            obj = obj['Body'].read()
            #binary_data = BytesIO(obj)
            return obj,lm
        else:
            with open(uri, "rb") as f:
                return f.read(),datetime.today()


class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            cols = ['id', 'tipo', 'usuario', 'so_movil', 'version_app', 'instalacion', 'instalacion_abrv', 'fecha', 'fecha_inicio', 'fecha_final', 'valor', 'valor_sin_bonificar', 'bonificacion', 'tipo_de_vehiculo', 'duracion', 'saldo_previo', 'cambio', 'matricula', 'numero_denuncia', 'fecha_descuento', 'valor_descuento', 'referencia_operacion', 'codigo_transaccion', 'codigo_autorizacion', 'hash_tarjeta', 'referencia_tarjeta', 'sistema_tarjeta', 'numero_tarjeta_enmascarado', 'fecha_expiracion_tarjeta', 'id_externo_1', 'id_externo_2', 'id_externo_3', 'latitud', 'longitud', 'iva_1', 'iva_2', 'valor_iva', 'comision', 'comision_max', 'valor_comision_porc', 'comision_fija', 'valor_comision_fija', 'valor_total', 'nuevo_saldo_pagatelia', 'operacion_comerciante', 'total_comerciante', 'beneficio_comerciante', 'matricula_2', 'matricula_3', 'matricula_4', 'matricula_5', 'matricula_6', 'matricula_7', 'matricula_8', 'matricula_9', 'matricula_10', 'renovacion_auto_abono', 'expiracion_abono', 'valor_final_descuento', 'moneda_saldo_previo_descuento', 'saldo_previo_descuento', 'cambio_descuento', 'fecha_insercion_descuento_utc', 'valor_devuelto', 'sector', 'tarifa', 'moneda_valor_descuento', 'tipo_servicio_carga', 'tipo_de_recarga', 'estado_transaccion', 'campana', 'diferencia_importe_con_sin_bonificacion', 'id_de_tramo', 'tramo_de_calle']
            df = pd.read_excel(file,dtype='string',sheet_name=0) 
            df.columns = cols

            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            
            return df
            
        except pd.io.common.EmptyDataError:
            cols = ['id', 'tipo', 'usuario', 'so_movil', 'version_app', 'instalacion', 'instalacion_abrv', 'fecha', 'fecha_inicio', 'fecha_final', 'valor', 'valor_sin_bonificar', 'bonificacion', 'tipo_de_vehiculo', 'duracion', 'saldo_previo', 'cambio', 'matricula', 'numero_denuncia', 'fecha_descuento', 'valor_descuento', 'referencia_operacion', 'codigo_transaccion', 'codigo_autorizacion', 'hash_tarjeta', 'referencia_tarjeta', 'sistema_tarjeta', 'numero_tarjeta_enmascarado', 'fecha_expiracion_tarjeta', 'id_externo_1', 'id_externo_2', 'id_externo_3', 'latitud', 'longitud', 'iva_1', 'iva_2', 'valor_iva', 'comision', 'comision_max', 'valor_comision_porc', 'comision_fija', 'valor_comision_fija', 'valor_total', 'nuevo_saldo_pagatelia', 'operacion_comerciante', 'total_comerciante', 'beneficio_comerciante', 'matricula_2', 'matricula_3', 'matricula_4', 'matricula_5', 'matricula_6', 'matricula_7', 'matricula_8', 'matricula_9', 'matricula_10', 'renovacion_auto_abono', 'expiracion_abono', 'valor_final_descuento', 'moneda_saldo_previo_descuento', 'saldo_previo_descuento', 'cambio_descuento', 'fecha_insercion_descuento_utc', 'valor_devuelto', 'sector', 'tarifa', 'moneda_valor_descuento', 'tipo_servicio_carga', 'tipo_de_recarga', 'estado_transaccion', 'campana', 'diferencia_importe_con_sin_bonificacion', 'id_de_tramo', 'tramo_de_calle']
            df = pd.DataFrame(columns = cols)
            df = df.append(pd.Series(), ignore_index=True)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            
            return df

        except Exception as e:
            print("Error al subir la fuente: ",e)