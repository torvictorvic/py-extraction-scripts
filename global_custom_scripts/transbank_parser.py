import pytz
import boto3
import pandas as pd

from io import StringIO
from datetime import datetime
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
            # session = boto3.Session(profile_name="sts")
            # s3 = session.client('s3')
            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read().decode()
            return obj,lm
        else:
            with open(uri) as f:
                return uri,datetime.today()

class ExtractorCCN:
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
            header_positions = [0,2,8,14,38,320]
            footer_positions =[0,2,8,14,21,34,40,46,52,58,321]
            positions = [0,2,6,8,20,26,32,36,44,64,83,96,105,106,108,111,119,135,151,161,162,168,174,180,183,203,212,216,242,243,251,271,275,276,278,321]
            col_specification =[]
            header_specification =[]
            footer_specification =[]
            for i in range(0,len(header_positions) -1):
                cordenate = (header_positions[i],header_positions[i+1] )
                header_specification.append(cordenate)
            for i in range(0,len(footer_positions) -1):
                cordenate = (footer_positions[i],footer_positions[i+1] )
                footer_specification.append(cordenate)
            for i in range(0,len(positions) -1):
                cordenate = (positions[i],positions[i+1] )
                col_specification.append(cordenate)
            header_df = StringIO(file)
            binary_df = StringIO(file)
            footer_df = StringIO(file)
            cols_header= ["header_type","fecha_proceso_header","hora_proceso_header","nombre_comercio_header","filler_header"]
            cols_footer=["footer_type","fecha_proceso_footer","hora_proceso_footer","total_registros_footer","monto_total_footer","fecha_menor_tx_footer","hora_menort_tx_footer","fecha_mayor_tx_footer","hora_mayor_tx_footer","filler_footer"]
            cols = ["detalle_type","tipo_tx","codigo_tx","valor_secuencia","fecha_tx","hora_tx","codigo_institucion","codigo_comercio","nombre_comercio","numero_tarjeta","monto_tx","monto_propina","tipo_cuota","cantidad_cuotas","codigo_respuesta","codigo_aprobacion","id_terminal","id_caja","numero_boleta","id_track_auth","fecha_venta","hora_venta","fecha_abono","codigo_rechazo","glosa_rechazo","valor_cuota","valor_tasa_interes","numero_unico","indicador_moneda","codigo_comercio_responsable","codigo_servicio","marca_webpay","mes_gracia","periodos_gracia","filler"]
            df_header = pd.read_fwf(header_df, colspecs=header_specification, nrows=1, header=None, dtype=object)
            df_header.columns = cols_header
            df = pd.read_fwf(binary_df, colspecs=col_specification, skiprows=1, header=None, dtype=object)
            df.columns = cols
            df = df[:-1]
            df_footer = pd.read_fwf(footer_df, colspecs=footer_specification, skiprows=len(df)+1, header=None, dtype=object)
            df_footer.columns = cols_footer
            for col in df_header:
                df[col] =df_header.loc[0,col] 
            for col in df_footer:
                df[col] =df_footer.loc[0,col] 

            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except pd.io.common.EmptyDataError:
            columns= ['detalle_type', 'tipo_tx', 'codigo_tx', 'valor_secuencia', 'fecha_tx', 'hora_tx', 'codigo_institucion', 'codigo_comercio', 'nombre_comercio', 'numero_tarjeta', 'monto_tx', 'monto_propina', 'tipo_cuota', 'cantidad_cuotas', 'codigo_respuesta', 'codigo_aprobacion', 'id_terminal', 'id_caja', 'numero_boleta', 'id_track_auth', 'fecha_venta', 'hora_venta', 'fecha_abono', 'codigo_rechazo', 'glosa_rechazo', 'valor_cuota', 'valor_tasa_interes', 'numero_unico', 'indicador_moneda', 'codigo_comercio_responsable', 'codigo_servicio', 'marca_webpay', 'mes_gracia', 'periodos_gracia', 'filler', 'header_type', 'fecha_proceso_header', 'hora_proceso_header', 'nombre_comercio_header', 'filler_header', 'footer_type', 'fecha_proceso_footer', 'hora_proceso_footer', 'total_registros_footer', 'monto_total_footer', 'fecha_menor_tx_footer', 'hora_menort_tx_footer', 'fecha_mayor_tx_footer', 'hora_mayor_tx_footer', 'filler_footer']
            df = pd.DataFrame(columns = columns)
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

class ExtractorCDN:
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
            header_positions = [0,2,8,14,39,240]
            footer_positions =[0,2,8,14,21,34,47,240]
            positions = [0,2,6,8,14,20,32,52,71,84,97,106,109,117,133,149,159,165,167,175,195,221,222,240]
            col_specification =[]
            header_specification =[]
            footer_specification =[]
            for i in range(0,len(header_positions) -1):
                cordenate = (header_positions[i],header_positions[i+1] )
                header_specification.append(cordenate)
            for i in range(0,len(footer_positions) -1):
                cordenate = (footer_positions[i],footer_positions[i+1] )
                footer_specification.append(cordenate)
            for i in range(0,len(positions) -1):
                cordenate = (positions[i],positions[i+1] )
                col_specification.append(cordenate)
            header_df = StringIO(file)
            binary_df = StringIO(file)
            footer_df = StringIO(file)
            cols_header= ["header_type","fecha_proceso_header","hora_proceso_header","nombre_comercio_header","filler_header"]
            cols_footer=["footer_type","fecha_proceso_footer","hora_proceso_footer","total_registros_footer","monto_total_footer","monto_total_comisiones_footer","filler_footer"]
            cols = ["detalle_type","tipo_tx","codigo_tx","fecha_tx","hora_tx","codigo_comercio","nombre_comercio","numero_tarjeta","monto_tx","monto_devuelto","monto_propina","codigo_respuesta","codigo_aprobacion","id_terminal","id_caja","numero_boleta","fecha_pago","id_host","codigo_comercio_responsable","codigo_servicio","numero_unico","prepago","filler"]
            df_header = pd.read_fwf(header_df, colspecs=header_specification, nrows=1, header=None, dtype=object)
            df_header.columns = cols_header
            df = pd.read_fwf(binary_df, colspecs=col_specification, skiprows=1, header=None, dtype=object)
            df.columns = cols
            df = df[:-1]
            df_footer = pd.read_fwf(footer_df, colspecs=footer_specification, skiprows=len(df)+1, header=None, dtype=object)
            df_footer.columns = cols_footer
            for col in df_header:
                df[col] =df_header.loc[0,col] 
            for col in df_footer:
                df[col] =df_footer.loc[0,col] 

            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except pd.io.common.EmptyDataError:
            columns= ['detalle_type', 'tipo_tx', 'codigo_tx', 'valor_secuencia', 'fecha_tx', 'hora_tx', 'codigo_institucion', 'codigo_comercio', 'nombre_comercio', 'numero_tarjeta', 'monto_tx', 'monto_propina', 'tipo_cuota', 'cantidad_cuotas', 'codigo_respuesta', 'codigo_aprobacion', 'id_terminal', 'id_caja', 'numero_boleta', 'id_track_auth', 'fecha_venta', 'hora_venta', 'fecha_abono', 'codigo_rechazo', 'glosa_rechazo', 'valor_cuota', 'valor_tasa_interes', 'numero_unico', 'indicador_moneda', 'codigo_comercio_responsable', 'codigo_servicio', 'marca_webpay', 'mes_gracia', 'periodos_gracia', 'filler', 'header_type', 'fecha_proceso_header', 'hora_proceso_header', 'nombre_comercio_header', 'filler_header', 'footer_type', 'fecha_proceso_footer', 'hora_proceso_footer', 'total_registros_footer', 'monto_total_footer', 'fecha_menor_tx_footer', 'hora_menort_tx_footer', 'fecha_mayor_tx_footer', 'hora_mayor_tx_footer', 'filler_footer']
            df = pd.DataFrame(columns = columns)
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

class ExtractorLCN:
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
            header_positions = [0,8,9,17,18,26,27,35]
            footer_positions =[0,10,11,24,25,35,36,49,50,63,76,89,102,134,140,229]
            positions = [0,8,16,24,32,51,53,64,65,67,71,79,87,113,119,121,125,136,147,158,169,171,206,208,226,229]
            col_specification =[]
            header_specification =[]
            footer_specification =[]
            for i in range(0,len(header_positions) -1):
                cordenate = (header_positions[i],header_positions[i+1] )
                header_specification.append(cordenate)
            for i in range(0,len(footer_positions) -1):
                cordenate = (footer_positions[i],footer_positions[i+1] )
                footer_specification.append(cordenate)
            for i in range(0,len(positions) -1):
                cordenate = (positions[i],positions[i+1] )
                col_specification.append(cordenate)
            header_df = StringIO(file)
            binary_df = StringIO(file)
            footer_df = StringIO(file)
            cols_header= ["abono_desde_header","filler","abono_hasta_header","filler_2","proceso_fecha_header","filler_3","abono_fecha_header"]
            cols_footer=["num_tx_abono_footer","vacio","monto_total_ventas_footer","vacio_2","numero_retenciones_footer","vacio_3","monto_total_retenciones_footer","vacio_4","monto_total_comision_footer","monto_total_comision_2_footer","monto_total_comision_3_footer","monto_total_comision_4_footer","vacio_5","vacio_6","vacio_7"]
            cols = ["codigo_comercio","fecha_proceso","fecha_venta","numero_microfilm","numero_tarjeta","tipo_tarjeta","monto_venta","tipo_moneda","tipo_tx","atributo_tx","codigo_casa_comercio","fecha_pago","numero_orden_pedido","codigo_auth","valor_auth_tx","valor_comision","valor_comision_adicional","valor_comision_adicional_iva","valor_retenciones","valor_retenciones_2","numero_cuotas","nombre_banco","tipo_cuenta","numero_cuenta_banco","moneda_cuenta_abono"]
            df_header = pd.read_fwf(header_df, colspecs=header_specification, nrows=1, header=None, dtype=object)
            df_header.columns = cols_header
            df = pd.read_fwf(binary_df, colspecs=col_specification, skiprows=1, header=None, dtype=object)
            df.columns = cols
            df = df[:-1]
            df_footer = pd.read_fwf(footer_df, colspecs=footer_specification, skiprows=len(df)+1, header=None, dtype=object)
            df_footer.columns = cols_footer
            for col in df_header:
                df[col] =df_header.loc[0,col] 
            for col in df_footer:
                df[col] =df_footer.loc[0,col] 
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except pd.io.common.EmptyDataError:
            columns= ['detalle_type', 'tipo_tx', 'codigo_tx', 'valor_secuencia', 'fecha_tx', 'hora_tx', 'codigo_institucion', 'codigo_comercio', 'nombre_comercio', 'numero_tarjeta', 'monto_tx', 'monto_propina', 'tipo_cuota', 'cantidad_cuotas', 'codigo_respuesta', 'codigo_aprobacion', 'id_terminal', 'id_caja', 'numero_boleta', 'id_track_auth', 'fecha_venta', 'hora_venta', 'fecha_abono', 'codigo_rechazo', 'glosa_rechazo', 'valor_cuota', 'valor_tasa_interes', 'numero_unico', 'indicador_moneda', 'codigo_comercio_responsable', 'codigo_servicio', 'marca_webpay', 'mes_gracia', 'periodos_gracia', 'filler', 'header_type', 'fecha_proceso_header', 'hora_proceso_header', 'nombre_comercio_header', 'filler_header', 'footer_type', 'fecha_proceso_footer', 'hora_proceso_footer', 'total_registros_footer', 'monto_total_footer', 'fecha_menor_tx_footer', 'hora_menort_tx_footer', 'fecha_mayor_tx_footer', 'hora_mayor_tx_footer', 'filler_footer']
            df = pd.DataFrame(columns = columns)
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

class ExtractorLDN:
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
            header_positions = [0,6,7,13,17,23,24,49,55,247]
            footer_positions =[0,10,11,24,25,35,36,49,58,68,74,87,88,101,247]
            positions = [0,8,14,20,26,45,58,60,68,70,78,104,117,130,143,156,157,192,194,212,215,225,245,247]
            col_specification =[]
            header_specification =[]
            footer_specification =[]
            for i in range(0,len(header_positions) -1):
                cordenate = (header_positions[i],header_positions[i+1] )
                header_specification.append(cordenate)
            for i in range(0,len(footer_positions) -1):
                cordenate = (footer_positions[i],footer_positions[i+1] )
                footer_specification.append(cordenate)
            for i in range(0,len(positions) -1):
                cordenate = (positions[i],positions[i+1] )
                col_specification.append(cordenate)
            header_df = StringIO(file)
            binary_df = StringIO(file)
            footer_df = StringIO(file)
            cols_header= ["fecha_inicio_periodo_header","vacio","fecha_final_periodo_header","vacio_2","fecha_liquidacion_header","vacio_3","nombre_comercio_header","indicador_registro_header","vacio_4"]
            cols_footer=["total_registros_footer","vacio","total_liquidaciones_footer","vacio_2","total_registros_retenciones_footer","vacio_3","total_registros_detalle_liq","fijos","fijos_2","indicador_registro_footer","total_comision_footer","vacio_4","total_reteciones_footer","vacio_5"]
            cols = ["codigo_comercio_interno","fecha_proceso_archivo","fecha_compra","codigo_auth","numero_tarjeta","monto_tx","tipo_tx","valor_fijo","retenciones","fecha_liq","numero_unico","valor_comision","relleno","valor_comision_mas_iva","relleno_2","prepago","nombre_banco","tipo_cuenta_banco","numero_cuenta_banco","moneda_cuenta_abono", "liq_rut_secundario", "liq_nombre_secundario", "liq_marca_card"]
            df_header = pd.read_fwf(header_df, colspecs=header_specification, nrows=1, header=None, dtype=object)
            df_header.columns = cols_header
            df = pd.read_fwf(binary_df, colspecs=col_specification, skiprows=1, header=None, dtype=object)
            df.columns = cols
            df = df[:-1]
            if df.empty:
                df.loc[len(df)] = 0
                df_footer = pd.read_fwf(footer_df, colspecs=footer_specification, skiprows=len(df), header=None, dtype=object)
            else:
                df_footer = pd.read_fwf(footer_df, colspecs=footer_specification, skiprows=len(df)+1, header=None, dtype=object)
            df_footer.columns = cols_footer
            for col in df_header:
                df[col] =df_header.loc[0,col] 
            for col in df_footer:
                df[col] =df_footer.loc[0,col] 

            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except pd.io.common.EmptyDataError:
            columns= ['detalle_type', 'tipo_tx', 'codigo_tx', 'valor_secuencia', 'fecha_tx', 'hora_tx', 'codigo_institucion', 'codigo_comercio', 'nombre_comercio', 'numero_tarjeta', 'monto_tx', 'monto_propina', 'tipo_cuota', 'cantidad_cuotas', 'codigo_respuesta', 'codigo_aprobacion', 'id_terminal', 'id_caja', 'numero_boleta', 'id_track_auth', 'fecha_venta', 'hora_venta', 'fecha_abono', 'codigo_rechazo', 'glosa_rechazo', 'valor_cuota', 'valor_tasa_interes', 'numero_unico', 'indicador_moneda', 'codigo_comercio_responsable', 'codigo_servicio', 'marca_webpay', 'mes_gracia', 'periodos_gracia', 'filler', 'header_type', 'fecha_proceso_header', 'hora_proceso_header', 'nombre_comercio_header', 'filler_header', 'footer_type', 'fecha_proceso_footer', 'hora_proceso_footer', 'total_registros_footer', 'monto_total_footer', 'fecha_menor_tx_footer', 'hora_menort_tx_footer', 'fecha_mayor_tx_footer', 'hora_mayor_tx_footer', 'filler_footer']
            df = pd.DataFrame(columns = columns)
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





