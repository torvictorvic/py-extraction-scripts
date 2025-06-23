import pytz
import boto3
import pandas as pd
import logging
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse
import re
import json

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

        columns=['numero_socio', 'numero_credito', 'numero_tarjeta', 'numero_solicitud', 'id_sucursal', 'nombre_sucursal'
                , 'id_producto', 'descripcion_tipo_producto', 'tipo_producto', 'tipo_cartera', 'modalidad_cnbv'
                , 'situacion_contable', 'id_situacion_contable', 'id_finalidad', 'descripcion_finalidad', 'cuenta_lineas'
                , 'cuenta_pagos', 'cod_actividad', 'actividad', 'plazo', 'plazo_original_revolvente', 'dia_pago', 'numero_pagos'
                , 'numero_mensualidades_vencidas_pm', 'numero_mensualidades_vencidas_pt', 'dias_mora', 'tipo_credito_relacionado'
                , 'socio_cumplido', 'estatus_credito', 'dentro_listas', 'estado_cuenta', 'saldo_actual', 'saldo_al_cierre_de_mes'
                , 'saldo_vigente', 'saldo_vencido', 'saldo_exigible', 'tasa_interes1', 'tasa_interes2', 'tasa_interes3'
                , 'tasa_interes_moratoria1', 'cat', 'monto_original', 'monto_comision_no_uso', 'monto_desembolso_inicial'
                , 'monto_pago_realizado', 'monto_primer_pago', 'monto_pago_minimo', 'monto_pago_minimo_factura_vencida'
                , 'pago_no_generar_intereses', 'pago_minimo_mas_meses_sin_y_o_con_intereses', 'monto_pago_antes_pase_perdidas'
                , 'monto_ultima_disposicion', 'monto_ultimo_pago_capital', 'monto_ultimo_pago_interes', 'monto_castigado'
                , 'comision_pago_tardio', 'interes_primer_pago', 'interes_moratorio_acumulado', 'interes_moratorio_adeudado'
                , 'interes_moratorio_no_acumulado', 'interes_ordinario_adeudado', 'interes_ordinario_no_acumulado'
                , 'iva_interes_moratorio_adeudado_noacum', 'iva_interes_moratorio_adeudado_acumulado', 'iva_interes_moratorio'
                , 'iva_interes_ordinario_acumulado', 'iva_interes_ordinario_no_acumulado', 'fecha_contratacion'
                , 'fecha_proximo_pago', 'fecha_vencimiento', 'fecha_aprob_castigo', 'fecha_cancelacion', 'fecha_ampliacion'
                , 'fecha_desembolso_inicial', 'fecha_ultima_disposicion', 'fecha_ultimo_movimiento_pago'
                , 'fecha_ultimo_pago_captial', 'fecha_ultimo_pago_interes', 'fecha_primer_mora', 'fecha_ultima_mora'
                , 'fecha_primer_traspaso_vencida', 'fecha_primer_pago_realizado', 'fecha_solicitud', 'fecha_resolucion'
                , 'fecha_grado_morosidad_maximo', 'num_registro_federal_contribuyentes', 'correo_electronico'
                , 'numero_telefono_celular', 'numero_telefono', 'numero_sucursal_solicito_prestamo', 'no_veces_30_dias_mora'
                , 'no_veces_60_dias_mora', 'no_veces_90_dias_mora', 'no_120_dias_mora', 'no_150_dias_mora', 'no_180_dias_mora'
                , 'fecha_efectiva_pago_prestamo', 'descripcion_pago_prestamo', 'consecutivo_pago', 'saldo_mi', 'saldo_mci'
                , 'fecha_corte', 'saldo_exigible_vencido', 'saldo_exigible_castigado', 'monto_condonado', 'fecha_condonacion'
                , 'comisiones_acumuladas', 'comisiones_no_acumulacion', 'iva_comisiones_acumuladas', 'iva_comisiones_no_acumulacion'
                , 'bonificacion_descuento', 'autorizacion_automatica','saldo_cancelacion','saldo_cuenta']
        columns2=['numero_socio','numero_credito','saldo_actual','interes_moratorio_acumulado','interes_ordinario_adeudado'
                ,'iva_interes_moratorio_adeudado_acumulado','iva_interes_ordinario_acumulado','saldo_mi','saldo_mci'
                ,'saldo_exigible_vencido','saldo_exigible_castigado','iva_comisiones_acumuladas','iva_comisiones_no_acumulacion'
                ,'fecha_condonacion','saldo_cuenta','raw_data']
        columns3=['numero_socio','numero_credito','saldo_actual','interes_moratorio_acumulado','interes_ordinario_adeudado'
                ,'iva_interes_moratorio_adeudado_acumulado','iva_interes_ordinario_acumulado','saldo_mi','saldo_mci'
                ,'saldo_exigible_vencido','saldo_exigible_castigado','iva_comisiones_acumuladas','iva_comisiones_no_acumulacion'
                ,'fecha_condonacion','raw_data']
        
        try:
            
            df = pd.read_csv(BytesIO(file), sep = ";", encoding='cp1252', header=None)
            df.columns=columns[0:len(df.columns)]
            df['raw_data'] =json.loads(df.to_json(orient = 'records')) 
            if len(df.columns)==113:
                df=df.loc[:,columns3]
                df['saldo_cuenta']=""
            else:
                df=df.loc[:,columns2] 
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            df['file_name'] = filename.split('/')[-1]
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values 
            print("Parseo exitoso")
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)