import pytz
import boto3
import pandas as pd
import re

from io import  BytesIO
from datetime import datetime
from urllib.parse import urlparse

def formatoFecha(fecha):
    fechNF = re.sub(r'(\d{4})-(\d{1,2})-(\d{1,2})', '\\3-\\2-\\1', fecha)
    fechNF = fechNF.replace('-', '/')
    return(fechNF)

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

            session = boto3.session.Session(region_name='us-east-1')
            s3 = session.client('s3')
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri) as f:
                return uri,datetime.today()

class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        if "pagos_payway" in filename.lower():
            file,lm = FileReader.read(filename)
        else:
            df = None
            return df

        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:

            df = pd.read_csv(file,encoding='latin1',dtype=str)
            originales = set(df.columns)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            df['original_filename'] = out
            return df
        except pd.io.common.EmptyDataError:
            if "pagos" in filename.lower():
                columns = ['id','archivo_id','empresa','fpres','tipo_reg','moneda','num_com','num_est','nroliq','fpag','tipoliq','impbruto','signo_1','impret','signo_2','impneto','signo_3','retesp','signo_4','retiva_esp','signo_5','percep_ba','signo_6','retiva_d1','signo_7','retiva_d2','signo_8','cargo_pex','signo_9','retiva_pex1','signo_10','retiva_pex2','signo_11','costo_cuoemi','signo_12','retiva_cuo1','signo_13','retiva_cuo2','signo_14','imp_serv','signo_15','iva1_xlj','signo_16','iva2_xlj','signo_17','cargo_edc_e','signo_18','iva1_edc_e','signo_19','iva2_edc_e','signo_20','cargo_edc_b','signo_21','iva1_edc_b','signo_22','iva2_edc_b','signo_23','cargo_cit_e','signo_24','iva1_cit_e','signo_25','iva2_cit_e','signo_26','cargo_cit_b','signo_27','iva1_cit_b','signo_28','iva2_cit_b','signo_29','ret_iva','signo_30','ret_gcias','signo_31','ret_ingbru','signo_32','aster','casacta','tipcta','ctabco','cf_exento_iva','signo_04_1','ley_25063','signo_04_2','ali_ingbru','dto_campania','signo_04_3','iva1_dto_campania','signo_04_4','ret_ingbru2','signo_04_5','ali_ingbru2','tasa_pex','cargo_x_liq','signo_04_8','iva1_cargo_x_liq','signo_04_9','dealer','imp_db_cr','signo_04_10','cf_no_reduce_iva','signo_04_11','percep_ib_agip','signo_04_12','alic_percep_ib_agip','reten_ib_agip','signo_04_13','alic_reten_ib_agip','subtot_retiva_rg3130','signo_04_14','prov_ingbru','adic_plancuo','signo_04_15','iva1_ad_plancuo','signo_04_16','adic_opinter','signo_04_17','iva1_ad_opinter','signo_04_18','adic_altacom','signo_04_19','iva1_ad_altacom','signo_04_20','adic_cupmanu','signo_04_21','iva1_ad_cupmanu','signo_04_22','adic_altacom_bco','signo_04_23','iva1_ad_altacom_bco','signo_04_24','revisado','prov_sellos','ret_sellos','signo_04_29','cargo_tran','id_cargoliq','iva1_cargo_tran','signo_04_6','signo_04_7','ali_ingbru3','ret_ingbru3','signo_04_30','adic_pricing_lap','ali_ingbru4','ali_ingbru5','ali_ingbru6','iva1_adic_pricing_lap','ret_ingbru4','ret_ingbru5','ret_ingbru6','signo_04_25','signo_04_26','signo_04_31','signo_04_32','signo_04_33']
            elif 'autorizaciones' in filename:
                columns = ["id","nro_tarjeta","fecha_compra","fecha_pres","fecha_pago","nro_comprobante","nro_autorizacion","cant_cuotas","importe","cod_banco","establecimiento","costo_financiero","cod_operacion","nro_lote","transaccion_hash","movimiento_id","archivo_id","estado","uid","refund_id"]
            elif 'movimientos' in filename:
                columns = ["id","archivo_id","empresa","fpres","tipo_reg","num_com","cod_op","tipo_aplic","lote","cod_bco","cod_casa","bco_est","casa_est","num_tar","forig_compra","fpag","num_comp","importe","signo","num_aut","num_cuot","plan_cuot","rec_acep","rech_princ","rech_secun","imp_plan","signo_plan","mca_pex","nro_liq","cco_origen","cco_motivo","moneda","aster","promo_bonif_usu","promo_bonif_est","id_promo","dto_promo","signo_dto_promo","id_iva_cf","dealer","cuit_est","fpago_aju_lqe","cod_motivo_aju_lqe","porcdto_arancel","arancel","signo_arancel","tna","costo_fin","signo_cf","libre","id_fin_registro","bines","num_est","costo_finan_direct","id_deb_aut","revisado","consumo_id","agencia_viaje","id_tx","tipo_plan","producto","nombre_banco","uid","refund_id"]
            df = pd.DataFrame(columns = columns)
            df = df.append(pd.Series(), ignore_index=True)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            df['original_filename'] = out
            return df

        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorCirculares:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_csv(file,encoding='latin1',dtype=str,sep="\t")
        if "Marca_Tarjeta" in df.columns:
            ls_columns = ["cuit","nro_establecimiento","nro_comprobante","fecha_movimiento","monto_movimiento","nro_tarjeta","cod_banco_pagador","nro_caja","nro_lote","fecha_presentacion","fecha_pago","nro_autorizacion","cantidad_cuotas_total","nro_cuota","cod_tipo_operacion","dev_cco","marca_tarjeta"]
            df.columns = ls_columns
        else:
            ls_columns = ["cuit","nro_establecimiento","nro_comprobante","fecha_movimiento","monto_movimiento","nro_tarjeta","cod_banco_pagador","nro_caja","nro_lote","fecha_presentacion","fecha_pago","nro_autorizacion","cantidad_cuotas_total","nro_cuota","marca_pex","marca_cuota_cuota","marca_cobro_anticipado"]
            df.columns = ls_columns
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        df['original_filename'] = out
        return df


class ExtractorLiq:
    @staticmethod
    def run(filename, **kwargs):
        if "payway" in filename.lower():
            file,lm = FileReader.read(filename)
        else:
            df = None
            return df
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str)
            originales = set(df.columns)            
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            df['original_filename'] = out
            if "liquidadas" in filename.lower():
              df = df[['id', 'archivo_id', 'empresa', 'fpres', 'tipo_reg', 'num_com', 'cod_op','tipo_aplic', 'lote', 'cod_bco', 'cod_casa', 'bco_est', 
                      'casa_est','num_tar', 'forig_compra', 'fpag', 'num_comp', 'importe', 'signo','num_aut', 'num_cuot', 'plan_cuot', 'rec_acep', 
                      'rech_princ','rech_secun', 'imp_plan', 'signo_plan', 'mca_pex', 'nro_liq','cco_origen', 'cco_motivo', 'moneda', 'aster', 'promo_bonif_usu',
                      'promo_bonif_est', 'id_promo', 'dto_promo', 'signo_dto_promo','id_iva_cf', 'dealer', 'cuit_est', 'fpago_aju_lqe',
                      'cod_motivo_aju_lqe', 'porcdto_arancel', 'arancel', 'signo_arancel','tna', 'costo_fin', 'signo_cf', 'libre', 'id_fin_registro', 'bines',
                      'num_est', 'costo_finan_direct', 'id_deb_aut', 'revisado', 'consumo_id','agencia_viaje', 'id_tx', 'tipo_plan', 'producto', 'nombre_banco',
                      'uid', 'refund_id', 'upload_date', 'report_date', 'file_name','skt_extraction_rn', 
                      'original_filename','nro_terminal', 'sub_codigo_operacion','term_captura', 
                       'cantidad_dias_pago','id_carga', 'cc_motiv_master','id_cargo_liq']]
              
              columFech = df["fpres"]
              fechNF = columFech.apply(formatoFecha)
              df["fpres"] = fechNF

              columFech = df["fpag"]
              fechNF = columFech.apply(formatoFecha)
              df["fpag"] = fechNF

              columFech = df["forig_compra"]
              fechNF = columFech.apply(formatoFecha)
              df["forig_compra"] = fechNF
              
              return df
            else:
              return df
        except pd.io.common.EmptyDataError:
            if "pagos" in filename:
                columns = ['id','archivo_id','empresa','fpres','tipo_reg','moneda','num_com','num_est','nroliq','fpag','tipoliq','impbruto','signo_1','impret','signo_2','impneto','signo_3','retesp','signo_4','retiva_esp','signo_5','percep_ba','signo_6','retiva_d1','signo_7','retiva_d2','signo_8','cargo_pex','signo_9','retiva_pex1','signo_10','retiva_pex2','signo_11','costo_cuoemi','signo_12','retiva_cuo1','signo_13','retiva_cuo2','signo_14','imp_serv','signo_15','iva1_xlj','signo_16','iva2_xlj','signo_17','cargo_edc_e','signo_18','iva1_edc_e','signo_19','iva2_edc_e','signo_20','cargo_edc_b','signo_21','iva1_edc_b','signo_22','iva2_edc_b','signo_23','cargo_cit_e','signo_24','iva1_cit_e','signo_25','iva2_cit_e','signo_26','cargo_cit_b','signo_27','iva1_cit_b','signo_28','iva2_cit_b','signo_29','ret_iva','signo_30','ret_gcias','signo_31','ret_ingbru','signo_32','aster','casacta','tipcta','ctabco','cf_exento_iva','signo_04_1','ley_25063','signo_04_2','ali_ingbru','dto_campania','signo_04_3','iva1_dto_campania','signo_04_4','ret_ingbru2','signo_04_5','ali_ingbru2','tasa_pex','cargo_x_liq','signo_04_8','iva1_cargo_x_liq','signo_04_9','dealer','imp_db_cr','signo_04_10','cf_no_reduce_iva','signo_04_11','percep_ib_agip','signo_04_12','alic_percep_ib_agip','reten_ib_agip','signo_04_13','alic_reten_ib_agip','subtot_retiva_rg3130','signo_04_14','prov_ingbru','adic_plancuo','signo_04_15','iva1_ad_plancuo','signo_04_16','adic_opinter','signo_04_17','iva1_ad_opinter','signo_04_18','adic_altacom','signo_04_19','iva1_ad_altacom','signo_04_20','adic_cupmanu','signo_04_21','iva1_ad_cupmanu','signo_04_22','adic_altacom_bco','signo_04_23','iva1_ad_altacom_bco','signo_04_24','revisado','prov_sellos','ret_sellos','signo_04_29','cargo_tran','id_cargoliq','iva1_cargo_tran','signo_04_6','signo_04_7','ali_ingbru3','ret_ingbru3','signo_04_30','adic_pricing_lap','ali_ingbru4','ali_ingbru5','ali_ingbru6','iva1_adic_pricing_lap','ret_ingbru4','ret_ingbru5','ret_ingbru6','signo_04_25','signo_04_26','signo_04_31','signo_04_32','signo_04_33']
            elif 'autorizaciones' in filename:
                columns = ["id","nro_tarjeta","fecha_compra","fecha_pres","fecha_pago","nro_comprobante","nro_autorizacion","cant_cuotas","importe","cod_banco","establecimiento","costo_financiero","cod_operacion","nro_lote","transaccion_hash","movimiento_id","archivo_id","estado","uid","refund_id"]
            elif 'liquidadas' in filename.lower():
                columns = ["id","archivo_id","empresa","fpres","tipo_reg","num_com","cod_op","tipo_aplic","lote","cod_bco","cod_casa","bco_est","casa_est","num_tar","forig_compra","fpag","num_comp","importe","signo","num_aut","num_cuot","plan_cuot","rec_acep","rech_princ","rech_secun","imp_plan","signo_plan","mca_pex","nro_liq","cco_origen","cco_motivo","moneda","aster","promo_bonif_usu","promo_bonif_est","id_promo","dto_promo","signo_dto_promo","id_iva_cf","dealer","cuit_est","fpago_aju_lqe","cod_motivo_aju_lqe","porcdto_arancel","arancel","signo_arancel","tna","costo_fin","signo_cf","libre","id_fin_registro","bines","num_est","costo_finan_direct","id_deb_aut","revisado","consumo_id","agencia_viaje","id_tx","tipo_plan","producto","nombre_banco","uid","refund_id","id_consumo"]
            df = pd.DataFrame(columns = columns)
            df = df.append(pd.Series(), ignore_index=True)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            df['original_filename'] = out
            return df

        except Exception as e:
            print("Error al subir la fuente: ",e)


class ExtractorCircularesV:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_csv(file,encoding='latin1',dtype=str,sep="\t")
        if "DEVCCO" in filename:
            ls_columns = ["cuit","nro_establecimiento","nro_comprobante","fecha_movimiento","monto_movimiento","nro_tarjeta","cod_banco_pagador","nro_caja","nro_lote","fecha_presentacion","fecha_pago","nro_autorizacion","cantidad_cuotas_total","nro_cuota","cod_tipo_operacion","dev_cco"]
        elif "CaC" in filename:
            ls_columns = ["cuit","nro_establecimiento","nro_comprobante","fecha_movimiento","monto_movimiento","nro_tarjeta","nro_lote","fecha_presentacion","fecha_pago","nro_autorizacion","cantidad_cuotas_total","nro_cuota_siguiente","marca_pex","marca_cuota_cuota"]
        else:
            ls_columns = ["cuit","nro_establecimiento","nro_comprobante","fecha_movimiento","monto_movimiento","nro_tarjeta","cod_banco_pagador","nro_caja","nro_lote","fecha_presentacion","fecha_pago","nro_autorizacion","cantidad_cuotas_total","nro_cuota","marca_pex","marca_cuota_cuota","marca_cobro_anticipado"]
        df.columns = ls_columns
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df