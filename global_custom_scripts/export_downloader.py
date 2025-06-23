import boto3
import pandas as pd
import paramiko
from io import BytesIO
from sqlalchemy import create_engine
from datetime import datetime
import pytz
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

class Descargador:

    @staticmethod
    def run(filename, **kwargs ):
        print('Iniciando Export')
        bucket_pk = "global-cards-sources-test"
        key="pk_cards.pem"
        pk="/tmp/pk"
        bucket_dest = "global-in-sources-xxxxxxxxxx"
        s3 = boto3.resource('s3')
        s3_client= (boto3.client('s3'))
        user = [
        "global_prod_inbound",["mi-reconciliations"],"AAA_Archivos/EXPORTS_DOWNLOAD/","/ml-sftp/Global-Prod/reconciliations/global/inbound/","xxxxx.mercadolibre.xxx"
        ]

        # Lectura y limpieza del archivo con los files que hay que descargar
        file,lm = FileReader.read(filename)
        files=[]
        try:
            df = pd.read_csv(file,encoding='utf-8',dtype=str,sep=",")
            for label,aux in df.items():
                for i in range(len(aux)):
                    files.append(aux[i])
        except Exception as e:
            print("Error al leer los archivos a descargar: ",e)

        # Obtener la llave privada
        obj = s3.Object(bucket_pk, key)
        obj.download_file(pk)

        # Lógica conexión, descarga, limpieza y subida de archivos
        try:
            username = user[0]
            hostname = user[4]
            pref = user[2]
            print('Iniciando conexión')
            key=paramiko.RSAKey.from_private_key_file(pk)
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname,username=username,pkey=key)
            with ssh_client.open_sftp() as sftp:
                print('Conexión exitosa')
                for archive in sftp.listdir_attr(user[3]):
                    for prefix in files:
                        if prefix in archive.filename:
                            dt2_object = datetime.fromtimestamp(archive.st_mtime) 
                            if dt2_object:
                                ts = str(archive.st_mtime)
                                pag = s3_client.get_paginator('list_objects_v2')
                                pages = pag.paginate(Bucket=bucket_dest, Prefix=pref)
                                key_list = []
                                for page in pages:
                                    for obj in page['Contents']:
                                        key_list.append(obj['Key'])
                                try:
                                    if any( ts + ((archive.filename)).split('/')[-1] in s for s in key_list):
                                        print('El archivo ya está', archive.filename)
                                        esta = True
                                        continue
                                    else:
                                        esta=False
                                except:
                                    esta=False
                                    pass
                                if not esta:
                                    print('Se abre el sftp')
                                    with sftp.open(user[3] + (archive.filename), "rb") as fl:
                                        fl.prefetch()
                                        fl.set_pipelined(pipelined=True)
                                        print('Subiendo: ' + user[2] + ts + archive.filename)
                                        s3_client.upload_fileobj(fl,Bucket=bucket_dest,Key=user[2] + ts + archive.filename)
        except Exception as e:
            print("Error al conectar")
            print("Error: ",e)
        df = None
        return df

class Extractor_Exports:
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
        try:
            df = pd.read_csv(file,encoding='utf-8',dtype=str,sep=",")
            if df.shape[1]==1:
                df = pd.read_csv(file,encoding='latin1',dtype=str,sep=";")
            df.columns = ["skt_id_conci_1","skt_id_conci_2","report_name","nro_directivas","conciliation_datetime","tipo_conciliacion","fecha_novedad","liq_site","liq_acquirer","liq_payment_method","liq_merchant","liq_tipo_comercio","liq_nro_liq","liq_nro_lote","liq_trazability","liq_terminal","liq_fecha_liquidacion","liq_fecha_pago","liq_tipo_operacion","liq_nro_tarjeta","liq_importe","liq_cuotas","liq_nro_cuota","liq_ticket","liq_cip","liq_codigo_autorizacion","liq_fecha_compra","liq_fecha_presentacion","liq_tipo_tarjeta","liq_marca_td","liq_mot_cco","liq_desc_mot_cco","liq_transaction_id","liq_refund_id","liq_dat_univoco","liq_fecha_creado_skt","liq_fecha_actualizado_skt","liq_skt_id","liq_skt_id_uniq","liq_payer_id","liq_payment_id","liq_op_type","liq_tipo_terminal","liq_digito_verificador","liq_reference_id_off1","liq_reference_id_off2","liq_fecha_proceso","liq_fecha_header","liq_token","liq_codigo_operacion","liq_ente","liq_tipo_medio","liq_fecha_liquidacion_esperada_pago","liq_importe_total_cuotas","liq_moneda","liq_nsu","cal_dif_liq","cal_oportunidad_liq","cal_plazo_on","cal_plazo_off","cal_oportunidad_site","plazo_pago","plazo_conci","cal_dif_site_pres","cal_dif_acredit","gtwr_refund_id","pay_site","gtwt_acquirer","site_bandera","pay_payment_id","gtwt_transaction_id","gtwt_merchant_number","gtwt_merchant_type","mov_creation_date","mov_creation_date_teradata","mov_amount","mov_id","mov_type","operation","site_tipo_operacion","gtwo_status","gtwc_status","pay_status_id","pay_status_detail_code","gtwr_refund_type","pay_flag_carrito","gtwt_card_number","gtwt_installments","gtwa_authorization_code","gtwc_authorization_code","gtwo_authorization_code","gtwr_authorization_code","gtwa_new_trunc_card_number","gtwc_new_trunc_card_number","gtwo_new_trunc_card_number","gtwa_ticket_id","gtwc_ticket_id","gtwo_ticket_id","gtwc_batch_id","gtwo_batch_id","gtwr_batch_id","gtwr_ticket_id","gtwr_amount","pay_reference_id_off1","pay_reference_id_off2","pay_provider_reference","pay_provider_reference_refund","pay_method_metadata","pay_payer_id","gtwr_processing_mode","gtwr_creation_date","site_fecha_creado_skt","site_fecha_actualizado_skt","site_skt_id","fecha_esperada_pago","fecha_esperada_pago_2","fecha_esperada_conci","site_gtwt_transaction_id","campo_adicional_1","campo_adicional_2","campo_adicional_3","campo_adicional_4","campo_adicional_5","site_merchant","site_tipo_medio","site_procesadora","site_fecha_liquidacion_esperada_pago","vencido","index","site_skt_id_original","rn","size_db","num_installment","mov_amount_install","liq_importe_calc","site_importe_calc","site_skt_id_uniq","skt_id_report","skt_id_report_hash","pendiente","partidas","conciliado","desconciliado","conciliado_bolsa","skt_id_report_hash_full","estado","sub_estado_skt","liq_trazability_liq","sub_estado_liq","id_reclamo","payment_id_gestion","ultima_fecha_modificacion_liq","ultimo_usuario_liq","motivo_anterior_liq","array_sub_estados_liq","liq_trazability_site","sub_estado_site","ultima_fecha_modificacion_site","ultimo_usuario_site","motivo_anterior_site","array_sub_estados_site","novedad","fecha_generacion_archivo"]
            print('Columnas adicionales')
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)