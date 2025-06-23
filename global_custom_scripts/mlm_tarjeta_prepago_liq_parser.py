import pytz
import boto3
import pandas as pd


from urllib.parse import urlparse
from io import BytesIO
from datetime import datetime

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
            binary_data = obj['Body'].read()
            #binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri, "rb") as f:
                return f.read(),datetime.now()

class Extractor():
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        #df = pd.read_excel(file,dtype=str)
        a=[]
        df = pd.read_csv(BytesIO(file),dtype=str, header=None,on_bad_lines=lambda li: a.append(li[:55]),engine='python')
        dfnew=pd.DataFrame(a)
        df = pd.concat([df, dfnew], ignore_index=True)
        if len(df.columns)>54:
            df=df.loc[:,:53]
        df.columns = [
        'numero_de_cuenta',
        'numero_de_tarjeta',
        'codigo_tipo_de_transaccion',
        'tipo_de_transaccion',
        'mti',
        'processing_code',
        'codigo_de_respuesta',
        'codigo_de_autorizacion',
        'codigo_del_modo_de_entrada',
        'modo_de_entrada',
        'plan_de_venta',
        'mcc',
        'marca',
        'codigo_del_producto',
        'fecha_de_la_transaccion',
        'moneda_original',
        'importe_total_original',
        'numero_de_cuota',
        'cantidad_de_cuotas',
        'amount_transaction_fee',
        'moneda_billng_amount',
        'importe_total_billing_amount',
        'importe_original_de_cashback',
        'importe_de_liquidacion_de_cashback',
        'importe_original_de_propina',
        'importe_de_liquidacion_de_propina',
        'codigo_del_pais',
        'codigo_de_la_institucion_adquirente',
        'terminal',
        'codigo_del_motivo_del_ajuste',
        'motivo_del_ajuste',
        'codigo_del_motivo_de_honra',
        'motivo_de_honra',
        'moneda_del_ajuste',
        'importe_del_ajuste',
        'transaction_identifier',
        'acquirer_reference_number_arn',
        'amount_settlement',
        'transmission_date_and_time_gmtautc_mmddhhmmss',
        'conversion_rate_cardholder_billing',
        'system_trace_audit_number_stan',
        'application_pan_sequence_number',
        'monto_de_cuota',
        'card_acceptor_identification_code',
        'card_acceptor_namealocation',
        'currency_code_settlement',
        'transactionstatus',
        'id_operacion',
        'disponible_de_cuenta',
        'disponible_de_linea_de_credito_1_compras',
        'disponible_de_linea_de_credito_1_avances',
        'periodo_de_gracia',
        'rrn',
        'oti'
    ]
                
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        upload_date = lm.astimezone(new_timezone)
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df