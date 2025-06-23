import boto3
import numpy
import pandas as pd
import io
from io import StringIO, BytesIO
from datetime import date, timedelta, datetime
import zipfile
import glob
import os
import os.path
import sys
import pytz
import time
import pandas as pd
from pandas import DataFrame
from enum import Enum
import math
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
            session = boto3.Session()
            s3 = session.client('s3')
            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            data = obj['Body'].read().decode()
            data = data.split('\n')
            return data,lm
        else:
            f = open(uri, 'r')
            data = f.readlines()
            return data, datetime.today()

class StringConverter(dict):
    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return str

    def get(self, default=None):
        return str

AmexRecordType = {
    'HEADER' :'0100',
    'CARGOYABONO' :'0210',
    'COMISIONES' :'0310',
    'SOBRETASA' :'0510',
    'VENTAS' :'0610',
    'CONTRACARGOS' :'0710',
    'TRAILER' :'0900'
}

class AMEXManifest:

    rules = [
        {
            "record_type": 'HEADER',
            "layout": [
                {
                    "len": 4,
                    "column_name": "TIPO_REGISTRO"
                },
                {
                    "len": 30,
                    "column_name": "NOMBRE_ADQUIRENTE"
                },
                {
                    "len": 8,
                    "column_name": "AFILIACION"
                },
                {
                    "len": 1,
                    "column_name": "TIPO_DESPLOGE"
                },
                {
                    "len": 40,
                    "column_name": "NOMBRE_NEGOCIO"
                },
                {
                    "len": 4,
                    "column_name": "SUCURSAL_ABONO"
                },
                {
                    "len": 7,
                    "column_name": "CUENTA_CH_ABONO"
                },
                {
                    "len": 4,
                    "column_name": "SUCURSAL_CARGO"
                },
                {
                    "len": 7,
                    "column_name": "CUENTA_CH_CARGO"
                },
                {
                    "len": 3,
                    "column_name": "DIFERIMIENTO_BANCARIO"
                }
                ,
                {
                    "len": 3,
                    "column_name": "DIFERIMIENTO_PAGO"
                }
                ,
                {
                    "len": 2,
                    "column_name": "CODIGO_PRODUCTO"
                }
                ,
                {
                    "len": 14,
                    "column_name": "TASA_CUOTA_CREDITO"
                }
                ,
                {
                    "len": 2,
                    "column_name": "CODIGO_PRODUCTO_DEBITO"
                }
                ,
                {
                    "len": 14,
                    "column_name": "TASA_CUOTA_DEBITO"
                }
                ,
                {
                    "len": 2,
                    "column_name": "CODIGO_PRODUCTO_CUENTA_MAESTRO"
                }
                ,
                {
                    "len": 14,
                    "column_name": "TASA_CUOTA_CUENTA_MAESTRO"
                }
                ,
                {
                    "len": 2,
                    "column_name": "CODIGO_PRODUCTO_INTERNACIONAL"
                }
                ,
                {
                    "len": 14,
                    "column_name": "TASA_CUOTA_INTERNACIONAL"
                }
                ,
                {
                    "len": 2,
                    "column_name": "CODIGO_PRODUCTO_OPT_BLUE"
                }
                ,
                {
                    "len": 14,
                    "column_name": "TASA_CUOTA_CUENTA_OPT_BLUE"
                }
                ,
                {
                    "len": 8,
                    "column_name": "FECHA_PROCESO"
                }
                ,
                {
                    "len": 3,
                    "column_name": "TIPO_MONEDA"
                }
            ]
        },
        {
            "record_type": 'CARGOYABONO',
            "layout": [
                {
                    "len": 4,
                    "column_name": "TIPO_REGISTRO"
                },
                {
                    "len": 16,
                    "column_name": "NUMERO_AFILIACION"
                },
                {
                    "len": 18,
                    "column_name": "IMPORTE_COMISIONES_POR_TASA_DESCUENTO"
                },
                {
                    "len": 6,
                    "column_name": "NUMERO_TOTAL_DE_TRANSACCIONES_COMISIONES"
                },
                {
                    "column_name":"IMPORTE_DE_CUOTA",
                    "len":18,
                },
                {
                    "column_name":"NUMERO_TOTAL_TRANSACCIONES_CUOTAS",
                    "len":6,
                },
                {
                    "column_name":"IMPORTE_DE_SOBRETASA",
                    "len":18,
                },
                {
                    "column_name":"NUMERO_TOTAL_TRANSACCIONES_SOBRETASA",
                    "len":6,
                },
                {
                    "column_name":"IMPORTE_VENTAS_DEVOLUCIONES",
                    "len":18,
                },
                {
                    "column_name":"TOTAL_TRANSACCIONES_VENTAS_DEVOLUCIONES",
                    "len":6,
                },
                {
                    "column_name":"IMPORTE_RETIRO_EFECTIVO",
                    "len":18,
                },
                {
                    "column_name":"NO_TOTAL_TRANSACCIONES_RETIRO_EFECTIVO",
                    "len":6,
                },
                {
                    "column_name":"IMPORTE_TRANSACCIONES_RECHAZADAS",
                    "len":18,
                },
                {
                    "column_name":"NO_TRANSACCIONES_RECHAZADAS",
                    "len":6,
                },
                {
                    'column_name':'IMPORTE_TOTAL_CONTRACARGOS',
                    'len':18
                },
                {
                    'column_name':'NUMERO_TOTAL_REGISTROS_CONTRACARGOS',
                    'len':6
                },
                {
                    'column_name':'IMPORTE_TOTAL_COBRO_SERVICIOS',
                    'len':18
                },
                {
                    'column_name':'NUMERO_TOTAL_REGISTROS_COBRO_SERVICIOS',
                    'len':6
                },
                {
                    'column_name':'IMPORTE_TOTAL_SOBRETASA_DUALES',
                    'len':18
                },

                {
                    'column_name':'NUMERO_TOTAL_REGISTROS_SOBRETASA_DUALES',
                    'len':6
                },
                {
                    'column_name':'IMPORTE_TOTAL_CARGO_O_ABONO_MANUAL',
                    'len':18
                },

                {
                    'column_name':'NUMERO_TOTAL_REGISTROS_CARGO_O_ABONO_MANUAL',
                    'len':6
                },
                {
                    'column_name':'IMPORTE_TOTAL_COBRANZA',
                    'len':18
                },

                {
                    'column_name':'NUMERO_TOTAL_REGISTROS_COBRANZA',
                    'len':6
                },
                {
                    'column_name':'IMPORTE_BRUTO',
                    'len':18
                },

                {
                    'column_name':'IMPORTE_NETO_REAL',
                    'len':18
                },

                {
                    'column_name':'DESCUENTOS',
                    'len':18
                },
            ]
        },
        {
            "record_type": 'COMISIONES',
            "layout": [
                {
                    'column_name':"TIPO_DE_REGISTRO",
                    'len':4
                },
                {
                    'column_name':"CODIGO_DEL_PRODUCTO_CREDITO",
                    'len':2
                },
                {
                    'column_name':"IMPORTE_TOTAL_COMISION_TARJETA_CREDITO",
                    'len':14
                },
                {
                    'column_name':"IVA_COMISION_TARJETA_CREDITO",
                    'len':14
                },
                {
                    'column_name':"COMISION_TARJETA_CREDITO",
                    'len':14
                },
                {
                    'column_name':"NUMERO_TRANSACCIQONES_TARJETAS_CREDITO",
                    'len':6
                },
                {
                    'column_name':"CODIGO_PRODUCTO_DEBITO",
                    'len':2
                },
                {
                    'column_name':"IMPORTE_TOTAL_COMISION_TARJETA_DEBITO",
                    'len':14
                },
                {
                    'column_name':"IVA_COMISION_TARJETA",
                    'len':14
                },
                {
                    'column_name':"COMISION_TARJETA_DEBITO",
                    'len':14
                },
                {
                    'column_name':"NUMERO_TRANSACCIONES_DEBITO",
                    'len':6
                },
                {
                    'column_name':"CODIGO_DEL_PRODUCTO_CUENTA_MAESTRA",
                    'len':2
                },
                {
                    'column_name':"IMPORTE_TOTAL_COMISION_TARJETA_MAESTRA",
                    'len':14
                },
                {
                    'column_name':"IVA_COMISION_TARJETA_CUENTA_MAESTRA",
                    'len':14
                },
                {
                    'column_name':"COMISION_TARJETA_CUENTA_MAESTRA",
                    'len':14
                },
                {
                    'column_name':"NUMERO_TRANSACCIONES_CUENTA_MAESTRA",
                    'len':6
                },
                {
                    'column_name':"CODIGO_PRODUCTO_EXTRANJERA",
                    'len':2
                },
                {
                    'column_name':"IMPORTE_TOTAL_COMISION_TARJETA_EXTRANJERA",
                    'len':14
                },
                {
                    'column_name':"IVA_COMISION_TARJETA_EXTRANJERA",
                    'len':14
                },
                {
                    'column_name':"COMISION_TARJETA_EXTRANJERA",
                    'len':14
                },
                {
                    'column_name':"CODIGO_DEL_PRODUCTO_AMEX_OPTBLUE",
                    'len':2
                },
                {
                    'column_name':"IMPORTE_TOTAL_COMISION_TARJETA_AMEX_OPTBLUE",
                    'len':18
                },
                {
                    'column_name':"IVA_COMISION_TARJETA_AMEX_OPTBLUE",
                    'len':18
                },
                {
                    'column_name':"COMISION_TARJETA_AMEX_OPTBLUE",
                    'len':18
                },
                {
                    'column_name':"NUMERO_TRANSACCIONES_AMEX_OPTBLUE",
                    'len':6
                }
            ]
        },
        {
            "record_type": 'SOBRETASA',
            "layout": [
                {
                    'column_name':'TIPO_DE_REGISTRO',
                    'len':4
                },
                {
                    'column_name':'DIFERIMIENTO',
                    'len':2
                },
                {
                    'column_name':'NUMERO_DE_PAGOS',
                    'len':2
                },
                {
                    'column_name':'TIPO_DE_PLAN',
                    'len':2
                },
                {
                    'column_name':'CODIGO_DEL_PRODUCTO',
                    'len':2
                },
                {
                    'column_name':'FECHA_INICIO_PROMOCION',
                    'len':8
                },
                {
                    'column_name':'FECHA_FIN_PROMOCION',
                    'len':8
                },
                {
                    'column_name':'VALOR_DE_SOBRETASA',
                    'len':14
                },
                {
                    'column_name':'IMPORTE_TOTAL_SOBRETASA',
                    'len':14
                },
                {
                    'column_name':'IVA_SOBRETASA',
                    'len':14
                },
                {
                    'column_name':'NUMERO_TRANSACCIONES_CON_PROMOCION',
                    'len':6
                } 
            ]
        },
        {
            "record_type": 'VENTAS',
            "layout": [
                {
                    'column_name':'TIPO_DE_REGISTRO',
                    'len':4
                },
                {
                    'column_name':'NUMERO_CONSECUTIVO_DE_REGISTRO',
                    'len':8
                },
                {
                    'column_name':'NUMERO_DE_TARJETA',
                    'len':20
                },
                {
                    'column_name':'IMPORTE_DE_TRANSACCION',
                    'len':18
                },
                {
                    'column_name':'IMPORTE_RETIRO_DE_EFECTIVO',
                    'len':18
                },
                {
                    'column_name':'MONEDA_DE_TRANSACCION',
                    'len':4
                },
                {
                    'column_name':'CODIGO_DE_TRANSACCION',
                    'len':2
                },
                {
                    'column_name':'TRANSACCION_ACEPTADO_RECHAZADO',
                    'len':2
                },
                {
                    'column_name':'CODIGO_DE_RESPUESTA',
                    'len':4
                },
                {
                    'column_name':'FECHA_TRANSACCION',
                    'len':8
                },
                {
                    'column_name':'HORA_TRANSACCION',
                    'len':6
                },
                {
                    'column_name':'NUMERO_REFERENCIA_RRN',
                    'len':12
                },
                {
                    'column_name':'TIPO_DE_TARJETA',
                    'len':1
                },
                {
                    'column_name':'DIFERIMIENTO',
                    'len':2
                },
                {
                    'column_name':'NUMERO_DE_PAGOS',
                    'len':2
                },
                {
                    'column_name':'TIPO_DE_PLAN',
                    'len':2
                },
                {
                    'column_name':'REFERENCIA_DE_23_POSICIONES',
                    'len':23
                },
                {
                    'column_name':'NUMERO_DE_AUTORIZACION',
                    'len':6
                },
                {
                    'column_name':'EMISOR',
                    'len':2
                },
                {
                    'column_name':'CODIGO_PRODUCTO_COMISION',
                    'len':2
                },
                {
                    'column_name':'COMISION',
                    'len':18
                },
                {
                    'column_name':'IMPORTE_COMISION',
                    'len':18
                },
                {
                    'column_name':'IVA_COMISION',
                    'len':18
                }
            ]
        },
        {
            "record_type": 'CONTRACARGOS',
            "layout": [
                {
                    'column_name':'TIPO_DE_REGISTRO',
                    'len':4
                },
                {
                    'column_name':'NUMERO_CONSECUTIVO_DE_REGISTRO',
                    'len':8
                },
                {
                    'column_name':'NUMERO_DE_TARJETA',
                    'len':20
                },
                {
                    'column_name':'IMPORTE_ORIGINAL_DE_TRANSACCION',
                    'len':18
                },
                {
                    'column_name':'MONEDA_DE_TRANSACCION',
                    'len':4
                },
                {
                    'column_name':'CODIGO_DE_TRANSACCION',
                    'len':4
                },
                {
                    'column_name':'FECHA_DE_TRANSACCION',
                    'len':8
                },
                {
                    'column_name':'HORA_DE_TRANSACCION',
                    'len':6
                },
                {
                    'column_name':'FORMA_DE_TRANSACCION',
                    'len':2
                },
                {
                    'column_name':'NUMERO_AUTORIZACION',
                    'len':6
                },
                {
                    'column_name':'FOLIO_EG',
                    'len':6
                },
                {
                    'column_name':'REFERENCIA_DE_23_POSICIONES',
                    'len':23
                },
                {
                    'column_name':'TIPO_DE_DOCUMENTO',
                    'len':2
                },
                {
                    'column_name':'NUMERO_DE_CONTRACARGO',
                    'len':1
                },
                {
                    'column_name':'CODIGO_DE__LEYENDA',
                    'len':4
                },
                {
                    'column_name':'FECHA_DE_APLICACION_DEL_CARGO',
                    'len':8
                },
                {
                    'column_name':'CARGO_OPERATIVO',
                    'len':18
                },
                {
                    'column_name':'IVA_CARGO_OPERATIVO',
                    'len':18
                },
                {
                    'column_name':'NUMERO_REFERENCIA_RRN',
                    'len':12
                }
            ]
        },        
        {
            "record_type": 'TRAILER',
            "layout": [
                {
                    'column_name':'TIPO_REGISTRO',
                    'len':4
                },
                {
                    'column_name':'NOM_ADQUIRIENTE',
                    'len':30
                },
                {
                    'column_name':'NOM_AFILIACIoN',
                    'len':30
                },
                {
                    'column_name':'FECHA_PROCESO',
                    'len':8
                },
                {
                    'column_name':'NUM',
                    'len':10
                }
            ]
        }
        
    ]
    
class AmexExtractor:

    path: str
    record_type: AmexRecordType

    def __init__(self, path, record_type):
        self.path = path
        self.record_type = record_type

    @staticmethod
    def split(data: list, record_type) -> DataFrame:
        
        dataF = []
        manifest = list(filter(lambda x: x['record_type'] == record_type, AMEXManifest.rules))[0]
        for line in data:
            item = {}

            if line.startswith(AmexRecordType[record_type]):
                ix = 0
                for col in manifest['layout']:
                    item[col['column_name']] = line[ix: ix + col['len']]
                    ix += col['len']
                dataF.append(item)
        return pd.DataFrame(dataF)

    def run(self) -> DataFrame:
        return AmexExtractor.split(data=self.path, record_type=self.record_type)


class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        tipo_tabla = kwargs['tipo_tabla']
        for tipo in AmexRecordType:
            if tipo_tabla in str(tipo).split('.')[-1]:
                tipos=tipo
        data,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)

        emp2 = AmexExtractor(data,tipos)
        df_amex = AmexExtractor.run(emp2)
        df_amex.columns = df_amex.columns.str.lower()
        df_amex['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df_amex['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1]).replace('.TXT','.csv')
        df_amex['file_name'] = out
        df_amex.reset_index(drop=True)
        df_amex['skt_extraction_rn'] = df_amex.index.values
        return df_amex

