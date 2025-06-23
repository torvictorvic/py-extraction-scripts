import pandas as pd
from pandas import DataFrame
from enum import Enum
from urllib.parse import urlparse
from io import BytesIO,StringIO,TextIOWrapper
import boto3 
from datetime import datetime
import pytz


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
            # session = boto3.Session(profile_name="sts")
            # s3 = session.client('s3')
            session = boto3.session.Session()
            s3 = session.client('s3')
            #     #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)['Body'].read()
            bytes_io = BytesIO(obj)
            text_io = TextIOWrapper(bytes_io)
            return text_io,lm

        else:
            with open(uri) as f:
                return uri,datetime.now()


class FileReaderCirculares:

    @staticmethod
    def read(uri: str):
        origin = urlparse(uri, allow_fragments=False)
        if origin.scheme in ('s3', 's3a'):
            session = boto3.session.Session()
            s3 = session.client('s3')
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read().decode('latin1').encode('latin1')
            bytes_io = BytesIO(obj)
            return bytes_io, lm
        else:
            with open(uri,encoding='utf-8') as f:
                return f.readlines(), datetime.today()


class StringConverter(dict):
    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return str

    def get(self, default=None):
        return str

class CabalRecordType(Enum):
    RECORD1 = 1
    RECORD2 = 2
    RECORD3 = 3
    RECORD4 = 4
    RECORD5 = 5
    RECORD6 = 6

class CabalManifest:

    rules = [
        {
            "record_type": 1,
            "layout": [
                {
                    "start_at": 1,
                    "ends_at": 1,
                    "column_name": "tiporegistro"
                },
                {
                    "start_at": 2,
                    "ends_at": 12,
                    "column_name": "numerocomercio"
                },
                {
                    "start_at": 13,
                    "ends_at": 14,
                    "column_name": "codigooperacion"
                },
                {
                    "start_at": 15,
                    "ends_at": 30,
                    "column_name": "numerotarjeta"
                },
                {
                    "start_at": 31,
                    "ends_at": 36,
                    "column_name": "fechacompra"
                },
                {
                    "start_at": 37,
                    "ends_at": 42,
                    "column_name": "fechapresentacion"
                },
                {
                    "start_at": 43,
                    "ends_at": 47,
                    "column_name": "numeroautorizacion"
                },
                {
                    "start_at": 48,
                    "ends_at": 50,
                    "column_name": "numerolote"
                },
                {
                    "start_at": 51,
                    "ends_at": 56,
                    "column_name": "numeroliquidacion"
                },
                {
                    "start_at": 57,
                    "ends_at": 61,
                    "column_name": "tasacostofinanciero"
                },
                {
                    "start_at": 62,
                    "ends_at": 64,
                    "column_name": "codigoentidademisoratarjeta"
                },
                {
                    "start_at": 65,
                    "ends_at": 65,
                    "column_name": "tipoentidademisora"
                },
                {
                    "start_at": 66,
                    "ends_at": 74,
                    "column_name": "importeventa"
                },
                {
                    "start_at": 75,
                    "ends_at": 76,
                    "column_name": "subcodigovarios"
                },
                {
                    "start_at": 77,
                    "ends_at": 78,
                    "column_name": "libre"
                },
                {
                    "start_at": 79,
                    "ends_at": 82,
                    "column_name": "numerocupon"
                },
                {
                    "start_at": 83,
                    "ends_at": 88,
                    "column_name": "fechapago"
                },
                {
                    "start_at": 89,
                    "ends_at": 89,
                    "column_name": "codigocuponaceptadorechazado"
                },
                {
                    "start_at": 90,
                    "ends_at": 98,
                    "column_name": "numeroterminal"
                },
                {
                    "start_at": 99,
                    "ends_at": 100,
                    "column_name": "libre1"
                },
                {
                    "start_at": 101,
                    "ends_at": 102,
                    "column_name": "motivocontracargo"
                },
                {
                    "start_at": 103,
                    "ends_at": 104,
                    "column_name": "cantidadcuotas"
                },
                {
                    "start_at": 105,
                    "ends_at": 105,
                    "column_name": "monedapago"
                },
                {
                    "start_at": 106,
                    "ends_at": 114,
                    "column_name": "importearancelcompra"
                },
                {
                    "start_at": 115,
                    "ends_at": 123,
                    "column_name": "costofinancierocupon"
                },
                {
                    "start_at": 124,
                    "ends_at": 135,
                    "column_name": "numerocuit"
                },
                {
                    "start_at": 136,
                    "ends_at": 139,
                    "column_name": "numerodeliquidacion"
                },
                {
                    "start_at": 140,
                    "ends_at": 140,
                    "column_name": "ventatarjetaprecargada"
                },
                {
                    "start_at": 140,
                    "ends_at": 160,
                    "column_name": "numerodesocio"
                },
                 {
                    "start_at": 161,
                    "ends_at": 171,
                    "column_name": "cuit"
                }, 
                 {
                    "start_at": 172,
                    "ends_at": 176,
                    "column_name": "libres"
                },
                {
                    "start_at": 177,
                    "ends_at": 178,
                    "column_name": "codigoprovinciasucursalemisora"
                },
                {
                    "start_at": 179,
                    "ends_at": 189,
                    "column_name": "importeventanuevo"
                },
                {
                    "start_at": 190,
                    "ends_at": 195,
                    "column_name": "fechadeproceso"
                },
                {
                    "start_at": 191,
                    "ends_at": 386,
                    "column_name": "libres1"
                },
                {
                    "start_at": 387,
                    "ends_at": 388,
                    "column_name": "marca"
                },
                {
                    "start_at": 389,
                    "ends_at": 389,
                    "column_name": "libres2"
                },
                {
                    "start_at": 390,
                    "ends_at": 393,
                    "column_name": "codigobilleteradepago"
                },
                {
                    "start_at": 394,
                    "ends_at": 410,
                    "column_name": "importedelconsumo"
                },
                {
                    "start_at": 411,
                    "ends_at": 427,
                    "column_name": "importearanceldelconsumo"
                },
                {
                    "start_at": 428,
                    "ends_at": 444,
                    "column_name": "costofinancierodelconsumo"
                },
                {
                    "start_at": 445,
                    "ends_at": 461,
                    "column_name": "importedelconsumonuevo"
                },
                {
                    "start_at": 462,
                    "ends_at": 1000,
                    "column_name": "libres3"
                }

            ]
        },
        {
            "record_type": 2,
            "layout": [
                {
                    "start_at": 1,
                    "ends_at": 1,
                    "column_name": "tiporegistro"
                },
                {
                    "start_at": 2,
                    "ends_at": 12,
                    "column_name": "numerocomercio"
                },
                {
                    "start_at": 13,
                    "ends_at": 24,
                    "column_name": "importeventa"
                },
                {
                    "start_at": 25,
                    "ends_at": 25,
                    "column_name": "signoimportebruto"
                },
                {
                    "start_at": 26,
                    "ends_at": 37,
                    "column_name": "importearancel"
                },
                {
                    "start_at": 38,
                    "ends_at": 38,
                    "column_name": "signoimportearancel"
                },
                {
                    "start_at": 39,
                    "ends_at": 50,
                    "column_name": "importeivasobrearancel"
                },
                {
                    "start_at": 51,
                    "ends_at": 51,
                    "column_name": "signoivasobrearancel"
                },
                {
                    "start_at": 52,
                    "ends_at": 63,
                    "column_name": "campo1"
                },
                {
                    "start_at": 64,
                    "ends_at": 64,
                    "column_name": "campo2"
                },
                {
                    "start_at": 65,
                    "ends_at": 76,
                    "column_name": "retencioniva"
                },
                {
                    "start_at": 77,
                    "ends_at": 77,
                    "column_name": "signoretencioniva"
                },
                {
                    "start_at": 78,
                    "ends_at": 89,
                    "column_name": "retencionganancias"
                },
                {
                    "start_at": 90,
                    "ends_at": 90,
                    "column_name": "signoretencionganancias"
                },
                {
                    "start_at": 91,
                    "ends_at": 102,
                    "column_name": "retencioningresosbrutos"
                },
                {
                    "start_at": 103,
                    "ends_at": 103,
                    "column_name": "signoretencioningresosbrutos"
                },
                {
                    "start_at": 104,
                    "ends_at": 115,
                    "column_name": "percepcionrg3377"
                },
                {
                    "start_at": 116,
                    "ends_at": 116,
                    "column_name": "signopercepcionrg3377"
                },
                {
                    "start_at": 117,
                    "ends_at": 128,
                    "column_name": "importenetofinal"
                },
                {
                    "start_at": 129,
                    "ends_at": 129,
                    "column_name": "signoimportenetofinal"
                },
                {
                   "start_at": 101,
                   "ends_at": 102,
                   "column_name": "motivocontracargo"
                },
                {
                    "start_at": 130,
                    "ends_at": 135,
                    "column_name": "fechapagoliquidacion"
                },
                {
                    "start_at": 136,
                    "ends_at": 139,
                    "column_name": "numeroliquidacion"
                },
                {
                    "start_at": 140,
                    "ends_at": 140,
                    "column_name": "monedapago"
                },
                {
                    "start_at": 141,
                    "ends_at": 189,
                    "column_name": "libre"
                },
                {
                    "start_at": 190,
                    "ends_at": 195,
                    "column_name": "fechadeproceso"
                },
                {
                    "start_at": 196,
                    "ends_at": 212,
                    "column_name": "importetotaldeconsumo"
                },
                {
                    "start_at": 213,
                    "ends_at": 213,
                    "column_name": "signodelimportebruto"
                },
                {
                    "start_at": 214,
                    "ends_at": 230,
                    "column_name": "importetotalarancel"
                },
                {
                    "start_at": 231,
                    "ends_at": 231,
                    "column_name": "signoimportedelarancel"
                },
                {
                    "start_at": 232,
                    "ends_at": 248,
                    "column_name": "importetotalivaarancel"
                },
                {
                    "start_at": 249,
                    "ends_at": 249,
                    "column_name": "signoivasobrearancel1"
                },
                {
                    "start_at": 250,
                    "ends_at": 266,
                    "column_name": "importetotalretencioniva"
                },
                {
                    "start_at": 267,
                    "ends_at": 267,
                    "column_name": "signoderetenciondeiva"
                },
                {
                    "start_at": 268,
                    "ends_at": 284,
                    "column_name": "importeretencionganancias"
                },
                {
                    "start_at": 285,
                    "ends_at": 285,
                    "column_name": "signoretencionganancias1"
                },
                {
                    "start_at": 286,
                    "ends_at": 302,
                    "column_name": "importetotalretencioningresosbrutos"
                },
                {
                    "start_at": 303,
                    "ends_at": 303,
                    "column_name": "signoretencioningresosbrutos1"
                },
                {
                    "start_at": 304,
                    "ends_at": 320,
                    "column_name": "importetotalpercepcion3337"
                },
                {
                    "start_at": 321,
                    "ends_at": 321,
                    "column_name": "signodepercepcion3337"
                },
                {
                    "start_at": 322,
                    "ends_at": 338,
                    "column_name": "importetotalnetofinal"
                },
                {
                    "start_at": 339,
                    "ends_at": 339,
                    "column_name": "signoimportetotalnetofinal"
                },
                {
                    "start_at": 340,
                    "ends_at": 386,
                    "column_name": "libre2"
                },
                {
                    "start_at": 387,
                    "ends_at": 388,
                    "column_name": "marca"
                },
                {
                    "start_at": 389,
                    "ends_at": 389,
                    "column_name": "libre3"
                },
                {
                    "start_at": 390,
                    "ends_at": 400,
                    "column_name": "campocuit"
                },
                
                {
                    "start_at": 401,
                    "ends_at": 1000,
                    "column_name": "libre4"
                }


            ]
        },
        {
            "record_type": 3,
            "layout": [
                {
                    "start_at": 1,
                    "ends_at": 1,
                    "column_name": "tiporegistro"
                },
                {
                    "start_at": 2,
                    "ends_at": 12,
                    "column_name": "numerocomercio"
                },
                {
                    "start_at": 13,
                    "ends_at": 24,
                    "column_name": "percepcioningresosbrutos"
                },
                {
                    "start_at": 25,
                    "ends_at": 25,
                    "column_name": "signopercepcioningresosbrutos"
                },
                {
                    "start_at": 26,
                    "ends_at": 37,
                    "column_name": "selladoprovincial"
                },
                {
                    "start_at": 38,
                    "ends_at": 38,
                    "column_name": "signosellado"
                },
                {
                    "start_at": 39,
                    "ends_at": 50,
                    "column_name": "costofinanciero"
                },
                {
                    "start_at": 51,
                    "ends_at": 51,
                    "column_name": "signocostofinanciero"
                },
                {
                    "start_at": 52,
                    "ends_at": 63,
                    "column_name": "ivacostofinanciero"
                },
                {
                    "start_at": 64,
                    "ends_at": 64,
                    "column_name": "signoivacostofinanciero"
                },
                {
                    "start_at": 65,
                    "ends_at": 76,
                    "column_name": "libre"
                },
                {
                    "start_at": 77,
                    "ends_at": 77,
                    "column_name": "libre1"
                },
                {
                    "start_at": 78,
                    "ends_at": 89,
                    "column_name": "percepcionrg2126"
                },
                {
                    "start_at": 90,
                    "ends_at": 90,
                    "column_name": "signopercepcionrg2126"
                },
                {
                    "start_at": 91,
                    "ends_at": 103,
                    "column_name": "libre2"
                },
                {
                    "start_at": 104,
                    "ends_at": 110,
                    "column_name": "impuestoley25413"
                },
                {
                    "start_at": 111,
                    "ends_at": 111,
                    "column_name": "signoimpuestoley25413"
                },
                {
                    "start_at": 112,
                    "ends_at": 113,
                    "column_name": "provincia"
                },
                {
                    "start_at": 114,
                    "ends_at": 129,
                    "column_name": "libre3"
                },
                {
                    "start_at": 130,
                    "ends_at": 135,
                    "column_name": "fechapagoliquidacion"
                },
                {
                    "start_at": 136,
                    "ends_at": 139,
                    "column_name": "numeroliquidacion"
                },
                {
                    "start_at": 140,
                    "ends_at": 140,
                    "column_name": "monedapago"
                },
                {
                    "start_at": 141,
                    "ends_at": 189,
                    "column_name": "libre4"
                },
                {
                    "start_at": 190,
                    "ends_at": 195,
                    "column_name": "fechadeproceso"
                },
                {
                    "start_at": 196,
                    "ends_at": 212,
                    "column_name": "percepcioningresosbrutos1"
                },
                {
                    "start_at": 213,
                    "ends_at": 213,
                    "column_name": "signopercepcioningresosbrutos1"
                },
                {
                    "start_at": 214,
                    "ends_at": 230,
                    "column_name": "selladoprovincial1"
                },
                {
                    "start_at": 231,
                    "ends_at": 231,
                    "column_name": "signodesellado"
                },
                {
                    "start_at": 232,
                    "ends_at": 248,
                    "column_name": "costofinanciero1"
                },
                {
                    "start_at": 249,
                    "ends_at": 249,
                    "column_name": "signocostofinanciero1"
                },
                {
                    "start_at": 250,
                    "ends_at": 266,
                    "column_name": "ivacostofinanciero1"
                },
                {
                    "start_at": 267,
                    "ends_at": 267,
                    "column_name": "signoivacostofinanciero1"
                },
                {
                    "start_at": 268,
                    "ends_at": 284,
                    "column_name": "ivaalicuota21"
                },
                 {
                    "start_at": 285,
                    "ends_at": 285,
                    "column_name": "signoivaalicuota21"
                },
                 {
                    "start_at": 286,
                    "ends_at": 302,
                    "column_name": "percepcionrg"
                },
                 {
                    "start_at": 303,
                    "ends_at": 304,
                    "column_name": "signopercepcionrg"
                },
                {
                    "start_at": 304,
                    "ends_at": 320,
                    "column_name": "ivaalicuota10_5"
                },
                {
                    "start_at": 321,
                    "ends_at": 321,
                    "column_name": "signoivaalicuota10_5"
                },
                {
                    "start_at": 322,
                    "ends_at": 338,
                    "column_name": "impuestoley25413_1"
                },
                {
                    "start_at": 339,
                    "ends_at": 339,
                    "column_name": "signoimpuestoley25413_1"
                },
                {
                    "start_at": 340,
                    "ends_at": 356,
                    "column_name": "rg140_98"
                },
                {
                    "start_at": 357,
                    "ends_at": 357,
                    "column_name": "signorg140_98"
                },
                {
                    "start_at": 358,
                    "ends_at": 386,
                    "column_name": "libre5"
                },
                {
                    "start_at": 387,
                    "ends_at": 388,
                    "column_name": "marca"
                },
                {
                    "start_at": 389,
                    "ends_at": 389,
                    "column_name": "libre6"
                },
                {
                    "start_at": 390,
                    "ends_at": 400,
                    "column_name": "cuit"
                },
                {
                    "start_at": 401,
                    "ends_at": 1000,
                    "column_name": "libre7"
                },
            ]
        },
        {
            "record_type": 4,
            "layout": [
                {
                    "start_at": 1,
                    "ends_at": 1,
                    "column_name": "tiporegistro"
                },
                {
                    "start_at": 2,
                    "ends_at": 12,
                    "column_name": "numerocomercio"
                },
                {
                    "start_at": 13,
                    "ends_at": 22,
                    "column_name": "importeventastc"
                },
                {
                    "start_at": 23,
                    "ends_at": 32,
                    "column_name": "importearancelventastc"
                },
                {
                    "start_at": 33,
                    "ends_at": 42,
                    "column_name": "importebrutoventastd"
                },
                {
                    "start_at": 43,
                    "ends_at": 52,
                    "column_name": "importearancelventastd"
                },
                {
                    "start_at": 53,
                    "ends_at": 62,
                    "column_name": "importeivaambosaranceles"
                },
                {
                    "start_at": 63,
                    "ends_at": 72,
                    "column_name": "retencionivatarjetac"
                },
                {
                    "start_at": 73,
                    "ends_at": 82,
                    "column_name": "retencionivatarjetad"
                },
                {
                    "start_at": 83,
                    "ends_at": 92,
                    "column_name": "retenciongananciastdtc"
                },
                {
                    "start_at": 93,
                    "ends_at": 102,
                    "column_name": "retencioningresosbrutostdtc"
                },
                {
                    "start_at": 103,
                    "ends_at": 112,
                    "column_name": "percepcionrg3377tdtc"
                },
                {
                    "start_at": 113,
                    "ends_at": 113,
                    "column_name": "signoimporteneto"
                },
                {
                    "start_at": 114,
                    "ends_at": 123,
                    "column_name": "importenetotdtc"
                },
                {
                    "start_at": 124,
                    "ends_at": 129,
                    "column_name": "fechapagoliquidacion"
                },
                {
                    "start_at": 130,
                    "ends_at": 133,
                    "column_name": "numeroliquidacion"
                },
                {
                    "start_at": 134,
                    "ends_at": 134,
                    "column_name": "monedapago"
                },
                {
                 "start_at": 135,
                   "ends_at": 140,
                   "column_name": "importeretencioniva_rg14098"
                },
                {
                    "start_at": 135,
                    "ends_at": 189,
                    "column_name": "libre"
                },
                {
                    "start_at": 190,
                    "ends_at": 195,
                    "column_name": "fechadeproceso"
                },
                {
                    "start_at": 196,
                    "ends_at": 212,
                    "column_name": "importeconsumotarjetacredito"
                },
                {
                    "start_at": 213,
                    "ends_at": 229,
                    "column_name": "importearancelventastc"
                },
                {
                    "start_at": 230,
                    "ends_at": 246,
                    "column_name": "importeconsumotd"
                },
                {
                    "start_at": 247,
                    "ends_at": 263,
                    "column_name": "importearancelventastd"
                },
                {
                    "start_at": 265,
                    "ends_at": 280,
                    "column_name": "importeivaambosaranceles1"
                },
                {
                    "start_at": 281,
                    "ends_at": 297,
                    "column_name": "retencionivatc"
                },
                {
                    "start_at": 298,
                    "ends_at": 314,
                    "column_name": "retencionivatd"
                },
                {
                    "start_at": 315,
                    "ends_at": 331,
                    "column_name": "retenciongananciastc_td"
                },
                {
                    "start_at": 332,
                    "ends_at": 348,
                    "column_name": "retencioningresosbrutos1"
                },
                {
                    "start_at": 349,
                    "ends_at": 365,
                    "column_name": "percepcionrg3337"
                },
                {
                    "start_at": 366,
                    "ends_at": 382,
                    "column_name": "importeneto_tc_td"
                },
                {
                    "start_at": 383,
                    "ends_at": 383,
                    "column_name": "signoimporteneto1"
                },
                {
                    "start_at": 384,
                    "ends_at": 386,
                    "column_name": "libre_1"
                },
                {
                    "start_at": 387,
                    "ends_at": 388,
                    "column_name": "marca"
                },
                {
                    "start_at": 389,
                    "ends_at": 389,
                    "column_name": "libre_2"
                },
                {
                    "start_at": 390,
                    "ends_at": 400,
                    "column_name": "cuit"
                },
                {
                    "start_at": 401,
                    "ends_at": 1000,
                    "column_name": "libre_3"
                }
            ]
        },
        {
            "record_type": 5,
            "layout": [
                {
                    "start_at": 1,
                    "ends_at": 1,
                    "column_name": "tiporegistro"
                },
                {
                    "start_at": 2,
                    "ends_at": 12,
                    "column_name": "numerocomercio"
                },
                {
                    "start_at": 13,
                    "ends_at": 23,
                    "column_name": "numerocuit_entidadpagadora"
                },
                {
                    "start_at": 24,
                    "ends_at": 33,
                    "column_name": "importebaseimponible_ingresosbrutos"
                },
                {
                    "start_at": 34,
                    "ends_at": 38,
                    "column_name": "alicuotaimpuestoingresosbrutos"
                },
                {
                    "start_at": 39,
                    "ends_at": 48,
                    "column_name": "importebaseimponible_percepcioningresosbrutos"
                },
                {
                    "start_at": 49,
                    "ends_at": 53,
                    "column_name": "alicuotapercepcioningresosbrutos"
                },
                {
                    "start_at": 54,
                    "ends_at": 63,
                    "column_name": "baseimponiblecostofinancieroiva21"
                },
                {
                    "start_at": 64,
                    "ends_at": 64,
                    "column_name": "signobaseimponiblecostofinancieroiva21"
                },
                {
                    "start_at": 65,
                    "ends_at": 70,
                    "column_name": "iva21sobrecostofinanciero"
                },
                {
                    "start_at": 71,
                    "ends_at": 71,
                    "column_name": "signoiva21sobrecostofinanciero"
                },
                {
                    "start_at": 72,
                    "ends_at": 81,
                    "column_name": "baseimponiblecostofinancieroiva10"
                },
                {
                    "start_at": 82,
                    "ends_at": 82,
                    "column_name": "signobaseimponiblecostofinancieroiva10"
                },
                {
                    "start_at": 83,
                    "ends_at": 88,
                    "column_name": "iva10sobrecostofinanciero"
                },
                {
                    "start_at": 89,
                    "ends_at": 89,
                    "column_name": "signoiva10sobrecostofinanciero"
                },
                {
                    "start_at": 90,
                    "ends_at": 93,
                    "column_name": "numeroliquidacion"
                },
                {
                    "start_at": 94,
                    "ends_at": 96,
                    "column_name": "entidadpagadora"
                },
                {
                    "start_at": 97,
                    "ends_at": 108,
                    "column_name": "importeretencionrg339"
                },
                {
                    "start_at": 109,
                    "ends_at": 109,
                    "column_name": "signoimporteretencionrg339"
                },
                {
                    "start_at": 110,
                    "ends_at": 121,
                    "column_name": "importeretencionrg443"
                },
                {
                    "start_at": 122,
                    "ends_at": 122,
                    "column_name": "signoimporteretencionrg443"
                },
                {
                    "start_at": 123,
                    "ends_at": 134,
                    "column_name": "importepercepcionrg339"
                },
                {
                    "start_at": 135,
                    "ends_at": 135,
                    "column_name": "signoimportepercepcionrg339"
                },
                {
                    "start_at": 136,
                    "ends_at": 139,
                    "column_name": "libre"
                },
                {
                    "start_at": 140,
                    "ends_at": 140,
                    "column_name": "monedapago"
                },
                {
                    "start_at": 141,
                    "ends_at": 189,
                    "column_name": "libre_2"
                },
                {
                    "start_at": 190,
                    "ends_at": 195,
                    "column_name": "fechadeproceso"
                },
                {
                    "start_at": 196,
                    "ends_at": 212,
                    "column_name": "importebase_ingresosbrutos"
                },
                {
                    "start_at": 213,
                    "ends_at": 229,
                    "column_name": "importebase_percepcion_ingresosbrutos"
                },
                {
                    "start_at": 230,
                    "ends_at": 246,
                    "column_name": "base_imponible_costofinanciero_iva21"
                },
                {
                    "start_at": 247,
                    "ends_at": 247,
                    "column_name": "signo_base_imponible_costofinanciero_iva21"
                },
                {
                    "start_at": 248,
                    "ends_at": 264,
                    "column_name": "base_imponible_costofinanciero_iva10_5"
                },
                {
                    "start_at": 265,
                    "ends_at": 265,
                    "column_name": "signo_base_imponible_costofinanciero_iva10_5"
                },
                {
                    "start_at": 266,
                    "ends_at": 282,
                    "column_name": "importeretencionrg6_20"
                },
                {
                    "start_at": 283,
                    "ends_at": 283,
                    "column_name": "signoimporteretencionrg6_20"
                },
                {
                    "start_at": 284,
                    "ends_at": 300,
                    "column_name": "importeretencioniibb_sirtac"
                },
                {
                    "start_at": 301,
                    "ends_at": 301,
                    "column_name": "signoimporteretencioniibb_sirtac"
                },
                {
                    "start_at": 302,
                    "ends_at": 318,
                    "column_name": "importeretencioniibb_sirtac_1"
                },
                {
                    "start_at": 319,
                    "ends_at": 319,
                    "column_name": "signoimporteretencioniibb_sirtac_1"
                },
                {
                    "start_at": 320,
                    "ends_at": 386,
                    "column_name": "libre_3"
                },
                {
                    "start_at": 387,
                    "ends_at": 388,
                    "column_name": "marca"
                },
                {
                    "start_at": 389,
                    "ends_at": 389,
                    "column_name": "libre_4"
                },
                {
                    "start_at": 390,
                    "ends_at": 400,
                    "column_name": "cuit"
                },
                {
                    "start_at": 401,
                    "ends_at": 1000,
                    "column_name": "libre_5"
                },
                
            ]
        },
        {
            "record_type": 6,
            "layout": [
                {
                    "start_at": 1,
                    "ends_at": 1,
                    "column_name": "tiporegistro"
                },
                {
                    "start_at": 2,
                    "ends_at": 12,
                    "column_name": "numerocomercio"
                },
                {
                    "start_at": 13,
                    "ends_at": 23,
                    "column_name": "importedebitospagoexpreso"
                },
                {
                    "start_at": 24,
                    "ends_at": 24,
                    "column_name": "signoimportedebitospagoexpreso"
                },
                {
                    "start_at": 25,
                    "ends_at": 35,
                    "column_name": "importecreditospagoexpreso"
                },
                {
                    "start_at": 36,
                    "ends_at": 36,
                    "column_name": "signoimportecreditospagoexpreso"
                },
                {
                    "start_at": 37,
                    "ends_at": 45,
                    "column_name": "aranceldebitospagoexpreso"
                },
                {
                    "start_at": 46,
                    "ends_at": 46,
                    "column_name": "signoaranceldebitospagoexpreso"
                },
                {
                    "start_at": 47,
                    "ends_at": 55,
                    "column_name": "arancelcreditoscostopagoexpreso"
                },
                {
                    "start_at": 56,
                    "ends_at": 56,
                    "column_name": "signoarancelcreditoscostopagoexpreso"
                },
                {
                    "start_at": 57,
                    "ends_at": 65,
                    "column_name": "importecargofijo_debitopagoexpreso"
                },
                {
                    "start_at": 66,
                    "ends_at": 66,
                    "column_name": "signoimportecargofijo_debitopagoexpreso"
                },
                {
                    "start_at": 67,
                    "ends_at": 75,
                    "column_name": "importecargofijo_creditocargoexpreso"
                },
                {
                    "start_at": 76,
                    "ends_at": 76,
                    "column_name": "signoimportecargofijo_creditocargoexpreso"
                },
                {
                    "start_at": 77,
                    "ends_at": 81,
                    "column_name": "numeroliquidacion"
                },
                {
                    "start_at": 82,
                    "ends_at": 96,
                    "column_name": "libre"
                },
                {
                    "start_at": 97,
                    "ends_at": 105,
                    "column_name": "importeivacomisionescargopagoexpreso"
                },
                {
                    "start_at": 106,
                    "ends_at": 106,
                    "column_name": "signoimporteivacomisionescargopagoexpreso"
                },
                {
                    "start_at": 107,
                    "ends_at": 115,
                    "column_name": "libre1"
                },
                {
                    "start_at": 116,
                    "ends_at": 116,
                    "column_name": "libre2"
                },
                {
                    "start_at": 117,
                    "ends_at": 125,
                    "column_name": "importepercepcionivacomisionespagoexpreso"
                },
                {
                    "start_at": 126,
                    "ends_at": 126,
                    "column_name": "signoimportepercepcionivacomisionespagoexpreso"
                },
                {
                    "start_at": 127,
                    "ends_at": 135,
                    "column_name": "importepercepcioningresosbrutoscomisionespagoexpreso"
                },
                {
                    "start_at": 136,
                    "ends_at": 136,
                    "column_name": "signopercepcioningresosbrutoscomisionespagoexpreso"
                },
                {
                    "start_at": 137,
                    "ends_at": 140,
                    "column_name": "libre3"
                }
            ]
        }
    ]
class CabalParser:

    io: str
    type: CabalRecordType

    def __init__(self, io, _type):
        self.io = io
        self.type = _type

    def parse(self):
        data = []
        manifest = list(filter(lambda x: x['record_type'] == self.type.value, CabalManifest.rules))[0]
        for line in self.io:
            item = {}
            if line.startswith(str(self.type.value)):
                for col in manifest['layout']:
                    item[col['column_name']] = line[col['start_at'] - 1: col['ends_at']]
                data.append(item)
        else:
            if not data:
                item = {}
                for col in manifest['layout']:
                    item[col['column_name']] = float('nan')
                data.append(item)

        return data

    def run(self) -> DataFrame:
        data = self.parse()
        # Construct dataframe
        return pd.DataFrame(data)


class Extractor:

    @staticmethod
    def run(filename, **kwargs ):
        df_dicts ={}
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        
        for r in [CabalRecordType.RECORD1,CabalRecordType.RECORD2,CabalRecordType.RECORD3,CabalRecordType.RECORD4,CabalRecordType.RECORD5,CabalRecordType.RECORD6]:
            if kwargs['tipo_tabla'] in str(r):
                record = r 
        # Read file and process it
        file,lm = FileReader.read(filename)
        raw_data = file.readlines()
        df = CabalParser(raw_data, record).run()
        # Append Report Date and Filename
        df['report_date'] =arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        df['filename'] = filename.split('/')[-1]
        df.reset_index(drop=True)
        df['skt_extraction_rn']  = df.index.values
        df_dicts[str(record).split('.')[1]] = df
        tipo_tabla = kwargs['tipo_tabla']
        return df_dicts[tipo_tabla]
        

class ExtractorCirculares:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReaderCirculares.read(filename)
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_excel(file,dtype=str)
        df.columns = ["bco_emisor","suc_emisor","nro_cuenta","cod_operacion","sub_codigo","nro_comercio","nro_cuit","nro_tarjeta","fecha_compra","fecha_pago","fecha_facturacion","fecha_proceso","nro_autorizacion","nro_caja","nro_cupon","cantidad_cuotas","importe_compra","importe_cuota","importe_total"]
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df