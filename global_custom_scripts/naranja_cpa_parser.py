from typing import List
from urllib.parse import urlparse
from io import BytesIO,StringIO,TextIOWrapper
import pdb
import boto3 as boto3
import pandas as pd
from datetime import datetime
import glob
import json
import pandas as pd
import os
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
            session = boto3.session.Session()
            s3 = session.client('s3')
            # session = boto3.Session(profile_name="sts")
            # s3 = session.client('s3')
            # session = boto3.session.Session()
            # s3 = session.client('s3')
            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)      
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)['Body'].read()
            bytes_io = BytesIO(obj)
            text_io = TextIOWrapper(bytes_io)
            return text_io
        else:
            with open(uri) as f:
                return f.read()

class Extractor:
    @staticmethod
    def run(filename, **kwargs ):
        file = FileReader.read(filename)       
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        diccionario = {
            "H": {
                "start": 0,
                "n_rows": 41,
                "df": [],
                "layout": {
                    "0": {
                        "Orden": 1,
                        "Desde": 1,
                        "Hasta": 9,
                        "Long": 9,
                        "Nombre de Campo": "comercio",
                        "Tipo": "N",
                        "Contenido": "Nro. de comercio"
                    },
                    "1": {
                        "Orden": 2,
                        "Desde": 10,
                        "Hasta": 15,
                        "Long": 6,

                        "Nombre de Campo": "fecha_proceso",
                        "Tipo": "N",
                        "Contenido": "Fecha de Procesamiento de datos (AAMMDD)"
                    },
                    "2": {
                        "Orden": 3,
                        "Desde": 16,
                        "Hasta": 21,
                        "Long": 6,
                        "Nombre de Campo": "fecha_pago",
                        "Tipo": "N",
                        "Contenido": "Fecha de Liquidacion (AAMMDD)"
                    },
                    "3": {
                        "Orden": 4,
                        "Desde": 22,
                        "Hasta": 27,
                        "Long": 6,
                        "Nombre de Campo": "numero_liquidacion",
                        "Tipo": "N",
                        "Contenido": "Nro. de la liquidaci\u00f3n"
                    },
                    "4": {
                        "Orden": 5,
                        "Desde": 28,
                        "Hasta": 38,
                        "Long": 11,
                        "Nombre de Campo": "cuit_ag_retencion",
                        "Tipo": "N",
                        "Contenido": "Cuit del Agente de Retenci\u00f3n de Ingresos Brutos (11 enteros)"
                    },
                    "5": {
                        "Orden": 6,
                        "Desde": 39,
                        "Hasta": 39,
                        "Long": 1,
                        "Nombre de Campo": "signo_total_descuentos",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "6": {
                        "Orden": 7,
                        "Desde": 40,
                        "Hasta": 51,
                        "Long": 12,
                        "Nombre de Campo": "total_descuentos",
                        "Tipo": "N",
                        "Contenido": "Importe de descuentos que pertencen a facturaciones anteriores"
                    },
                    "7": {
                        "Orden": 8,
                        "Desde": 52,
                        "Hasta": 52,
                        "Long": 1,
                        "Nombre de Campo": "signo_ret_iva_140",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "8": {
                        "Orden": 9,
                        "Desde": 53,
                        "Hasta": 64,
                        "Long": 12,
                        "Nombre de Campo": "retencion_iva_140",
                        "Tipo": "N",
                        "Contenido": "Importe de la Retencion IVA RG(Afip)140 (8 enteros, 2 decimales)"
                    },
                    "9": {
                        "Orden": 10,
                        "Desde": 65,
                        "Hasta": 65,
                        "Long": 1,
                        "Nombre de Campo": "signo_ret_ganancias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "10": {
                        "Orden": 11,
                        "Desde": 66,
                        "Hasta": 77,
                        "Long": 12,
                        "Nombre de Campo": "retencion_ganancias",
                        "Tipo": "N",
                        "Contenido": "Importe de la Retencion Ganancias RG 3311(8 enteros, 2 decimales)"
                    },
                    "11": {
                        "Orden": 12,
                        "Desde": 78,
                        "Hasta": 78,
                        "Long": 1,
                        "Nombre de Campo": "signo_ret_iva_3130",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "12": {
                        "Orden": 13,
                        "Desde": 79,
                        "Hasta": 90,
                        "Long": 12,
                        "Nombre de Campo": "retencion_3130",
                        "Tipo": "N",
                        "Contenido": "Importe de la Retencion IVA 3130 (8 enteros, 2 decimales)"
                    },
                    "13": {
                        "Orden": 14,
                        "Desde": 91,
                        "Hasta": 91,
                        "Long": 1,
                        "Nombre de Campo": "signo_ret_ing_bru",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo netaivo"
                    },
                    "14": {
                        "Orden": 15,
                        "Desde": 92,
                        "Hasta": 103,
                        "Long": 12,
                        "Nombre de Campo": "base_imponible_ing_bru",
                        "Tipo": "N",
                        "Contenido": "Importe de la Base Imponible de Ing. Brutos (8 enteros, 2 decimales)"
                    },
                    "15": {
                        "Orden": 16,
                        "Desde": 104,
                        "Hasta": 110,
                        "Long": 7,
                        "Nombre de Campo": "alicuota_ing_brutos",
                        "Tipo": "N",
                        "Contenido": "Alicuota para calcular Ing. Brutos (5 enteros, 2 decimales)"
                    },
                    "16": {
                        "Orden": 17,
                        "Desde": 111,
                        "Hasta": 111,
                        "Long": 1,
                        "Nombre de Campo": "signo_ret_ing_brutos",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "17": {
                        "Orden": 18,
                        "Desde": 112,
                        "Hasta": 123,
                        "Long": 12,
                        "Nombre de Campo": "ret_ingresos_brutos",
                        "Tipo": "N",
                        "Contenido": "Importe Retenci\u00f3n de Ingresos Brutos (8 enteros, 2 decimales)"
                    },
                    "18": {
                        "Orden": 19,
                        "Desde": 124,
                        "Hasta": 124,
                        "Long": 1,
                        "Nombre de Campo": "signo_retencion_municipal",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "19": {
                        "Orden": 20,
                        "Desde": 125,
                        "Hasta": 136,
                        "Long": 12,
                        "Nombre de Campo": "retencion_municipal",
                        "Tipo": "N",
                        "Contenido": "Importe de la Retenci\u00f3n Municipal (8 enteros, 2 decimales)"
                    },
                    "20": {
                        "Orden": 21,
                        "Desde": 137,
                        "Hasta": 137,
                        "Long": 1,
                        "Nombre de Campo": "signo_debitos_creditos",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "21": {
                        "Orden": 22,
                        "Desde": 138,
                        "Hasta": 149,
                        "Long": 12,
                        "Nombre de Campo": "debitos_creditos",
                        "Tipo": "N",
                        "Contenido": "Importe de otros Debitos / Cr\u00e9ditos (8 enteros, 2 decimales)"
                    },
                    "22": {
                        "Orden": 23,
                        "Desde": 150,
                        "Hasta": 150,
                        "Long": 1,
                        "Nombre de Campo": "signo_percepcion_1135",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "23": {
                        "Orden": 24,
                        "Desde": 151,
                        "Hasta": 162,
                        "Long": 12,
                        "Nombre de Campo": "percepcion_1135",
                        "Tipo": "N",
                        "Contenido": "Monto Percepci\u00f3n Imp. D\u00e9bitos y Cr\u00e9ditos. RG. 1135(8 enteros, 2 decimales)"
                    },
                    "24": {
                        "Orden": 25,
                        "Desde": 163,
                        "Hasta": 163,
                        "Long": 1,
                        "Nombre de Campo": "signo_liq_negativa_ant",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "25": {
                        "Orden": 26,
                        "Desde": 164,
                        "Hasta": 175,
                        "Long": 12,
                        "Nombre de Campo": "liq_negativa_dia_ant",
                        "Tipo": "N",
                        "Contenido": "Monto Liquidaci\u00f3n Negativa d\u00eda anterior(8 enteros, 2 decimales)"
                    },
                    "26": {
                        "Orden": 27,
                        "Desde": 176,
                        "Hasta": 176,
                        "Long": 1,
                        "Nombre de Campo": "signo_embargo_cesiones",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "27": {
                        "Orden": 28,
                        "Desde": 177,
                        "Hasta": 188,
                        "Long": 12,
                        "Nombre de Campo": "embargo_cesiones",
                        "Tipo": "N",
                        "Contenido": "Importe de Embargos y Cesiones (8 enteros, 2 decimales)"
                    },
                    "28": {
                        "Orden": 29,
                        "Desde": 189,
                        "Hasta": 189,
                        "Long": 1,
                        "Nombre de Campo": "signo_neto",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "29": {
                        "Orden": 30,
                        "Desde": 190,
                        "Hasta": 201,
                        "Long": 12,
                        "Nombre de Campo": "neto",
                        "Tipo": "N",
                        "Contenido": "Monto Neto a liquidar (8 enteros, 2 decimales)"
                    },
                    "30": {
                        "Orden": 31,
                        "Desde": 202,
                        "Hasta": 206,
                        "Long": 5,
                        "Nombre de Campo": "rubro",
                        "Tipo": "N",
                        "Contenido": "Rubro comercial"
                    },
                    "31": {
                        "Orden": 32,
                        "Desde": 207,
                        "Hasta": 207,
                        "Long": 1,
                        "Nombre de Campo": "signo_total_otros_debitos",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "32": {
                        "Orden": 33,
                        "Desde": 208,
                        "Hasta": 219,
                        "Long": 12,
                        "Nombre de Campo": "importe_total_otros_debitos",
                        "Tipo": "N",
                        "Contenido": "Importe Total de Otros Debitos (8 enteros, 2 decimales)"
                    },
                    "33": {
                        "Orden": 34,
                        "Desde": 220,
                        "Hasta": 220,
                        "Long": 1,
                        "Nombre de Campo": "signo_total_creditos",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "34": {
                        "Orden": 35,
                        "Desde": 221,
                        "Hasta": 232,
                        "Long": 12,
                        "Nombre de Campo": "importe_total_creditos",
                        "Tipo": "N",
                        "Contenido": "Importe Total de Creditos (8 enteros, 2 decimales)"
                    },
                    "35": {
                        "Orden": 36,
                        "Desde": 233,
                        "Hasta": 233,
                        "Long": 1,
                        "Nombre de Campo": "signo_total_anticipos",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "36": {
                        "Orden": 37,
                        "Desde": 234,
                        "Hasta": 245,
                        "Long": 12,
                        "Nombre de Campo": "importe_total_anticipos",
                        "Tipo": "N",
                        "Contenido": "Importe Total de Anticipos (8 enteros, 2 decimales)"
                    },
                    "37": {
                        "Orden": 38,
                        "Desde": 246,
                        "Hasta": 246,
                        "Long": 1,
                        "Nombre de Campo": "signo_total_cheque_diferido",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "38": {
                        "Orden": 39,
                        "Desde": 247,
                        "Hasta": 258,
                        "Long": 12,
                        "Nombre de Campo": "importe_total_cheque_diferido",
                        "Tipo": "N",
                        "Contenido": "Importe Total de Cheques Diferidos (8 enteros, 2 decimales)"
                    },
                    "39": {
                        "Orden": 40,
                        "Desde": 259,
                        "Hasta": 607,
                        "Long": 349,
                        "Nombre de Campo": "filler",
                        "Tipo": "X",
                        "Contenido": "Espacios en Blanco"
                    },
                    "40": {
                        "Orden": 41,
                        "Desde": 608,
                        "Hasta": 608,
                        "Long": 1,
                        "Nombre de Campo": "marca",
                        "Tipo": "A",
                        "Contenido": "Tipo de Registro (Siempre \"H\")"
                    }
                }
            },
            "F": {
                "start": 46,
                "n_rows": 89,
                "df": [],
                "layout": {
                    "0": {
                        "Orden": 1,
                        "Desde": 1,
                        "Hasta": 9,
                        "Long": 9,
                        "Nombre de Campo": "comercio",
                        "Tipo": "N",
                        "Contenido": "Nro. de comercio"
                    },
                    "1": {
                        "Orden": 2,
                        "Desde": 10,
                        "Hasta": 10,
                        "Long": 1,
                        "Nombre de Campo": "signo_presentaciones_a_pagar_vto",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "2": {
                        "Orden": 3,
                        "Desde": 11,
                        "Hasta": 22,
                        "Long": 12,
                        "Nombre de Campo": "importe_a_pagar_vto",
                        "Tipo": "N",
                        "Contenido": "Importe de Presentaciones a Pagar en Vto."
                    },
                    "3": {
                        "Orden": 4,
                        "Desde": 23,
                        "Hasta": 23,
                        "Long": 1,
                        "Nombre de Campo": "signo_ara_vto",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "4": {
                        "Orden": 5,
                        "Desde": 24,
                        "Hasta": 35,
                        "Long": 12,
                        "Nombre de Campo": "importe_ara_vto",
                        "Tipo": "N",
                        "Contenido": "Importe facturado de arancel a descontar a Vto"
                    },
                    "5": {
                        "Orden": 6,
                        "Desde": 36,
                        "Hasta": 36,
                        "Long": 1,
                        "Nombre de Campo": "signo_int_z_vto",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "6": {
                        "Orden": 7,
                        "Desde": 37,
                        "Hasta": 48,
                        "Long": 12,
                        "Nombre de Campo": "imp_int_z_vto",
                        "Tipo": "N",
                        "Contenido": "Importe facturado de inter\u00e9s por anticipo plan Zeta a Vto"
                    },
                    "7": {
                        "Orden": 8,
                        "Desde": 49,
                        "Hasta": 49,
                        "Long": 1,
                        "Nombre de Campo": "sig_int_plan_esp_vto",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "8": {
                        "Orden": 9,
                        "Desde": 50,
                        "Hasta": 61,
                        "Long": 12,
                        "Nombre de Campo": "imp_int_plan_esp_vto",
                        "Tipo": "N",
                        "Contenido": "Importe facturado de inter\u00e9s anticipo Plan Especial a Vto"
                    },
                    "9": {
                        "Orden": 10,
                        "Desde": 62,
                        "Hasta": 62,
                        "Long": 1,
                        "Nombre de Campo": "sig_acre_liq_ant_vto",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "10": {
                        "Orden": 11,
                        "Desde": 63,
                        "Hasta": 74,
                        "Long": 12,
                        "Nombre de Campo": "imp_acre_liq_ant_vto",
                        "Tipo": "N",
                        "Contenido": "Importe facturado por acreditaci\u00f3n de liquidaci\u00f3n anterior a Vto"
                    },
                    "11": {
                        "Orden": 12,
                        "Desde": 75,
                        "Hasta": 75,
                        "Long": 1,
                        "Nombre de Campo": "sig_iva_21_vto",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "12": {
                        "Orden": 13,
                        "Desde": 76,
                        "Hasta": 87,
                        "Long": 12,
                        "Nombre de Campo": "imp_iva_21_vto",
                        "Tipo": "N",
                        "Contenido": "Importe facturado por IVA Inscripto 21% a descontar en 30 d\u00edas"
                    },
                    "13": {
                        "Orden": 14,
                        "Desde": 88,
                        "Hasta": 88,
                        "Long": 1,
                        "Nombre de Campo": "sig_perc_iva_vto",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "14": {
                        "Orden": 15,
                        "Desde": 89,
                        "Hasta": 100,
                        "Long": 12,
                        "Nombre de Campo": "imp_perc_iva_vto",
                        "Tipo": "N",
                        "Contenido": "Importe facturado por Percepci\u00f3n de IVA a descontar en 30 d\u00edas"
                    },
                    "15": {
                        "Orden": 16,
                        "Desde": 101,
                        "Hasta": 101,
                        "Long": 1,
                        "Nombre de Campo": "sig_perc_iibb_vto",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "16": {
                        "Orden": 17,
                        "Desde": 102,
                        "Hasta": 113,
                        "Long": 12,
                        "Nombre de Campo": "imp_perc_iibb_vto",
                        "Tipo": "N",
                        "Contenido": "Importe facturado por Percepci\u00f3n de IIBB a descontar en 30 d\u00edas"
                    },
                    "17": {
                        "Orden": 18,
                        "Desde": 114,
                        "Hasta": 114,
                        "Long": 1,
                        "Nombre de Campo": "signo_presentaciones_a_pagar_a_30_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "18": {
                        "Orden": 19,
                        "Desde": 115,
                        "Hasta": 126,
                        "Long": 12,
                        "Nombre de Campo": "importe_a_pagar_a_30_dias",
                        "Tipo": "N",
                        "Contenido": "Importe de Presentaciones a Pagar a 30 d\u00edas"
                    },
                    "19": {
                        "Orden": 20,
                        "Desde": 127,
                        "Hasta": 127,
                        "Long": 1,
                        "Nombre de Campo": "signo_ara_dif_30_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "20": {
                        "Orden": 21,
                        "Desde": 128,
                        "Hasta": 139,
                        "Long": 12,
                        "Nombre de Campo": "importe_ara_dif_30_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado de arancel a descontar en 30 d\u00edas  "
                    },
                    "21": {
                        "Orden": 22,
                        "Desde": 140,
                        "Hasta": 140,
                        "Long": 1,
                        "Nombre de Campo": "signo_int_z_dif_30_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "22": {
                        "Orden": 23,
                        "Desde": 141,
                        "Hasta": 152,
                        "Long": 12,
                        "Nombre de Campo": "imp_int_z_dif_30_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado de inter\u00e9s por anticipo plan Zeta a descontar en 30 d\u00edas"
                    },
                    "23": {
                        "Orden": 24,
                        "Desde": 153,
                        "Hasta": 153,
                        "Long": 1,
                        "Nombre de Campo": "sig_int_plan_esp_dif_30_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "24": {
                        "Orden": 25,
                        "Desde": 154,
                        "Hasta": 165,
                        "Long": 12,
                        "Nombre de Campo": "imp_int_plan_esp_dif_30_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado de inter\u00e9s anticipo Plan Especial a descontar en 30 d\u00edas"
                    },
                    "25": {
                        "Orden": 26,
                        "Desde": 166,
                        "Hasta": 166,
                        "Long": 1,
                        "Nombre de Campo": "sig_acre_liq_ant_dif_30_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "26": {
                        "Orden": 27,
                        "Desde": 167,
                        "Hasta": 178,
                        "Long": 12,
                        "Nombre de Campo": "imp_acre_liq_ant_dif_30_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado por acreditaci\u00f3n de liquidaci\u00f3n anterior a descontar en 30 d\u00edas"
                    },
                    "27": {
                        "Orden": 28,
                        "Desde": 179,
                        "Hasta": 179,
                        "Long": 1,
                        "Nombre de Campo": "sig_iva_21_dif_30_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "28": {
                        "Orden": 29,
                        "Desde": 180,
                        "Hasta": 191,
                        "Long": 12,
                        "Nombre de Campo": "imp_iva_21_dif_30_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado por IVA Inscripto 21% a descontar en 30 d\u00edas"
                    },
                    "29": {
                        "Orden": 30,
                        "Desde": 191,
                        "Hasta": 192,
                        "Long": 1,
                        "Nombre de Campo": "sig_perc_iva_dif_30_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "30": {
                        "Orden": 31,
                        "Desde": 193,
                        "Hasta": 204,
                        "Long": 12,
                        "Nombre de Campo": "imp_perc_iva_dif_30_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado por Percepci\u00f3n de IVA a descontar en 30 d\u00edas"
                    },
                    "31": {
                        "Orden": 32,
                        "Desde": 205,
                        "Hasta": 205,
                        "Long": 1,
                        "Nombre de Campo": "sig_perc_iibb_dif_30_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "32": {
                        "Orden": 33,
                        "Desde": 206,
                        "Hasta": 217,
                        "Long": 12,
                        "Nombre de Campo": "imp_perc_iibb_dif_30_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado por Percepci\u00f3n de IIBB a descontar en 30 d\u00edas"
                    },
                    "33": {
                        "Orden": 34,
                        "Desde": 218,
                        "Hasta": 218,
                        "Long": 1,
                        "Nombre de Campo": "signo_presentaciones_a_pagar_a_60_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "34": {
                        "Orden": 35,
                        "Desde": 219,
                        "Hasta": 230,
                        "Long": 12,
                        "Nombre de Campo": "importe_a_pagar_a_60_dias",
                        "Tipo": "N",
                        "Contenido": "Importe de Presentaciones a Pagar a 60 d\u00edas"
                    },
                    "35": {
                        "Orden": 36,
                        "Desde": 231,
                        "Hasta": 231,
                        "Long": 1,
                        "Nombre de Campo": "signo_ara_dif_60_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "36": {
                        "Orden": 37,
                        "Desde": 232,
                        "Hasta": 243,
                        "Long": 12,
                        "Nombre de Campo": "importe_ara_dif_60_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado de arancel Pago Diferido 60 d\u00edas a descontar en pr\u00f3ximas liquidaciones"
                    },
                    "37": {
                        "Orden": 38,
                        "Desde": 244,
                        "Hasta": 244,
                        "Long": 1,
                        "Nombre de Campo": "signo_int_z_dif_60_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "38": {
                        "Orden": 39,
                        "Desde": 245,
                        "Hasta": 256,
                        "Long": 12,
                        "Nombre de Campo": "imp_int_z_dif_60_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado de inter\u00e9s por anticipo plan Zeta a descontar en 60 d\u00edas"
                    },
                    "39": {
                        "Orden": 40,
                        "Desde": 257,
                        "Hasta": 257,
                        "Long": 1,
                        "Nombre de Campo": "sig_int_plan_esp_dif_60_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "40": {
                        "Orden": 41,
                        "Desde": 258,
                        "Hasta": 269,
                        "Long": 12,
                        "Nombre de Campo": "imp_int_plan_esp_dif_60_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado de inter\u00e9s anticipo Plan Especial a descontar en 60 d\u00edas"
                    },
                    "41": {
                        "Orden": 42,
                        "Desde": 270,
                        "Hasta": 270,
                        "Long": 1,
                        "Nombre de Campo": "sig_acre_liq_ant_dif_60_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "42": {
                        "Orden": 43,
                        "Desde": 271,
                        "Hasta": 282,
                        "Long": 12,
                        "Nombre de Campo": "imp_acre_liq_ant_dif_60_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado por acreditaci\u00f3n de liquidaci\u00f3n anterior a descontar en 60 d\u00edas"
                    },
                    "43": {
                        "Orden": 44,
                        "Desde": 283,
                        "Hasta": 283,
                        "Long": 1,
                        "Nombre de Campo": "sig_iva_21_dif_60_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "44": {
                        "Orden": 45,
                        "Desde": 284,
                        "Hasta": 295,
                        "Long": 12,
                        "Nombre de Campo": "imp_iva_21_dif_60_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado por IVA Inscripto 21% a descontar en 60 d\u00edas"
                    },
                    "45": {
                        "Orden": 46,
                        "Desde": 296,
                        "Hasta": 296,
                        "Long": 1,
                        "Nombre de Campo": "sig_perc_iva_dif_60_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "46": {
                        "Orden": 47,
                        "Desde": 297,
                        "Hasta": 308,
                        "Long": 12,
                        "Nombre de Campo": "imp_perc_iva_dif_60_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado por Percepci\u00f3n de IVA a descontar en 60 d\u00edas"
                    },
                    "47": {
                        "Orden": 48,
                        "Desde": 309,
                        "Hasta": 309,
                        "Long": 1,
                        "Nombre de Campo": "sig_perc_iibb_dif_60_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "48": {
                        "Orden": 49,
                        "Desde": 310,
                        "Hasta": 321,
                        "Long": 12,
                        "Nombre de Campo": "imp_perc_iibb_dif_60_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado por Percepci\u00f3n de IIBB a descontar en 60 d\u00edas"
                    },
                    "49": {
                        "Orden": 50,
                        "Desde": 322,
                        "Hasta": 322,
                        "Long": 1,
                        "Nombre de Campo": "signo_presentaciones_a_pagar_a_90_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "50": {
                        "Orden": 51,
                        "Desde": 323,
                        "Hasta": 334,
                        "Long": 12,
                        "Nombre de Campo": "importe_a_pagar_a_90_dias",
                        "Tipo": "N",
                        "Contenido": "Importe de Presentaciones a Pagar a 90 d\u00edas"
                    },
                    "51": {
                        "Orden": 52,
                        "Desde": 335,
                        "Hasta": 335,
                        "Long": 1,
                        "Nombre de Campo": "signo_ara_dif_90_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "52": {
                        "Orden": 53,
                        "Desde": 336,
                        "Hasta": 347,
                        "Long": 12,
                        "Nombre de Campo": "importe_ara_dif_90_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado de arancel Pago Diferido 90 d\u00edas a descontar en pr\u00f3ximas liquidaciones"
                    },
                    "53": {
                        "Orden": 54,
                        "Desde": 348,
                        "Hasta": 348,
                        "Long": 1,
                        "Nombre de Campo": "signo_int_z_dif_90_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "54": {
                        "Orden": 55,
                        "Desde": 349,
                        "Hasta": 360,
                        "Long": 12,
                        "Nombre de Campo": "imp_int_z_dif_90_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado de inter\u00e9s por anticipo plan Zeta a descontar en 90 d\u00edas"
                    },
                    "55": {
                        "Orden": 56,
                        "Desde": 361,
                        "Hasta": 361,
                        "Long": 1,
                        "Nombre de Campo": "sig_int_plan_esp_dif_90_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "56": {
                        "Orden": 57,
                        "Desde": 362,
                        "Hasta": 373,
                        "Long": 12,
                        "Nombre de Campo": "imp_int_plan_esp_dif_90_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado de inter\u00e9s anticipo Plan Especial a descontar en 90 d\u00edas"
                    },
                    "57": {
                        "Orden": 58,
                        "Desde": 374,
                        "Hasta": 374,
                        "Long": 1,
                        "Nombre de Campo": "sig_acre_liq_ant_dif_90_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "58": {
                        "Orden": 59,
                        "Desde": 375,
                        "Hasta": 386,
                        "Long": 12,
                        "Nombre de Campo": "imp_acre_liq_ant_dif_90_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado por acreditaci\u00f3n de liquidaci\u00f3n anterior a descontar en 90 d\u00edas"
                    },
                    "59": {
                        "Orden": 60,
                        "Desde": 386,
                        "Hasta": 387,
                        "Long": 1,
                        "Nombre de Campo": "sig_iva_21_dif_90_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "60": {
                        "Orden": 61,
                        "Desde": 387,
                        "Hasta": 388,
                        "Long": 12,
                        "Nombre de Campo": "imp_iva_21_dif_90_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado por IVA Inscripto 21% a descontar en 90 d\u00edas"
                    },
                    "61": {
                        "Orden": 62,
                        "Desde": 400,
                        "Hasta": 400,
                        "Long": 1,
                        "Nombre de Campo": "sig_perc_iva_dif_90_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "62": {
                        "Orden": 63,
                        "Desde": 401,
                        "Hasta": 412,
                        "Long": 12,
                        "Nombre de Campo": "imp_perc_iva_dif_90_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado por Percepci\u00f3n de IVA a descontar en 90 d\u00edas"
                    },
                    "63": {
                        "Orden": 64,
                        "Desde": 413,
                        "Hasta": 413,
                        "Long": 1,
                        "Nombre de Campo": "sig_perc_iibb_dif_90_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "64": {
                        "Orden": 65,
                        "Desde": 414,
                        "Hasta": 425,
                        "Long": 12,
                        "Nombre de Campo": "imp_perc_iibb_dif_90_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado por Percepci\u00f3n de IIBB a descontar en 90 d\u00edas"
                    },
                    "65": {
                        "Orden": 66,
                        "Desde": 426,
                        "Hasta": 426,
                        "Long": 1,
                        "Nombre de Campo": "signo_presentaciones_a_pagar_a_120_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "66": {
                        "Orden": 67,
                        "Desde": 427,
                        "Hasta": 438,
                        "Long": 12,
                        "Nombre de Campo": "importe_a_pagar_a_120_dias",
                        "Tipo": "N",
                        "Contenido": "Importe de Presentaciones a Pagar a 120 d\u00edas"
                    },
                    "67": {
                        "Orden": 68,
                        "Desde": 439,
                        "Hasta": 439,
                        "Long": 1,
                        "Nombre de Campo": "signo_ara_dif_120_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "68": {
                        "Orden": 69,
                        "Desde": 440,
                        "Hasta": 451,
                        "Long": 12,
                        "Nombre de Campo": "importe_ara_dif_120_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado de arancel Pago Diferido 120 d\u00edas a descontar en pr\u00f3ximas liquidaciones"
                    },
                    "69": {
                        "Orden": 70,
                        "Desde": 452,
                        "Hasta": 452,
                        "Long": 1,
                        "Nombre de Campo": "signo_int_z_dif_120_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "70": {
                        "Orden": 71,
                        "Desde": 453,
                        "Hasta": 464,
                        "Long": 12,
                        "Nombre de Campo": "imp_int_z_dif_120_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado de inter\u00e9s por anticipo plan Zeta a descontar en 120 d\u00edas"
                    },
                    "71": {
                        "Orden": 72,
                        "Desde": 465,
                        "Hasta": 465,
                        "Long": 1,
                        "Nombre de Campo": "sig_int_plan_esp_dif_120_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "72": {
                        "Orden": 73,
                        "Desde": 466,
                        "Hasta": 477,
                        "Long": 12,
                        "Nombre de Campo": "imp_int_plan_esp_dif_120_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado de inter\u00e9s anticipo Plan Especial a descontar en 120 d\u00edas"
                    },
                    "73": {
                        "Orden": 74,
                        "Desde": 478,
                        "Hasta": 478,
                        "Long": 1,
                        "Nombre de Campo": "sig_acre_liq_ant_dif_120_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "74": {
                        "Orden": 75,
                        "Desde": 479,
                        "Hasta": 490,
                        "Long": 12,
                        "Nombre de Campo": "imp_acre_liq_ant_dif_120_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado por acreditaci\u00f3n de liquidaci\u00f3n anterior a descontar en 120 d\u00edas"
                    },
                    "75": {
                        "Orden": 76,
                        "Desde": 490,
                        "Hasta": 491,
                        "Long": 1,
                        "Nombre de Campo": "sig_iva_21_dif_120_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "76": {
                        "Orden": 77,
                        "Desde": 492,
                        "Hasta": 503,
                        "Long": 12,
                        "Nombre de Campo": "imp_iva_21_dif_120_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado por IVA Inscripto 21% a descontar en 120 d\u00edas"
                    },
                    "77": {
                        "Orden": 78,
                        "Desde": 504,
                        "Hasta": 504,
                        "Long": 1,
                        "Nombre de Campo": "sig_perc_iva_dif_120_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "78": {
                        "Orden": 79,
                        "Desde": 505,
                        "Hasta": 516,
                        "Long": 12,
                        "Nombre de Campo": "imp_perc_iva_dif_120_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado por Percepci\u00f3n de IVA a descontar en 120 d\u00edas"
                    },
                    "79": {
                        "Orden": 80,
                        "Desde": 517,
                        "Hasta": 517,
                        "Long": 1,
                        "Nombre de Campo": "sig_perc_iibb_dif_120_dias",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "80": {
                        "Orden": 81,
                        "Desde": 518,
                        "Hasta": 529,
                        "Long": 12,
                        "Nombre de Campo": "imp_perc_iibb_dif_120_dias",
                        "Tipo": "N",
                        "Contenido": "Importe facturado por Percepci\u00f3n de IIBB a descontar en 120 d\u00edas"
                    },
                    "81": {
                        "Orden": 82,
                        "Desde": 530,
                        "Hasta": 530,
                        "Long": 1,
                        "Nombre de Campo": "sig_int_pago_anticipado",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "82": {
                        "Orden": 83,
                        "Desde": 531,
                        "Hasta": 542,
                        "Long": 12,
                        "Nombre de Campo": "interes_pago_anticipado",
                        "Tipo": "N",
                        "Contenido": "Importe facturado por Inter\u00e9s Pago Anticipado (solo para Comercios Amigos con modalidad de pago autom\u00e1tico)"
                    },
                    "83": {
                        "Orden": 84,
                        "Desde": 543,
                        "Hasta": 543,
                        "Long": 1,
                        "Nombre de Campo": "sig_perc_2408_vto",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "84": {
                        "Orden": 85,
                        "Desde": 544,
                        "Hasta": 555,
                        "Long": 12,
                        "Nombre de Campo": "imp_perc_2408_vto",
                        "Tipo": "N",
                        "Contenido": "Importe descontado por Percepci\u00f3n 2408 Vto"
                    },
                    "85": {
                        "Orden": 86,
                        "Desde": 556,
                        "Hasta": 556,
                        "Long": 1,
                        "Nombre de Campo": "sig_perc_2408_dif_30",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "86": {
                        "Orden": 87,
                        "Desde": 557,
                        "Hasta": 568,
                        "Long": 12,
                        "Nombre de Campo": "imp_perc_2408_dif_30",
                        "Tipo": "N",
                        "Contenido": "Importe descontado por Percepci\u00f3n 2408 a descontar en 30 d\u00edas"
                    },
                    "87": {
                        "Orden": 88,
                        "Desde": 569,
                        "Hasta": 569,
                        "Long": 1,
                        "Nombre de Campo": "sig_perc_2408_dif_60",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "88": {
                        "Orden": 89,
                        "Desde": 570,
                        "Hasta": 581,
                        "Long": 12,
                        "Nombre de Campo": "imp_perc_iva_dif_60",
                        "Tipo": "N",
                        "Contenido": "Importe descontado por Percepci\u00f3n 2408 a descontar en 60 d\u00edas"
                    },
                    "89": {
                        "Orden": 90,
                        "Desde": 582,
                        "Hasta": 582,
                        "Long": 1,
                        "Nombre de Campo": "sig_perc_2408_dif_90",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                        },
                    "90": {
                        "Orden": 91,
                        "Desde": 583,
                        "Hasta": 594,
                        "Long": 12,
                        "Nombre de Campo": "imp_perc_2408_dif_90",
                        "Tipo": "N",
                        "Contenido": "Importe descontado por Percepci\u00f3n 2408 a descontar en 90 d\u00edas"
                        },
                    "91": {
                        "Orden": 92,
                        "Desde": 595,
                        "Hasta": 595,
                        "Long": 12,
                        "Nombre de Campo": "sig_perc_2408_dif_120",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"  
                        },    
                    "92": {
                        "Orden": 93,
                        "Desde": 596,
                        "Hasta": 607,
                        "Long": 12,
                        "Nombre de Campo": "imp_perc_2408_dif_120",
                        "Tipo": "N",
                        "Contenido": "Importe descontado por Percepci\u00f3n 2408 a descontar en 120 d\u00edas"
                        },
                    "93": {
                        "Orden": 94,
                        "Desde": 608,
                        "Hasta": 608,
                        "Long": 1,
                        "Nombre de Campo": "filler",
                        "Tipo": "G",
                        "Contenido": "Tipo de Registro (Siempre \"F\")"
                        },                                                                                 
                    }
                },
            "D": {
                "start": 145,
                "n_rows": 26,
                "df": [],
                "layout": {
                    "0": {
                        "Orden": 1,
                        "Desde": 1,
                        "Hasta": 9,
                        "Long": 9,
                        "Nombre de Campo": "comercio",
                        "Tipo": "N",
                        "Contenido": "Nro. de comercio"
                    },
                    "1": {
                        "Orden": 2,
                        "Desde": 10,
                        "Hasta": 15,
                        "Long": 6,
                        "Nombre de Campo": "fecha_presentacion",
                        "Tipo": "N",
                        "Contenido": "Fecha de Presentaci\u00f3n de Recap (AAMMDD)"
                    },
                    "2": {
                        "Orden": 3,
                        "Desde": 16,
                        "Hasta": 21,
                        "Long": 6,
                        "Nombre de Campo": "numero_recap",
                        "Tipo": "N",
                        "Contenido": "N\u00famero de Recap"
                    },
                    "3": {
                        "Orden": 4,
                        "Desde": 22,
                        "Hasta": 27,
                        "Long": 6,
                        "Nombre de Campo": "cupon",
                        "Tipo": "N",
                        "Contenido": "Nro. de Cup\u00f3n"
                    },
                    "4": {
                        "Orden": 5,
                        "Desde": 28,
                        "Hasta": 43,
                        "Long": 16,
                        "Nombre de Campo": "tarjeta",
                        "Tipo": "N",
                        "Contenido": "Nro. de Tarjeta o Pl\u00e1stico"
                    },
                    "5": {
                        "Orden": 6,
                        "Desde": 44,
                        "Hasta": 49,
                        "Long": 6,
                        "Nombre de Campo": "fecha_compra",
                        "Tipo": "N",
                        "Contenido": "Fecha de Compra (AAMMDD)"
                    },
                    "6": {
                        "Orden": 7,
                        "Desde": 50,
                        "Hasta": 50,
                        "Long": 1,
                        "Nombre de Campo": "moneda",
                        "Tipo": "N",
                        "Contenido": "Moneda de la compra: 0=Pesos;1=D\u00f3lar; 2=Bonos; 3=Zeta"
                    },
                    "7": {
                        "Orden": 8,
                        "Desde": 51,
                        "Hasta": 52,
                        "Long": 2,
                        "Nombre de Campo": "plan",
                        "Tipo": "N",
                        "Contenido": "N\u00famero de Plan"
                    },
                    "8": {
                        "Orden": 9,
                        "Desde": 53,
                        "Hasta": 64,
                        "Long": 12,
                        "Nombre de Campo": "compra",
                        "Tipo": "N",
                        "Contenido": "Importe Total de Compra (9 enteros, 2 decimales)"
                    },
                    "9": {
                        "Orden": 10,
                        "Desde": 65,
                        "Hasta": 76,
                        "Long": 12,
                        "Nombre de Campo": "entrega",
                        "Tipo": "N",
                        "Contenido": "Importe Total de Entrega (9 enteros, 2 decimales)"
                    },
                    "10": {
                        "Orden": 11,
                        "Desde": 77,
                        "Hasta": 82,
                        "Long": 6,
                        "Nombre de Campo": "fecha_cuota",
                        "Tipo": "N",
                        "Contenido": "Fecha de Cobro de 1er. Cuota (AAMMDD)"
                    },
                    "11": {
                        "Orden": 12,
                        "Desde": 83,
                        "Hasta": 94,
                        "Long": 12,
                        "Nombre de Campo": "importe_cuota",
                        "Tipo": "N",
                        "Contenido": "Importe de Cuota (9 enteros, 2 decimales)"
                    },
                    "12": {
                        "Orden": 13,
                        "Desde": 95,
                        "Hasta": 96,
                        "Long": 2,
                        "Nombre de Campo": "numero_cuota",
                        "Tipo": "N",
                        "Contenido": "N\u00famero de Cuota"
                    },
                    "13": {
                        "Orden": 14,
                        "Desde": 97,
                        "Hasta": 97,
                        "Long": 1,
                        "Nombre de Campo": "tipo_mov",
                        "Tipo": "A",
                        "Contenido": "Tipo de Movimiento: \" \"=Informativo;\"D\"=Suma Cta.Cte.comercio;\"H\"=Resta Cta.Cte.comercio"
                    },
                    "14": {
                        "Orden": 15,
                        "Desde": 98,
                        "Hasta": 98,
                        "Long": 1,
                        "Nombre de Campo": "estado",
                        "Tipo": "A",
                        "Contenido": "Estado de Movimiento: \" \" =Cup\u00f3n OK; \"R\"=Cup\u00f3n Rechaz; \"X\"=D\u00e9b/Cr\u00e9d. en Cta.Cte. comercio"
                    },
                    "15": {
                        "Orden": 16,
                        "Desde": 99,
                        "Hasta": 128,
                        "Long": 30,
                        "Nombre de Campo": "descripcion",
                        "Tipo": "A",
                        "Contenido": "Descripci\u00f3n del Estado"
                    },
                    "16": {
                        "Orden": 17,
                        "Desde": 129,
                        "Hasta": 134,
                        "Long": 6,
                        "Nombre de Campo": "codigo_aut",
                        "Tipo": "N",
                        "Contenido": "C\u00f3digo de Autorizaci\u00f3n"
                    },
                    "17": {
                        "Orden": 18,
                        "Desde": 135,
                        "Hasta": 135,
                        "Long": 1,
                        "Nombre de Campo": "tipo_op",
                        "Tipo": "A",
                        "Contenido": "Tipo de Operaci\u00f3n:\"O\"=Autom\u00e1tico; \"M\"=Manual"
                    },
                    "18": {
                        "Orden": 19,
                        "Desde": 136,
                        "Hasta": 141,
                        "Long": 6,
                        "Nombre de Campo": "numero_devolucion",
                        "Tipo": "N",
                        "Contenido": "N\u00famero de devoluci\u00f3n de cupones - Si esta en cero no es una devoluci\u00f3n."
                    },
                    "19": {
                        "Orden": 20,
                        "Desde": 142,
                        "Hasta": 142,
                        "Long": 1,
                        "Nombre de Campo": "tipo_cd",
                        "Tipo": "A",
                        "Contenido": "Tipo Devoluci\u00f3n/Contracargo: \"C\"=Contracargo, \"D\"=Devoluci\u00f3n,"
                    },
                    "20": {
                        "Orden": 21,
                        "Desde": 143,
                        "Hasta": 150,
                        "Long": 8,
                        "Nombre de Campo": "numero_terminal",
                        "Tipo": "N",
                        "Contenido": "N\u00famero de Terminal"
                    },
                    "21": {
                        "Orden": 22,
                        "Desde": 151,
                        "Hasta": 154,
                        "Long": 4,
                        "Nombre de Campo": "numero_lote",
                        "Tipo": "N",
                        "Contenido": "N\u00famero de Lote"
                    },
                    "22": {
                        "Orden": 23,
                        "Desde": 155,
                        "Hasta": 160,
                        "Long": 6,
                        "Nombre de Campo": "codigo_especial",
                        "Tipo": "N",
                        "Contenido": "C\u00f3digo Especial para uso del comercio que identifica el movimiento contable"
                    },
                    "23": {
                        "Orden": 24,
                        "Desde": 161,
                        "Hasta": 190,
                        "Long": 30,
                        "Nombre de Campo": "numero_debito",
                        "Tipo": "A",
                        "Contenido": "Numero de Referencia del D\u00e9bito Autom\u00e1tico"
                    },
                    "24": {
                        "Orden": 25,
                        "Desde": 191,
                        "Hasta": 607,
                        "Long": 417,
                        "Nombre de Campo": "filler",
                        "Tipo": "A",
                        "Contenido": "Espacios en Blanco"
                    },
                    "25": {
                        "Orden": 26,
                        "Desde": 608,
                        "Hasta": 608,
                        "Long": 1,
                        "Nombre de Campo": "marca",
                        "Tipo": "A",
                        "Contenido": "Tipo de Registro (Siempre \"D\")"
                    }
                }
            },
            "T": {
                "start": 175,
                "n_rows": 13,
                "df": [],
                "layout": {
                    "0": {
                        "Orden": 1,
                        "Desde": 1,
                        "Hasta": 9,
                        "Long": 9,
                        "Nombre de Campo": "comercio",
                        "Tipo": "N",
                        "Contenido": "Nro. de comercio"
                    },
                    "1": {
                        "Orden": 2,
                        "Desde": 10,
                        "Hasta": 15,
                        "Long": 6,
                        "Nombre de Campo": "fecha_proceso",
                        "Tipo": "N",
                        "Contenido": "Fecha de Procesamiento de datos (AAMMDD)"
                    },
                    "2": {
                        "Orden": 3,
                        "Desde": 16,
                        "Hasta": 26,
                        "Long": 11,
                        "Nombre de Campo": "cantidad_movimientos",
                        "Tipo": "N",
                        "Contenido": "Total de Movimientos Procesados"
                    },
                    "3": {
                        "Orden": 4,
                        "Desde": 27,
                        "Hasta": 37,
                        "Long": 11,
                        "Nombre de Campo": "cantidad_aceptados",
                        "Tipo": "N",
                        "Contenido": "Total de Movimientos con Estado \" \" y \"X\""
                    },
                    "4": {
                        "Orden": 5,
                        "Desde": 38,
                        "Hasta": 48,
                        "Long": 11,
                        "Nombre de Campo": "cantidad_rechazados",
                        "Tipo": "N",
                        "Contenido": "Total de Movimientos con Estado \"R\""
                    },
                    "5": {
                        "Orden": 6,
                        "Desde": 49,
                        "Hasta": 49,
                        "Long": 1,
                        "Nombre de Campo": "signo_compra",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "6": {
                        "Orden": 7,
                        "Desde": 50,
                        "Hasta": 61,
                        "Long": 12,
                        "Nombre de Campo": "total_compra",
                        "Tipo": "N",
                        "Contenido": "Total de Compras (9 enteros, 2 decimales)"
                    },
                    "7": {
                        "Orden": 8,
                        "Desde": 62,
                        "Hasta": 62,
                        "Long": 1,
                        "Nombre de Campo": "signo_entrega",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "8": {
                        "Orden": 9,
                        "Desde": 63,
                        "Hasta": 74,
                        "Long": 12,
                        "Nombre de Campo": "total_entrega",
                        "Tipo": "N",
                        "Contenido": "Total de Entregas (9 enteros, 2 decimales)"
                    },
                    "9": {
                        "Orden": 10,
                        "Desde": 75,
                        "Hasta": 75,
                        "Long": 1,
                        "Nombre de Campo": "signo_cuota",
                        "Tipo": "A",
                        "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                    },
                    "10": {
                        "Orden": 11,
                        "Desde": 76,
                        "Hasta": 87,
                        "Long": 12,
                        "Nombre de Campo": "total_cuota",
                        "Tipo": "N",
                        "Contenido": "Total de Cuotas (9 enteros, 2 decimales)"
                    },
                    "11": {
                        "Orden": 12,
                        "Desde": 88,
                        "Hasta": 607,
                        "Long": 520,
                        "Nombre de Campo": "filler",
                        "Tipo": "A",
                        "Contenido": "Espacios en Blanco"
                    },
                    "12": {
                        "Orden": 13,
                        "Desde": 608,
                        "Hasta": 608,
                        "Long": 1,
                        "Nombre de Campo": "marca",
                        "Tipo": "A",
                        "Contenido": "Tipo de Registro (Siempre \"T\")"
                    }
                }
            }
            }
        def parse_line(line, layout):
            dict_line = {}
            for key, val in layout.items():
                dict_line[val["Nombre de Campo"]] = line[val['Desde'] - 1: val['Desde'] - 1 + val['Long']]
            return dict_line

        layouts =  {
                        'H' : {'start' : 0 , 'n_rows': 41,  'df': []},
                        'F' : {'start' : 57 - 11 , 'n_rows': 89, 'df': []},
                        'D' : {'start' : 156 - 11 , 'n_rows': 26,  'df': []},
                        'T' : {'start' : 186 - 11, 'n_rows': 13,  'df': []},
                    }
        loaded = diccionario
        for key in layouts.keys():
            layouts[key] = loaded[key]


        f = file

        for line in f:
            line = line.strip()
            marca = line[-1]
            layouts[marca]['df'].append(parse_line(line, layouts[marca]['layout']))

        df_dicts ={}  
        for key, val in layouts.items():

            columns = [val['layout'][i]['Nombre de Campo'] for i in sorted([k for k in val['layout'].keys() ])]
            layouts[key]['df'] = pd.DataFrame(layouts[key]['df'], columns = columns)
            try:
                layouts[key]['df']['head_nro_liquidacion'] =  layouts['H']['df']['numero_liquidacion'][0]
            except:
                layouts[key]['df']['head_nro_liquidacion'] =  float('nan')
            df_dicts[key] = layouts[key]['df']
        
        tipo_tabla = kwargs['tipo_tabla']
        if df_dicts[tipo_tabla].empty:
            df_dicts[tipo_tabla] = df_dicts[tipo_tabla].append(pd.Series(), ignore_index=True)
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        df_dicts[tipo_tabla]['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        df_dicts[tipo_tabla]['file_name'] = filename.split('/')[-1]
        df_dicts[tipo_tabla].reset_index(drop=True)
        df_dicts[tipo_tabla]['skt_extraction_rn'] = df_dicts[tipo_tabla].index.values
        return df_dicts[tipo_tabla]



class ExtractorCme:
    @staticmethod
    def run(filename, **kwargs ):
        file = FileReader.read(filename)       
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        diccionario = {
                    "H": {
                        "start": 0,
                        "n_rows": 41,
                        "df": [],
                        "layout": {
                            "0": {
                                "Orden": 1,
                                "Desde": 1,
                                "Hasta": 9,
                                "Long": 9,
                                "Nombre de Campo": "comercio",
                                "Tipo": "N",
                                "Contenido": "Nro. de Comercio"
                            },
                            "1": {
                                "Orden": 2,
                                "Desde": 10,
                                "Hasta": 15,
                                "Long": 6,
                                "Nombre de Campo": "fecha_proceso",
                                "Tipo": "N",
                                "Contenido": "Fecha de Procesamiento de datos (AAMMDD)"
                            },
                            "2": {
                                "Orden": 3,
                                "Desde": 16,
                                "Hasta": 21,
                                "Long": 6,
                                "Nombre de Campo": "fecha_pago",
                                "Tipo": "N",
                                "Contenido": "Fecha de Liquidacion (AAMMDD)"
                            },
                            "3": {
                                "Orden": 4,
                                "Desde": 22,
                                "Hasta": 27,
                                "Long": 6,
                                "Nombre de Campo": "nro_liquidacion",
                                "Tipo": "N",
                                "Contenido": "Nro. de la liquidaci\u00f3n"
                            },
                            "4": {
                                "Orden": 5,
                                "Desde": 28,
                                "Hasta": 38,
                                "Long": 11,
                                "Nombre de Campo": "cuit_ag_retencion",
                                "Tipo": "N",
                                "Contenido": "Cuit del Agente de Retenci\u00f3n de Ingresos Brutos (11 enteros)"
                            },
                            "5": {
                                "Orden": 6,
                                "Desde": 39,
                                "Hasta": 39,
                                "Long": 1,
                                "Nombre de Campo": "signo_total_descuentos",
                                "Tipo": "A",
                                "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                            },
                            "6": {
                                "Orden": 7,
                                "Desde": 40,
                                "Hasta": 51,
                                "Long": 12,
                                "Nombre de Campo": "total_descuentos",
                                "Tipo": "N",
                                "Contenido": "Importe de descuentos que pertencen a facturaciones anteriores"
                            },
                            "7": {
                                "Orden": 8,
                                "Desde": 52,
                                "Hasta": 52,
                                "Long": 1,
                                "Nombre de Campo": "signo_ret_iva_140",
                                "Tipo": "A",
                                "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                            },
                            "8": {
                                "Orden": 9,
                                "Desde": 53,
                                "Hasta": 64,
                                "Long": 12,
                                "Nombre de Campo": "retencion_iva_140",
                                "Tipo": "N",
                                "Contenido": "Importe de la Retencion IVA RG(Afip)140 (8 enteros, 2 decimales)"
                            },
                            "9": {
                                "Orden": 10,
                                "Desde": 65,
                                "Hasta": 65,
                                "Long": 1,
                                "Nombre de Campo": "signo_ret_ganancias",
                                "Tipo": "A",
                                "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                            },
                            "10": {
                                "Orden": 11,
                                "Desde": 66,
                                "Hasta": 77,
                                "Long": 12,
                                "Nombre de Campo": "retencion_ganancias",
                                "Tipo": "N",
                                "Contenido": "Importe de la Retencion Ganancias RG 3311(8 enteros, 2 decimales)"
                            },
                            "11": {
                                "Orden": 12,
                                "Desde": 78,
                                "Hasta": 78,
                                "Long": 1,
                                "Nombre de Campo": "signo_ret_iva_3130",
                                "Tipo": "A",
                                "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                            },
                            "12": {
                                "Orden": 13,
                                "Desde": 79,
                                "Hasta": 90,
                                "Long": 12,
                                "Nombre de Campo": "retencion_3130",
                                "Tipo": "N",
                                "Contenido": "Importe de la Retencion IVA 3130 (8 enteros, 2 decimales)"
                            },
                            "13": {
                                "Orden": 14,
                                "Desde": 91,
                                "Hasta": 91,
                                "Long": 1,
                                "Nombre de Campo": "signo_ret_ing_bru",
                                "Tipo": "A",
                                "Contenido": "\"+\" Signo positivo / \"-\" Signo netaivo"
                            },
                            "14": {
                                "Orden": 15,
                                "Desde": 92,
                                "Hasta": 103,
                                "Long": 12,
                                "Nombre de Campo": "base_imponible_ing_bru",
                                "Tipo": "N",
                                "Contenido": "Importe de la Base Imponible de Ing. Brutos (8 enteros, 2 decimales)"
                            },
                            "15": {
                                "Orden": 16,
                                "Desde": 104,
                                "Hasta": 110,
                                "Long": 7,
                                "Nombre de Campo": "alicuota_ing_brutos",
                                "Tipo": "N",
                                "Contenido": "Alicuota para calcular Ing. Brutos (5 enteros, 2 decimales)"
                            },
                            "16": {
                                "Orden": 17,
                                "Desde": 111,
                                "Hasta": 111,
                                "Long": 1,
                                "Nombre de Campo": "signo_ret_ing_brutos",
                                "Tipo": "A",
                                "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                            },
                            "17": {
                                "Orden": 18,
                                "Desde": 112,
                                "Hasta": 123,
                                "Long": 12,
                                "Nombre de Campo": "ret_ingresos_brutos",
                                "Tipo": "N",
                                "Contenido": "Importe Retenci\u00f3n de Ingresos Brutos (8 enteros, 2 decimales)"
                            },
                            "18": {
                                "Orden": 19,
                                "Desde": 124,
                                "Hasta": 124,
                                "Long": 1,
                                "Nombre de Campo": "signo_retencion_municipal",
                                "Tipo": "A",
                                "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                            },
                            "19": {
                                "Orden": 20,
                                "Desde": 125,
                                "Hasta": 136,
                                "Long": 12,
                                "Nombre de Campo": "retencion_municipal",
                                "Tipo": "N",
                                "Contenido": "Importe de la Retenci\u00f3n Municipal (8 enteros, 2 decimales)"
                            },
                            "20": {
                                "Orden": 21,
                                "Desde": 137,
                                "Hasta": 137,
                                "Long": 1,
                                "Nombre de Campo": "signo_dbtos_cdtos",
                                "Tipo": "A",
                                "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                            },
                            "21": {
                                "Orden": 22,
                                "Desde": 138,
                                "Hasta": 149,
                                "Long": 12,
                                "Nombre de Campo": "debitos_creditos",
                                "Tipo": "N",
                                "Contenido": "Importe de otros Debitos / Cr\u00e9ditos (8 enteros, 2 decimales)"
                            },
                            "22": {
                                "Orden": 23,
                                "Desde": 150,
                                "Hasta": 150,
                                "Long": 1,
                                "Nombre de Campo": "signo_percepcion_1135",
                                "Tipo": "A",
                                "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                            },
                            "23": {
                                "Orden": 24,
                                "Desde": 151,
                                "Hasta": 162,
                                "Long": 12,
                                "Nombre de Campo": "percepcion_1135",
                                "Tipo": "N",
                                "Contenido": "Monto Percepci\u00f3n Imp. D\u00e9bitos y Cr\u00e9ditos. RG. 1135(8 enteros, 2 decimales)"
                            },
                            "24": {
                                "Orden": 25,
                                "Desde": 163,
                                "Hasta": 163,
                                "Long": 1,
                                "Nombre de Campo": "signo_liq_negativa_ant",
                                "Tipo": "A",
                                "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                            },
                            "25": {
                                "Orden": 26,
                                "Desde": 164,
                                "Hasta": 175,
                                "Long": 12,
                                "Nombre de Campo": "liq_negativa_dia_ant",
                                "Tipo": "N",
                                "Contenido": "Monto Liquidaci\u00f3n Negativa d\u00eda anterior(8 enteros, 2 decimales)"
                            },
                            "26": {
                                "Orden": 27,
                                "Desde": 176,
                                "Hasta": 176,
                                "Long": 1,
                                "Nombre de Campo": "signo_embargo_cesiones",
                                "Tipo": "A",
                                "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                            },
                            "27": {
                                "Orden": 28,
                                "Desde": 177,
                                "Hasta": 188,
                                "Long": 12,
                                "Nombre de Campo": "embargo_cesiones",
                                "Tipo": "N",
                                "Contenido": "Importe de Embargos y Cesiones (8 enteros, 2 decimales)"
                            },
                            "28": {
                                "Orden": 29,
                                "Desde": 189,
                                "Hasta": 189,
                                "Long": 1,
                                "Nombre de Campo": "signo_neto",
                                "Tipo": "A",
                                "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                            },
                            "29": {
                                "Orden": 30,
                                "Desde": 190,
                                "Hasta": 201,
                                "Long": 12,
                                "Nombre de Campo": "neto",
                                "Tipo": "N",
                                "Contenido": "Monto Neto a liquidar (8 enteros, 2 decimales)"
                            },
                            "30": {
                                "Orden": 31,
                                "Desde": 202,
                                "Hasta": 206,
                                "Long": 5,
                                "Nombre de Campo": "rubro",
                                "Tipo": "N",
                                "Contenido": "Rubro comercial"
                            },
                            "31": {
                                "Orden": 32,
                                "Desde": 207,
                                "Hasta": 207,
                                "Long": 1,
                                "Nombre de Campo": "signo_total_otros_debitos",
                                "Tipo": "A",
                                "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                            },
                            "32": {
                                "Orden": 33,
                                "Desde": 208,
                                "Hasta": 219,
                                "Long": 12,
                                "Nombre de Campo": "importe_total_otros_debitos",
                                "Tipo": "N",
                                "Contenido": "Importe Total de Otros Debitos (8 enteros, 2 decimales)"
                            },
                            "33": {
                                "Orden": 34,
                                "Desde": 220,
                                "Hasta": 220,
                                "Long": 1,
                                "Nombre de Campo": "signo_total_creditos",
                                "Tipo": "A",
                                "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                            },
                            "34": {
                                "Orden": 35,
                                "Desde": 221,
                                "Hasta": 232,
                                "Long": 12,
                                "Nombre de Campo": "importe_total_creditos",
                                "Tipo": "N",
                                "Contenido": "Importe Total de Creditos (8 enteros, 2 decimales)"
                            },
                            "35": {
                                "Orden": 36,
                                "Desde": 233,
                                "Hasta": 233,
                                "Long": 1,
                                "Nombre de Campo": "signo_total_anticipos",
                                "Tipo": "A",
                                "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                            },
                            "36": {
                                "Orden": 37,
                                "Desde": 234,
                                "Hasta": 245,
                                "Long": 12,
                                "Nombre de Campo": "importe_total_anticipos",
                                "Tipo": "N",
                                "Contenido": "Importe Total de Anticipos (8 enteros, 2 decimales)"
                            },
                            "37": {
                                "Orden": 38,
                                "Desde": 246,
                                "Hasta": 246,
                                "Long": 1,
                                "Nombre de Campo": "signo_total_cheque_diferido",
                                "Tipo": "A",
                                "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                            },
                            "38": {
                                "Orden": 39,
                                "Desde": 247,
                                "Hasta": 258,
                                "Long": 12,
                                "Nombre de Campo": "importe_total_cheque_diferido",
                                "Tipo": "N",
                                "Contenido": "Importe Total de Cheques Diferidos (8 enteros, 2 decimales)"
                            },
                            "39": {
                                "Orden": 40,
                                "Desde": 259,
                                "Hasta": 607,
                                "Long": 349,
                                "Nombre de Campo": "filler",
                                "Tipo": "X",
                                "Contenido": "Espacios en Blanco"
                            },
                            "40": {
                                "Orden": 41,
                                "Desde": 608,
                                "Hasta": 608,
                                "Long": 1,
                                "Nombre de Campo": "marca",
                                "Tipo": "A",
                                "Contenido": "Tipo de Registro (Siempre \"H\")"
                            }
                        }
                    },
                    "P": {
                        "start": 45,
                        "n_rows": 83,
                        "df": [],
                        "layout": {
                            "0": {
                                "Orden": 1,
                                "Desde": 1,
                                "Hasta": 9,
                                "Long": 9,
                                "Nombre de Campo": "comercio",
                                "Tipo": "N",
                                "Contenido": "Nro. de Comercio"
                            },
                            "1": {
                                "Orden": 2,
                                "Desde": 10,
                                "Hasta": 10,
                                "Long": 1,
                                "Nombre de Campo": "signo_ara_vto",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "2": {
                                "Orden": 3,
                                "Desde": 11,
                                "Hasta": 22,
                                "Long": 12,
                                "Nombre de Campo": "importe_ara_vto",
                                "Tipo": "N",
                                "Contenido": "Importe descontado de arancel facturado en Vto"
                            },
                            "3": {
                                "Orden": 4,
                                "Desde": 23,
                                "Hasta": 23,
                                "Long": 1,
                                "Nombre de Campo": "signo_int_z_vto",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "4": {
                                "Orden": 5,
                                "Desde": 24,
                                "Hasta": 35,
                                "Long": 12,
                                "Nombre de Campo": "imp_int_z_vto",
                                "Tipo": "N",
                                "Contenido": "Importe descontado de inter\u00e9s por anticipo plan Zeta facturado en Vto"
                            },
                            "5": {
                                "Orden": 6,
                                "Desde": 36,
                                "Hasta": 36,
                                "Long": 1,
                                "Nombre de Campo": "sig_int_plan_esp_vto",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "6": {
                                "Orden": 7,
                                "Desde": 37,
                                "Hasta": 48,
                                "Long": 12,
                                "Nombre de Campo": "imp_int_plan_esp_vto",
                                "Tipo": "N",
                                "Contenido": "Importe descontado de inter\u00e9s anticipo Plan Especial facturado en Vto"
                            },
                            "7": {
                                "Orden": 8,
                                "Desde": 49,
                                "Hasta": 49,
                                "Long": 1,
                                "Nombre de Campo": "sig_acre_liq_ant_vto",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "8": {
                                "Orden": 9,
                                "Desde": 50,
                                "Hasta": 61,
                                "Long": 12,
                                "Nombre de Campo": "imp_acre_liq_ant_vto",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por acreditacion de liquidacion anterior facturado en Vto"
                            },
                            "9": {
                                "Orden": 10,
                                "Desde": 62,
                                "Hasta": 62,
                                "Long": 1,
                                "Nombre de Campo": "sig_iva_21_vto",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "10": {
                                "Orden": 11,
                                "Desde": 63,
                                "Hasta": 74,
                                "Long": 12,
                                "Nombre de Campo": "imp_iva_21_vto",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por IVA Inscripto 21% facturado en Vto"
                            },
                            "11": {
                                "Orden": 12,
                                "Desde": 75,
                                "Hasta": 75,
                                "Long": 1,
                                "Nombre de Campo": "sig_perc_iva_vto",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "12": {
                                "Orden": 13,
                                "Desde": 76,
                                "Hasta": 87,
                                "Long": 12,
                                "Nombre de Campo": "imp_perc_iva_vto",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por Percepcion de IVA facturado en Vto"
                            },
                            "13": {
                                "Orden": 14,
                                "Desde": 88,
                                "Hasta": 88,
                                "Long": 1,
                                "Nombre de Campo": "sig_perc_iibb_vto",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "14": {
                                "Orden": 15,
                                "Desde": 89,
                                "Hasta": 100,
                                "Long": 12,
                                "Nombre de Campo": "imp_perc_iibb_vto",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por Percepcion de IIBB facturado en Vto"
                            },
                            "15": {
                                "Orden": 16,
                                "Desde": 101,
                                "Hasta": 101,
                                "Long": 1,
                                "Nombre de Campo": "signo_ara_facturado_30",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "16": {
                                "Orden": 17,
                                "Desde": 102,
                                "Hasta": 113,
                                "Long": 12,
                                "Nombre de Campo": "importe_ara_facturado_30",
                                "Tipo": "N",
                                "Contenido": "Importe descontado de arancel facturado hace 30 d\u00edas"
                            },
                            "17": {
                                "Orden": 18,
                                "Desde": 114,
                                "Hasta": 114,
                                "Long": 1,
                                "Nombre de Campo": "signo_int_z_facturado_30",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "18": {
                                "Orden": 19,
                                "Desde": 115,
                                "Hasta": 126,
                                "Long": 12,
                                "Nombre de Campo": "imp_int_z_facturado_30",
                                "Tipo": "N",
                                "Contenido": "Importe descontado de inter\u00e9s por anticipo plan Zeta facturado hace 30 d\u00edas"
                            },
                            "19": {
                                "Orden": 20,
                                "Desde": 127,
                                "Hasta": 127,
                                "Long": 1,
                                "Nombre de Campo": "sig_int_plan_esp_facturado_30",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "20": {
                                "Orden": 21,
                                "Desde": 128,
                                "Hasta": 139,
                                "Long": 12,
                                "Nombre de Campo": "imp_int_plan_esp_facturado_30",
                                "Tipo": "N",
                                "Contenido": "Importe descontado de inter\u00e9s anticipo Plan Especial facturado hace 30 d\u00edas"
                            },
                            "21": {
                                "Orden": 22,
                                "Desde": 140,
                                "Hasta": 140,
                                "Long": 1,
                                "Nombre de Campo": "sig_acre_liq_ant_facturado_30",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "22": {
                                "Orden": 23,
                                "Desde": 141,
                                "Hasta": 152,
                                "Long": 12,
                                "Nombre de Campo": "imp_acre_liq_ant_facturado_30",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por acreditacion de liquidacion anterior facturado hace 30 d\u00edas"
                            },
                            "23": {
                                "Orden": 24,
                                "Desde": 153,
                                "Hasta": 153,
                                "Long": 1,
                                "Nombre de Campo": "sig_iva_21_facturado_30",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "24": {
                                "Orden": 25,
                                "Desde": 154,
                                "Hasta": 165,
                                "Long": 12,
                                "Nombre de Campo": "imp_iva_21_facturado_30",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por IVA Inscripto 21% facturado hace 30 d\u00edas"
                            },
                            "25": {
                                "Orden": 26,
                                "Desde": 166,
                                "Hasta": 166,
                                "Long": 1,
                                "Nombre de Campo": "sig_perc_iva_facturado_30",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "26": {
                                "Orden": 27,
                                "Desde": 167,
                                "Hasta": 178,
                                "Long": 12,
                                "Nombre de Campo": "imp_perc_iva_facturado_30",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por Percepcion de IVA facturado hace 30 d\u00edas"
                            },
                            "27": {
                                "Orden": 28,
                                "Desde": 179,
                                "Hasta": 179,
                                "Long": 1,
                                "Nombre de Campo": "sig_perc_iibb_facturado_30",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "28": {
                                "Orden": 29,
                                "Desde": 180,
                                "Hasta": 191,
                                "Long": 12,
                                "Nombre de Campo": "imp_perc_iibb_facturado_30",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por Percepcion de IIBB facturado hace 30 d\u00edas"
                            },
                            "29": {
                                "Orden": 30,
                                "Desde": 192,
                                "Hasta": 192,
                                "Long": 1,
                                "Nombre de Campo": "signo_ara_facturado_60",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "30": {
                                "Orden": 31,
                                "Desde": 193,
                                "Hasta": 204,
                                "Long": 12,
                                "Nombre de Campo": "importe_ara_facturado_60",
                                "Tipo": "N",
                                "Contenido": "Importe descontado de arancel facturado hace 60 d\u00edas"
                            },
                            "31": {
                                "Orden": 32,
                                "Desde": 205,
                                "Hasta": 205,
                                "Long": 1,
                                "Nombre de Campo": "signo_int_z_facturado_60",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "32": {
                                "Orden": 33,
                                "Desde": 206,
                                "Hasta": 217,
                                "Long": 12,
                                "Nombre de Campo": "imp_int_z_facturado_60",
                                "Tipo": "N",
                                "Contenido": "Importe descontado de inter\u00e9s por anticipo plan Zeta facturado hace 60 d\u00edas"
                            },
                            "33": {
                                "Orden": 34,
                                "Desde": 218,
                                "Hasta": 218,
                                "Long": 1,
                                "Nombre de Campo": "sig_int_plan_esp_facturado_60",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "34": {
                                "Orden": 35,
                                "Desde": 219,
                                "Hasta": 230,
                                "Long": 12,
                                "Nombre de Campo": "imp_int_plan_esp_facturado_60",
                                "Tipo": "N",
                                "Contenido": "Importe descontado de inter\u00e9s anticipo Plan Especial facturado hace 60 d\u00edas"
                            },
                            "35": {
                                "Orden": 36,
                                "Desde": 231,
                                "Hasta": 231,
                                "Long": 1,
                                "Nombre de Campo": "sig_acre_liq_ant_facturado_60",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "36": {
                                "Orden": 37,
                                "Desde": 232,
                                "Hasta": 243,
                                "Long": 12,
                                "Nombre de Campo": "imp_acre_liq_ant_facturado_60",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por acreditacion de liquidacion anterior facturado hace 60 d\u00edas"
                            },
                            "37": {
                                "Orden": 38,
                                "Desde": 244,
                                "Hasta": 244,
                                "Long": 1,
                                "Nombre de Campo": "sig_iva_21_facturado_60",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "38": {
                                "Orden": 39,
                                "Desde": 245,
                                "Hasta": 256,
                                "Long": 12,
                                "Nombre de Campo": "imp_iva_21_facturado_60",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por IVA Inscripto 21% facturado hace 60 d\u00edas"
                            },
                            "39": {
                                "Orden": 40,
                                "Desde": 257,
                                "Hasta": 257,
                                "Long": 1,
                                "Nombre de Campo": "sig_perc_iva_facturado_60",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "40": {
                                "Orden": 41,
                                "Desde": 258,
                                "Hasta": 269,
                                "Long": 12,
                                "Nombre de Campo": "imp_perc_iva_facturado_60",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por Percepcion de IVA facturado hace 60 d\u00edas"
                            },
                            "41": {
                                "Orden": 42,
                                "Desde": 270,
                                "Hasta": 270,
                                "Long": 1,
                                "Nombre de Campo": "sig_perc_iibb_facturado_60",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "42": {
                                "Orden": 43,
                                "Desde": 271,
                                "Hasta": 282,
                                "Long": 12,
                                "Nombre de Campo": "imp_perc_iibb_facturado_60",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por Percepcion de IIBB facturado hace 60 d\u00edas"
                            },
                            "43": {
                                "Orden": 44,
                                "Desde": 283,
                                "Hasta": 283,
                                "Long": 1,
                                "Nombre de Campo": "signo_ara_facturado_90",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "44": {
                                "Orden": 45,
                                "Desde": 284,
                                "Hasta": 295,
                                "Long": 12,
                                "Nombre de Campo": "importe_ara_facturado_90",
                                "Tipo": "N",
                                "Contenido": "Importe descontado de arancel facturado hace 90 d\u00edas"
                            },
                            "45": {
                                "Orden": 46,
                                "Desde": 296,
                                "Hasta": 296,
                                "Long": 1,
                                "Nombre de Campo": "signo_int_z_facturado_90",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "46": {
                                "Orden": 47,
                                "Desde": 297,
                                "Hasta": 308,
                                "Long": 12,
                                "Nombre de Campo": "imp_int_z_facturado_90",
                                "Tipo": "N",
                                "Contenido": "Importe descontado de inter\u00e9s por anticipo plan Zeta facturado hace 90 d\u00edas"
                            },
                            "47": {
                                "Orden": 48,
                                "Desde": 309,
                                "Hasta": 309,
                                "Long": 1,
                                "Nombre de Campo": "sig_int_plan_esp_facturado_90",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "48": {
                                "Orden": 49,
                                "Desde": 310,
                                "Hasta": 321,
                                "Long": 12,
                                "Nombre de Campo": "imp_int_plan_esp_facturado_90",
                                "Tipo": "N",
                                "Contenido": "Importe descontado de inter\u00e9s anticipo Plan Especial facturado hace 90 d\u00edas"
                            },
                            "49": {
                                "Orden": 50,
                                "Desde": 322,
                                "Hasta": 322,
                                "Long": 1,
                                "Nombre de Campo": "sig_acre_liq_ant_facturado_90",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "50": {
                                "Orden": 51,
                                "Desde": 323,
                                "Hasta": 334,
                                "Long": 12,
                                "Nombre de Campo": "imp_acre_liq_ant_facturado_90",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por acreditacion de liquidacion anterior facturado hace 90 d\u00edas"
                            },
                            "51": {
                                "Orden": 42,
                                "Desde": 335,
                                "Hasta": 335,
                                "Long": 1,
                                "Nombre de Campo": "sig_iva_21_facturado_90",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "52": {
                                "Orden": 53,
                                "Desde": 336,
                                "Hasta": 347,
                                "Long": 12,
                                "Nombre de Campo": "imp_iva_21_facturado_90",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por IVA Inscripto 21% facturado hace 90 d\u00edas"
                            },
                            "53": {
                                "Orden": 54,
                                "Desde": 348,
                                "Hasta": 348,
                                "Long": 1,
                                "Nombre de Campo": "sig_perc_iva_facturado_90",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "54": {
                                "Orden": 55,
                                "Desde": 349,
                                "Hasta": 360,
                                "Long": 12,
                                "Nombre de Campo": "imp_perc_iva_facturado_90",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por Percepcion de IVA facturado hace 90 d\u00edas"
                            },
                            "55": {
                                "Orden": 56,
                                "Desde": 361,
                                "Hasta": 361,
                                "Long": 1,
                                "Nombre de Campo": "sig_perc_iibb_facturado_90",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "56": {
                                "Orden": 57,
                                "Desde": 362,
                                "Hasta": 373,
                                "Long": 12,
                                "Nombre de Campo": "imp_perc_iibb_facturado_90",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por Percepcion de IIBB facturado hace 90 d\u00edas"
                            },
                            "57": {
                                "Orden": 58,
                                "Desde": 374,
                                "Hasta": 374,
                                "Long": 1,
                                "Nombre de Campo": "signo_ara_facturado_120",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "58": {
                                "Orden": 59,
                                "Desde": 375,
                                "Hasta": 386,
                                "Long": 12,
                                "Nombre de Campo": "importe_ara_facturado_120",
                                "Tipo": "N",
                                "Contenido": "Importe descontado de arancel facturado hace 120 d\u00edas"
                            },
                            "59": {
                                "Orden": 60,
                                "Desde": 387,
                                "Hasta": 387,
                                "Long": 1,
                                "Nombre de Campo": "signo_int_z_facturado_120",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "60": {
                                "Orden": 61,
                                "Desde": 388,
                                "Hasta": 399,
                                "Long": 12,
                                "Nombre de Campo": "imp_int_z_facturado_120",
                                "Tipo": "N",
                                "Contenido": "Importe descontado de inter\u00e9s por anticipo plan Zeta facturado hace 120 d\u00edas"
                            },
                            "61": {
                                "Orden": 62,
                                "Desde": 400,
                                "Hasta": 400,
                                "Long": 1,
                                "Nombre de Campo": "sig_int_plan_esp_facturado_120",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "62": {
                                "Orden": 63,
                                "Desde": 401,
                                "Hasta": 412,
                                "Long": 12,
                                "Nombre de Campo": "imp_int_plan_esp_facturado_120",
                                "Tipo": "N",
                                "Contenido": "Importe descontado de inter\u00e9s anticipo Plan Especial facturado hace 120 d\u00edas"
                            },
                            "63": {
                                "Orden": 64,
                                "Desde": 413,
                                "Hasta": 413,
                                "Long": 1,
                                "Nombre de Campo": "sig_acre_liq_ant_facturado_120",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "64": {
                                "Orden": 65,
                                "Desde": 414,
                                "Hasta": 425,
                                "Long": 12,
                                "Nombre de Campo": "imp_acre_liq_ant_facturado_120",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por acreditacion de liquidacion anterior facturado hace 120 d\u00edas"
                            },
                            "65": {
                                "Orden": 66,
                                "Desde": 426,
                                "Hasta": 426,
                                "Long": 1,
                                "Nombre de Campo": "sig_iva_21_facturado_120",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "66": {
                                "Orden": 67,
                                "Desde": 427,
                                "Hasta": 438,
                                "Long": 12,
                                "Nombre de Campo": "imp_iva_21_facturado_120",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por IVA Inscripto 21% facturado hace 120 d\u00edas"
                            },
                            "67": {
                                "Orden": 68,
                                "Desde": 439,
                                "Hasta": 439,
                                "Long": 1,
                                "Nombre de Campo": "sig_perc_iva_facturado_120",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "68": {
                                "Orden": 69,
                                "Desde": 440,
                                "Hasta": 451,
                                "Long": 12,
                                "Nombre de Campo": "imp_perc_iva_facturado_120",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por Percepcion de IVA facturado hace 120 d\u00edas"
                            },
                            "69": {
                                "Orden": 70,
                                "Desde": 452,
                                "Hasta": 452,
                                "Long": 1,
                                "Nombre de Campo": "sig_perc_iibb_facturado_120",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "70": {
                                "Orden": 71,
                                "Desde": 453,
                                "Hasta": 464,
                                "Long": 12,
                                "Nombre de Campo": "imp_perc_iibb_facturado_120",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por Percepcion de IIBB facturado hace 120 d\u00edas"
                            },
                            "71": {
                                "Orden": 72,
                                "Desde": 465,
                                "Hasta": 465,
                                "Long": 1,
                                "Nombre de Campo": "sig_perc_2408_vto",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "72": {
                                "Orden": 73,
                                "Desde": 466,
                                "Hasta": 477,
                                "Long": 12,
                                "Nombre de Campo": "imp_perc_2408_vto",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por Percepcion 2408 facturado en Vto"
                            },
                            "73": {
                                "Orden": 74,
                                "Desde": 478,
                                "Hasta": 478,
                                "Long": 1,
                                "Nombre de Campo": "sig_perc_2408_facturado_30",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "74": {
                                "Orden": 75,
                                "Desde": 479,
                                "Hasta": 490,
                                "Long": 12,
                                "Nombre de Campo": "imp_perc_2408_facturado_30",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por Percepcion 2408 facturado hace 30 d\u00edas"
                            },
                            "75": {
                                "Orden": 76,
                                "Desde": 491,
                                "Hasta": 491,
                                "Long": 1,
                                "Nombre de Campo": "sig_perc_2408_facturado_60",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "76": {
                                "Orden": 77,
                                "Desde": 492,
                                "Hasta": 503,
                                "Long": 12,
                                "Nombre de Campo": "imp_perc_2408_facturado_60",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por Percepcion 2408 facturado hace 60 d\u00edas"
                            },
                            "77": {
                                "Orden": 78,
                                "Desde": 504,
                                "Hasta": 504,
                                "Long": 1,
                                "Nombre de Campo": "sig_perc_2408_facturado_90",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "78": {
                                "Orden": 79,
                                "Desde": 505,
                                "Hasta": 516,
                                "Long": 12,
                                "Nombre de Campo": "imp_perc_2408_facturado_90",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por Percepcion 2408 facturado hace 90 d\u00edas"
                            },
                            "79": {
                                "Orden": 80,
                                "Desde": 517,
                                "Hasta": 517,
                                "Long": 1,
                                "Nombre de Campo": "sig_perc_2408_facturado_120",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "80": {
                                "Orden": 81,
                                "Desde": 518,
                                "Hasta": 529,
                                "Long": 12,
                                "Nombre de Campo": "imp_perc_2408_facturado_120",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por Percepcion 2408 facturado hace 120 d\u00edas"
                            },
                            "81": {
                                "Orden": 82,
                                "Desde": 530,
                                "Hasta": 607,
                                "Long": 78,
                                "Nombre de Campo": "filler",
                                "Tipo": "A",
                                "Contenido": "Espacios en Blanco"
                            },
                            "82": {
                                "Orden": 83,
                                "Desde": 608,
                                "Hasta": 608,
                                "Long": 1,
                                "Nombre de Campo": "marca",
                                "Tipo": "A",
                                "Contenido": "Tipo de Registro (Siempre \"P\")"
                            }
                        }
                    },
                    "F": {
                        "start": 132,
                        "n_rows": 95,
                        "df": [],
                        "layout": {
                            "0": {
                                "Orden": 1,
                                "Desde": 1,
                                "Hasta": 9,
                                "Long": 9,
                                "Nombre de Campo": "comercio",
                                "Tipo": "N",
                                "Contenido": "Nro. de Comercio"
                            },
                            "1": {
                                "Orden": 2,
                                "Desde": 10,
                                "Hasta": 10,
                                "Long": 1,
                                "Nombre de Campo": "signo_presentaciones_a_pagar_vto",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "2": {
                                "Orden": 3,
                                "Desde": 11,
                                "Hasta": 22,
                                "Long": 12,
                                "Nombre de Campo": "importe_a_pagar_vto",
                                "Tipo": "N",
                                "Contenido": "Importe de Presentaciones a Pagar en Vto."
                            },
                            "3": {
                                "Orden": 4,
                                "Desde": 23,
                                "Hasta": 23,
                                "Long": 1,
                                "Nombre de Campo": "signo_arancel_vto",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "4": {
                                "Orden": 5,
                                "Desde": 24,
                                "Hasta": 35,
                                "Long": 12,
                                "Nombre de Campo": "importe_arancel_vto",
                                "Tipo": "N",
                                "Contenido": "Importe facturado de arancel a descontar a Vto"
                            },
                            "5": {
                                "Orden": 6,
                                "Desde": 36,
                                "Hasta": 36,
                                "Long": 1,
                                "Nombre de Campo": "signo_interes_zeta_vto",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "6": {
                                "Orden": 7,
                                "Desde": 37,
                                "Hasta": 48,
                                "Long": 12,
                                "Nombre de Campo": "importe_interes_zeta_vto",
                                "Tipo": "N",
                                "Contenido": "Importe facturado de inter\u00e9s por anticipo plan Zeta a Vto"
                            },
                            "7": {
                                "Orden": 8,
                                "Desde": 49,
                                "Hasta": 49,
                                "Long": 1,
                                "Nombre de Campo": "signo_interes_plan_especial_vto",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "8": {
                                "Orden": 9,
                                "Desde": 50,
                                "Hasta": 61,
                                "Long": 12,
                                "Nombre de Campo": "importe_interes_plan_especial_vto",
                                "Tipo": "N",
                                "Contenido": "Importe facturado de inter\u00e9s anticipo Plan Especial a Vto"
                            },
                            "9": {
                                "Orden": 10,
                                "Desde": 62,
                                "Hasta": 62,
                                "Long": 1,
                                "Nombre de Campo": "signo_acreditacion_liquidacion_anterior_vto",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "10": {
                                "Orden": 11,
                                "Desde": 63,
                                "Hasta": 74,
                                "Long": 12,
                                "Nombre de Campo": "importe_acreditacion_liquidacion_anterior_vto",
                                "Tipo": "N",
                                "Contenido": "Importe facturado por acreditacion de liquidacion anterior a Vto"
                            },
                            "11": {
                                "Orden": 12,
                                "Desde": 75,
                                "Hasta": 75,
                                "Long": 1,
                                "Nombre de Campo": "signo_iva_21__vto",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "12": {
                                "Orden": 13,
                                "Desde": 76,
                                "Hasta": 87,
                                "Long": 12,
                                "Nombre de Campo": "importe_iva_21__vto",
                                "Tipo": "N",
                                "Contenido": "Importe facturado por IVA Inscripto 21% a descontar en 30 d\u00edas"
                            },
                            "13": {
                                "Orden": 14,
                                "Desde": 88,
                                "Hasta": 88,
                                "Long": 1,
                                "Nombre de Campo": "signo_percepcion_iva_vto",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "14": {
                                "Orden": 15,
                                "Desde": 89,
                                "Hasta": 100,
                                "Long": 12,
                                "Nombre de Campo": "importe_percepcion_iva_vto",
                                "Tipo": "N",
                                "Contenido": "Importe facturado por Percepcion de IVA a descontar en 30 d\u00edas"
                            },
                            "15": {
                                "Orden": 16,
                                "Desde": 101,
                                "Hasta": 101,
                                "Long": 1,
                                "Nombre de Campo": "signo_percepcion_iibb_vto",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "16": {
                                "Orden": 17,
                                "Desde": 102,
                                "Hasta": 113,
                                "Long": 12,
                                "Nombre de Campo": "importe_percepcion_iibb_vto",
                                "Tipo": "N",
                                "Contenido": "Importe facturado por Percepcion de IIBB a descontar en 30 d\u00edas"
                            },
                            "17": {
                                "Orden": 18,
                                "Desde": 114,
                                "Hasta": 114,
                                "Long": 1,
                                "Nombre de Campo": "signo_presentaciones_a_pagar_30_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "18": {
                                "Orden": 19,
                                "Desde": 115,
                                "Hasta": 126,
                                "Long": 12,
                                "Nombre de Campo": "importe_a_pagar_30_dias",
                                "Tipo": "N",
                                "Contenido": "Importe de Presentaciones a Pagar a 30 d\u00edas"
                            },
                            "19": {
                                "Orden": 20,
                                "Desde": 127,
                                "Hasta": 127,
                                "Long": 1,
                                "Nombre de Campo": "signo_arancel_dif_30_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "20": {
                                "Orden": 21,
                                "Desde": 128,
                                "Hasta": 139,
                                "Long": 12,
                                "Nombre de Campo": "importe_arancel_dif_30_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado de arancel a descontar en 30 d\u00edas  "
                            },
                            "21": {
                                "Orden": 22,
                                "Desde": 140,
                                "Hasta": 140,
                                "Long": 1,
                                "Nombre de Campo": "signo_interes_zeta_dif_30_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "22": {
                                "Orden": 23,
                                "Desde": 141,
                                "Hasta": 152,
                                "Long": 12,
                                "Nombre de Campo": "importe_interes_zeta_dif_30_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado de inter\u00e9s por anticipo plan Zeta a descontar en 30 d\u00edas"
                            },
                            "23": {
                                "Orden": 24,
                                "Desde": 153,
                                "Hasta": 153,
                                "Long": 1,
                                "Nombre de Campo": "signo_interes_plan_especial_dif_30_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "24": {
                                "Orden": 25,
                                "Desde": 154,
                                "Hasta": 165,
                                "Long": 12,
                                "Nombre de Campo": "importe_interes_plan_especial_dif_30_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado de inter\u00e9s anticipo Plan Especial a descontar en 30 d\u00edas"
                            },
                            "25": {
                                "Orden": 26,
                                "Desde": 166,
                                "Hasta": 166,
                                "Long": 1,
                                "Nombre de Campo": "signo_acreditacion_liquidacion_anterior_dif_30_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "26": {
                                "Orden": 27,
                                "Desde": 167,
                                "Hasta": 178,
                                "Long": 12,
                                "Nombre de Campo": "importe_acreditacion_liquidacion_anterior_dif_30_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado por acreditacion de liquidacion anterior a descontar en 30 d\u00edas"
                            },
                            "27": {
                                "Orden": 28,
                                "Desde": 179,
                                "Hasta": 179,
                                "Long": 1,
                                "Nombre de Campo": "signo_iva_21__dif_30_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "28": {
                                "Orden": 29,
                                "Desde": 180,
                                "Hasta": 191,
                                "Long": 12,
                                "Nombre de Campo": "importe_iva_21__dif_30_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado por IVA Inscripto 21% a descontar en 30 d\u00edas"
                            },
                            "29": {
                                "Orden": 30,
                                "Desde": 192,
                                "Hasta": 192,
                                "Long": 1,
                                "Nombre de Campo": "signo_percepcion_iva_dif_30_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "30": {
                                "Orden": 31,
                                "Desde": 193,
                                "Hasta": 204,
                                "Long": 12,
                                "Nombre de Campo": "importe_percepcion_iva_dif_30_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado por Percepcion de IVA a descontar en 30 d\u00edas"
                            },
                            "31": {
                                "Orden": 32,
                                "Desde": 205,
                                "Hasta": 205,
                                "Long": 1,
                                "Nombre de Campo": "signo_percepcion_iibb_dif_30_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "32": {
                                "Orden": 33,
                                "Desde": 206,
                                "Hasta": 217,
                                "Long": 12,
                                "Nombre de Campo": "importe_percepcion_iibb_dif_30_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado por Percepcion de IIBB a descontar en 30 d\u00edas"
                            },
                            "33": {
                                "Orden": 34,
                                "Desde": 218,
                                "Hasta": 218,
                                "Long": 1,
                                "Nombre de Campo": "signo_presentaciones_a_pagar_a_60_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "34": {
                                "Orden": 35,
                                "Desde": 219,
                                "Hasta": 230,
                                "Long": 12,
                                "Nombre de Campo": "importe_a_pagar_a_60_dias",
                                "Tipo": "N",
                                "Contenido": "Importe de Presentaciones a Pagar a 60 d\u00edas"
                            },
                            "35": {
                                "Orden": 36,
                                "Desde": 231,
                                "Hasta": 231,
                                "Long": 1,
                                "Nombre de Campo": "signo_arancel_dif_60_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "36": {
                                "Orden": 37,
                                "Desde": 232,
                                "Hasta": 243,
                                "Long": 12,
                                "Nombre de Campo": "importe_arancel_dif_60_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado de arancel Pago Diferido 60 d\u00edas a descontar en proximas liquidaciones"
                            },
                            "37": {
                                "Orden": 38,
                                "Desde": 244,
                                "Hasta": 244,
                                "Long": 1,
                                "Nombre de Campo": "signo_interes_zeta_dif_60_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "38": {
                                "Orden": 39,
                                "Desde": 245,
                                "Hasta": 256,
                                "Long": 12,
                                "Nombre de Campo": "importe_interes_zeta_dif_60_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado de inter\u00e9s por anticipo plan Zeta a descontar en 60 d\u00edas"
                            },
                            "39": {
                                "Orden": 40,
                                "Desde": 257,
                                "Hasta": 257,
                                "Long": 1,
                                "Nombre de Campo": "signo_interes_plan_especial_dif_60_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "40": {
                                "Orden": 41,
                                "Desde": 258,
                                "Hasta": 269,
                                "Long": 12,
                                "Nombre de Campo": "importe_interes_plan_especial_dif_60_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado de inter\u00e9s anticipo Plan Especial a descontar en 60 d\u00edas"
                            },
                            "41": {
                                "Orden": 42,
                                "Desde": 270,
                                "Hasta": 270,
                                "Long": 1,
                                "Nombre de Campo": "signo_acreditacion_liquidacion_anterior_dif_60_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "42": {
                                "Orden": 43,
                                "Desde": 271,
                                "Hasta": 282,
                                "Long": 12,
                                "Nombre de Campo": "importe_acreditacion_liquidacion_anterior_dif_60_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado por acreditacion de liquidacion anterior a descontar en 60 d\u00edas"
                            },
                            "43": {
                                "Orden": 44,
                                "Desde": 283,
                                "Hasta": 283,
                                "Long": 1,
                                "Nombre de Campo": "signo_iva_21__dif_60_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "44": {
                                "Orden": 45,
                                "Desde": 284,
                                "Hasta": 295,
                                "Long": 12,
                                "Nombre de Campo": "importe_iva_21__dif_60_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado por IVA Inscripto 21% a descontar en 60 d\u00edas"
                            },
                            "45": {
                                "Orden": 46,
                                "Desde": 296,
                                "Hasta": 296,
                                "Long": 1,
                                "Nombre de Campo": "signo_percepcion_iva_dif_60_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "46": {
                                "Orden": 47,
                                "Desde": 297,
                                "Hasta": 308,
                                "Long": 12,
                                "Nombre de Campo": "importe_percepcion_iva_dif_60_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado por Percepcion de IVA a descontar en 60 d\u00edas"
                            },
                            "47": {
                                "Orden": 48,
                                "Desde": 309,
                                "Hasta": 309,
                                "Long": 1,
                                "Nombre de Campo": "signo_percepcion_iibb_dif_60_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "48": {
                                "Orden": 49,
                                "Desde": 310,
                                "Hasta": 321,
                                "Long": 12,
                                "Nombre de Campo": "importe_percepcion_iibb_dif_60_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado por Percepcion de IIBB a descontar en 60 d\u00edas"
                            },
                            "49": {
                                "Orden": 50,
                                "Desde": 322,
                                "Hasta": 322,
                                "Long": 1,
                                "Nombre de Campo": "signo_presentaciones_a_pagar_a_90_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "50": {
                                "Orden": 51,
                                "Desde": 323,
                                "Hasta": 334,
                                "Long": 12,
                                "Nombre de Campo": "importe_a_pagar_a_90_dias",
                                "Tipo": "N",
                                "Contenido": "Importe de Presentaciones a Pagar a 90 d\u00edas"
                            },
                            "51": {
                                "Orden": 52,
                                "Desde": 335,
                                "Hasta": 335,
                                "Long": 1,
                                "Nombre de Campo": "signo_arancel_dif_90_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "52": {
                                "Orden": 53,
                                "Desde": 336,
                                "Hasta": 347,
                                "Long": 12,
                                "Nombre de Campo": "importe_arancel_dif_90_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado de arancel Pago Diferido 90 d\u00edas a descontar en proximas liquidaciones"
                            },
                            "53": {
                                "Orden": 54,
                                "Desde": 348,
                                "Hasta": 348,
                                "Long": 1,
                                "Nombre de Campo": "signo_interes_zeta_dif_90_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "54": {
                                "Orden": 55,
                                "Desde": 349,
                                "Hasta": 360,
                                "Long": 12,
                                "Nombre de Campo": "importe_interes_zeta_dif_90_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado de inter\u00e9s por anticipo plan Zeta a descontar en 90 d\u00edas"
                            },
                            "55": {
                                "Orden": 56,
                                "Desde": 361,
                                "Hasta": 361,
                                "Long": 1,
                                "Nombre de Campo": "signo_interes_plan_especial_dif_90_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "56": {
                                "Orden": 57,
                                "Desde": 362,
                                "Hasta": 373,
                                "Long": 12,
                                "Nombre de Campo": "importe_interes_plan_especial_dif_90_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado de inter\u00e9s anticipo Plan Especial a descontar en 90 d\u00edas"
                            },
                            "57": {
                                "Orden": 58,
                                "Desde": 374,
                                "Hasta": 374,
                                "Long": 1,
                                "Nombre de Campo": "signo_acreditacion_liquidacion_anterior_dif_90_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "58": {
                                "Orden": 59,
                                "Desde": 375,
                                "Hasta": 386,
                                "Long": 12,
                                "Nombre de Campo": "importe_acreditacion_liquidacion_anterior_dif_90_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado por acreditacion de liquidacion anterior a descontar en 90 d\u00edas"
                            },
                            "59": {
                                "Orden": 60,
                                "Desde": 386,
                                "Hasta": 387,
                                "Long": 1,
                                "Nombre de Campo": "signo_iva_21__dif_90_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "60": {
                                "Orden": 61,
                                "Desde": 387,
                                "Hasta": 399,
                                "Long": 12,
                                "Nombre de Campo": "importe_iva_21__dif_90_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado por IVA Inscripto 21% a descontar en 90 d\u00edas"
                            },
                            "61": {
                                "Orden": 62,
                                "Desde": 400,
                                "Hasta": 400,
                                "Long": 1,
                                "Nombre de Campo": "signo_percepcion_iva_dif_90_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "62": {
                                "Orden": 63,
                                "Desde": 401,
                                "Hasta": 412,
                                "Long": 12,
                                "Nombre de Campo": "importe_percepcion_iva_dif_90_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado por Percepcion de IVA a descontar en 90 d\u00edas"
                            },
                            "63": {
                                "Orden": 64,
                                "Desde": 413,
                                "Hasta": 413,
                                "Long": 1,
                                "Nombre de Campo": "signo_percepcion_iibb_dif_90_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "64": {
                                "Orden": 65,
                                "Desde": 414,
                                "Hasta": 425,
                                "Long": 12,
                                "Nombre de Campo": "importe_percepcion_iibb_dif_90_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado por Percepcion de IIBB a descontar en 90 d\u00edas"
                            },
                            "65": {
                                "Orden": 66,
                                "Desde": 426,
                                "Hasta": 426,
                                "Long": 1,
                                "Nombre de Campo": "signo_presentaciones_a_pagar_a_120_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "66": {
                                "Orden": 67,
                                "Desde": 427,
                                "Hasta": 438,
                                "Long": 12,
                                "Nombre de Campo": "importe_a_pagar_a_120_dias",
                                "Tipo": "N",
                                "Contenido": "Importe de Presentaciones a Pagar a 120 d\u00edas"
                            },
                            "67": {
                                "Orden": 68,
                                "Desde": 439,
                                "Hasta": 439,
                                "Long": 1,
                                "Nombre de Campo": "signo_arancel_dif_120_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "68": {
                                "Orden": 69,
                                "Desde": 440,
                                "Hasta": 451,
                                "Long": 12,
                                "Nombre de Campo": "importe_arancel_dif_120_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado de arancel Pago Diferido 120 d\u00edas a descontar en proximas liquidaciones"
                            },
                            "69": {
                                "Orden": 70,
                                "Desde": 452,
                                "Hasta": 452,
                                "Long": 1,
                                "Nombre de Campo": "signo_interes_zeta_dif_120_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "70": {
                                "Orden": 71,
                                "Desde": 453,
                                "Hasta": 464,
                                "Long": 12,
                                "Nombre de Campo": "importe_interes_zeta_dif_120_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado de inter\u00e9s por anticipo plan Zeta a descontar en 120 d\u00edas"
                            },
                            "71": {
                                "Orden": 72,
                                "Desde": 465,
                                "Hasta": 465,
                                "Long": 1,
                                "Nombre de Campo": "signo_interes_plan_especial_dif_120_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "72": {
                                "Orden": 73,
                                "Desde": 466,
                                "Hasta": 477,
                                "Long": 12,
                                "Nombre de Campo": "importe_interes_plan_especial_dif_120_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado de inter\u00e9s anticipo Plan Especial a descontar en 120 d\u00edas"
                            },
                            "73": {
                                "Orden": 74,
                                "Desde": 478,
                                "Hasta": 478,
                                "Long": 1,
                                "Nombre de Campo": "signo_acreditacion_liquidacion_anterior_dif_120_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "74": {
                                "Orden": 75,
                                "Desde": 479,
                                "Hasta": 490,
                                "Long": 12,
                                "Nombre de Campo": "importe_acreditacion_liquidacion_anterior_dif_120_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado por acreditacion de liquidacion anterior a descontar en 120 d\u00edas"
                            },
                            "75": {
                                "Orden": 76,
                                "Desde": 491,
                                "Hasta": 491,
                                "Long": 1,
                                "Nombre de Campo": "signo_iva_21__dif_120_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "76": {
                                "Orden": 77,
                                "Desde": 492,
                                "Hasta": 503,
                                "Long": 12,
                                "Nombre de Campo": "importe_iva_21__dif_120_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado por IVA Inscripto 21% a descontar en 120 d\u00edas"
                            },
                            "77": {
                                "Orden": 78,
                                "Desde": 504,
                                "Hasta": 504,
                                "Long": 1,
                                "Nombre de Campo": "signo_percepcion_iva_dif_120_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "78": {
                                "Orden": 79,
                                "Desde": 505,
                                "Hasta": 516,
                                "Long": 12,
                                "Nombre de Campo": "importe_percepcion_iva_dif_120_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado por Percepcion de IVA a descontar en 120 d\u00edas"
                            },
                            "79": {
                                "Orden": 80,
                                "Desde": 517,
                                "Hasta": 517,
                                "Long": 1,
                                "Nombre de Campo": "signo_percepcion_iibb_dif_120_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "80": {
                                "Orden": 81,
                                "Desde": 518,
                                "Hasta": 529,
                                "Long": 12,
                                "Nombre de Campo": "importe_percepcion_iibb_dif_120_dias",
                                "Tipo": "N",
                                "Contenido": "Importe facturado por Percepcion de IIBB a descontar en 120 d\u00edas"
                            },
                            "81": {
                                "Orden": 82,
                                "Desde": 530,
                                "Hasta": 530,
                                "Long": 1,
                                "Nombre de Campo": "signo_interes_pago_anticipado",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "82": {
                                "Orden": 83,
                                "Desde": 531,
                                "Hasta": 542,
                                "Long": 12,
                                "Nombre de Campo": "importe_interes_pago_anticipado",
                                "Tipo": "N",
                                "Contenido": "Importe facturado por Inter\u00e9s Pago Anticipado (solo para Comercios Amigos con modalidad de pago autom\u00e1tico)"
                            },
                            "83": {
                                "Orden": 84,
                                "Desde": 543,
                                "Hasta": 543,
                                "Long": 1,
                                "Nombre de Campo": "signo_percepcion_2408_vto",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "84": {
                                "Orden": 85,
                                "Desde": 544,
                                "Hasta": 555,
                                "Long": 12,
                                "Nombre de Campo": "importe_percepcion_2408_vto",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por Percepcion 2408 Vto"
                            },
                            "85": {
                                "Orden": 86,
                                "Desde": 556,
                                "Hasta": 556,
                                "Long": 1,
                                "Nombre de Campo": "signo_percepcion_2408_dif_30_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "86": {
                                "Orden": 87,
                                "Desde": 557,
                                "Hasta": 568,
                                "Long": 12,
                                "Nombre de Campo": "importe_percepcion_2408_dif_30_dias",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por Percepcion 2408 a descontar en 30 d\u00edas"
                            },
                            "87": {
                                "Orden": 88,
                                "Desde": 569,
                                "Hasta": 569,
                                "Long": 1,
                                "Nombre de Campo": "signo_percepcion_2408_dif_60_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "88": {
                                "Orden": 89,
                                "Desde": 570,
                                "Hasta": 581,
                                "Long": 12,
                                "Nombre de Campo": "importe_percepcion_2408_dif_60_dias",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por Percepcion 2408 a descontar en 60 d\u00edas"
                            },
                            "89": {
                                "Orden": 90,
                                "Desde": 582,
                                "Hasta": 582,
                                "Long": 1,
                                "Nombre de Campo": "signo_percepcion_2408_dif_90_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "90": {
                                "Orden": 91,
                                "Desde": 583,
                                "Hasta": 594,
                                "Long": 12,
                                "Nombre de Campo": "importe_percepcion_2408_dif_90_dias",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por Percepcion 2408 a descontar en 90 d\u00edas"
                            },
                            "91": {
                                "Orden": 92,
                                "Desde": 595,
                                "Hasta": 595,
                                "Long": 1,
                                "Nombre de Campo": "signo_percepcion_2408_dif_120_dias",
                                "Tipo": "A",
                                "Contenido": " Signo positivo Signo negativo"
                            },
                            "92": {
                                "Orden": 93,
                                "Desde": 596,
                                "Hasta": 607,
                                "Long": 12,
                                "Nombre de Campo": "importe_percepcion_2408_dif_120_dias",
                                "Tipo": "N",
                                "Contenido": "Importe descontado por Percepcion 2408 a descontar en 120 d\u00edas"
                            },
                            "93": {
                                "Orden": 94,
                                "Desde": 608,
                                "Hasta": 608,
                                "Long": 1,
                                "Nombre de Campo": "filler",
                                "Tipo": "G",
                                "Contenido": "Tipo de registro (siempre = F)"
                            }
                        }
                    },
                    "D": {
                        "start": 231,
                        "n_rows": 26,
                        "df": [],
                        "layout": {
                            "0": {
                                "Orden": 1,
                                "Desde": 1,
                                "Hasta": 9,
                                "Long": 9,
                                "Nombre de Campo": "comercio",
                                "Tipo": "N",
                                "Contenido": "Nro. de Comercio"
                            },
                            "1": {
                                "Orden": 2,
                                "Desde": 10,
                                "Hasta": 15,
                                "Long": 6,
                                "Nombre de Campo": "fecha_presentacion",
                                "Tipo": "F",
                                "Contenido": "Fecha de Presentacion de Recap (AAMMDD)"
                            },
                            "2": {
                                "Orden": 3,
                                "Desde": 16,
                                "Hasta": 21,
                                "Long": 6,
                                "Nombre de Campo": "numero_recap",
                                "Tipo": "N",
                                "Contenido": "Nro. De recap"
                            },
                            "3": {
                                "Orden": 4,
                                "Desde": 22,
                                "Hasta": 27,
                                "Long": 6,
                                "Nombre de Campo": "cupon",
                                "Tipo": "n",
                                "Contenido": "Nro. De cupon"
                            },
                            "4": {
                                "Orden": 5,
                                "Desde": 28,
                                "Hasta": 43,
                                "Long": 16,
                                "Nombre de Campo": "tarjeta",
                                "Tipo": "N",
                                "Contenido": "Nro. De tarjeta o pl\u00e1stico"
                            },
                            "5": {
                                "Orden": 6,
                                "Desde": 44,
                                "Hasta": 49,
                                "Long": 6,
                                "Nombre de Campo": "fecha_compra",
                                "Tipo": "F",
                                "Contenido": "Fecha de Compra (AAMMDD)"
                            },
                            "6": {
                                "Orden": 7,
                                "Desde": 50,
                                "Hasta": 50,
                                "Long": 1,
                                "Nombre de Campo": "moneda",
                                "Tipo": "N",
                                "Contenido": "Moneda de compra: 0=pesos 1=dolar 2=bonos 3=zeta"
                            },
                            "7": {
                                "Orden": 8,
                                "Desde": 51,
                                "Hasta": 52,
                                "Long": 2,
                                "Nombre de Campo": "plan",
                                "Tipo": "N",
                                "Contenido": "Nro. De plan"
                            },
                            "8": {
                                "Orden": 9,
                                "Desde": 53,
                                "Hasta": 64,
                                "Long": 12,
                                "Nombre de Campo": "compra",
                                "Tipo": "N",
                                "Contenido": "Importe total de compras (10 enteros + 2 decimales=12)"
                            },
                            "9": {
                                "Orden": 10,
                                "Desde": 65,
                                "Hasta": 76,
                                "Long": 12,
                                "Nombre de Campo": "entrega",
                                "Tipo": "N",
                                "Contenido": "Importe total de entrega (10 enteros + 2 decimales=12)"
                            },
                            "10": {
                                "Orden": 11,
                                "Desde": 77,
                                "Hasta": 82,
                                "Long": 6,
                                "Nombre de Campo": "fecha_cuota",
                                "Tipo": "F",
                                "Contenido": "Fecha de Cobro de 1er. Cuota (AAMMDD)"
                            },
                            "11": {
                                "Orden": 12,
                                "Desde": 83,
                                "Hasta": 94,
                                "Long": 12,
                                "Nombre de Campo": "importe_cuota",
                                "Tipo": "N",
                                "Contenido": "Importe de cuota (10 enteros + 2 decimales=12)"
                            },
                            "12": {
                                "Orden": 13,
                                "Desde": 95,
                                "Hasta": 96,
                                "Long": 2,
                                "Nombre de Campo": "numero_cuota",
                                "Tipo": "N",
                                "Contenido": "numero de cuota "
                            },
                            "13": {
                                "Orden": 14,
                                "Desde": 97,
                                "Hasta": 97,
                                "Long": 1,
                                "Nombre de Campo": "tipo_mov",
                                "Tipo": "A",
                                "Contenido": "Tipo de Movimiento: \" \"=Informativo;\"D\"=Suma Cta.Cte.Comercio;\"H\"=Resta Cta.Cte.Comercio"
                            },
                            "14": {
                                "Orden": 15,
                                "Desde": 98,
                                "Hasta": 98,
                                "Long": 1,
                                "Nombre de Campo": "estado",
                                "Tipo": "A",
                                "Contenido": "Estado de Movimiento: \" \" =Cupon OK; \"R\"=Cupon Rechaz; \"X\"=D\u00e9Cr\u00e9d. en Cta.Cte. Comercio"
                            },
                            "15": {
                                "Orden": 16,
                                "Desde": 99,
                                "Hasta": 128,
                                "Long": 30,
                                "Nombre de Campo": "descripcion",
                                "Tipo": "A",
                                "Contenido": "Descripcion del estado"
                            },
                            "16": {
                                "Orden": 17,
                                "Desde": 129,
                                "Hasta": 134,
                                "Long": 6,
                                "Nombre de Campo": "codigo_aut",
                                "Tipo": "N",
                                "Contenido": "codigo autorizacion"
                            },
                            "17": {
                                "Orden": 18,
                                "Desde": 135,
                                "Hasta": 135,
                                "Long": 1,
                                "Nombre de Campo": "tipo_op",
                                "Tipo": "A",
                                "Contenido": "Tipo de Operacion:\"O\"=Autom\u00e1tico; \"M\"=Manual"
                            },
                            "18": {
                                "Orden": 19,
                                "Desde": 136,
                                "Hasta": 141,
                                "Long": 6,
                                "Nombre de Campo": "numero_devolucion",
                                "Tipo": "N",
                                "Contenido": "nro. De devolucion de cupones. Si est\u00e1 en cero no es una devolucion"
                            },
                            "19": {
                                "Orden": 20,
                                "Desde": 142,
                                "Hasta": 142,
                                "Long": 1,
                                "Nombre de Campo": "tipo_cd",
                                "Tipo": "A",
                                "Contenido": "Tipo DevolucioContracargo: \"C\"=Contracargo, \"D\"=Devolucion,"
                            },
                            "20": {
                                "Orden": 21,
                                "Desde": 143,
                                "Hasta": 150,
                                "Long": 8,
                                "Nombre de Campo": "nro_terminal",
                                "Tipo": "N",
                                "Contenido": "Nro. De terminal"
                            },
                            "21": {
                                "Orden": 22,
                                "Desde": 151,
                                "Hasta": 154,
                                "Long": 4,
                                "Nombre de Campo": "nro_lote",
                                "Tipo": "A",
                                "Contenido": "Nro. De lote"
                            },
                            "22": {
                                "Orden": 23,
                                "Desde": 155,
                                "Hasta": 160,
                                "Long": 6,
                                "Nombre de Campo": "codigo_especial",
                                "Tipo": "N",
                                "Contenido": "Codigo especial para uso del comercio que identifica el movimiento contable"
                            },
                            "23": {
                                "Orden": 24,
                                "Desde": 161,
                                "Hasta": 190,
                                "Long": 30,
                                "Nombre de Campo": "nro_debito",
                                "Tipo": "A",
                                "Contenido": "Nro. De referencia del d\u00e9bito autom\u00e1tico"
                            },
                            "24": {
                                "Orden": 25,
                                "Desde": 191,
                                "Hasta": 607,
                                "Long": 417,
                                "Nombre de Campo": "filler",
                                "Tipo": "A",
                                "Contenido": "Espacios en Blanco"
                            },
                            "25": {
                                "Orden": 26,
                                "Desde": 608,
                                "Hasta": 608,
                                "Long": 1,
                                "Nombre de Campo": "marca",
                                "Tipo": "A",
                                "Contenido": "Tipo de Registro (Siempre \"D\")"
                            }
                        }
                    },
                    "T": {
                        "start": 261,
                        "n_rows": 13,
                        "df": [],
                        "layout": {
                            "0": {
                                "Orden": 1,
                                "Desde": 1,
                                "Hasta": 9,
                                "Long": 9,
                                "Nombre de Campo": "comercio",
                                "Tipo": "N",
                                "Contenido": "Nro. de Comercio"
                            },
                            "1": {
                                "Orden": 2,
                                "Desde": 10,
                                "Hasta": 15,
                                "Long": 6,
                                "Nombre de Campo": "fecha_proceso",
                                "Tipo": "N",
                                "Contenido": "Fecha de Procesamiento de datos (AAMMDD)"
                            },
                            "2": {
                                "Orden": 3,
                                "Desde": 16,
                                "Hasta": 26,
                                "Long": 11,
                                "Nombre de Campo": "cant_movim",
                                "Tipo": "N",
                                "Contenido": "Total de Movimientos Procesados"
                            },
                            "3": {
                                "Orden": 4,
                                "Desde": 27,
                                "Hasta": 37,
                                "Long": 11,
                                "Nombre de Campo": "cant_aceptados",
                                "Tipo": "N",
                                "Contenido": "Total de Movimientos con Estado \" \" y \"X\""
                            },
                            "4": {
                                "Orden": 5,
                                "Desde": 38,
                                "Hasta": 48,
                                "Long": 11,
                                "Nombre de Campo": "cant_rechazados",
                                "Tipo": "N",
                                "Contenido": "Total de Movimientos con Estado \"R\""
                            },
                            "5": {
                                "Orden": 6,
                                "Desde": 49,
                                "Hasta": 49,
                                "Long": 1,
                                "Nombre de Campo": "signo_compra",
                                "Tipo": "A",
                                "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                            },
                            "6": {
                                "Orden": 7,
                                "Desde": 50,
                                "Hasta": 61,
                                "Long": 12,
                                "Nombre de Campo": "total_compra",
                                "Tipo": "N",
                                "Contenido": "Total de Compras (9 enteros, 2 decimales)"
                            },
                            "7": {
                                "Orden": 8,
                                "Desde": 62,
                                "Hasta": 62,
                                "Long": 1,
                                "Nombre de Campo": "signo_entrega",
                                "Tipo": "A",
                                "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                            },
                            "8": {
                                "Orden": 9,
                                "Desde": 63,
                                "Hasta": 74,
                                "Long": 12,
                                "Nombre de Campo": "total_entrega",
                                "Tipo": "N",
                                "Contenido": "Total de Entregas (9 enteros, 2 decimales)"
                            },
                            "9": {
                                "Orden": 10,
                                "Desde": 75,
                                "Hasta": 75,
                                "Long": 1,
                                "Nombre de Campo": "signo_cuota",
                                "Tipo": "A",
                                "Contenido": "\"+\" Signo positivo / \"-\" Signo negativo"
                            },
                            "10": {
                                "Orden": 11,
                                "Desde": 76,
                                "Hasta": 87,
                                "Long": 12,
                                "Nombre de Campo": "total_cuota",
                                "Tipo": "N",
                                "Contenido": "Total de Cuotas (9 enteros, 2 decimales)"
                            },
                            "11": {
                                "Orden": 12,
                                "Desde": 88,
                                "Hasta": 607,
                                "Long": 520,
                                "Nombre de Campo": "filler",
                                "Tipo": "A",
                                "Contenido": "Espacios en Blanco"
                            },
                            "12": {
                                "Orden": 13,
                                "Desde": 608,
                                "Hasta": 608,
                                "Long": 1,
                                "Nombre de Campo": "marca",
                                "Tipo": "A",
                                "Contenido": "Tipo de Registro (Siempre \"T\")"
                            }
                        }
                    }
                }    
        
        def parse_line(line, layout):
            dict_line = {}
            for key, val in layout.items():
                dict_line[val["Nombre de Campo"]] = line[val['Desde'] - 1: val['Desde'] - 1 + val['Long']]
            return dict_line

        layouts =  {
                        'H' : {'start' : 0 , 'n_rows': 41,  'df': []},
                        'P' : {'start' : 56 - 11 , 'n_rows': 83, 'df': []},
                        'F' : {'start' : 143 - 11 , 'n_rows': 95,  'df': []},
                        'D' : {'start' : 242 - 11, 'n_rows': 26,  'df': []},
                        'T' : {'start' : 272 - 11 , 'n_rows': 13,  'df': []}
                    }
        
        loaded = diccionario
        for key in layouts.keys():
            layouts[key] = loaded[key]

        f = file
        for line in f:
            line = line.strip()
            marca = line[-1]
            layouts[marca]['df'].append(parse_line(line, layouts[marca]['layout']))
        df_dicts ={}  

        for key, val in layouts.items():
            
            columns = [val['layout'][i]['Nombre de Campo'] for i in sorted([k for k in val['layout'].keys() ])]
            #pdb.set_trace()
            layouts[key]['df'] = pd.DataFrame(layouts[key]['df'], columns = columns)
            try:
                layouts[key]['df']['head_nro_liquidacion'] =  layouts['H']['df']['nro_liquidacion'][0]
            except:
                layouts[key]['df']['head_nro_liquidacion'] =  float('nan')

            df_dicts[key] = layouts[key]['df']
            
        tipo_tabla = kwargs['tipo_tabla']
        if df_dicts[tipo_tabla].empty:
            df_dicts[tipo_tabla] = df_dicts[tipo_tabla].append(pd.Series(), ignore_index=True)
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        df_dicts[tipo_tabla]['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        df_dicts[tipo_tabla]['file_name'] = filename.split('/')[-1]
        df_dicts[tipo_tabla].reset_index(drop=True)
        df_dicts[tipo_tabla]['skt_extraction_rn'] = df_dicts[tipo_tabla].index.values
        return df_dicts[tipo_tabla]

