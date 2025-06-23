import pytz
import json
import boto3
import pandas as pd

from io import BytesIO
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

            # session = boto3.Session(profile_name="sts")
            # s3 = session.client('s3')
            session = boto3.session.Session(region_name='us-east-1')
            s3 = session.client('s3')

            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri) as f:
                return uri,datetime.now()

class StringConverter(dict):
    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return str

    def get(self, default=None):
        return str


#Inicio parser FirstData

class FirstdataProcessor():

    def __init__(self, json_dict, data_frame, registry, write = False):
        self.json = json_dict
        #self.lines = lines
        self.df = data_frame
        self.registry_to_return = registry
        self.write = write
        #self.file_name = file_name

    def read_by_json(self, json_list, lines):
        columns = {col['name']: [] for col in json_list}
        for line in lines:
            line = str(line).strip()
            for col in json_list:
                columns[col['name']].append(line[int(col['start']) - 1: int(col['start']) + int(col['lon']) - 1])

        return columns

    def clasificar_filas(self):

        registros = {
        '1': [],
        '2': [],
        '3': [],
        '6': [],
        '7': [],
        '8': [],
        '9': []
                    }
        registros_df = {
        '1': [],
        '2': [],
        '3': [],
        '6': [],
        '7': [],
        '8': [],
        '9': []
                    }
        if self.df.empty:
            for i in registros:
                registros[i].append("")
            for key, val in registros.items():

                if key == self.registry_to_return:
                    registros_df[key] = pd.DataFrame(self.read_by_json(self.json[key], val))
                    registros_df[key] = registros_df[key].replace("", float('nan'))

        else:

            for row in self.df.iterrows():

                registros[row[1].values[0][0]].append(row[1].values[0][:])
            for key, val in registros.items():

                if key == self.registry_to_return:
                    registros_df[key] = pd.DataFrame(self.read_by_json(self.json[key], val))

        return registros_df


class FirstdataExtractor():

    def __init__(self, data_frame, json_file_path, registry, write = False):
        self.json_file = json_file_path
        self.fields_descriptions = self.read_json_file()
        #self.data_file = data_file_path
        self.data_frame = data_frame
        #self.file_lines = self.read_data_file()
        self.registry_to_return = registry
        self.write = write
        #self.file_name = data_file_path.split('/')[-1]

    def read_json_file(self):
        '''
        returns a dict
        '''
        field_descriptions = json.loads(self.json_file)
        # with open(self.json_file) as file:
        #     field_descriptions = json.load(file)
        return field_descriptions

    def read_data_file(self):
        '''
        returns a list of lists (lines)
        '''
        with open(self.data_file) as file:
            lines = file.readlines()

        return lines

    def run(self):
        proc =  FirstdataProcessor(self.fields_descriptions, self.data_frame, self.registry_to_return, self.write )
        dataFrames = proc.clasificar_filas()


        return dataFrames[self.registry_to_return]

class Extractor():

    @staticmethod
    def run(filename, **kwargs):
        text_file = """{
            "1": [
                {
                    "start": "001",
                    "lon": "001",
                    "name": "TIPO_REGISTRO"
                },
                {
                    "start": "002",
                    "lon": "006",
                    "name": "NOMBRE_ARCHIVO"
                },
                {
                    "start": "008",
                    "lon": "008",
                    "name": "COMERCIO_CENTRALIZADOR"
                },
                {
                    "start": "016",
                    "lon": "001",
                    "name": "PRODUCTO"
                },
                {
                    "start": "017",
                    "lon": "003",
                    "name": "MONEDA"
                },
                {
                    "start": "020",
                    "lon": "002",
                    "name": "GRUPO_DE_PRESENTACION"
                },
                {
                    "start": "022",
                    "lon": "002",
                    "name": "PLAZO_DE_PAGO"
                },
                {
                    "start": "024",
                    "lon": "001",
                    "name": "TIPO_DE_PLAZO_DE_PAGO"
                },
                {
                    "start": "025",
                    "lon": "008",
                    "name": "FECHA_DE_PRESENTACION"
                },
                {
                    "start": "033",
                    "lon": "008",
                    "name": "FECHA_VENCIMIENTO_CLEARING"
                }
            ],
            "2": [
                {
                    "start": "001",
                    "lon": "001",
                    "name": "TIPO_REGISTRO"
                },
                {
                    "start": "002",
                    "lon": "006",
                    "name": "NOMBRE_ARCHIVO"
                },
                {
                    "start": "008",
                    "lon": "008",
                    "name": "COMERCIO_CENTRALIZADOR"
                },
                {
                    "start": "016",
                    "lon": "001",
                    "name": "PRODUCTO"
                },
                {
                    "start": "017",
                    "lon": "003",
                    "name": "MONEDA"
                },
                {
                    "start": "020",
                    "lon": "002",
                    "name": "GRUPO_DE_PRESENTACION"
                },
                {
                    "start": "022",
                    "lon": "002",
                    "name": "PLAZO_DE_PAGO"
                },
                {
                    "start": "024",
                    "lon": "001",
                    "name": "TIPO_DE_PLAZO_DE_PAGO"
                },
                {
                    "start": "025",
                    "lon": "008",
                    "name": "FECHA_DE_PRESENTACION"
                },
                {
                    "start": "033",
                    "lon": "008",
                    "name": "FECHA_VENCIMIENTO_CLEARING"
                },
                {
                    "start": "041",
                    "lon": "008",
                    "name": "COMERCIO_PARTICIPANTE"
                },
                {
                    "start": "049",
                    "lon": "003",
                    "name": "ENTIDAD_PAGADORA"
                },
                {
                    "start": "052",
                    "lon": "003",
                    "name": "SUCURSAL_PAGADORA"
                },
                {
                    "start": "055",
                    "lon": "007",
                    "name": "NUMERO_LIQUIDACION"
                },
                {
                    "start": "062",
                    "lon": "001",
                    "name": "MARCA_VENTA_DE_LIQUID"
                },
                {
                    "start": "063",
                    "lon": "001",
                    "name": "MARCA_ACUERDO_MINOR_MAY"
                },
                {
                    "start": "064",
                    "lon": "013",
                    "name": "RUT_COMERCIO_MINOR_MAY"
                },
                {
                    "start": "077",
                    "lon": "001",
                    "name": "PROVINCIA_DEL_COMERCIO"
                },
                {
                    "start": "078",
                    "lon": "001",
                    "name": "PROVINCIA_DE_ING_BRUTOS"
                },
                {
                    "start": "079",
                    "lon": "013",
                    "name": "CUIT_RUT"
                }
            ],
            "3": [
                {
                    "start": "001",
                    "lon": "001",
                    "name": "TIPO_REGISTRO"
                },
                {
                    "start": "002",
                    "lon": "006",
                    "name": "NOMBRE_ARCHIVO"
                },
                {
                    "start": "008",
                    "lon": "008",
                    "name": "COMERCIO_CENTRALIZADOR"
                },
                {
                    "start": "016",
                    "lon": "001",
                    "name": "PRODUCTO"
                },
                {
                    "start": "017",
                    "lon": "003",
                    "name": "MONEDA"
                },
                {
                    "start": "020",
                    "lon": "002",
                    "name": "GRUPO_DE_PRESENTACION"
                },
                {
                    "start": "022",
                    "lon": "002",
                    "name": "PLAZO_DE_PAGO"
                },
                {
                    "start": "024",
                    "lon": "001",
                    "name": "TIPO_DE_PLAZO_DE_PAGO"
                },
                {
                    "start": "025",
                    "lon": "008",
                    "name": "FECHA_DE_PRESENTACION"
                },
                {
                    "start": "033",
                    "lon": "008",
                    "name": "F_VENCIMIENTO_CLEARING"
                },
                {
                    "start": "041",
                    "lon": "008",
                    "name": "COMERCIO_PARTICIPANTE"
                },
                {
                    "start": "049",
                    "lon": "003",
                    "name": "ENTIDAD_PAGADORA"
                },
                {
                    "start": "052",
                    "lon": "003",
                    "name": "SUCURSAL_PAGADORA"
                },
                {
                    "start": "055",
                    "lon": "007",
                    "name": "NUMERO_LIQUIDACION"
                },
                {
                    "start": "062",
                    "lon": "008",
                    "name": "FECHA_OPERACION"
                },
                {
                    "start": "070",
                    "lon": "003",
                    "name": "CODIGO_MOVIMIENTO"
                },
                {
                    "start": "073",
                    "lon": "001",
                    "name": "CODIGO_DE_ORIGEN"
                },
                {
                    "start": "074",
                    "lon": "009",
                    "name": "CAJA_NRO_C_POSNT"
                },
                {
                    "start": "083",
                    "lon": "009",
                    "name": "CARATULA_TERMINAL_POSNET"
                },
                {
                    "start": "092",
                    "lon": "003",
                    "name": "RESUMEN_LOTE_POSNET"
                },
                {
                    "start": "095",
                    "lon": "005",
                    "name": "CUPON_CUPON_POSNET"
                },
                {
                    "start": "100",
                    "lon": "002",
                    "name": "CUOTAS_PLAN"
                },
                {
                    "start": "102",
                    "lon": "002",
                    "name": "CUOTA_VIGENTE"
                },
                {
                    "start": "104",
                    "lon": "013",
                    "name": "IMPORTE_TOTAL"
                },
                {
                    "start": "117",
                    "lon": "001",
                    "name": "SIGNO1"
                },
                {
                    "start": "118",
                    "lon": "013",
                    "name": "IMPORTE_SIN_DTO"
                },
                {
                    "start": "131",
                    "lon": "001",
                    "name": "SIGNO2"
                },
                {
                    "start": "132",
                    "lon": "013",
                    "name": "IMPORTE_FINAL"
                },
                {
                    "start": "145",
                    "lon": "001",
                    "name": "SIGNO3"
                },
                {
                    "start": "146",
                    "lon": "005",
                    "name": "PORC_DESCUENTO"
                },
                {
                    "start": "151",
                    "lon": "001",
                    "name": "MARCA_ERROR"
                },
                {
                    "start": "152",
                    "lon": "001",
                    "name": "TIPO_PLAN_CUOTAS"
                },
                {
                    "start": "153",
                    "lon": "019",
                    "name": "NRO_DE_TARJETA"
                },
                {
                    "start": "172",
                    "lon": "003",
                    "name": "MOTIVO_DE_RECHAZO_1"
                },
                {
                    "start": "175",
                    "lon": "003",
                    "name": "MOTIVO_DE_RECHAZO_2"
                },
                {
                    "start": "178",
                    "lon": "003",
                    "name": "MOTIVO_DE_RECHAZO_3"
                },
                {
                    "start": "181",
                    "lon": "003",
                    "name": "MOTIVO_DE_RECHAZO_4"
                },
                {
                    "start": "184",
                    "lon": "008",
                    "name": "FECHA_DE_PRESENT_ORIGINAL"
                },
                {
                    "start": "192",
                    "lon": "002",
                    "name": "MOTIVO_REVERSION"
                },
                {
                    "start": "194",
                    "lon": "002",
                    "name": "TIPO_DE_OPERACION"
                },
                {
                    "start": "196",
                    "lon": "001",
                    "name": "MARCA_CAMPANA"
                },
                {
                    "start": "197",
                    "lon": "003",
                    "name": "CODIGO_DE_CARGO_PAGO"
                },
                {
                    "start": "200",
                    "lon": "003",
                    "name": "ENTIDAD_EMISORA"
                },
                {
                    "start": "203",
                    "lon": "009",
                    "name": "IMPORTE_ARANCEL"
                },
                {
                    "start": "212",
                    "lon": "001",
                    "name": "SIGNO4"
                },
                {
                    "start": "213",
                    "lon": "009",
                    "name": "IVA_ARANCEL"
                },
                {
                    "start": "222",
                    "lon": "001",
                    "name": "SIGNO5"
                },
                {
                    "start": "223",
                    "lon": "001",
                    "name": "PROMOCION_CUOTAS"
                },
                {
                    "start": "224",
                    "lon": "005",
                    "name": "TNA"
                },
                {
                    "start": "229",
                    "lon": "009",
                    "name": "IMPORTE_COSTO_FINANCIERO"
                },
                {
                    "start": "238",
                    "lon": "001",
                    "name": "SIGNO6"
                },
                {
                    "start": "239",
                    "lon": "009",
                    "name": "IVA_COSTO_FINANCIERO"
                },
                {
                    "start": "248",
                    "lon": "001",
                    "name": "SIGNO7"
                },
                {
                    "start": "249",
                    "lon": "005",
                    "name": "PORCENTAJE_TASA_DIRECTA"
                },
                {
                    "start": "254",
                    "lon": "009",
                    "name": "IMPORTE_COSTO_TASA_DTA"
                },
                {
                    "start": "263",
                    "lon": "001",
                    "name": "SIGNO8"
                },
                {
                    "start": "264",
                    "lon": "009",
                    "name": "IVA_COSTO_TASA_DTA"
                },
                {
                    "start": "273",
                    "lon": "001",
                    "name": "SIGNO9"
                },
                {
                    "start": "274",
                    "lon": "008",
                    "name": "NRO_AUTORIZ"
                },
                {
                    "start": "282",
                    "lon": "005",
                    "name": "ALICUOTA_IVA_FO"
                },
                {
                    "start": "287",
                    "lon": "001",
                    "name": "MARCA_CASHBACK"
                },
                {
                    "start": "288",
                    "lon": "003",
                    "name": "RESUMEN_LOTE_ORIG"
                },
                {
                    "start": "291",
                    "lon": "001",
                    "name": "MARCA_CUPON_RIESGO"
                }
            ],
            "6": [
                {
                    "start": "001",
                    "lon": "001",
                    "name": "TIPO_REGISTRO"
                },
                {
                    "start": "002",
                    "lon": "006",
                    "name": "NOMBRE_ARCHIVO"
                },
                {
                    "start": "008",
                    "lon": "008",
                    "name": "COMERCIO_CENTRALIZADOR"
                },
                {
                    "start": "016",
                    "lon": "001",
                    "name": "PRODUCTO"
                },
                {
                    "start": "017",
                    "lon": "003",
                    "name": "MONEDA"
                },
                {
                    "start": "020",
                    "lon": "002",
                    "name": "GRUPO_DE_PRESENTACION"
                },
                {
                    "start": "022",
                    "lon": "002",
                    "name": "PLAZO_DE_PAGO"
                },
                {
                    "start": "024",
                    "lon": "001",
                    "name": "TIPO_DE_PLAZO_DE_PAGO"
                },
                {
                    "start": "025",
                    "lon": "008",
                    "name": "FECHA_DE_PRESENTACION"
                },
                {
                    "start": "033",
                    "lon": "008",
                    "name": "F_VENCIMIENTO_CLEARING"
                },
                {
                    "start": "041",
                    "lon": "008",
                    "name": "COMERCIO_PARTICIPANTE"
                },
                {
                    "start": "049",
                    "lon": "003",
                    "name": "ENTIDAD_PAGADORA"
                },
                {
                    "start": "052",
                    "lon": "003",
                    "name": "SUCURSAL_PAGADORA"
                },
                {
                    "start": "055",
                    "lon": "007",
                    "name": "NUMERO_LIQUIDACION"
                },
                {
                    "start": "062",
                    "lon": "013",
                    "name": "TOTAL_IMPORTE_PRESENTADO"
                },
                {
                    "start": "075",
                    "lon": "001",
                    "name": "SIGNO1"
                },
                {
                    "start": "076",
                    "lon": "013",
                    "name": "NETO_LIQUID_ORIGINAL"
                },
                {
                    "start": "089",
                    "lon": "001",
                    "name": "SIGNO2"
                },
                {
                    "start": "090",
                    "lon": "013",
                    "name": "DESCUENTO_DE_FINANCIACION"
                },
                {
                    "start": "103",
                    "lon": "001",
                    "name": "SIGNO3"
                },
                {
                    "start": "104",
                    "lon": "013",
                    "name": "IMPUESTO_LEY_25063"
                },
                {
                    "start": "117",
                    "lon": "001",
                    "name": "SIGNO4"
                },
                {
                    "start": "118",
                    "lon": "013",
                    "name": "IVA_DTO_FINANCIACION"
                },
                {
                    "start": "131",
                    "lon": "001",
                    "name": "SIGNO5"
                },
                {
                    "start": "132",
                    "lon": "013",
                    "name": "PERCEP_IVA_RG_3337"
                },
                {
                    "start": "145",
                    "lon": "001",
                    "name": "SIGNO6"
                },
                {
                    "start": "146",
                    "lon": "013",
                    "name": "PERCEPCION_ING_BRUTOS"
                },
                {
                    "start": "159",
                    "lon": "001",
                    "name": "SIGNO7"
                },
                {
                    "start": "160",
                    "lon": "013",
                    "name": "NETO_AL_COMERCIO"
                },
                {
                    "start": "173",
                    "lon": "001",
                    "name": "SIGNO8"
                },
                {
                    "start": "174",
                    "lon": "174",
                    "name": "FEC_PAGO_VENTA_LIQUID"
                },
                {
                    "start": "182",
                    "lon": "013",
                    "name": "SELLADOS_VENTA_LIQUID"
                },
                {
                    "start": "195",
                    "lon": "001",
                    "name": "SIGNO9"
                }
            ],
            "7": [
                {
                    "start": "001" ,
                    "lon": "001",
                    "name": "TIPO_REGISTRO"
                },
                {
                    "start": "002" ,
                    "lon": "006",
                    "name": "NOMBRE_ARCHIVO"
                },
                {
                    "start": "008" ,
                    "lon": "008",
                    "name": "COMERCIO_CENTRALIZADOR"
                },
                {
                    "start": "016" ,
                    "lon": "001",
                    "name": "PRODUCTO"
                },
                {
                    "start": "017" ,
                    "lon": "003",
                    "name": "MONEDA"
                },
                {
                    "start": "020" ,
                    "lon": "002",
                    "name": "GRUPO_DE_PRESENTACION"
                },
                {
                    "start": "022" ,
                    "lon": "002",
                    "name": "PLAZO_DE_PAGO"
                },
                {
                    "start": "024" ,
                    "lon": "001",
                    "name": "TIPO_DE_PLAZO_DE_PAGO"
                },
                {
                    "start": "025" ,
                    "lon": "008",
                    "name": "FECHA_DE_PRESENTACION"
                },
                {
                    "start": "033" ,
                    "lon": "008",
                    "name": "F_VENCIMIENTO_CLEARING"
                },
                {
                    "start": "041" ,
                    "lon": "008",
                    "name": "COMERCIO_PARTICIPANTE"
                },
                {
                    "start": "049" ,
                    "lon": "003",
                    "name": "ENTIDAD_PAGADORA"
                },
                {
                    "start": "052" ,
                    "lon": "003",
                    "name": "SUCURSAL_PAGADORA"
                },
                {
                    "start": "055" ,
                    "lon": "007",
                    "name": "NUMERO_LIQUIDACION"
                },
                {
                    "start": "062" ,
                    "lon": "013",
                    "name": "TOTAL_IMPORTE_TOTAL"
                },
                {
                    "start": "075" ,
                    "lon": "001",
                    "name": "SIGNO1"
                },
                {
                    "start": "076" ,
                    "lon": "013",
                    "name": "TOTAL_IMPORTE_SIN_DTO"
                },
                {
                    "start": "089" ,
                    "lon": "001",
                    "name": "SIGNO2"
                },
                {
                    "start": "090" ,
                    "lon": "013",
                    "name": "TOTAL_IMPORTE_FINAL"
                },
                {
                    "start": "103" ,
                    "lon": "001",
                    "name": "SIGNO3"
                },
                {
                    "start": "104" ,
                    "lon": "013",
                    "name": "ARANCELES_CTO_FIN"
                },
                {
                    "start": "117" ,
                    "lon": "001",
                    "name": "SIGNO4"
                },
                {
                    "start": "118" ,
                    "lon": "013",
                    "name": "RETENCIONES_FISCALES"
                },
                {
                    "start": "131" ,
                    "lon": "001",
                    "name": "SIGNO5"
                },
                {
                    "start": "132" ,
                    "lon": "013",
                    "name": "OTROS_DEBITOS"
                },
                {
                    "start": "145" ,
                    "lon": "001",
                    "name": "SIGNO6"
                },
                {
                    "start": "146" ,
                    "lon": "013",
                    "name": "OTROS_CREDITOS"
                },
                {
                    "start": "159" ,
                    "lon": "001",
                    "name": "SIGNO7"
                },
                {
                    "start": "160" ,
                    "lon": "013",
                    "name": "NETO_AL_COMERCIOS"
                },
                {
                    "start": "173" ,
                    "lon": "001",
                    "name": "SIGNO8"
                },
                {
                    "start": "174",
                    "lon": "007",
                    "name": "TOTAL_REGISTROS_DETALLE"
                },
                {
                    "start": "181",
                    "lon": "013",
                    "name": "MONTO_PEND_DE_CUOTAS"
                },
                {
                    "start": "194",
                    "lon": "001",
                    "name": "SIGNO9"
                }
            ],
            "8": [
                {
                    "start": "001" ,
                    "lon": "001",
                    "name": "TIPO_REGISTRO"
                },
                {
                    "start": "002" ,
                    "lon": "006",
                    "name": "NOMBRE_ARCHIVO"
                },
                {
                    "start": "008" ,
                    "lon": "008",
                    "name": "COMERCIO_CENTRALIZADOR"
                },
                {
                    "start": "016" ,
                    "lon": "001",
                    "name": "PRODUCTO"
                },
                {
                    "start": "017" ,
                    "lon": "003",
                    "name": "MONEDA"
                },
                {
                    "start": "020" ,
                    "lon": "002",
                    "name": "GRUPO_DE_PRESENTACION"
                },
                {
                    "start": "022" ,
                    "lon": "002",
                    "name": "PLAZO_DE_PAGO"
                },
                {
                    "start": "024" ,
                    "lon": "001",
                    "name": "TIPO_DE_PLAZO_DE_PAGO"
                },
                {
                    "start": "025" ,
                    "lon": "008",
                    "name": "FECHA_DE_PRESENTACION"
                },
                {
                    "start": "033" ,
                    "lon": "008",
                    "name": "F_VENCIMIENTO_CLEARING"
                },
                {
                    "start": "041" ,
                    "lon": "008",
                    "name": "COMERCIO_PARTICIPANTE"
                },
                {
                    "start": "049" ,
                    "lon": "003",
                    "name": "ENTIDAD_PAGADORA"
                },
                {
                    "start": "052" ,
                    "lon": "003",
                    "name": "SUCURSAL_PAGADORA"
                },
                {
                    "start": "055" ,
                    "lon": "007",
                    "name": "NUMERO_LIQUIDACION"
                },
                {
                    "start": "062" ,
                    "lon": "002",
                    "name": "SUBTIPO_DE_REGISTRO"
                },
                {
                    "start": "064" ,
                    "lon": "013",
                    "name": "IVA_ARANCELES_R_I"
                },
                {
                    "start": "077" ,
                    "lon": "001",
                    "name": "SIGNO1"
                },
                {
                    "start": "078" ,
                    "lon": "013",
                    "name": "IMPUESTO_DEB_CRED"
                },
                {
                    "start": "091" ,
                    "lon": "001",
                    "name": "SIGNO2"
                },
                {
                    "start": "092" ,
                    "lon": "013",
                    "name": "IVA_DTO_PAGO_ANTICIPADO"
                },
                {
                    "start": "105" ,
                    "lon": "001",
                    "name": "SIGNO3"
                },
                {
                    "start": "106" ,
                    "lon": "013",
                    "name": "RET_IVA_VENTAS"
                },
                {
                    "start": "119" ,
                    "lon": "001",
                    "name": "SIGNO4"
                },
                {
                    "start": "120" ,
                    "lon": "013",
                    "name": "PERCEPCION_ANTICIPADA_IVA"
                },
                {
                    "start": "133" ,
                    "lon": "001",
                    "name": "SIGNO5"
                },
                {
                    "start": "134" ,
                    "lon": "013",
                    "name": "RET_IMP_GANANCIAS"
                },
                {
                    "start": "147" ,
                    "lon": "001",
                    "name": "SIGNO6"
                },
                {
                    "start": "148" ,
                    "lon": "013",
                    "name": "RET_IMP_INGRESOS_BRUTOS"
                },
                {
                    "start": "161" ,
                    "lon": "001",
                    "name": "SIGNO7"
                },
                {
                    "start": "162" ,
                    "lon": "013",
                    "name": "PERCEP_INGR_BRUTOS"
                },
                {
                    "start": "175" ,
                    "lon": "001",
                    "name": "SIGNO8"
                },
                {
                    "start": "176" ,
                    "lon": "013",
                    "name": "IVA_SERVICIOS"
                },
                {
                    "start": "189" ,
                    "lon": "001",
                    "name": "SIGNO9"
                },
                {
                    "start": "190" ,
                    "lon": "001",
                    "name": "CATEGORIA_IVA"
                },
                {
                    "start": "191" ,
                    "lon": "009",
                    "name": "IMP_S__RESESLEY"
                },
                {
                    "start": "200" ,
                    "lon": "001",
                    "name": "SIGNO10"
                },
                {
                    "start": "201" ,
                    "lon": "011",
                    "name": "ARANCEL"
                },
                {
                    "start": "212" ,
                    "lon": "001",
                    "name": "SIGNO11"
                },
                {
                    "start": "213" ,
                    "lon": "011",
                    "name": "COSTO_FINANCIERO"
                },
                {
                    "start": "224" ,
                    "lon": "001",
                    "name": "SIGNO12"
                },
                {
                    "start": "225" ,
                    "lon": "009",
                    "name": "RETENCION_AFAM"
                },
                {
                    "start": "234" ,
                    "lon": "001",
                    "name": "SIGNO13"
                },
                {
                    "start": "235" ,
                    "lon": "011",
                    "name": "INGRESOS_BRUTOS_CORDOBA"
                },
                {
                    "start": "246" ,
                    "lon": "001",
                    "name": "SIGNO14"
                },
                {
                    "start": "247" ,
                    "lon": "011",
                    "name": "SALDO_DEUDOR"
                },
                {
                    "start": "258" ,
                    "lon": "001",
                    "name": "SIGNO15"
                },
                {
                    "start": "259" ,
                    "lon": "011",
                    "name": "SELLADOS"
                },
                {
                    "start": "270" ,
                    "lon": "001",
                    "name": "SIGNO16"
                }
            ],
            "9": [
                {
                    "start": "001" ,
                    "lon": "001",
                    "name": "TIPO_REGISTRO"
                },
                {
                    "start": "002" ,
                    "lon": "006",
                    "name": "NOMBRE_ARCHIVO"
                },
                {
                    "start": "008" ,
                    "lon": "008",
                    "name": "COMERCIO_CENTRALIZADOR"
                },
                {
                    "start": "016" ,
                    "lon": "001",
                    "name": "PRODUCTO"
                },
                {
                    "start": "017" ,
                    "lon": "003",
                    "name": "MONEDA"
                },
                {
                    "start": "020" ,
                    "lon": "002",
                    "name": "GRUPO_DE_PRESENTACION"
                },
                {
                    "start": "022" ,
                    "lon": "002",
                    "name": "PLAZO_DE_PAGO"
                },
                {
                    "start": "024" ,
                    "lon": "001",
                    "name": "TIPO_DE_PLAZO_DE_PAGO"
                },
                {
                    "start": "025" ,
                    "lon": "008",
                    "name": "FECHA_DE_PRESENTACION"
                },
                {
                    "start": "033" ,
                    "lon": "008",
                    "name": "F_VENCIMIENTO_CLEARING"
                },
                {
                    "start": "041" ,
                    "lon": "013",
                    "name": "TOTAL_GRAL_IMPORTE_TOTAL"
                },
                {
                    "start": "054" ,
                    "lon": "001",
                    "name": "SIGNO1"
                },
                {
                    "start": "055" ,
                    "lon": "013",
                    "name": "TOTAL_GRAL_IMPORTE_SIN"
                },
                {
                    "start": "068" ,
                    "lon": "001",
                    "name": "SIGNO2"
                },
                {
                    "start": "069" ,
                    "lon": "013",
                    "name": "TOTAL_GRAL_IMPORTE_FINAL"
                },
                {
                    "start": "082" ,
                    "lon": "001",
                    "name": "SIGNO3"
                },
                {
                    "start": "083" ,
                    "lon": "013",
                    "name": "TOTAL_GRAL_ARANC_Y_C_FIN"
                },
                {
                    "start": "096" ,
                    "lon": "001",
                    "name": "SIGNO4"
                },
                {
                    "start": "097" ,
                    "lon": "013",
                    "name": "TOTAL_GRAL_RETENC_FISC"
                },
                {
                    "start": "110" ,
                    "lon": "001",
                    "name": "SIGNO5"
                },
                {
                    "start": "111" ,
                    "lon": "013",
                    "name": "TOTAL_GRAL_OTROS_DEBITOS"
                },
                {
                    "start": "124" ,
                    "lon": "001",
                    "name": "SIGNO6"
                },
                {
                    "start": "125" ,
                    "lon": "013",
                    "name": "TOTAL_GRAL_OTROS_CREDITOS"
                },
                {
                    "start": "138" ,
                    "lon": "001",
                    "name": "SIGNO7"
                },
                {
                    "start": "139" ,
                    "lon": "013",
                    "name": "TOTAL_GRAL_NETO_A"
                },
                {
                    "start": "152" ,
                    "lon": "001",
                    "name": "SIGNO8"
                },
                {
                    "start": "153" ,
                    "lon": "007",
                    "name": "TOTAL_GRAL_REG_DETALLE"
                },
                {
                    "start": "160" ,
                    "lon": "007",
                    "name": "TOTAL_GRAL_REG_TRAILER"
                },
                {
                    "start": "167" ,
                    "lon": "013",
                    "name": "TOTAL_GRAL_MONTO"
                },
                {
                    "start": "180" ,
                    "lon": "001",
                    "name": "SIGNO9"
                },
                {
                    "start": "181" ,
                    "lon": "013",
                    "name": "TOTAL_GRAL_ARANCEL"
                },
                {
                    "start": "194" ,
                    "lon": "001",
                    "name": "SIGNO10"
                },
                {
                    "start": "195" ,
                    "lon": "013",
                    "name": "TOTAL_GRAL_CTO_FINANCIERO"
                },
                {
                    "start": "208" ,
                    "lon": "001",
                    "name": "SIGNO11"
                }
            ]
        }"""
        """REGS: {
        '1': 'Header de Comercio Centralizador',
        '2': 'Header de LiquidaciÃ³n a Comercio Participante',
        '3': 'Registro de Detalle de LiquidaciÃ³n a Comercio Participante',
        '6': 'Trailer de Venta de LiquidaciÃ³n',
        '7': 'Trailer de LiquidaciÃ³n a Comercio Participante (BÃ¡sico)',
        '8': 'Trailer de LiquidaciÃ³n a Comercio Participante (Impuestos)',
        '9': 'Trailer de Comercio Centralizador'
        }"""

        #json_object = json.dumps(text_file).encode('utf-8')
        json_object = text_file
        tipo = kwargs['tipo_tabla']
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,header=None)
        except pd.io.common.EmptyDataError:
            df = pd.DataFrame()
            df = df.append(pd.Series(), ignore_index=True)
        ext = FirstdataExtractor(json_file_path = json_object, data_frame = df, registry = tipo, write = True)
        df_fd = ext.run()
        if df_fd.empty:
            df_fd = df_fd.append(pd.Series(), ignore_index=True)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        upload_date = lm.astimezone(new_timezone)
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        df_fd['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df_fd['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df_fd['file_name'] = out
        df_fd.reset_index(drop=True)
        df_fd['skt_extraction_rn'] = df_fd.index.values
        df_fd['original_filename'] = out
        return df_fd

class ExtractorCirculares():
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        upload_date = lm.astimezone(new_timezone)
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        df = pd.read_excel(file, engine='pyxlsb',dtype=str)
        if "Auditoria" in filename:
            ls_columns = ["fecha_pago","cod_comercio","nombre_comercio","nro_liquidacion","cod_marca_producto","fecha_operacion","fecha_presentacion","cuota_plan","nro_cupon","codigo_movimiento","nro_tarjeta","moti_rev_concepto","desc_tipo_financiacion","cuota_vigente","desc_marca_producto","nro_autorizacion","imp_con_descuento"]
        elif "cupones" in filename:
            ls_columns = ["fecha_pago","cod_comercio","nombre_comercio","nro_liquidacion","cod_marca_producto","fecha_operacion","fecha_presentacion","cuota_plan","nro_cupon","codigo_movimiento","nro_tarjeta","moti_rev_concepto","desc_tipo_financiacion","cuota_vigente","desc_marca_producto","nro_autorizacion","imp_con_descuento", 'detalle']
        else:
            ls_columns = ["fecha_pago","cod_comercio","nombre_comercio","nro_liquidacion","cod_marca_producto","fecha_operacion","fecha_presentacion","cuota_plan","nro_cupon","codigo_movimiento","bin","moti_rev_concepto","desc_tipo_financiacion","cuota_vigente","desc_marca_producto","nro_autorizacion","imp_con_descuento"]
        df.columns = ls_columns
        df = df.dropna(subset=['fecha_pago', 'cod_comercio'])
        fechas = ["fecha_pago","fecha_operacion","fecha_presentacion"]
        for fecha in fechas:
            result = list(map(lambda n: datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(n) - 2), df[fecha]))
            df[fecha]  = result
            df[fecha] = df[fecha].astype(str)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df

class ExtractorCircularesTXT():
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_csv(file,dtype=str, sep='\\t', engine='python',encoding='utf-16')
        ls_columns = ["fecha_operacion","fecha_clearing","cod_comercio","bin_tj","fin_tj","cod_movimiento","nro_cupon","cod_ent_pagadora","cod_ent_emisora","nro_terminal","des_movimiento","fecha_presentacion","imp_local_total"]
        columns = []
        columns = df.columns
        columns = [x.lower() for x in columns]
        columns = [x.replace(" ","_") for x in columns]
        columns = [x.replace("á","a") for x in columns]
        columns = [x.replace("é","e") for x in columns]
        columns = [x.replace("í","i") for x in columns]
        columns = [x.replace("ó","o") for x in columns]
        columns = [x.replace("ú","u") for x in columns]
        df.columns = columns

        if len(df.columns) > len(ls_columns):
            df = df[df.columns.intersection(ls_columns)]

        if list(df.columns) != ls_columns:
            df = df.reindex(columns=ls_columns)

        df.columns = ls_columns
        df['imp_local_total'] = df.imp_local_total.str.replace('.' , '')
        df['imp_local_total'] = df.imp_local_total.str.replace('$' , '')
        df['imp_local_total'] = df.imp_local_total.str.replace(',' , '.')
        df['imp_local_total'] = df.imp_local_total.str.replace(' ' , '')
        df['nro_terminal'] = df.nro_terminal.str.replace('.' , '')
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

class ExtractorCircularesTXTConceptos():
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_csv(file,dtype=str, sep='\\t', engine='python',encoding='utf-16')
        df.columns = ["fecha_presentacion","fecha_pago","cod_concepto_clearing","desc_concepto_clearing","nro_liquidacion","cod_entidad_pagadora","cod_sucursal_pagadora","cod_comercio","nombre_comercio","cod_marca_producto","importe_concepto","cuit_ruc_num"]
        df['importe_concepto'] = df.importe_concepto.str.replace('.' , '')
        df['importe_concepto'] = df.importe_concepto.str.replace('$' , '')
        df['importe_concepto'] = df.importe_concepto.str.replace(',' , '.')
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

class ExtractorCircularesConceptosDic():
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        df = pd.read_excel(file, engine='pyxlsb',dtype=str)
        ls_columns = ["fecha_presentacion","fecha_pago","cod_clearing","desc_clearing","nro_liquidacion","cod_sucursal_pagadora", "cod_comercio","nombre_comercio","cod_entidad_pagadora","cod_marca_producto","importe_concepto","cuit_ruc_num"]
        df.columns = ls_columns
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        upload_date = lm.astimezone(new_timezone)
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        fechas = ["fecha_pago","fecha_presentacion"]
        df.fillna(0,inplace=True)
        for fecha in fechas:
            result = list(map(lambda n: datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(n) - 2), df[fecha]))
            df[fecha]  = result
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        df['original_filename'] = out
        for col in ["cod_comercio", "nombre_comercio", "nro_liquidacion", "cod_marca_producto"]:
            df = df[(df[col]!=0)]
        return df


class ExtractorCircularesV3():

    @staticmethod

    def run(filename, **kwargs):

        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')

        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        upload_date = lm.astimezone(new_timezone)
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        df = pd.read_excel(file, engine='pyxlsb',dtype=str)


        ls_columns = [
            "fecha_operacion",
            "fecha_presentacion",
            "fecha_pago",
            'cod_comercio',
            'nro_cupon',
            'nro_autorizacion',
            'imp_con_descuento',
            'cuota_plan',
            'cuota_vigente',
            'nro_liquidacion',
            'financing_type_desc',
            'tarjeta_enmascarada',
            'marca_producto'
        ]
        
        df.columns = ls_columns

        #df = df.dropna(subset=['fecha_pago', 'cod_comercio'])

        fechas = ["fecha_operacion","fecha_presentacion","fecha_pago"]

        for fecha in fechas:

            result = list(map(lambda n: datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(n) - 2), df[fecha]))
            df[fecha]  = result
            df[fecha] = df[fecha].astype(str)

        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')

        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values

        return df

class ExtractorCircularesV4():
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        upload_date = lm.astimezone(new_timezone)
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        df = pd.read_excel(file, dtype=str)
        ls_columns = ["fecha_operacion","fecha_presentacion","fecha_pago",'cod_comercio','nro_cupon','nro_autorizacion','imp_con_descuento','imp_descuento','cuota_plan','cuota_vigente','nro_liquidacion','financing_type_desc','tarjeta_enmascarada','marca_producto']     
        df.columns = ls_columns
        #df = df.dropna(subset=['fecha_pago', 'cod_comercio'])
        fechas = ["fecha_operacion","fecha_presentacion","fecha_pago"]
        #for fecha in fechas:
            #result = list(map(lambda n: datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(n) - 2), df[fecha]))
            #df[fecha]  = result
            #df[fecha] = df[fecha].astype(str)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = (filename.split('/')[-1])
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df