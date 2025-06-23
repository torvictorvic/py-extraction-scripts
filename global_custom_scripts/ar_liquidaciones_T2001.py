import pandas as pd
from pandas import DataFrame
from io import BytesIO,StringIO,TextIOWrapper
from enum import Enum
from urllib.parse import urlparse
from datetime import date, timedelta, datetime
import pytz
import boto3
import os
import patoolib
import shutil
import tempfile
#from rarfile import RarFile
#from unrar import rarfile
import os

#import zipfile as zf


class RecordType(Enum):
    HEADER = 1
    BODY = 2
    TRAILER = 3

class RulesT2001:

    rules = [
        {
            "record_type": 1,
            "layout": [
                {
                    "start_at": 1,
                    "ends_at": 1,
                    "column_name": "tipo_resgistro_h"
                },
                {
                    "start_at": 2,
                    "ends_at": 6,
                    "column_name": "marca_h"
                },
                {
                    "start_at": 7,
                    "ends_at": 11,
                    "column_name": "entidad_h"
                },
                {
                    "start_at": 12,
                    "ends_at": 19,
                    "column_name": "fecha_proceso_h"
                },
                {
                    "start_at": 20,
                    "ends_at": 27,
                    "column_name": "hora_proceso_h"
                },
                {
                    "start_at": 28,
                    "ends_at": 35,
                    "column_name": "archivo_h"
                },
                {
                    "start_at": 36,
                    "ends_at": 65,
                    "column_name": "descripcion_archivo_h"
                },
                {
                    "start_at": 66,
                    "ends_at": 70,
                    "column_name": "filler_h"
                }
            ]
        },
        {
            "record_type": 2,
            "layout": [
                {
                    "start_at": 1,
                    "ends_at": 1500,
                    "column_name": "raw_body"
                },
                {
                    "start_at": 1,
                    "ends_at": 1,
                    "column_name": "tipo_resgitro"
                },
                {
                    "start_at": 2,
                    "ends_at": 17,
                    "column_name": "numero_tarjeta"
                },
                {
                    "start_at": 18,
                    "ends_at": 22,
                    "column_name": "marca"
                },
                {
                    "start_at": 23,
                    "ends_at": 27,
                    "column_name": "entidad_emisora"
                },
                {
                    "start_at": 28,
                    "ends_at": 32,
                    "column_name": "sucursal"
                },
                {
                    "start_at": 33,
                    "ends_at": 42,
                    "column_name": "numero_cuenta"
                },
                {
                    "start_at": 43,
                    "ends_at": 44,
                    "column_name": "tipo_socio"
                },
                {
                    "start_at": 45,
                    "ends_at": 47,
                    "column_name": "grupo_cuenta_corriente"
                },
                {
                    "start_at": 48,
                    "ends_at": 50,
                    "column_name": "tipo_transaccion"
                },
                {
                    "start_at": 51,
                    "ends_at": 65,
                    "column_name": "numero_comercio"
                },
                {
                    "start_at": 66,
                    "ends_at": 87,
                    "column_name": "nombre_fantasia_comercio"
                },
                {
                    "start_at": 88,
                    "ends_at": 97,
                    "column_name": "codigo_postal_comercio"
                },
                {
                    "start_at": 98,
                    "ends_at": 103,
                    "column_name": "codigo_autorizacion"
                },
                {
                    "start_at": 104,
                    "ends_at": 114,
                    "column_name": "ica_adquirente"
                },
                {
                    "start_at": 115,
                    "ends_at": 119,
                    "column_name": "codigo_movimiento"
                },
                {
                    "start_at": 120,
                    "ends_at": 149,
                    "column_name": "descripcion_movimiento"
                },
                {
                    "start_at": 150,
                    "ends_at": 172,
                    "column_name": "comprobante"
                },
                {
                    "start_at": 173,
                    "ends_at": 174,
                    "column_name": "tipo_plan"
                },
                {
                    "start_at": 175,
                    "ends_at": 177,
                    "column_name": "plan_cuotas"
                },
                {
                    "start_at": 178,
                    "ends_at": 180,
                    "column_name": "numero_cuota_vigente"
                },
                {
                    "start_at": 181,
                    "ends_at": 188,
                    "column_name": "fecha_operacion"
                },
                {
                    "start_at": 189,
                    "ends_at": 194,
                    "column_name": "hora_operacion"
                },
                {
                    "start_at": 195,
                    "ends_at": 202,
                    "column_name": "fecha_presentacion"
                },
                {
                    "start_at": 203,
                    "ends_at": 210,
                    "column_name": "fecha_cierre_cuenta_corriente"
                },
                {
                    "start_at": 211,
                    "ends_at": 218,
                    "column_name": "fecha_clearing"
                },
                {
                    "start_at": 219,
                    "ends_at": 226,
                    "column_name": "fecha_diferimiento"
                },
               
                {
                    "start_at": 227,
                    "ends_at": 231,
                    "column_name": "mcc"
                },
                {
                    "start_at": 232,
                    "ends_at": 234,
                    "column_name": "codigo_moneda"
                },
                {
                    "start_at": 235,
                    "ends_at": 248,
                    "column_name": "importe_modena_movimiento"
                },
                {
                    "start_at":249,
                    "ends_at":251,
                    "column_name":"codigo_moneda_original"
                }
                ,{
                    "start_at":252,
                    "ends_at":265,
                    "column_name":"importe_moneda_original"
                }
                ,{
                    "start_at":266,
                    "ends_at":279,
                    "column_name":"importe_interes"
                }
                ,{
                    "start_at":280,
                    "ends_at":293,
                    "column_name":"importe_iva"
                }
                ,{
                    "start_at":294,
                    "ends_at":307,
                    "column_name":"importe_total_movimiento"
                }
                ,{
                    "start_at":308,
                    "ends_at":321,
                    "column_name":"importe_descuento_financ_otrog"
                }
                ,{
                    "start_at":322,
                    "ends_at":335,
                    "column_name":"importe_descuento_financ_otrog_iva"
                }
                ,{
                    "start_at":336,
                    "ends_at":349,
                    "column_name":"importe_cuota"
                }
                ,{
                    "start_at":350,
                    "ends_at":363,
                    "column_name":"importe_interes_cuota"
                }
                ,{
                    "start_at":364,
                    "ends_at":377,
                    "column_name":"importe_iva_cuota"
                }
                ,{
                    "start_at":378,
                    "ends_at":391,
                    "column_name":"importe_total_cuota"
                }
                ,{
                    "start_at":392,
                    "ends_at":392,
                    "column_name":"DEBCRED"
                }
                ,{
                    "start_at":393,
                    "ends_at":393,
                    "column_name":"tipo_amortizacion"
                }
                ,{
                    "start_at":394,
                    "ends_at":394,
                    "column_name":"filler"
                }
                ,{
                    "start_at":395,
                    "ends_at":403,
                    "column_name":"TNA"
                }
                ,{
                    "start_at":404,
                    "ends_at":412,
                    "column_name":"TEA"
                }
                ,{
                    "start_at":413,
                    "ends_at":421,
                    "column_name":"tasa_intercambio"
                }
                ,{
                    "start_at":422,
                    "ends_at":434,
                    "column_name":"arancel_emisor"
                }
                ,{
                    "start_at":435,
                    "ends_at":435,
                    "column_name":"signo_iva_arancel"
                }
                ,{
                    "start_at":436,
                    "ends_at":448,
                    "column_name":"iva_arancel_emisor"
                }
                ,{
                    "start_at":449,
                    "ends_at":449,
                    "column_name":"signo_iva_arancel_2"
                }
                ,{
                    "start_at":450,
                    "ends_at":453,
                    "column_name":"motivo_mensaje"
                }
                ,{
                    "start_at":454,
                    "ends_at":466,
                    "column_name":"importe_compensacion"
                }
                ,{
                    "start_at":467,
                    "ends_at":467,
                    "column_name":"signo_importe_compensacion"
                }
                ,{
                    "start_at":468,
                    "ends_at":480,
                    "column_name":"importe_percep_rg4240"
                }
                ,{
                    "start_at":481,
                    "ends_at":481,
                    "column_name":"signo_percep_rg4240"
                }
              
                ,{
                    "start_at":482,
                    "ends_at":484,
                    "column_name":"tipo_producto"
                }
                ,{
                    "start_at":485,
                    "ends_at":492,
                    "column_name":"fecha_cierre_comercios"
                }
                ,{
                    "start_at":493,
                    "ends_at":495,
                    "column_name":"plan"
                }
                ,{
                    "start_at":496,
                    "ends_at":498,
                    "column_name":"moneda_compensacion"
                }
                ,{
                    "start_at":499,
                    "ends_at":511,
                    "column_name":"importe_autorizacion"
                }
                ,{
                    "start_at":512,
                    "ends_at":512,
                    "column_name":"signo_importe_autorizacion"
                }
                ,{
                    "start_at":513,
                    "ends_at":515,
                    "column_name":"moneda_autorizacion"
                }
                ,{
                    "start_at":516,
                    "ends_at":528,
                    "column_name":"importe_autorizacion_convertido"
                }
                ,{
                    "start_at":529,
                    "ends_at":529,
                    "column_name":"signo_importe_autorizacion_convertido"
                }
                ,{
                    "start_at":530,
                    "ends_at":532,
                    "column_name":"moneda_autorizacion_convertido"
                }
                ,{
                    "start_at":533,
                    "ends_at":545,
                    "column_name":"importe_ipm_d6"
                }
                 ,{
                    "start_at":546,
                    "ends_at":546,
                    "column_name":"signo_importe_ipm_d6"
                } 
                ,{
                    "start_at":547,
                    "ends_at":549,
                    "column_name":"moneda_ipm_d351"
                }
                ,{
                    "start_at":550,
                    "ends_at":559,
                    "column_name":"autoid"
                }    
                ,{
                    "start_at":560,
                    "ends_at":569,
                    "column_name":"autocodi"
                }
                ,{
                    "start_at":570,
                    "ends_at":1069,
                    "column_name":"cuenta_externa"
                }
                ,{
                    "start_at":1070,
                    "ends_at":1119,
                    "column_name":"request_id"
                }
                ,{
                    "start_at":1198,
                    "ends_at":1198,
                    "column_name":"extracash"
                }
                ,{
                    "start_at":1199,
                    "ends_at":1211,
                    "column_name":"importe_extracash"
                }
                ,{
                    "start_at":1212,
                    "ends_at":1212,
                    "column_name":"signo"
                }
                ,{
                    "start_at":1227,
                    "ends_at":1227,
                    "column_name":"local_Internacional"
                }
                ,{
                    "start_at":1120,
                    "ends_at":1500,
                    "column_name":"filler_2"
                }
                ,{
                    "start_at":1246,
                    "ends_at":1281,
                    "column_name":"bari_id"
                }    
            ]
        },
        {
            "record_type": 3,
            "layout": [
                {
                    "start_at": 1,
                    "ends_at": 1,
                    "column_name": "tipo_registro_t"
                },
                {
                    "start_at": 2,
                    "ends_at": 2,
                    "column_name": "entidad_t"
                },
                {
                    "start_at": 3,
                    "ends_at": 7,
                    "column_name": "cant_reg_t"
                },
                {
                    "start_at": 19,
                    "ends_at": 1500,
                    "column_name": "filler_t"
                }
            ]
        }
    ]
                                 




class T2001Parser:

    io: str
    type: RecordType

    def __init__(self, io, _type):
        self.io = io
        self.type = _type

    def parse(self):
        data = []
        manifest = [dic for dic in RulesT2001.rules if dic['record_type']==self.type.value][0]
        for line in self.io:
            item = {}
            for col in manifest['layout']:
                item[col['column_name']] = line[col['start_at'] - 1: col['ends_at']]
            data.append(item)
        return data

    def run(self) -> DataFrame:
        data = self.parse()
        # Construct dataframe
        return pd.DataFrame(data)
class Extractor:
    
    def run(self,filename, **kwargs):
        file,lm = self.file.body, self.file.last_modified
        filename= self.file.key
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        #rar_file = rarfile.RarFile(BytesIO(file))
        if '.rar' in filename.lower():
            with open('archivo.rar', 'wb') as f:
                f.write(file)
            rar_file=patoolib.extract_archive("archivo.rar")
            with open(rar_file,encoding='latin1') as f:
                raw_data = f.readlines()
            os.remove(rar_file)
            os.remove('archivo.rar')
        elif '.txt' in filename.lower():
            raw_data=BytesIO(file).readlines()
        
        df =None
        formato = ['raw_body', 'tipo_resgitro', 'numero_tarjeta', 'marca',
                    'entidad_emisora', 'sucursal', 'numero_cuenta', 'tipo_socio',
                    'grupo_cuenta_corriente', 'tipo_transaccion', 'numero_comercio',
                    'nombre_fantasia_comercio', 'codigo_postal_comercio',
                    'codigo_autorizacion', 'ica_adquirente', 'codigo_movimiento',
                    'descripcion_movimiento', 'comprobante', 'tipo_plan', 'plan_cuotas',
                    'numero_cuota_vigente', 'fecha_operacion', 'hora_operacion',
                    'fecha_presentacion', 'fecha_cierre_cuenta_corriente', 'fecha_clearing',
                    'fecha_diferimiento', 'mcc', 'codigo_moneda',
                    'importe_modena_movimiento', 'codigo_moneda_original',
                    'importe_moneda_original', 'importe_interes', 'importe_iva',
                    'importe_total_movimiento', 'importe_descuento_financ_otrog',
                    'importe_descuento_financ_otrog_iva', 'importe_cuota',
                    'importe_interes_cuota', 'importe_iva_cuota', 'importe_total_cuota',
                    'DEBCRED', 'tipo_amortizacion', 'filler', 'TNA', 'TEA',
                    'tasa_intercambio', 'arancel_emisor', 'signo_iva_arancel',
                    'iva_arancel_emisor', 'signo_iva_arancel_2', 'motivo_mensaje',
                    'importe_compensacion', 'signo_importe_compensacion',
                    'importe_percep_rg4240', 'signo_percep_rg4240', 'tipo_producto',
                    'fecha_cierre_comercios', 'plan', 'moneda_compensacion',
                    'importe_autorizacion', 'signo_importe_autorizacion',
                    'moneda_autorizacion', 'importe_autorizacion_convertido',
                    'signo_importe_autorizacion_convertido',
                    'moneda_autorizacion_convertido', 'importe_ipm_d6',
                    'signo_importe_ipm_d6', 'moneda_ipm_d351', 'autoid', 'autocodi',
                    'cuenta_externa', 'request_id', 'extracash', 'importe_extracash',
                    'signo', 'local_Internacional', 'filler_2', 'bari_id', 'tipo_resgistro_h',
                    'marca_h', 'entidad_h', 'fecha_proceso_h', 'hora_proceso_h',
                    'archivo_h', 'descripcion_archivo_h', 'filler_h', 'tipo_registro_t',
                    'entidad_t', 'cant_reg_t', 'filler_t']
        try:

            final = []
            for raw in raw_data:
                final.append(raw)
            df_header = T2001Parser([final[0].strip()], RecordType.HEADER).run()
            df_trailer = T2001Parser([final[-1].strip()], RecordType.TRAILER).run()
            new_raw_data = []
            for line in final[1:-1]:
                new_raw_data.append(line.strip())
            df_body = T2001Parser(new_raw_data, RecordType.BODY).run()
            df_final = df_body.join(df_header)
            df_final[df_header.columns] = df_final[df_header.columns].ffill()
            df = df_final.join(df_trailer)
            df[df_trailer.columns] = df[df_trailer.columns].ffill()
            df.columns = formato
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            
            print("Parseo exitoso")
            return df 
        except Exception as e:
            print("Error al subir la fuente: ",e)