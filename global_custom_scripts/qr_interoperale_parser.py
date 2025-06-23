import pytz
import boto3
import pandas as pd
import numpy as np
from io import TextIOWrapper
from datetime import datetime
from urllib.parse import urlparse
from io import StringIO, BytesIO
from datetime import date, timedelta, datetime
import zipfile
import glob
import os
import os.path
import sys
import time
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
            session = boto3.session.Session()
            s3 = session.client('s3')
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri) as f:
                return uri,datetime.now()
def get_owner(string):
    if string == 'G00':
        return 'G'
    elif string == 'C00':
        return 'C'
    elif string == 'M00':
        return 'M'
    else:
        return ''

# Inicio Parser Sucredito
class ExtractorDetalle:

    
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        #returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        colspecs = [(0,1), (1,15), (15,38), (38,59), (59,62), (62,71), (71,121), (121,124), (124,127), (127,142), (142,157), (157,172), (172,187), (187,202), (202,217),
                   (217,218),(218,219),(219,220),(220,221),(221,236),(236,251),(251,252),(252,253),(253,254),(254,255),(255,270),(270,285),(285,288),(288,289),(289,290),(290,291),
                   (291,292),(292,307),(307,322),(322,323),(323,324),(324,325),(325,326),(326,341),(341,356),(356,359),(359,360),(360,361),(361,362),(362,363),(363,378),(378,393),
                   (393,394),(394,395),(395,396),(396,397),(397,412),(412,427),(427,430)]

        df = pd.read_fwf(file, colspecs=colspecs, header=None, encoding='utf8')
        countRows = 0
        print("For 1...")
        for rows in df[1]:

            print( "Row : " + str(countRows) )
            print( "Lenght : " + str(len(rows)) )
            print( rows )
            countRows = countRows + 1
            print( "------------------------------------------------------" )

            if 'LIQA' in str(rows):
                df['tipo_archivo']='A'
            elif 'LIQB' in str(rows):
                df['tipo_archivo'] = 'B'

            if countRows == 10:
                break

        cadena=''.join((df.loc[0,:]).astype(str))
        # Validation if "nan1LIQA20240...." appears, it is cleared
        # SDFPSINIC-16131
        if cadena.startswith("nan"):
            cadena = cadena.replace("nan", "", 1)

        df['fecha_creacion']=cadena[5:19]
        df['fecha_negocio']=cadena[19:27]

        df.columns = ['Tipo_de_registro', 'Fecha_operacion', 'ID_operacion', 'ID_operacion_original', 'Concepto', 'Id_billetera', 'Billetera_nombre', 'Codigo_banco_pagador', 
                      'Comision', 'Importe_bruto', 'Comision_aceptador', 'Comision_billetera', 'Comision_COELSA', 'Importe_neto', 'Importe_IVA','Aceptador_agente_retencion_IVA',
                      'Aceptador_agente_percepcion_IVA','Aceptador_exclusion_retencion_IVA','Aceptador_exclusion_percepcion_IVA','Aceptador_importe_retencion_IVA',
                      'Aceptador_importe_percepcion_IVA','Aceptador_agente_retencion_IBB','Aceptador_agente_percepcion_IBB','Aceptador_exclusion_retencion_IIBB',
                      'Aceptador_exclusion_percepcion_IIBB','Aceptador_importe_retencion_IIBB','Aceptador_importe_percepcion_IIBB','Jurisdiccion_IIBB_aceptador',
                      'Billetera_agente_retencion_IVA','Billetera_agente_percepcion_IVA','Billetera_exclusion_retencion_IVA','Billetera_exclusion_percepcion_IVA',
                      'Billetera_importe_retencion_IVA','Billetera_importe_percepcion_IVA','Billetera_agente_retencion_IIBB','Billetera_agente_percepcion_IIBB',
                      'Billetera_exclusion_retencion_IIBB','Billetera_exclusion_percepcion_IIBB','Billetera_importe_retencion_IIBB','Billetera_importe_percepcion_IIBB',
                      'Jurisdiccion_IIBB_Billetera','COELSA_agente_retencion_IVA','COELSA_agente_percepcion_IVA','COELSA_exclusion_retencion_IVA','COELSA_exclusion_percepcion_IVA',
                      'COELSA_importe_retencion_IVA','COELSA_importe_percepcion_IVA','COELSA_agente_retencion_IIBB','COELSA_agente_percepcion_IIBB','COELSA_exclusion_retencion_IIBB',
                  'COELSA_exclusion_percepcion_IIBB','COELSA_importe_retencion_IIBB','COELSA_importe_percepcion_IIBB','Jurisdiccion_IIBB_COELSA','tipo_archivo','fecha_creacion','fecha_negocio']

        df['tamano_comercio'] = df["Importe_bruto"].apply(get_owner)
        df = df.replace('',np.nan).ffill()
        df = df[(df["Tipo_de_registro"] == 3)].reset_index(drop=True)
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values

        print("For out - Before return")
        print("Total sd : " + str(len(df)) )
        print(df)

        return df



    @staticmethod
    def runCurrent(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        #returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        colspecs = [(0,1), (1,15), (15,38), (38,59), (59,62), (62,71), (71,121), (121,124), (124,127), (127,142), (142,157), (157,172), (172,187), (187,202), (202,217),
                   (217,218),(218,219),(219,220),(220,221),(221,236),(236,251),(251,252),(252,253),(253,254),(254,255),(255,270),(270,285),(285,288),(288,289),(289,290),(290,291),
                   (291,292),(292,307),(307,322),(322,323),(323,324),(324,325),(325,326),(326,341),(341,356),(356,359),(359,360),(360,361),(361,362),(362,363),(363,378),(378,393),
                   (393,394),(394,395),(395,396),(396,397),(397,412),(412,427),(427,430),(430,444)]
        try:
            print("Ingreso al try")
            df = pd.read_fwf(file, colspecs=colspecs, header=None, encoding='latin1')
            print("lee el archivo")
            for rows in df[1]:
                if 'LIQA' in str(rows):
                    df['tipo_archivo']='A'
                elif 'LIQB' in str(rows):
                    df['tipo_archivo'] = 'B'

            cadena=''.join((df.loc[0,:]).astype(str))
            df['fecha_creacion']=cadena[5:19]
            df['fecha_negocio']=cadena[19:27]

            df.columns = ['Tipo_de_registro', 'Fecha_operacion', 'ID_operacion', 'ID_operacion_original', 'Concepto', 'Id_billetera', 'Billetera_nombre', 'Codigo_banco_pagador', 
                          'Comision', 'Importe_bruto', 'Comision_aceptador', 'Comision_billetera', 'Comision_COELSA', 'Importe_neto', 'Importe_IVA','Aceptador_agente_retencion_IVA',
                          'Aceptador_agente_percepcion_IVA','Aceptador_exclusion_retencion_IVA','Aceptador_exclusion_percepcion_IVA','Aceptador_importe_retencion_IVA',
                          'Aceptador_importe_percepcion_IVA','Aceptador_agente_retencion_IBB','Aceptador_agente_percepcion_IBB','Aceptador_exclusion_retencion_IIBB',
                          'Aceptador_exclusion_percepcion_IIBB','Aceptador_importe_retencion_IIBB','Aceptador_importe_percepcion_IIBB','Jurisdiccion_IIBB_aceptador',
                          'Billetera_agente_retencion_IVA','Billetera_agente_percepcion_IVA','Billetera_exclusion_retencion_IVA','Billetera_exclusion_percepcion_IVA',
                          'Billetera_importe_retencion_IVA','Billetera_importe_percepcion_IVA','Billetera_agente_retencion_IIBB','Billetera_agente_percepcion_IIBB',
                          'Billetera_exclusion_retencion_IIBB','Billetera_exclusion_percepcion_IIBB','Billetera_importe_retencion_IIBB','Billetera_importe_percepcion_IIBB',
                          'Jurisdiccion_IIBB_Billetera','COELSA_agente_retencion_IVA','COELSA_agente_percepcion_IVA','COELSA_exclusion_retencion_IVA','COELSA_exclusion_percepcion_IVA',
                          'COELSA_importe_retencion_IVA','COELSA_importe_percepcion_IVA','COELSA_agente_retencion_IIBB','COELSA_agente_percepcion_IIBB','COELSA_exclusion_retencion_IIBB',
                      'COELSA_exclusion_percepcion_IIBB','COELSA_importe_retencion_IIBB','COELSA_importe_percepcion_IIBB','Jurisdiccion_IIBB_COELSA','provider_id','tipo_archivo','fecha_creacion','fecha_negocio']

            df['tamano_comercio'] = df["Importe_bruto"].apply(get_owner)
            df = df.replace('',np.nan).ffill()
            df = df[(df["Tipo_de_registro"] == 3)].reset_index(drop=True)

            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            print("retorno df")
            return df
        except Exception as e:
            print("Error al subir la fuente: ", e)