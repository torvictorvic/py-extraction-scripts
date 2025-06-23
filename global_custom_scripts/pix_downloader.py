import os
import boto3
import pysftp
import paramiko
import threading

from io import BytesIO
from sqlalchemy import create_engine
from datetime import date, timedelta, datetime

def private_key():
    llave ="""-----BEGIN PGP PRIVATE KEY BLOCK-----
            ** FAKE ** TEST ** ** FAKE ** TEST ** 
            -----END PGP PRIVATE KEY BLOCK-----
    """
    return llave

class ExtractorPix:
    @staticmethod
    def run(filename, **kwargs ):
        print('Iniciando Pix-bacen')
        text_file = open("/tmp/pk", "w")
        text_file.write(private_key())
        text_file.close()
        ambiente= os.environ.get('ENVIRONMENT')
        if "test" in ambiente:
            bucket_dest = "global-in-sources-xxxxxxxxxx"
        else:
            bucket_dest = "global-in-sources-xxxxxxxxxx"

        s3_client= (boto3.client('s3'))
        yesterday = datetime.today() - timedelta(10)


        # Lógica conexión, descarga, limpieza y subida de archivos
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        users = [
                    ("global_prod",[""],"BR/PRUEBA_PIX/","/ml-sftp/Global-Prod/reconciliations/global/transfers/mlb/pix/","xxxxxx.mercadolibre.xxxxx")
                ]
        for user in users:
            try:
                username = user[0]
                hostname = user[4]
             
                with pysftp.Connection(host=hostname,username=username,private_key="/tmp/pk",cnopts=cnopts) as sftp:
                    
                    for archive in sftp.listdir_attr(user[3]):
                        for prefix in user[1]:
                            if prefix in archive.filename:
                                dt2_object = datetime.fromtimestamp(archive.st_mtime)
                                if yesterday < dt2_object:
                                    ts = str(archive.st_mtime)
                                    pag = s3_client.get_paginator('list_objects_v2')
                                    pages = pag.paginate(Bucket=bucket_dest, Prefix=user[2])
                                    key_list = []
                                    for page in pages:
                                        for obj in page['Contents']:
                                            key_list.append(obj['Key'])

                                    try:
                                        if any(archive.filename in s for s in key_list):
                                            print('El archivo ya está', archive.filename)
                                            esta = True
                                            continue
                                        else:
                                            esta=False
                                            print("el archivo no está")
                                    except:
                                        esta=False
                                        print("el archivo no está")
                                        pass
                                    
                                    if not esta:                         
                                        fl = sftp.open((user[3] + archive.filename), "rb")
                                        fl.prefetch()
                                        b_ncfile = fl.read(fl.stat().st_size) 
                                        datos = BytesIO(b_ncfile) 
                                        print('Subiendo: ' + user[2] + archive.filename)
                                        s3_client.put_object(Body=datos.getvalue(),Bucket=bucket_dest,Key= user[2] + archive.filename)
                                          

            except Exception as e:
                print("Error: ",e)
                continue
            

        df =None
        return df
        
class ExtractorBacen:
    @staticmethod
    def run(filename, **kwargs ):
        print('Iniciando Pix-bacen')
        text_file = open("/tmp/pk", "w")
        text_file.write(private_key())
        text_file.close()
        ambiente= os.environ.get('ENVIRONMENT')
        if "test" in ambiente:
            bucket_dest = "global-in-sources-xxxxxxxxxx"
        else:
            bucket_dest = "global-in-sources-xxxxxxxxxx"

        s3_client= (boto3.client('s3'))
        yesterday = datetime.today() - timedelta(7)

        users = [
            ("global_prod",[""],"BR/PRUEBA_PIX/","/ml-sftp/Global-Prod/reconciliations/global/transfers/mlb/bacen/","xxxxxx.mercadolibre.xxxxx")
        ]

        # Lógica conexión, descarga, limpieza y subida de archivos
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        for user in users:
            try:
                username = user[0]
                hostname = user[4]
                pref = user[2]
                with pysftp.Connection(host=hostname,username=username,private_key="/tmp/pk",cnopts=cnopts) as sftp:
                    for archive in sftp.listdir_attr(user[3]):
                        #print(archive.filename)
                        for prefix in user[1]:
                            if prefix in archive.filename:
                            
                                dt2_object = datetime.fromtimestamp(archive.st_mtime)
                                if yesterday < dt2_object:
                                    ts = str(archive.st_mtime)
                                    pag = s3_client.get_paginator('list_objects_v2')
                                    pages = pag.paginate(Bucket=bucket_dest, Prefix=pref)
                                    key_list = []
                                    for page in pages:
                                        for obj in page['Contents']:
                                            key_list.append(obj['Key'])
                                    try:
                                        if any((archive.filename).split('/')[-1]  in s for s in key_list):
                                            print('El archivo ya está', archive.filename)
                                            esta = True
                                            continue
                                        else:
                                            esta=False
                                            print("el archivo no está", archive.filename)
                                    except:
                                        esta=False
                                        print("el archivo no está", archive.filename)
                                        pass 
                                    
                                    if not esta:                         
                                        fl = sftp.open((user[3]+archive.filename), "rb")
                                        fl.prefetch()
                                        b_ncfile = fl.read(fl.stat().st_size) 
                                        datos = BytesIO(b_ncfile) 
                                        print('Subiendo: ' + user[2]  + archive.filename)
                                        s3_client.put_object(Body=datos.getvalue(),Bucket=bucket_dest,Key=user[2] + archive.filename)
            except Exception as e:
                print("Error: ",e)
                continue
        df =None
        return df