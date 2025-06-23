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


class Extractor:
    @staticmethod
    def run(filename, **kwargs ):
        print('Iniciando MELI - naranja')
        text_file = open("/tmp/pk", "w")
        text_file.write(private_key())
        text_file.close()
        ambiente= os.environ.get('ENVIRONMENT')
        if "test" in ambiente:
            bucket_dest = "global-in-sources-xxxxxxxxxx"
        else:
            bucket_dest = "global-in-sources-xxxxxxxxxx"

        s3_client= (boto3.client('s3'))
        yesterday = datetime.today() - timedelta(20)

        users = [
            ("global_prod",[""],"","/ml-sftp/Global-Prod/reconciliations/global/mi/mla/naranja/","xxxxxx.mercadolibre.xxxxx")
        ]

        # Lógica conexión, descarga, limpieza y subida de archivos
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        for user in users:
            try:
                username = user[0]
                hostname = user[4]
             
                with pysftp.Connection(host=hostname,username=username,private_key="/tmp/pk",cnopts=cnopts) as sftp:
                    
                    for archive in sftp.listdir_attr(user[3]):
                        dt2_object = datetime.fromtimestamp(archive.st_mtime)
                        if yesterday < dt2_object:
                            ts = str(archive.st_mtime)
                            if "cpa" in archive.filename:

                                objlist = s3_client.list_objects_v2(Bucket=bucket_dest, Prefix= "ARG_LIQUIDACIONES_NARANJA/CPA/")

                                try:
                                    if any( ts + ((archive.filename)).split('/')[-1]  in s['Key'] for s in objlist['Contents']):
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
                                    fl = sftp.open(user[3] + (archive.filename), "rb")
                                    fl.prefetch()
                                    b_ncfile = fl.read(fl.stat().st_size) 
                                    datos = BytesIO(b_ncfile) 
                                    print('Subiendo: ' + user[2] + ts + archive.filename)
                                    s3_client.put_object(Body=datos.getvalue(),Bucket=bucket_dest,Key= "ARG_LIQUIDACIONES_NARANJA/CPA/" + ts + archive.filename)
                            
                    

                            elif "cme" in archive.filename:
                                print(archive.filename)
                                
                                objlist = s3_client.list_objects_v2(Bucket=bucket_dest, Prefix="ARG_LIQUIDACIONES_NARANJA/CME/" )
        
                                try:
                                    if any( ts + ((archive.filename)).split('/')[-1]  in s['Key'] for s in objlist['Contents']):
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
                                    fl = sftp.open(user[3] + (archive.filename), "rb")
                                    fl.prefetch()
                                    b_ncfile = fl.read(fl.stat().st_size) 
                                    datos = BytesIO(b_ncfile) 
                                    print('Subiendo: ' + user[2] + ts + archive.filename)
                                    s3_client.put_object(Body=datos.getvalue(),Bucket=bucket_dest,Key="ARG_LIQUIDACIONES_NARANJA/CME/"  + ts + archive.filename)

            except Exception as e:
                print("Error: ",e)
                continue
            

        df =None
        return df