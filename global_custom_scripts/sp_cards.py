import os
import boto3
import pysftp
import paramiko
import threading

from io import BytesIO
from sqlalchemy import create_engine
from datetime import date, timedelta, datetime


def private_keypospago():
    llave ="""-----BEGIN PGP PRIVATE KEY BLOCK-----
            ** FAKE ** TEST ** ** FAKE ** TEST ** 
            -----END PGP PRIVATE KEY BLOCK-----
    """
    return llave


class ExtractorPospago:
    print('Hola_1')
    @staticmethod
    def run(filename, **kwargs ):
        print('Iniciando pago_facil')
        text_file = open("/tmp/pk_pospago", "w")
        text_file.write(private_keypospago())
        text_file.close()
        ambiente= os.environ.get('BACKEND_HOSTNAME')
        if "prueba" in ambiente:
            bucket_dest = "global-sp-sources-test"
        else:
            bucket_dest = "global-sp-sources-production"

        s3_client= (boto3.client('s3'))
        yesterday = datetime.today() - timedelta(7)

        users = [
        ("pagofacil-servicios-mp",["PF"],"SFTP/RECARGAS/MLA/PRUEBA/","/utility-pagofacil/liquidations/","singleplayer.providersml.com")
        ]

        # Lógica conexión, descarga, limpieza y subida de archivos
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        for user in users:
            try:
                username = user[0]
                hostname = user[4]
                pref = user[2]
                with pysftp.Connection(host=hostname,username=username,private_key="/tmp/pk_pospago",cnopts=cnopts) as sftp:
                    for archive in sftp.listdir_attr(user[3]):
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
                                        fl = sftp.open(user[3] + (archive.filename), "rb")
                                        fl.prefetch()
                                        b_ncfile = fl.read(fl.stat().st_size)
                                        datos = BytesIO(b_ncfile)
                                        print('Subiendo: ' + user[2] + ts + archive.filename)
                                        s3_client.put_object(Body=datos.getvalue(),Bucket=bucket_dest,Key=user[2] + ts + archive.filename)
            except Exception as e:
                print("Error: ",e)
                continue
        df =None
        return df
