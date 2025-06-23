import os
import boto3
import pysftp
import paramiko
import threading

from io import BytesIO
from sqlalchemy import create_engine
from datetime import date, timedelta, datetime


def private_keyGenova():
    llave ="""-----BEGIN PGP PRIVATE KEY BLOCK-----
            ** FAKE ** TEST ** ** FAKE ** TEST ** 
            -----END PGP PRIVATE KEY BLOCK-----
    """
    return llave


class ExtractorGenovaMla:
    @staticmethod
    def run(filename, **kwargs ):
        print('Iniciando Genova')
        text_file = open("/tmp/pk_genova", "w")
        text_file.write(private_keyGenova())
        text_file.close()
        ambiente= os.environ.get('ENVIRONMENT')
        if "test" in ambiente:
            bucket_dest = "global-in-sources-xxxxxxxxxx"
        else:
            bucket_dest = "global-in-sources-xxxxxxxxxx"

        key=paramiko.RSAKey.from_private_key_file("/tmp/pk_genova")
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        s3_client= (boto3.client('s3'))
        yesterday = datetime.today() - timedelta(2)

        users = [
        ("global_prod",["eventos"],"AR/GENOVA/EVENTS/","/ml-sftp/Global-Prod/reconciliations/global/mi/mla/genova/mastercard/","xxxxxx.mercadolibre.xxxxx"),
        ("global_prod",["eventos"],"AR/GENOVA/EVENTS/","/ml-sftp/Global-Prod/reconciliations/global/mi/mla/genova/visa/","xxxxxx.mercadolibre.xxxxx"),
        ("global_prod",["genova-report-mastercard_transaccional"],"AR/GENOVA/REPORT/","/ml-sftp/Global-Prod/reconciliations/global/mi/mla/genova/mastercard/","xxxxxx.mercadolibre.xxxxx"),
        ("global_prod",["genova-report-mastercard_transaccional"],"AR/GENOVA/REPORT/","/ml-sftp/Global-Prod/reconciliations/global/mi/mla/genova/visa/","xxxxxx.mercadolibre.xxxxx"),
        ("global_prod",["genova-report-visa_transaccional"],"AR/GENOVA/VISA/","/ml-sftp/Global-Prod/reconciliations/global/mi/mla/genova/mastercard/","xxxxxx.mercadolibre.xxxxx"),
        ("global_prod",["genova-report-visa_transaccional"],"AR/GENOVA/VISA/","/ml-sftp/Global-Prod/reconciliations/global/mi/mla/genova/visa/","xxxxxx.mercadolibre.xxxxx"),
        ("global_prod",["preadvances_mla"],"AR/GENOVA/PREADVANCES/","/ml-sftp/Global-Prod/reconciliations/global/mi/mla/genova/preadvances/","xxxxxx.mercadolibre.xxxxx"),
        ("global_prod",["genova-report-maestro_transaccional"],"BR/GENOVA/MAESTRO/","/ml-sftp/Global-Prod/reconciliations/global/mi/mla/genova/maestro/","xxxxxx.mercadolibre.xxxxx")
        ]

        # Lógica conexión, descarga, limpieza y subida de archivos
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        for user in users:
            try:
                username = user[0]
                hostname = user[4]
                pref = user[2]
                ssh_client.connect(hostname,username=username,pkey=key)
                with ssh_client.open_sftp() as sftp:
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
                                        if archive.st_size > 20000000:
                                            fl = sftp.open(user[3] + (archive.filename), "rb")
                                            fl.prefetch()
                                            fl.set_pipelined(pipelined=True)
                                            print('Subiendo: ' + user[2] + ts + archive.filename)
                                            s3_client.upload_fileobj(fl,Bucket=bucket_dest,Key=user[2] + ts + archive.filename)
                                        else:
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

