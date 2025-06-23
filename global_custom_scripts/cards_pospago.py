import os
import boto3
import pysftp

from zipfile import ZipFile
from io import StringIO, BytesIO
from datetime import date, timedelta, datetime

private_keyCond ="""-----BEGIN PGP PRIVATE KEY BLOCK-----
            ** FAKE ** TEST ** ** FAKE ** TEST ** 
            -----END PGP PRIVATE KEY BLOCK-----
    """


hostname= 'xxxxxxxxxx.mercadolibre.xxxxxxxxxxxx'
username = 'xxxxxxxxxx'
route = 'SFTP/PREPAGO/MLB/CONDUCTOR/'
aiming = '/ml-sftp/Global-Prod/reconciliations/global/cards/mlb/conductor/'
name = 'VISA_225_'
s3_client= (boto3.client('s3', region_name='us-east-1'))
yesterday = datetime.today() - timedelta(1)
#pag = s3_client.get_paginator('list_objects_v2')
bucket_dest = "global-cards-sources-test"
#pages = pag.paginate(Bucket=bucket_dest, Prefix=route)


def lista_archivos(cliente, bucket, route):
    """
    cliente -> s3 client, example boto3.client('s3')
    bucket -> Bucket where you want to read file names. example global-cards-sources-test
    prefix -> route inside bucket
    """
    pag = cliente.get_paginator('list_objects_v2')    
    pages = pag.paginate(Bucket=bucket, Prefix=route)
    key_list = []
    for page in pages:
        for obj in page['Contents']:
            key_list.append(obj['Key'])
    return key_list 


class ExtractorConductor:

    @staticmethod
    def run(filename, **kwargs ):
        print('Iniciando conductor')
        text_file = open("/tmp/pk_conductor", "w")
        text_file.write(private_keyCond)
        text_file.close()
        key_list = lista_archivos(s3_client,bucket_dest,route)
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        try:

            with pysftp.Connection(host=hostname,username=username,private_key="/tmp/pk_conductor",cnopts=cnopts) as sftp:
                for archive in sftp.listdir_attr(aiming):
                    dt2_object = datetime.fromtimestamp(archive.st_mtime)
                    # Estas condiciones podrían verse mejor de la siguiente manera:
                    if name in archive.filename and yesterday < dt2_object:
                        
                        
                        #if yesterday < dt2_object:
                        ts = str(archive.st_mtime)
                        if not any( ts + ((archive.filename)).split('/')[-1] in s for s in key_list):
                            fl = sftp.open(aiming + (archive.filename), "rb")
                            fl.prefetch()
                            b_ncfile = fl.read(fl.stat().st_size)
                            datos = BytesIO(b_ncfile)
                            print('Subiendo: ' + route + ts + archive.filename)
                            s3_client.put_object(Body=datos.getvalue(),Bucket=bucket_dest,Key=route + ts + archive.filename)                  
        except Exception as e:
            print("Error:", e)
        print('Termina conductor')