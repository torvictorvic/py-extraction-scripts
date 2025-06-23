import os
import boto3
import pysftp


from io import StringIO, BytesIO
from datetime import date, timedelta, datetime


def private_keyCupones():
    llave ="""-----BEGIN PGP PRIVATE KEY BLOCK-----
            ** FAKE ** TEST ** ** FAKE ** TEST ** 
            -----END PGP PRIVATE KEY BLOCK-----
    """
    return llave

class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        print('Iniciando MELI CUPONES - Test')
        text_file = open("/tmp/pk_cupones", "w")
        text_file.write(private_keyCupones())
        text_file.close()
        ambiente= os.environ.get('ENVIRONMENT')
        if "test" in ambiente:
            bucket_dest = "global-in-sources-xxxxxxxxxx"
        else:
            bucket_dest = "global-in-sources-xxxxxxxxxx"

        s3_client= (boto3.client('s3'))
        yesterday = datetime.today() - timedelta(10)
        users = [("global_comafi_qa",[""],"AR/CUPONES/RAW/","/ml-sftp/global/securitizacion-cupones-qa/mla/comafi/in/","xxxxxx.mercadolibre.xxxxx")
            ,("global_acfin_qa",[""],"CL/CUPONES/","/ml-sftp/global/securitizacion-cupones-qa/mlc/acfin/in/","xxxxxx.mercadolibre.xxxxx")
            ,("global_bval_qa",[""],"AR/CUPONES/RAW/","/ml-sftp/global/securitizacion-cupones-qa/mla/bval/in/","xxxxxx.mercadolibre.xxxxx")
            ,("global_cmf_qa",[""],"AR/CUPONES/RAW/","/ml-sftp/global/securitizacion-cupones-qa/mla/cmf/in/","xxxxxx.mercadolibre.xxxxx")
            ,("global_gpetersen_qa",[""],"AR/CUPONES/RAW/","/ml-sftp/global/securitizacion-cupones-qa/mla/gpetersen/ber/in/","xxxxxx.mercadolibre.xxxxx")
            ,("global_gpetersen_qa",[""],"AR/CUPONES/RAW/","/ml-sftp/global/securitizacion-cupones-qa/mla/gpetersen/bsc/in/","xxxxxx.mercadolibre.xxxxx")
            ,("global_gpetersen_qa",[""],"AR/CUPONES/RAW/","/ml-sftp/global/securitizacion-cupones-qa/mla/gpetersen/bsj/in/","xxxxxx.mercadolibre.xxxxx")
            ,("global_mla_qa",[""],"AR/CUPONES/RAW/","/treasury_coupons_qa/","sftp-qa.mercadolibre.io")
            ,("global_mlc_qa",[""],"CL/CUPONES/","/treasury_analytics_qa/","sftp-qa.mercadolibre.io")]

        # Lógica conexión, descarga, limpieza y subida de archivos
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        for user in users:
            try:
                username = user[0]
                hostname = user[4]
                pref = user[2]
                with pysftp.Connection(host=hostname,username=username,private_key="/tmp/pk_cupones",cnopts=cnopts) as sftp:
                    for archive in sftp.listdir_attr(user[3]):
                        print(archive.filename)
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
                                            print('El archivo ya está meli_cupones ', archive.filename)
                                            esta = True
                                            continue
                                        else:
                                            print('El archivo no está meli_cupones: ', archive.filename)
                                            esta=False
                                    except:
                                        esta=False
                                        pass
                                    if not esta:
                                        with sftp.open(user[3] + (archive.filename), "rb") as fl:
                                            print('melicupones - entró al if no está')
                                            print('Descargando: ', archive.filename)
                                            fl.prefetch(file_size=archive.st_size)
                                            print('pasó meli-cupones carga 1')
                                            b_ncfile = fl.read(archive.st_size)
                                            print('pasó b_ncfile')
                                            datos = BytesIO(b_ncfile)
                                            print('Subiendo: ' + user[2] + ts + archive.filename)
                                            s3_client.put_object(Body=datos.getvalue(),Bucket=bucket_dest,Key=user[2] + ts + archive.filename)
                                            print('archivo subido al s3')
                                        
            except Exception as e:
                print("Error: ",e)
                continue
        df =None
        return df