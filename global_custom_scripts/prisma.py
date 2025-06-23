import os
import boto3
import pysftp

from zipfile import ZipFile
from io import StringIO, BytesIO
from datetime import date, timedelta, datetime

def private_key():
    llave ="""-----BEGIN PGP PRIVATE KEY BLOCK-----
            ** FAKE ** TEST ** ** FAKE ** TEST ** 
            -----END PGP PRIVATE KEY BLOCK-----
    """
    return llave

def private_keypospago():
    llave ="""-----BEGIN PGP PRIVATE KEY BLOCK-----
            ** FAKE ** TEST ** ** FAKE ** TEST ** 
            -----END PGP PRIVATE KEY BLOCK-----
    """
    return llave



class Extractor:
    @staticmethod
    def run(filename, **kwargs ):
        print('Iniciando Prisma')
        text_file = open("/tmp/pk_prisma_meli", "w")
        text_file.write(private_key())
        text_file.close()
        ambiente= os.environ.get('ENVIRONMENT')
        print('el ambiente es',ambiente)
        if "test" in ambiente:
            bucket_dest = "global-in-sources-xxxxxxxxxx"
        else:
            bucket_dest = "global-in-sources-xxxxxxxxxx"
        s3_client= (boto3.client('s3')) 

        pag = s3_client.get_paginator('list_objects_v2')
        pages = pag.paginate(Bucket=bucket_dest, Prefix='ARG_LIQUIDACIONES_PRISMA/')
        pages_a = pag.paginate(Bucket=bucket_dest, Prefix='ARG_AGENDA_PRISMA/')
        pages_b = pag.paginate(Bucket=bucket_dest, Prefix='ARG_PAGOS_PRISMA/')
        page_ls = [pages, pages_a, pages_b]
        key_list = []
        for pages in page_ls:
            for page in pages:
                for obj in page['Contents']:
                    key_list.append(obj['Key'])

        #Credenciales Firstdata SFTP
        actual = date.today() 
        yesterday = date.today() - timedelta(7)
        hostname= 'ctp-fileserver-001.centralpos.com'
        username = 'mercadolibre'
        password = 'dai1anoh5naiGoo'
        
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        
        with pysftp.Connection(host=hostname,username=username,password=password,cnopts=cnopts) as sftp:
            for archive in sftp.listdir_attr('mercadolibre/liquidaciones/'): 
                dt2_object = datetime.fromtimestamp(archive.st_mtime).strftime('%Y-%m-%d')
                if (  ('autorizaciones' in str(archive.filename)) | ('movimientos' in str(archive.filename)) | ('pagos' in str(archive.filename)) )   :
                    if  (actual.strftime("%Y-%m-%d") in dt2_object) | (yesterday.strftime("%Y-%m-%d") in dt2_object)  :                     
                        ts = str(archive.st_mtime)
                        print('subiendo',ts + archive.filename)
                        try:
                            if any( (ts + archive.filename).split('.')[0] in s for s in key_list):
                                print('El archivo ya está', archive.filename)
                                esta = True
                                continue
                            elif any( ("prisma||" + archive.filename).split('.')[0] in s for s in key_list):
                                print('El archivo ya está', archive.filename)
                                esta = True
                                continue
                            else:
                                esta=False
                        except:
                            esta=False
                            pass
                        if not esta:                         
                            print('Extrayendo', archive.filename)
                            fl = sftp.open('mercadolibre/liquidaciones/' + (archive.filename), "rb")
                            fl.prefetch()
                            b_ncfile = fl.read(fl.stat().st_size)
                            datos = BytesIO(b_ncfile) 
                            zip_file = ZipFile(datos)
                            
                            for text_file in zip_file.infolist():
                                datos = BytesIO(zip_file.open(text_file.filename).read())
                                if( ('autorizaciones') in str(archive.filename)):
                                    s3_client.put_object(Body=datos.getvalue(),Bucket=bucket_dest,Key='ARG_AGENDA_PRISMA/' + ts + text_file.filename)
                                elif ('movimientos' in str(archive.filename)):
                                    s3_client.put_object(Body=datos.getvalue(),Bucket=bucket_dest,Key='ARG_LIQUIDACIONES_PRISMA/' + ts + text_file.filename)
                                elif( ('pagos') in str(archive.filename)):
                                    s3_client.put_object(Body=datos.getvalue(),Bucket=bucket_dest,Key='ARG_PAGOS_PRISMA/' + ts + text_file.filename)
                    
        print('Terminado Prisma')
        df =None
        return df


class ExtractorPospago:
    @staticmethod
    def run(filename, **kwargs ):
        print('Iniciando pagofacil')
        text_file = open("/tmp/pk_pospago", "w")
        text_file.write(private_keypospago())
        text_file.close()
        ambiente= os.environ.get('ENVIRONMENT')
        if "test" in ambiente:
            bucket_dest = "global-sp-sources-test"
        else:
            bucket_dest = "global-sp-sources-production"

        s3_client= (boto3.client('s3'))
        yesterday = datetime.today() - timedelta(7)

        users = [
        ("pagofacil-servicios-mp",["PF"],"SFTP/RECARGAS/MLA/PAGOFACIL_POSPAGO/","/utility-pagofacil/liquidations/","singleplayer.providersml.com")
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
        print('Terminado pagofacil')
        df =None
        return df