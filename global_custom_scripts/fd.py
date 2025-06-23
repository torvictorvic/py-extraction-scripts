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


def private_keyGenova():
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

def private_keyCond():
    llave ="""-----BEGIN PGP PRIVATE KEY BLOCK-----
            ** FAKE ** TEST ** ** FAKE ** TEST ** 
            -----END PGP PRIVATE KEY BLOCK-----
    """
    return llave


class Extractor:
    @staticmethod
    def run(filename, **kwargs ):
        print('Iniciando Firstada')
        text_file = open("/tmp/pk_firstdata", "w")
        text_file.write(private_key())
        text_file.close()
        ambiente= os.environ.get('ENVIRONMENT')
        if "test" in ambiente:
            bucket_dest = "global-in-sources-xxxxxxxxxx"
            download_file = "available/"
            download_file_2 = "downloaded/"
        else:
            bucket_dest = "global-in-sources-xxxxxxxxxx"
            download_file = "available/"
            download_file_2 = "downloaded/" 
        s3_client= (boto3.client('s3'))

        #Credenciales Firstdata SFTP
        def multifactor_auth(host, port, username, key, password):
            #Create an SSH transport configured to the host
            transport = paramiko.Transport((host, port))
            #Negotiate an SSH2 session
            transport.connect()
            #Attempt authenticating using a private key
            transport.auth_publickey(username, key)
            #Create an event for password auth
            password_auth_event = threading.Event()
            #Create password auth handler from transport
            password_auth_handler = paramiko.auth_handler.AuthHandler(transport)
            #Set transport auth_handler to password handler
            transport.auth_handler = password_auth_handler
            #Aquire lock on transport
            transport.lock.acquire()
            #Register the password auth event with handler
            password_auth_handler.auth_event = password_auth_event
            #Set the auth handler method to 'password'
            password_auth_handler.auth_method = 'password'
            #Set auth handler username
            password_auth_handler.username = username
            #Set auth handler password
            password_auth_handler.password = password
            #Create an SSH user auth message
            userauth_message = paramiko.message.Message()
            userauth_message.add_string('ssh-userauth')
            userauth_message.rewind()
            #Make the password auth attempt
            password_auth_handler._parse_service_accept(userauth_message)
            #Release lock on transport
            transport.lock.release()
            #Wait for password auth response
            password_auth_handler.wait_for_response(password_auth_event)
            #Create an open SFTP client channel
            transport.default_max_packet_size = 100000000
            transport.packetizer.REKEY_BYTES = pow(2, 40)  # 1TB max, this is a security degradation!
            transport.packetizer.REKEY_PACKETS = pow(2, 40)
            #transport.default_window_size=paramiko.common.MAX_WINDOW_SIZE
            #return transport.open_sftp_client()
            return paramiko.SFTPClient.from_transport(transport)


        yesterday = datetime.today() - timedelta(1)
        hostname = 'prod2-gw-lac.firstdataclients.com'
        username = 'LAGW-GASNA001'
        password = 'm@Y/Ty52C'
        file_path = "/tmp/pk_firstdata"

        pkey = paramiko.RSAKey.from_private_key_file(file_path)
        sftpClient = multifactor_auth(hostname, 6522, username, pkey, password)
        sftpClient.get_channel().in_window_size = 3145728
        sftpClient.get_channel().out_window_size = 3145728
        sftpClient.get_channel().in_max_packet_size = 3145728
        sftpClient.get_channel().out_max_packet_size = 3145728

        sftpClient.get_channel().settimeout(14400)
 
        pag = s3_client.get_paginator('list_objects_v2')
        pages = pag.paginate(Bucket=bucket_dest, Prefix='ARG_LIQUIDACIONES_FIRSTDATA/')
        key_list = []
        for page in pages:
            for obj in page['Contents']:
                key_list.append(obj['Key'])


        for archive in sftpClient.listdir_attr(download_file):
            try:
                if('.TXT' in archive.filename):
                    dt2_object = datetime.fromtimestamp(archive.st_mtime)
                    if yesterday < dt2_object: 
                        ts = str(archive.st_mtime)
                        print(ts + (archive.filename)+download_file)
                        try:
                            if any(ts + (archive.filename)  in s for s in key_list):
                                print('El archivo ya está: ',archive.filename)
                                esta = True
                                continue
                            else:
                                esta=False
                        except:
                            esta=False
                            pass
                        if not esta:
                            if archive.st_size > 20000000:
                                fl = sftpClient.open(download_file+ (archive.filename), "rb")
                                fl.prefetch(file_size=archive.st_size)
                                fl.set_pipelined(pipelined=True)
                                print('Subiendo: ' + ts + archive.filename)
                                s3_client.upload_fileobj(fl,Bucket=bucket_dest,Key='ARG_LIQUIDACIONES_FIRSTDATA/' + ts + archive.filename)
                            else:
                                fl = sftpClient.open(download_file + (archive.filename), "rb")
                                fl.prefetch(file_size=archive.st_size)
                                b_ncfile = fl.read(fl.stat().st_size)
                                data = BytesIO(b_ncfile)
                                print('Subiendo:' + ts + archive.filename)
                                s3_client.put_object(Body=data.getvalue(),Bucket=bucket_dest,Key='ARG_LIQUIDACIONES_FIRSTDATA/' + ts + archive.filename)
            except Exception as e:
                print("Error: ",e)
                continue
            
        for archive in sftpClient.listdir_attr(download_file_2):
            try:
                if('.TXT' in archive.filename):
                    dt2_object = datetime.fromtimestamp(archive.st_mtime)
                    if yesterday < dt2_object: 
                        ts = str(archive.st_mtime)
                        print(ts + (archive.filename)+download_file_2)
                        try:
                            if any(ts + (archive.filename)  in s for s in key_list):
                                print('El archivo ya está: ',archive.filename)
                                esta = True
                                continue
                            else:
                                esta=False
                        except:
                            esta=False
                            pass
                        if not esta:
                            if archive.st_size > 20000000:
                                fl = sftpClient.open(download_file_2+ (archive.filename), "rb")
                                fl.prefetch(file_size=archive.st_size)
                                fl.set_pipelined(pipelined=True)
                                print('Subiendo: ' + ts + archive.filename)
                                s3_client.upload_fileobj(fl,Bucket=bucket_dest,Key='ARG_LIQUIDACIONES_FIRSTDATA/' + ts + archive.filename)
                            else:
                                fl = sftpClient.open(download_file_2 + (archive.filename), "rb")
                                fl.prefetch(file_size=archive.st_size)
                                b_ncfile = fl.read(fl.stat().st_size)
                                data = BytesIO(b_ncfile)
                                print('Subiendo:' + ts + archive.filename)
                                s3_client.put_object(Body=data.getvalue(),Bucket=bucket_dest,Key='ARG_LIQUIDACIONES_FIRSTDATA/' + ts + archive.filename)
            except Exception as e:
                print("Error: ",e)
                continue

        df =None
        return df


class ExtractorGenova:

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
        yesterday = datetime.today() - timedelta(7)

        users = [
            ("global_prod",["genova-report-mastercard_transaccional"],"AR/GENOVA/REPORT/","/ml-sftp/Global-Prod/reconciliations/global/mi/mlb/genova/mastercard/","xxxxx.mercadolibre.xxx"),
            ("global_prod",["genova-report-mastercard_transaccional"],"AR/GENOVA/REPORT/","/ml-sftp/Global-Prod/reconciliations/global/mi/mla/genova/visa/","xxxxxx.mercadolibre.xxx")
            ]

        # Lógica conexión, descarga, limpieza y subida de archivos
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




