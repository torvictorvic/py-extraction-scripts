import boto3
import pysftp
from io import StringIO, BytesIO
from datetime import date, timedelta, datetime
import paramiko
import threading
import os
from sqlalchemy import create_engine
import pandas as pd


class Extractor:
    @staticmethod
    def run(filename, **kwargs ):

        bucket_dest = "bi.external.data"

        USER = 'SIMETRIK_TEST_IN'
        PASSWORD = 'NiyYYw2Mf3zQ'
        DB = 'IN_TESTING'
        WH = 'IN_TESTING_WH'
        ROLE = 'IN_TESTING'
        ACCOUNT='ik90710.us-east-1'
        SCHEMA='OUTPUTS'

        #Conexión boto3
        s3 = boto3.client('s3')
        #bi.external.data/ccn_mp/global/report_general_costos_financiacion

        direccion = ['ccn_mp/global/report_general_costos_financiacion/a_procesar/2020-08-25/']
        #direccion = ['ccn_mp/global/liquidaciones/pre/']
        #cambiar a hoy de ar

        print('Connecting to Snowflake')

        engine = create_engine(f'snowflake://{USER}:{PASSWORD}@{ACCOUNT}/{DB}/{SCHEMA}?warehouse={WH}&role={ROLE}')
        
        query = "SELECT * FROM OUTPUTS.REPORT_AR_GENERAL_COSTOS_FINANCIACION WHERE GREATEST(FECHA_NOVEDAD_LIQ::DATE,FECHA_NOVEDAD_SITE::DATE)=DATEADD('DAY',-100,CURRENT_DATE) limit 10"
        print('Empezando ciclo de subida de teradata costos')
        df = pd.read_sql(query , engine)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index= False, encoding='utf-8', sep=",")
        buff2 = BytesIO(csv_buffer.getvalue().encode())
        file_name ="prueba_subida.csv"
        s3.upload_fileobj(buff2, bucket_dest,  direccion[0] + file_name,ExtraArgs={'ACL': 'bucket-owner-full-control'})
        print('Finalizado ciclo de subida de teradata costos con ACL modificado')
        print(len(df))
        df =None
        return df


def private_key():
    llave ="""-----BEGIN PGP PRIVATE KEY BLOCK-----
            ** FAKE ** TEST ** ** FAKE ** TEST ** 
            -----END PGP PRIVATE KEY BLOCK-----
    """
    return llave


class ExtractorFD:
    @staticmethod
    def run(filename, **kwargs ):
        print('Iniciando Firstada')
        text_file = open("/tmp/pk_firstdata", "w")
        text_file.write(private_key())
        text_file.close()
        bucket_dest = "global-in-sources-xxxxxxxxxx"
        download_file = "downloaded/"
        s3_client= (boto3.client('s3')) 
        objlist = s3_client.list_objects_v2(Bucket=bucket_dest, Prefix='ARG_LIQUIDACIONES_FIRSTDATA/')

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
        

        actual = date.today() 
        yesterday = date.today() - timedelta(1)
        hostname = 'prod2-gw-lac.firstdataclients.com'
        username = 'LAGW-GASNA001'
        password = 'za5|%[sTc'
        file_path = "/tmp/pk_firstdata"
        
        pkey = paramiko.RSAKey.from_private_key_file(file_path)
        sftpClient = multifactor_auth(hostname, 6522, username, pkey, password)
        sftpClient.get_channel().in_window_size = 2097152
        sftpClient.get_channel().out_window_size = 2097152
        sftpClient.get_channel().in_max_packet_size = 2097152
        sftpClient.get_channel().out_max_packet_size = 2097152
        
        sftpClient.get_channel().settimeout(3600)
        
        for archive in sftpClient.listdir_attr(download_file): 

            if('.TXT' in archive.filename):
        
                dt2_object = datetime.fromtimestamp(archive.st_mtime).strftime('%Y-%m-%d')
                if  (actual.strftime("%Y-%m-%d") in dt2_object) | (yesterday.strftime("%Y-%m-%d") in dt2_object)  : 
                    ts = str(archive.st_mtime)
                    try:
                        if any( ( ts + (archive.filename)  in s['Key'] for s in objlist['Contents']) ) :
                            print('El archivo ya está: ',archive.filename)
                            esta = True
                            continue
                        elif any( ( "firstdata||" + (archive.filename)  in s['Key'] for s in objlist['Contents']) ) :
                            print('El archivo ya está: ',archive.filename)
                            esta = True
                            continue
                        else:
                            esta=False
                    except:
                        esta=False
                        pass
                    if not esta:
                        with sftpClient.open(download_file + (archive.filename), "rb") as fl:
                            print('Descargando: ', archive.filename)
                            fl.prefetch(file_size=archive.st_size)
                            b_ncfile = fl.read(archive.st_size)
                            data = BytesIO(b_ncfile) 
                            print('escribiendo  '+ ts + archive.filename)
                            s3_client.put_object(Body=data.getvalue(),Bucket=bucket_dest,Key='ARG_LIQUIDACIONES_FIRSTDATA/' + ts + archive.filename)
        df =None
        return df
