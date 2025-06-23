import pytz
import boto3
import pandas as pd

from io import StringIO
from datetime import datetime
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
            obj = obj['Body'].read().decode()
            obj = StringIO(obj)
            return obj,lm
        else:
            with open(uri) as f:
                return uri,datetime.today()

class ExtractorCCN:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            header_positions = [0,2,8,14,38,321]
            footer_positions =[0,2,8,14,21,34,40,46,52,58,321]
            positions = [0,2,6,8,20,26,32,36,44,64,83,96,105,106,108,111,119,135,151,161,162,168,174,180,183,203,212,216,242,243,251,271,275,276,278,321]
            header_specification =[]
            footer_specification =[]
            for i in range(0,len(header_positions) -1):
                cordenate = (header_positions[i],header_positions[i+1] )
                header_specification.append(cordenate)
            for i in range(0,len(footer_positions) -1):
                cordenate = (footer_positions[i],footer_positions[i+1] )
                footer_specification.append(cordenate)
            Datos=[]
            aux=0
            for line in file:
                if aux == 0:
                    cols_header=['DKTT_HR_REG','DKTT_HR_FECH_PROC','DKTT_HR_HORA_PROC','DKTT_HR_GLOSA','FILLER_H']
                    df_header = pd.read_fwf(StringIO(line), colspecs=header_specification, names=cols_header, header=None, dtype=object)
                    aux+=1
                else:
                    linea=[]
                    for i in range(0,len(positions) -1):
                        aux2 = line[positions[i]:positions[i+1]]
                        linea.append(aux2)
                    Datos.append(linea)
            linea2 = ''.join(linea[0:len(linea)])
            cols_footer=['DKTT_TR_REG','DKTT_TR_FECH_PROC','DKTT_TR_HORA_PROC','DKTT_TR_CANT_REG','DKTT_TR_ACUM_MONTO','DKTT_TR_FECH_DESDE','DKTT_TR_HORA_DESDE','DKTT_TR_FECH_HASTA','DKTT_TR_HORA_HASTA','FILLER_F']
            df_footer = pd.read_fwf(StringIO(linea2), colspecs=footer_specification, names=cols_footer, header=None, dtype=object)
            del file
            del linea
            del header_specification
            del footer_specification
            del cols_header
            del cols_footer
            Datos.pop(-1)
            cols =['DKTT_DT_REG','DKTT_DT_TYP','DKTT_DT_TC','DKTT_DT_SEQ_NUM','DKTT_DT_TRAN_DAT','DKTT_DT_TRAN_TIM','DKTT_DT_INST_RETAILER','DKTT_DT_ID_RETAILER','DKTT_DT_NAME_RETAILER','DKTT_DT_CARD','DKTT_DT_AMT_1','DKTT_DT_AMT_PROPINA','DKTT_DT_TIPO_CUOTA','DKTT_DT_CANTI_CUOTAS','DKTT_DT_RESP_CDE','DKTT_DT_APPRV_CDE','DKTT_DT_TERM_NAME','DKTT_DT_ID_CAJA','DKTT_DT_NUM_BOLETA','DKTT_DT_AUTH_TRACK2','DKTT_DT_FECHA_VENTA','DKTT_DT_HORA_VENTA','DKTT_DT_FECHA_PAGO','DKTT_DT_COD_RECHAZO','DKTT_DT_GLOSA_RECHAZO','DKTT_DT_VAL_CUOTA','DKTT_DT_VAL_TASA','DKTT_DT_NUMERO_UNICO','DKTT_DT_TIPO_MONEDA','DKTT_DT_ID_RETAILER_RE','DKTT_DT_COD_SERVICIO','DKTT_DT_VCI','DKTT_MES_GRACIA','DKTTE_PERIODO_GRACIA','RUT_SECUNDARIO']
            df = pd.DataFrame(data = Datos, columns = cols)
            del Datos
            del cols
            for col in df_header:
                df[col] =df_header.loc[0,col] 
            for col in df_footer:
                df[col] =df_footer.loc[0,col] 

            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df

        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorCDN:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            header_positions = [0,2,8,14,39,240]
            footer_positions =[0,2,8,14,21,34,47,240]
            positions = [0,2,6,8,14,20,32,52,71,84,97,106,109,117,133,149,159,165,167,175,195,221,222,240]
            header_specification =[]
            footer_specification =[]
            for i in range(0,len(header_positions) -1):
                cordenate = (header_positions[i],header_positions[i+1])
                header_specification.append(cordenate)
            for i in range(0,len(footer_positions) -1):
                cordenate = (footer_positions[i],footer_positions[i+1])
                footer_specification.append(cordenate)
            Datos=[]
            aux=0
            for line in file:
                if aux == 0:
                    cols_header= ['DSK_HR_REG','DSK_HR_FECHA_PROC','DSK_HR_HORA_PROC','DSK_HR_GLOSA','FILLER_H']
                    df_header = pd.read_fwf(StringIO(line), colspecs=header_specification, names=cols_header, header=None, dtype=object)
                    aux+=1
                else:
                    linea=[]
                    for i in range(0,len(positions) -1):
                        aux2 = line[positions[i]:positions[i+1]]
                        linea.append(aux2)
                    Datos.append(linea)
            linea2 = ''.join(linea[0:len(linea)])
            cols_footer=['DSK_TR_REG','DSK_TR_FECHA_PROC','DSK_TR_HORA_PROC','DSK_TR_TOT_REG','DSK_TR_MONTO','DSK_TR_MONTO_COM','FILLER_F']
            df_footer = pd.read_fwf(StringIO(linea2), colspecs=footer_specification, names=cols_footer, header=None, dtype=object)
            del file
            del linea
            del header_specification
            del footer_specification
            del cols_header
            del cols_footer
            Datos.pop(-1)
            cols = ['DSK_DT_REG',	'DSK_TYP',	'DSK_TC',	'DSK_TRAN_DAT',	'DSK_TRAN_TIM',	'DSK_ID_RETAILER',	'DSK_NAME_RETAILER',	'DSK_CARD',	'DSK_AMT_1',	'DSK_AMT_2',	'DSK_AMT_PROPINA',	'DSK_RESP_CDE',	'DSK_APPRV_CDE',	'DSK_TERM_NAME',	'DSK_ID_CAJA',	'DSK_NUM_BOLETA',	'DSK_FECHA_PAGO',	'DSK_IDENT',	'DSK_ID_RETAILER_RE',	'DSK_ID_COD_SERVI',	'DSK_ID_NRO_UNICO',	'DSK_PREPAGO', 'RUT_SECUNDARIO']
            df = pd.DataFrame(data = Datos, columns = cols)
            del Datos
            del cols
            for col in df_header:
                df[col] =df_header.loc[0,col] 
            for col in df_footer:
                df[col] =df_footer.loc[0,col] 

            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df

        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorLCN:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            header_positions = [0,8,9,17,18,26,27,35,40,60,134,140,229]
            footer_positions =[0,10,11,24,25,35,36,49,50,63,76,89,102,134,140,229]
            #positions = [0,8,16,24,32,51,53,64,65,67,71,79,87,113,119,121,125,136,147,158,169,171,206,208,226,229]
            header_specification =[]
            footer_specification =[]
            for i in range(0,len(header_positions) -1):
                cordenate = (header_positions[i],header_positions[i+1] )
                header_specification.append(cordenate)
            for i in range(0,len(footer_positions) -1):
                cordenate = (footer_positions[i],footer_positions[i+1] )
                footer_specification.append(cordenate)
            Datos=[]
            for line in file:
                if 'HEADER' in line:
                    cols_header=['ABONO_DESDE','FILLER_H1',	'ABONO_HASTA','FILLER_H2','PROCESO_FECHA','FILLER_H3','ABONO_FECHA','CODIGOCLIENTE','NOMBREDECLIENTE','FILLER_H4','FILLER_H5','FILLER_H6']
                    df_header = pd.read_fwf(StringIO(line), colspecs=header_specification, names=cols_header, header=None, dtype=object)
                elif 'FOOTER' in line:
                    cols_footer=['SAL_CONTADOR','FILLER_F1','SAL_MONTO','FILLER_F2','SAL_RRET','FILLER_F3','SAL_RET','FILLER_F4','SAL_CEIC','SAL_CAEICA','SAL_DCEIC','SAL_DCAEICA','FILLER_F5','FILLER_F6','FILLER_F7']
                    df_footer = pd.read_fwf(StringIO(line), colspecs=footer_specification, names=cols_footer, header=None, dtype=object)
                else:
                    linea=[line[0:8],line[8:16],line[16:24],line[24:32],line[32:51],line[51:53],line[53:64],line[64:65],line[65:67],line[67:71],line[71:79],line[79:87],line[87:113],line[113:119],line[119:121],line[121:125],line[125:136],line[136:147],line[147:158],line[158:169],line[169:171],line[171:206],line[206:208],line[208:226],line[226:229]]
                    Datos.append(linea)
            del file
            del linea
            del header_specification
            del footer_specification
            del cols_header
            del cols_footer
            cols=['LIQ_NUMC','LIQ_FPROC','LIQ_FCOM','LIQ_MICR','LIQ_NUMTA','LIQ_MARCA','LIQ_MONTO','LIQ_MONEDA','LIQ_TXS','LIQ_RETE','LIQ_CPRIN','LIQ_FPAGO','LIQ_ORPEDI','LIQ_CODAUT','LIQ_CUOTAS','LIQ_VCI','LIQ_CEIC','LIQ_CAEICA','LIQ_DCEIC','LIQ_DCAEICA','LIQ_NTC','LIQ_NOMBRE_BANCO','LIQ_TIPO_CUENTA_BANCO','LIQ_NUMERO_CUENTA_BANCO','LIQ_MONEDA_CUENTA_BANCO']
            df = pd.DataFrame(data = Datos, columns = cols)
            del Datos
            del cols
            for col in df_header:
                df[col] =df_header.loc[0,col] 
            for col in df_footer:
                df[col] =df_footer.loc[0,col] 
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df

        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorLDN:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow() 
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            header_positions = [0,6,7,13,17,23,24,49,55,215]
            footer_positions =[0,10,11,24,25,35,36,49,58,68,74,87,88,101,215]
            #positions = [0,8,14,20,26,45,58,60,68,70,78,104,117,130,143,156,157,192,194,212,215]
            header_specification =[]
            footer_specification =[]
            for i in range(0,len(header_positions) -1):
                cordenate = (header_positions[i],header_positions[i+1])
                header_specification.append(cordenate)
            for i in range(0,len(footer_positions) -1):
                cordenate = (footer_positions[i],footer_positions[i+1])
                footer_specification.append(cordenate)
            Datos=[]
            for line in file:
                if 'HEADER' in line:
                    print('Se extrae el header')
                    cols_header = ['ABONO_DESDE','FILLER_H1','ABONO_HASTA','FILLER_H2','LIQU_FECHA','FILLER_H3','NOMBRE_FAN','HEADER','FILLER_H4']
                    df_header = pd.read_fwf(StringIO(line), colspecs=header_specification, names=cols_header, header=None, dtype=object)
                elif 'FOOTER' in line:
                    cols_footer =['LIQ_NCOM','FILLER_F1','LIQ_TCOM','FILLER_F2','LIQ_NRET','FILLER_F3','LIQ_MRET','LIQ_VRET','LIQ_TRET','FOOTER','LIQ_TOT_COM_COMIV','FILLER_F4','LIQ_TOT_DECOM_IVCOM','LIQ_FILL']
                    df_footer = pd.read_fwf(StringIO(line), colspecs=footer_specification, names=cols_footer, header=None, dtype=object)
                else:
                    linea=[line[0:8],line[8:14],line[14:20],line[20:26],line[26:45],line[45:58],line[58:60],line[60:68],line[68:70],line[70:78],line[78:104],line[104:117],line[117:130],line[130:143],line[143:156],line[156:157],line[157:192],line[192:194],line[194:212],line[212:215]]
                    Datos.append(linea)
            del file
            del linea
            del header_specification
            del footer_specification
            del cols_header
            del cols_footer
            cols =['LIQ_CCRE','LIQ_FPRO','LIQ_FCOM','LIQ_APPR','LIQ_PAN','LIQ_AMT1','LIQ_TTRA','LIQ_CPRI','LIQ_MARC','LIQ_FEDI','LIQ_NRO_UNICO','LIQ_COM_COMIV','LIQ_CAD_CADIVA','LIQ_DECOM_IVCOM','LIQ_DCOAD_IVCOM','LIQ_PREPAGO','LIQ_NOMBRE_BANCO','LIQ_TIPO_CUENTA_BANCO','LIQ_NUMERO_CUENTA_BANCO','LIQ_MONEDA_CUENTA_BANCO']
            print('Se extrae el df')
            df = pd.DataFrame(data = Datos, columns = cols)
            del Datos
            del cols
            print('Se ensambla el df final')
            for col in df_header:
                df[col] =df_header.loc[0,col]
            del df_header
            print('Header Ensamblado')
            for col in df_footer:
                df[col] =df_footer.loc[0,col]
            del df_footer
            print('Footer ensamblado')

            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df

        except Exception as e:
            print("Error al subir la fuente: ",e)