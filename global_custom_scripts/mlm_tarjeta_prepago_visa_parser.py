import pytz
import boto3
import pandas as pd
import re


from urllib.parse import urlparse
from io import BytesIO
from datetime import datetime

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
            binary_data = obj['Body'].read().decode("utf-8")
            #binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri, "rb") as f:
                return f.read().decode("utf-8"),datetime.now()
                

class MyError(Exception):
    def __init__(self, message):
        self.message = message
        


class Extractor():
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        file = file.splitlines()
        print('--*--'*10)
        print(f'Uploading {filename} . . .')

        proc_date_dict, fund_trans_ammount_dict, report_date_dict, total_visa_charges_dict,net_settlemet_amount_dict,visa_charges_total_issuer_dict,total_total_issuer_dict,total_total_other_dict = [], [], [], [], [], [], [], []
        proc_date_n, fund_trans_ammount_n, report_date_n, total_visa_charges_n,net_settlemet_amount_n,visa_charges_total_issuer_n,total_total_issuer_n,total_total_other_n = None, None, None, None, None, None, None, None

        a,b,c,d,e,f,g,h,k,l,m,n,ñ,o,z = False, False,False, False, False, False, False, False, False, False, False, False, False, False, False
        for i,j in enumerate(file):
            if "REPORT ID:" in j and "VSS-110" in j.split() and a == False:
                print(j)
                a = True
            if a:
                if "REPORTING FOR:" in j and "9000624867 MERCADO LENDING" in j and b == False:
                    b = True
                if "ROLLUP TO:" in j and "9000624867 MERCADO LENDING" in j and c == False:
                    c = True
                if "FUNDS XFER ENTITY:" in j and "9000624867 MERCADO LENDING" in j and d == False:
                    d = True
                if "PROC DATE" in j and e == False:
                    proc_date_n = j.split()[-1]
                    print(proc_date_n)
                    e = True
                if "FUNDS TRANSFER AMOUNT" in j and f == False:
                    fund_trans_ammount_n = j.split()[-1]
                    print(fund_trans_ammount_n)

                    f = True
                if "REPORT DATE" in j and g == False:
                    report_date_n = j.split()[-1]
                    print(report_date_n)
                    g = True
                if "VISA CHARGES" in j and l == False:
                    l = True
                if "TOTAL ISSUER" in j and m == False and l==True:
                    visa_charges_total_issuer_n=j.split()[-1]
                    print(visa_charges_total_issuer_n)
                    m = True
                if "TOTAL"==j.strip() and n == False:
                    n = True
                if "TOTAL ISSUER" in j and ñ == False and n==True:
                    total_total_issuer_n=j.split()[-1]
                    print(total_total_issuer_n)
                    ñ = True
                if "TOTAL OTHER" in j and o == False and n==True:
                    total_total_other_n = j.split()[-1]
                    print(total_total_other_n)
                    o = True
                if "TOTAL VISA CHARGES" in j and h == False:
                    total_visa_charges_n=j.split()[-1]
                    print(total_visa_charges_n)
                    h = True
                if "NET SETTLEMENT AMOUNT" in j and k == False:
                    net_settlemet_amount_n=j.split()[-1]
                    print(net_settlemet_amount_n)
                    k = True
                if "END OF VSS-110 REPORT" in j and z== False:
                    print(j)
                    z = True
            if a*b*c*d*e*f*g*h*k*l*m*n*ñ*o*z == True:
                proc_date_dict.append(proc_date_n)
                fund_trans_ammount_dict.append(fund_trans_ammount_n)
                report_date_dict.append(report_date_n)
                total_visa_charges_dict.append(total_visa_charges_n)
                net_settlemet_amount_dict.append(net_settlemet_amount_n)
                visa_charges_total_issuer_dict.append(visa_charges_total_issuer_n)
                total_total_issuer_dict.append(total_total_issuer_n)
                total_total_other_dict.append(total_total_other_n)
                a,b,c,d,e,f,g,h,k,l,m,n,ñ,o,z = False, False,False, False, False, False, False, False, False, False, False, False, False, False, False
                proc_date_n, fund_trans_ammount_n, report_date_n, total_visa_charges_n,net_settlemet_amount_n,visa_charges_total_issuer_n,total_total_issuer_n, total_total_other_n = None, None, None, None, None, None, None, None
            elif z:
                a,b,c,d,e,f,g,h,k,l,m,n,ñ,o,z = False, False,False, False, False, False, False, False, False, False, False, False, False, False, False
                proc_date_n, fund_trans_ammount_n, report_date_n, total_visa_charges_n,net_settlemet_amount_n,visa_charges_total_issuer_n,total_total_issuer_n, total_total_other_n = None, None, None, None, None, None, None, None
        
        if 'AMOUNT' in fund_trans_ammount_dict[0]:
            raise MyError("El archivo no contiene cifra para Fund transfer amount")
        else:
            proc_date = proc_date_dict[-1].split()[-1]
            fund_trans_ammount = fund_trans_ammount_dict[-1].split()[-1]
            report_date = report_date_dict[-1].split()[-1]
            total_visa_charges=total_visa_charges_dict[-1].split()[-1]
            net_settlemet_amount=net_settlemet_amount_dict[-1].split()[-1]
            visa_charges_total_issuer=visa_charges_total_issuer_dict[-1].split()[-1]
            total_total_issuer=total_total_issuer_dict[-1].split()[-1]
            total_total_other=total_total_other_dict[-1].split()[-1]

            df = pd.DataFrame([proc_date,report_date,fund_trans_ammount,total_visa_charges,net_settlemet_amount,visa_charges_total_issuer,total_total_issuer,total_total_other]).T
            df.columns= ["set_proc_date","set_report_date","set_funds_transfer_amount","set_total_visa_charges","set_net_settlemet_amount","set_visa_charges_total_issuer","set_total_total_issuer", "set_total_total_other"]        
            my_timestamp = datetime.utcnow() 
            old_timezone = pytz.timezone("UTC")
            new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
            upload_date = lm.astimezone(new_timezone)
            arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = (filename.split('/')[-1])
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df