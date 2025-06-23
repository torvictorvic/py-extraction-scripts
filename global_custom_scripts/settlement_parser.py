from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import datetime
import os
import email
from email import message_from_file
import boto3
from urllib.parse import urlparse
from io import BytesIO, TextIOWrapper,StringIO
import pytz
import numpy as np

targetWord = 'TO TRANSFER AGENT:'


class SinglePlayer:

    io: str

    def __init__(self, io):
        self.io = io

    @staticmethod
    def parsedate(raw_date):
        raw_date = raw_date.replace('=', '')
        try:
            return datetime.strptime(raw_date, '%d %b %Y').strftime('%Y%m%d')
        except ValueError:
            date_ = ' '.join(raw_date.split(' ')[-3:])
            return datetime.strptime(date_, '%d %b %Y').strftime('%Y%m%d')

    def parse(self):
        try:
            soup = BeautifulSoup(self.io, 'lxml')

            targetelement = [elem for elem in soup.find_all(
                'div') if targetWord in elem.text][0]
            elements = re.sub('\xa0+', '', targetelement.text).split('\n')
        except AttributeError:
            elements = self.io.split('\n')
        except IndexError:
            elements = self.io.split('\n')
        num_li_1=0
        num_li_2=0
        is_dig=True
        for i,el in enumerate(elements):

            li_1=[x for x in el.split(' ') if x!='']
            
            if "EXCEPTION" in li_1:
                return "EXCEPTION REPORT"
            if 'PAGE:' in li_1:
                if li_1[1]!='1':
                    num_li_1=i
            if num_li_1>0 and len(li_1)>0:
                if li_1[0].isdigit() and is_dig:
                    num_li_2=i 
                    is_dig=False        
        if num_li_1>0 and num_li_2>0:
            for y in range(num_li_2-num_li_1+3):
                elements.pop(num_li_1-3)

        data = []
        splitelements = [re.sub(' +', ' ', x) for x in elements]
        for ix, elem in enumerate(splitelements):
            if 'COUNTRY CODE' in elem:
                countrycode = elem.split(':')[1].strip()
            elif 'SETTLEMENT DATE' in elem:
                settlement_date = self.parsedate(elem.split(':')[1].strip())
            elif 'CURRENCY CODE' in elem:
                currency_code = elem.split(':')[1].strip()
            elif 'VALUE DATE' in elem:
                value_date = self.parsedate(elem.split(':')[1].strip())
            elif 'MEMBER NAME' in elem:
                member_name = elem.split(':')[1].strip()
            elif 'RECON DATE' in elem:
                rec_ix = ix+2

            elif 'ORIGINATED' in elem:

                concepto = elem.split('/')[-2].strip() + '/' + \
                    splitelements[ix + 1].strip().split(' ')[-2] +\
                    ' ' + splitelements[ix + 1].strip().split(' ')[-1]
                for z in range(ix+2, len(splitelements)):
                    if 'MEMBER TOTALS' in splitelements[z]:
                        break
                    split_el = splitelements[z].strip().split(' ')
                    item = {}
                    item['file_id'] = ' '.join(splitelements[rec_ix].split(' '))[26:-1]
                    item ['input_id'] = ' '.join(splitelements[rec_ix].split(' '))[15:26]
                    if split_el[0].isnumeric():
                        
                        item['countrycode'] = countrycode
                        item['currency_code'] = currency_code
                        item['settlement_date'] = settlement_date
                        item['value_date'] = value_date
                        item['member_name'] = member_name
                        item['recon_date'] = self.parsedate(
                            ' '.join(splitelements[rec_ix].split(' ')[2:5]))
                        rec_ix += 1
                        item['concepto'] = concepto
                        item['net_amount'] = split_el[-2].replace(',', '')
                        item['tipo_op'] = split_el[-1].replace(',', '')
                        data.append(item)
            elif 'ACCOUNT TOTALS' in elem:
                item = {}
                item['countrycode'] = countrycode
                item['currency_code'] = currency_code
                item['settlement_date'] = ''
                item['value_date'] = value_date
                item['member_name'] = member_name
                item['recon_date'] = ''
                item['concepto'] = elem.split(':')[0]
                item['net_amount'] = splitelements[ix +
                                                   1].strip().split(' ')[-2].replace(',', '')
                item['tipo_op'] = splitelements[ix +
                                                1].strip().split(' ')[-1].replace(',', '')
                data.append(item)

        return data

    def run(self):
        data = self.parse()
        if 'EXCEPTION' in data:
            return data

        df = pd.DataFrame(data)

        df = df[['countrycode', 'concepto', 'currency_code', 'settlement_date',
                 'value_date', 'member_name', 'recon_date', 'net_amount', "file_id",'tipo_op','input_id']]
        return df

class Extractor:

    def run(self, filename, **kwargs):
        file= self.file.body
        lm= self.file.last_modified
        filename=self.file.key
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        
        raw_data = file.decode('utf-8')
        df = SinglePlayer(raw_data).run()
        if 'EXCEPTION' in df:
            print(df)
        else:
            for col in df.columns:
                try:
                    df[col] = (
                        df[col]
                        .astype(str)
                        .str.replace("\n", "")
                        .str.replace("\r", "")
                        .str.replace("\t", "")
                        .str.replace("\r\n", "")
                    )
                except AttributeError:
                    pass
            type_of_transfer = []
            for i in raw_data.splitlines():
                if "TRANSFER AGENT ADVISEMENT" in i or "DETAIL" in i:
                    type_of_transfer.append(i.strip())
                elif "TRANSFER AGENT ADVISEMENT" in i or "EXCEPTION REPORT" in i:
                    type_of_transfer.append(i.strip())
                elif "TRANSFER AGENT ID" in i:
                    transfer_agent_id=i.split(":")[-1].strip()
            df["transfer_agent_id"] = transfer_agent_id.strip()
            df["type_of_transfer"] = type_of_transfer[-1]
            df['file_id'] = df['file_id'].replace('nan',np.nan)
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df