from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import datetime
import os
import boto3
from urllib.parse import urlparse
from io import BytesIO,TextIOWrapper
import pytz

targetWord = 'TO TRANSFER AGENT:'


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
            #    ,aws_access_key_id = AWS_ACCESS_KEY,
            #   aws_secret_access_key = AWS_SECTRET_KEY)

            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read().decode('utf-8')
            text_io = obj

            return text_io,lm
        else:
            with open(uri,encoding='utf-8') as f:
                return f.read(), datetime.today()


class SinglePlayer:

    io: str

    def __init__(self, io):
        self.io = io

    @staticmethod
    def parsedate(raw_date):
        return datetime.strptime(raw_date, '%d %b %Y').strftime('%Y%m%d')

    def parse(self):
        soup = BeautifulSoup(self.io, 'lxml')

        targetelement = [elem for elem in soup.find_all(
            'div') if targetWord in elem.text][0]

        elements = re.sub('\xa0+', '', targetelement.text).split('\n')

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
            elif 'RECON DATE' in elem:
                recon_date = self.parsedate(
                    ' '.join(splitelements[ix+2].split(' ')[2:5]))
            elif 'NO ORIGINATED' in elem:
                concepto = elem.split('/')[-1].strip() + \
                    splitelements[ix+1] + ' ' + splitelements[ix + 2].strip().split(' ')[-1] \
                    + splitelements[ix+3]
                for z in range(ix+3, len(splitelements)):
                    if 'MEMBER TOTALS' in splitelements[z]:
                        break
                    split_el = splitelements[z].strip().split(' ')
                    if split_el[0].isnumeric():
                        item = {}
                        item['countrycode'] = countrycode
                        item['currency_code'] = currency_code
                        item['settlement_date'] = settlement_date
                        item['value_date'] = value_date
                        item['recon_date'] = recon_date
                        item['concepto'] = concepto
                        item['net_amount'] = split_el[-1].replace(',', '')
                        item['tipo_op'] = splitelements[z+1].strip()
                        data.append(item)
            elif 'ACCOUNT TOTALS' in elem:
                item = {}
                item['countrycode'] = countrycode
                item['currency_code'] = currency_code
                item['settlement_date'] = ''
                item['value_date'] = value_date
                item['recon_date'] = ''
                item['concepto'] = elem.split(':')[0]
                item['net_amount'] = splitelements[ix +
                                                   1].strip().split(' ')[-1].replace(',', '')
                item['tipo_op'] = splitelements[ix+2].strip()
                data.append(item)

        return data

    def run(self):
        data = self.parse()

        df = pd.DataFrame(data)

        df = df[['countrycode', 'concepto', 'currency_code', 'settlement_date',
                 'value_date', 'recon_date', 'net_amount', 'tipo_op']]
        return df


class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        file_, lm = FileReader.read(filename)
        raw_data = file_
        df = SinglePlayer(raw_data).run()

        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')

        arg_datetime = old_timezone.localize(
            my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
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
        df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
        df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
        out = filename.split('/')[-1]
        df['file_name'] = out
        df.reset_index(drop=True)
        df['skt_extraction_rn'] = df.index.values
        return df


