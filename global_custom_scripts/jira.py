import json
from jira import JIRA
import numpy as np
import pandas as pd
import snowflake.connector
from urllib.parse import urlparse
import pytz
import boto3
from io import StringIO, BytesIO
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

def extrac_histori(user,but):
    jiraOptions = {'server': "https://mercadolibre-administration-people.atlassian.net"}
    jira = JIRA(options=jiraOptions, basic_auth=(user,but))
    df = pd.DataFrame(index = [0], columns= ['key','item','status','created'])
    
    got = 100
    total = 0
    while got==100:
        issues = jira.search_issues(jql_str=''' 'Request Type' = 'Soporte GLOBAL (SDFPS)'  AND updated >= -2d   ''', expand='changelog',maxResults=100,startAt= total)
        for singleIssue in issues:
            if singleIssue.raw['key'] == 'SDFPS-79593':
                print('entro')
            for history in singleIssue.changelog.histories: 
                for item in  history.items:
                    if item.field == 'status':
                        dic_issues= {
                            'key': singleIssue.key,
                            'item':item.field,
                            'status': item.toString,
                            'created':  history.created
                            
                        }
                        df1 = pd.DataFrame(dic_issues,index = [0])
                        df = pd.concat([df,df1])
        got = len(issues)
        total += got
       
    return df 
      
    
def fields_slas(issue):
    #name = issue['name']
    try:
        le = len(issue['ongoingCycle'])
    except:
        le = 0
    try:
        lenn = len(issue['completedCycles'])
    except:
        lenn = 0
    if lenn > 0 :
        start_time = issue['completedCycles'][0]['startTime']['jira']
        goal_time =  issue['completedCycles'][0]['breachTime']['jira']
        hour_goal_time = issue['completedCycles'][0]['goalDuration']['millis']
        stop_time = issue['completedCycles'][0]['stopTime']['jira']
        elapsedTime = issue['completedCycles'][0]['elapsedTime']['millis']
        remainingTime =   issue['completedCycles'][0]['remainingTime']['millis']
        status = 'completed'
    elif   le  > 0 :
        start_time = issue['ongoingCycle']['startTime']['jira']
        goal_time = issue['ongoingCycle']['breachTime']['jira']
        hour_goal_time = issue['ongoingCycle']['goalDuration']['millis']
        stop_time  = None 
        elapsedTime = issue['ongoingCycle']['elapsedTime']['millis']
        remainingTime =   issue['ongoingCycle']['remainingTime']['millis']
        status = 'ongoing'
    else:
        start_time =  None
        goal_time =  None 
        hour_goal_time = None
        stop_time  = None 
        status = None 
        elapsedTime = None
        remainingTime = None
    dic_field = {
        'start_time': start_time,
        'goal_time' :  goal_time,
        'hour_goal_time' : hour_goal_time,
        'status': status,
        'stop_time' : stop_time,
        'elapsedtime':  elapsedTime,
        'remainingtime' : remainingTime,
        'name': None
    }
    return dic_field


def extrac(user,but ):
    jiraOptions = {'server': "https://mercadolibre-administration-people.atlassian.net"}
    jira = JIRA(options=jiraOptions, basic_auth=(user,but))
    df = pd.DataFrame(index = [0], columns= ['key', 'summary', 'created','updated' ,'creator', 'timeZone', 'description',
       'labels', 'aggregateprogress_progress', 'aggregateprogress_total',
       'comment', 'comment_created', 'comment_author', 'issuetype', 'priority',
       'resolutiondate', 'status','complejidad','reporter','asignado','Clave','Nombre','Nombre_Base','Amount','team_reporter',
    'Pais'                                     
       ,'Time_to_resolution_SK_start_time'
      ,'Time_to_resolution_SK_goal_time'
      ,'Time_to_resolution_SK_hour_goal_time'
      ,'Time_to_resolution_SK_status'
      ,'Time_to_resolution_SK_stop_time'
      ,'Time_to_resolution_SK_elapsedtime'
      ,'Time_to_resolution_SK_remainingtime'
      ,'Time_to_resolution_SK_name'
      ,'Time_in_Pending_start_time'
      ,'Time_in_Pending_goal_time'
      ,'Time_in_Pending_hour_goal_time'
      ,'Time_in_Pending_status'
      ,'Time_in_Pending_stop_time'
      ,'Time_in_Pending_elapsedtime'
      ,'Time_in_Pending_remainingtime'
      ,'Time_in_Pending_name'
      ,'Time_to_first_response_SK_start_time'
      ,'Time_to_first_response_SK_goal_time'
      ,'Time_to_first_response_SK_hour_goal_time'
      ,'Time_to_first_response_SK_status'
      ,'Time_to_first_response_SK_stop_time'
      ,'Time_to_first_response_SK_elapsedtime'
      ,'Time_to_first_response_SK_remainingtime'
      ,'Time_to_first_response_SK_name'
      ,'Time_to_resolution_start_time'
      ,'Time_to_resolution_goal_time'
      ,'Time_to_resolution_hour_goal_time'
      ,'Time_to_resolution_status'
      ,'Time_to_resolution_stop_time'
      ,'Time_to_resolution_elapsedtime'
      ,'Time_to_resolution_remainingtime'
      ,'Time_to_resolution_name'
      ,'Time_waiting_for_customer_start_time'
      ,'Time_waiting_for_customer_goal_time'
      ,'Time_waiting_for_customer_hour_goal_time'
      ,'Time_waiting_for_customer_status'
      ,'Time_waiting_for_customer_stop_time'
      ,'Time_waiting_for_customer_elapsedtime'
      ,'Time_waiting_for_customer_remainingtime'
      ,'Time_waiting_for_customer_name'
      ,'Time_waiting_for_customer_SK_start_time'
      ,'Time_waiting_for_customer_SK_goal_time'
      ,'Time_waiting_for_customer_SK_hour_goal_time'
      ,'Time_waiting_for_customer_SK_status'
      ,'Time_waiting_for_customer_SK_stop_time'
      ,'Time_waiting_for_customer_SK_elapsedtime'
      ,'Time_waiting_for_customer_SK_remainingtime'
      ,'Time_waiting_for_customer_SK_name'
      ,'Tiempo_de_Vida_del_Ticket_start_time'
      ,'Tiempo_de_Vida_del_Ticket_goal_time'
      ,'Tiempo_de_Vida_del_Ticket_hour_goal_time'
      ,'Tiempo_de_Vida_del_Ticket_status'
      ,'Tiempo_de_Vida_del_Ticket_stop_time'
      ,'Tiempo_de_Vida_del_Ticket_elapsedtime'
      ,'Tiempo_de_Vida_del_Ticket_remainingtime'
      ,'Tiempo_de_Vida_del_Ticket_name'
      ,'Tiempo_en_Mesa_de_Soporte_start_time'
      ,'Tiempo_en_Mesa_de_Soporte_goal_time'
      ,'Tiempo_en_Mesa_de_Soporte_hour_goal_time'
      ,'Tiempo_en_Mesa_de_Soporte_status'
      ,'Tiempo_en_Mesa_de_Soporte_stop_time'
      ,'Tiempo_en_Mesa_de_Soporte_elapsedtime'
      ,'Tiempo_en_Mesa_de_Soporte_remainingtime'
      ,'Tiempo_en_Mesa_de_Soporte_name'
      ,'Tiempo_en_Negocio_start_time'
      ,'Tiempo_en_Negocio_goal_time'
      ,'Tiempo_en_Negocio_hour_goal_time'
      ,'Tiempo_en_Negocio_status'
      ,'Tiempo_en_Negocio_stop_time'
      ,'Tiempo_en_Negocio_elapsedtime'
      ,'Tiempo_en_Negocio_remainingtime'
      ,'Tiempo_en_Negocio_name'                                  ])
    customfields =  [('Time to resolution SK','customfield_22374')
                         ,('Time in Pending','customfield_17163')
                         ,('Time to first response SK','customfield_22365')
                         ,('Time to resolution','customfield_12400')
                         ,('Time waiting for customer','customfield_13997')
                         ,('Time waiting for customer SK','customfield_22375')
                         ,('Tiempo de Vida del Ticket','customfield_21807')
                         ,('Tiempo en Mesa de Soporte','customfield_21884')
                         ,('Tiempo en Negocio','customfield_21885')
                        ]
    got = 100
    total = 0
    while got==100:
        issues = jira.search_issues(jql_str=''' 'Request Type' = 'Soporte GLOBAL (SDFPS)'  AND updated >= -2d   ''',maxResults=100,startAt= total)
        for singleIssue in issues :   
            dic_agg = {}
            for fields in customfields:
                dic_fiel = fields_slas(singleIssue.raw['fields'][fields[1]])
                dic_slas = {
                    str(fields[0]).replace(' ','_') + '_'+list(dic_fiel.keys())[0] : dic_fiel[list(dic_fiel.keys())[0]],
                    str(fields[0]).replace(' ','_') + '_'+list(dic_fiel.keys())[1] : dic_fiel[list(dic_fiel.keys())[1]],  
                    str(fields[0]).replace(' ','_') + '_'+list(dic_fiel.keys())[2] : dic_fiel[list(dic_fiel.keys())[2]],  
                    str(fields[0]).replace(' ','_') + '_'+list(dic_fiel.keys())[3] : dic_fiel[list(dic_fiel.keys())[3]], 
                    str(fields[0]).replace(' ','_') + '_'+list(dic_fiel.keys())[4] : dic_fiel[list(dic_fiel.keys())[4]],
                    str(fields[0]).replace(' ','_') + '_'+list(dic_fiel.keys())[5] : dic_fiel[list(dic_fiel.keys())[5]],
                    str(fields[0]).replace(' ','_') + '_'+list(dic_fiel.keys())[6] : dic_fiel[list(dic_fiel.keys())[6]],
                    str(fields[0]).replace(' ','_') + '_'+list(dic_fiel.keys())[7] : dic_fiel[list(dic_fiel.keys())[7]] 
                }
                dic_agg.update(dic_slas)
            try:
                reporter =   singleIssue.raw['fields']['reporter']['displayName']      
            except:  
                reporter = None
            try:
                asig =   singleIssue.raw['fields']['assignee']['displayName']    
            except:  
                asig = None
            try:
                com =   singleIssue.raw['fields']['customfield_18382']['value'] 
            except:  
                com = None
            try:
                pa =   singleIssue.raw['fields']['customfield_11500']['value'] 
            except:  
                pa = None
            try:
                tr =   singleIssue.raw['fields']['customfield_13274']['value'] 
            except:  
                tr = None
            
            if len(singleIssue.raw['fields']['comment']['comments']) >= 1:
                comments =  str(singleIssue.raw['fields']['comment']['comments'][-1]['body']).strip()[:300]
                comment_created =  singleIssue.raw['fields']['comment']['comments'][-1]['created']
                comment_author =  singleIssue.raw['fields']['comment']['comments'][-1]['author']['displayName']
            else:
                comments =  None
                comment_created = None
                comment_author =  None    
            dic_issues = { 
                 "key":singleIssue.key,
                 "summary":singleIssue.raw['fields']['summary'],
                 "created":singleIssue.raw['fields']['created'],
                 "updated" : singleIssue.raw['fields']['updated'],
                 "creator":singleIssue.raw['fields']['creator']['displayName'],
                 "timeZone": singleIssue.raw['fields']['creator']['timeZone'],
                 "description" : str(singleIssue.raw['fields']['description']).strip()[:300],
                 "labels" : str(singleIssue.raw['fields']['labels']),
                 "aggregateprogress_progress":singleIssue.raw['fields']['aggregateprogress']['progress'],
                 "aggregateprogress_total":singleIssue.raw['fields']['aggregateprogress']['total'],
                 "comment":comments,
                 "comment_created":comment_created,
                 "comment_author":comment_author,
                 "issuetype":singleIssue.raw['fields']['issuetype']['name'],
                 "priority":singleIssue.raw['fields']['priority']['name'],
                 "resolutiondate":singleIssue.raw['fields']['resolutiondate'],
                 "status":singleIssue.raw['fields']['status']['name'],
                 "complejidad" : com,
                 "reporter"  :  reporter ,
                 "asignado"  : asig  ,
                 "Clave"  : singleIssue.raw['fields']['customfield_13292'] ,
                 "Nombre"  : singleIssue.raw['fields']['customfield_17652'] ,
                 "Nombre_Base"  : singleIssue.raw['fields']['customfield_12463'],
                 "Amount"  : singleIssue.raw['fields']['customfield_16855'],
                 "team_reporter": tr , 
                  "Pais"   : pa
            
            }
            dic_issues.update(dic_agg)
            df1 = pd.DataFrame(dic_issues,index = [0])
            df = pd.concat([df,df1])
        got = len(issues)
        total += got
    return df 




        
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
            obj = obj['Body'].read().decode("utf-8").encode('ascii', 'xmlcharrefreplace')
            bytes_io = BytesIO(obj)
            return bytes_io,lm
        else:
            with open(uri) as f:
                return uri,datetime.today()
class Extractor:
    @staticmethod
    def run(filename, **kwargs ):
        file,lm = FileReader.read(filename) 
        print('Inicio')
        df = extrac("ext_vegaviri@mercadolibre.com","ATATT3xFfGF0GpGDDhaTUqvZURCCVfo6ZJNx0ve7Y3EsdZqaFj33NuJhn2RL0uVqCeypJm-3dyfxUUaMD_4-VlyOzIjbwwywQfpJvermryZFxEx1EcytGC-cM_2aqjhuZ_fmY_mAyRnB00Y91mDfLSvuJ7O4vJHI6tehJYzvfNMXrNrPAtgDh0M=3DB7C558")
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df['report_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')  
        print('snoflake')    
        return df

class Extractor_his:
    @staticmethod
    def run(filename, **kwargs ):
        file,lm = FileReader.read(filename) 
        print('Inicio')
        df = extrac_histori("ext_vegaviri@mercadolibre.com","ATATT3xFfGF0GpGDDhaTUqvZURCCVfo6ZJNx0ve7Y3EsdZqaFj33NuJhn2RL0uVqCeypJm-3dyfxUUaMD_4-VlyOzIjbwwywQfpJvermryZFxEx1EcytGC-cM_2aqjhuZ_fmY_mAyRnB00Y91mDfLSvuJ7O4vJHI6tehJYzvfNMXrNrPAtgDh0M=3DB7C558")
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        df['report_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S') 
        print('snoflake       ')    
        return df 
