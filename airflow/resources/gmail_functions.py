import requests
import json
import datetime
import psycopg2
import sys
import os
import dateutil.parser
import base64 as base64
import pandas as pd
import csv
from bs4 import BeautifulSoup as BeautifulSoup
from google.cloud import storage # pip install google-cloud-storage
from resources import get_token
from airflow.decorators import task
from airflow.hooks.base import BaseHook

# Connect into Google API
def google_auth():
    try:
        with open('token.json') as file:
            token = json.load(file)['token']
    except:
        token = get_token()['token']
    return token

# Connect into PostgreSQL
def db_auth():
    try:
        db_details = BaseHook.get_connection('gmail-etl')
        """
        conn = psycopg2.connect(
            user=os.environ.get("POSTGRESQL_USER"),
            password=os.environ.get("POSTGRESQL_PASSWORD"),
            host=os.environ.get("POSTGRESQL_HOST"),
            port=int(os.environ.get("POSTGRESQL_PORT")),
            dbname='gmail'
        )
        """
        conn = psycopg2.connect(
            user=db_details.login,
            password=db_details.password,
            host=db_details.host,
            port=db_details.port,
            dbname=db_details.schema
        )
        return conn
    except psycopg2.Error as e:
        print(f'Exception error trying to connect to postgresql server: {e}')
        sys.exit(1)

def write_to_gcs(data,bucket_name,blob_name):
    # Connect to Google Bucket
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']='ServiceKey_GoogleCloud.json'
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.open('w').write(data)
        print('File written to '+bucket_name+'/'+blob_name)
        return json.dumps({"StatusCode":200},indent=4)
    except Exception as e:
        print('Error in writting to Google Cloud Storage: '+str(e))
        return json.dumps({"StatusCode":400,"Error":"Error in writting to Google Cloud Storage"+str(e)},indent=4)

# Extract Data from Gmail API
@task()
def extract():
    msgs=[]
    nextPageToken=None
    query=''
    count=1
    limit=20 # Set limit of Email's to retrieve
    try:
        # Set Google Auth Header
        token = google_auth()
        print('Authorizing into Google API with token '+token)
        headers = {'Authorization':'Bearer '+token}
        # Connect to DB
        print('Authorizing into local DB...')
        conn = db_auth()
        cursor = conn.cursor()
        # Loop counter till limit
        while count <= limit:
            # Query Google API for List of emails at Page
            if nextPageToken:query='?pageToken='+str(nextPageToken)
            list_response = requests.get('https://gmail.googleapis.com/gmail/v1/users/me/messages'+query, headers=headers)
            print('Response Status Code: '+str(list_response.status_code))
            list_response_json = json.loads(list_response.text)
            # Loop through Message List for individual Messages
            for item in list_response_json['messages']:
                if count > limit: break
                item_id=str(item['id'])
                # Query DB to check if messages have been queried already
                cursor.execute(f"SELECT * FROM emails WHERE id = '{item_id}';")
                db_response = cursor.fetchone()
                if db_response:
                    print(str(item['id'])+' has been queried with results: '+str(db_response))
                else:
                    print(str(item['id'])+' has not been queried, adding item to array')
                    # Query Googel API for individual message
                    msg_response = requests.get('https://gmail.googleapis.com/gmail/v1/users/me/messages/'+str(item['id']), headers=headers)
                    msgs.append(json.loads(msg_response.text))
                    count+=1
            # Set nextPageToken
            nextPageToken=list_response_json['nextPageToken']
            print('********** next page token: '+str(nextPageToken)+' **********')
        print('Queried: '+str(len(msgs))+' Emails')
    except Exception as e:
        print('Extract Function Error: '+str(e))
    finally:
        conn.close()
    return msgs

# Write data to Google Cloud Storage
@task()
def write_raw(data):
    print(str(len(data))+' messages to write')
    if not len(data) > 0: return 'No messages to write'
    timestamp=datetime.datetime.now().strftime("%Y-%m-%dT%H%M%S")
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    # Connect to local DB
    try:
        conn = db_auth()
        cursor = conn.cursor()
    except Exception as e:
        print('Error connecting to local DB ' + str(e))
        conn.close()
        return

    for item in data:
        item_id = str(item['id'])
        # Query DB to check if messages have been queried already
        cursor.execute(f"SELECT * FROM emails WHERE id = '{item_id}';")
        db_response = cursor.fetchone()
        if db_response:
            print(str(item['id'])+' has been queried with results: '+str(db_response)+', removing from data array')
            # Remove queried item from data array
            data.remove(item)
        else:
            print(str('Adding '+item['id'])+' to local DB')
            cursor.execute(f"insert into emails (id,date) values ('{item_id}','{today}')")
    bucket_name = 'gmail-etl'
    blob_name = 'raw/'+str(timestamp)+'.json'
    r=write_to_gcs(json.dumps(data),bucket_name,blob_name)
    r=json.loads(r)
    if r['StatusCode']==200:conn.commit()
    else: print(json.dumps(r,indent=4))
    conn.close()
    return 'Data written to: '+bucket_name+'/'+blob_name

# Find JSON values by key
def find_json_values(key, json_repr):
    results=[]
    def _decode_dict(a_dict):
        try:
            results.append(a_dict[key])
        except KeyError:
            pass
        return a_dict
    json.loads(json_repr, object_hook=_decode_dict)
    return results

# Extract data from Indeed Emails
@task()
def extract_indeed(data):
    # Find all 'data' key in JSON and return an array of values
    body = find_json_values('data',json.dumps(data))
    text = []
    for item in body:
        text.append(base64.urlsafe_b64decode(item).decode('utf-8'))
    text = ' '.join(text)
    elements = []
    soup = BeautifulSoup(text,'html.parser')
    for item in soup.find(attrs={'dir':'rtl'}).find_all(['a', 'p']):
        elements.append(item.text.strip())
    # Update metadata
    try:indeed_data = {'role':elements[1],'org':elements[3],'location':elements[2].split(' - ')[1]}
    except:indeed_data = {}
    return indeed_data

# Extract data from LinkedIn Emails
def extract_linkedin(data):
    body = find_json_values('data',json.dumps(data))
    text = []
    for item in body:
        text.append(base64.urlsafe_b64decode(item).decode('utf-8'))
    text = ' '.join(text)
    elements = []
    is_application_sent = False
    soup = BeautifulSoup(text,'html.parser')
    title = soup.find('h2')
    for item in title:
        if 'Your application was sent to' in item.text.strip(): is_application_sent = True
    # Update metadata
    if is_application_sent:elements = [x.get_text() for x in soup.find('td').find_all('p')]
    try:linkedin_data = {'role':elements[1],'org':elements[2]}
    except:linkedin_data={}
    return linkedin_data

# Transform to Stage 1
def transform_raw(raw_data):
    formatted_data=[]
    for item in raw_data:
        print('Processing item: '+item['id'])
        # Extract standard meta data
        formatted_email = {
            'id':item['id'],
            'mimeType':item['payload']['mimeType']
        }
        # Loop through headers to find metadata
        for header in item['payload']['headers']:
            if 'subject' in header['name'].lower():
                formatted_email.update({'subject':header['value']})
            if 'date' in header['name'].lower():
                try:
                    formatted_date = dateutil.parser.parse(header['value']).strftime('%D %H:%M:%S')
                except:
                    formatted_date = dateutil.parser.parse(header['value'],fuzzy=True).strftime('%D %H:%M:%S')
                formatted_email.update({'date_string':formatted_date})
            if 'from' in header['name'].lower():
                formatted_email.update({'from':header['value']})
        
        # Find and extract all 'Body' data and translate Base64 to utf-8
        body_array = find_json_values('data',json.dumps(item))
        body_text = []
        for body in body_array:
            body_text.append(base64.urlsafe_b64decode(body).decode('utf-8'))
        # Join body of texts
        body_text = ' '.join(body_text)

        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(body_text,'html.parser')
        clean = soup.get_text(strip=True).encode('ascii','ignore').decode('utf-8').replace('\r','').replace('\n','')
        formatted_email.update({'body':clean})
        if('indeedapply@indeed.com' in formatted_email['from']):
            formatted_email.update(extract_indeed(item))
        if('jobs-noreply@linkedin.com' in formatted_email['from']):
            formatted_email.update(extract_linkedin(item))
        formatted_data.append(formatted_email)
    return formatted_data

@task()
def write_stage_1(formatted_data):
    timestamp=datetime.datetime.now().strftime("%Y-%m-%dT%H%M%S")
    df = pd.DataFrame(formatted_data)
    print(df.head())
    bucket_name = 'gmail-etl'
    blob_name = 'stage-1/'+str(timestamp)+'.csv'
    write_to_gcs(df.to_csv(index=False, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8-sig'),bucket_name,blob_name)
    return blob_name

if __name__ == '__main__':
    msgs = extract()
    if len(msgs) > 0:
        write_raw(msgs)
        formatted_msgs=transform_raw(msgs)
        write_stage_1(formatted_msgs)