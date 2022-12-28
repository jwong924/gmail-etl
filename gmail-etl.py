import requests
import json
import datetime
import psycopg2
import pathlib
import sys
import os
from resources.get_token import get_token

# Connect into Google API
def google_auth():
    try:
        with open('token.json') as file:
            token = json.load(file)['token']
    except:
        token = json.loads(get_token())['token']
    return token

# Connect into PostgreSQL
def db_auth():
    try:
        conn = psycopg2.connect(
            user=os.environ.get("POSTGRESQL_USER"),
            password=os.environ.get("POSTGRESQL_PASSWORD"),
            host=os.environ.get("POSTGRESQL_HOST"),
            port=int(os.environ.get("POSTGRESQL_PORT")),
            dbname='gmail'
        )
        return conn
    except psycopg2.Error as e:
        print(f'Exception error trying to connect to postgresql server: {e}')
        sys.exit(1)

# Extract Data from Gmail API
def extract():
    timestamp=str(datetime.datetime.now().strftime("%Y-%m-%dT%H%M%S"))
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    msgs=[]
    nextPageToken=None
    query=''
    count=1
    limit=200 # Set limit of Email's to retrieve
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
            print('Querying Google for Email List at page: ' + str(count))
            if nextPageToken:query='?pageToken='+str(nextPageToken)
            list_response = requests.get('https://gmail.googleapis.com/gmail/v1/users/me/messages'+query, headers=headers)
            print('Response Status Code: '+str(list_response.status_code))
            list_response_json = json.loads(list_response.text)
            # Loop through Message List for individual Messages
            for item in list_response_json['messages']:
                item_id=str(item['id'])
                # Query DB to check if messages have been queried already
                cursor.execute(f'select * from emails where id="{item_id}"')
                db_response = cursor.fetchone()
                if db_response:
                    print(str(item['id'])+' has been queried with results: '+str(db_response))
                else:
                    print(str(item['id'])+' has not been queried, adding item to db')
                    # Query Googel API for individual message
                    msg_response = requests.get('https://gmail.googleapis.com/gmail/v1/users/me/messages/'+str(item['id']), headers=headers)
                    msgs.append(json.loads(msg_response.text))
                    cursor.execute(f'insert into emails (id,date) values ({item_id},{today})')
                    count+=1
            # Set nextPageToken
            nextPageToken=list_response_json['nextPageToken']
            print('********** next page token: '+str(nextPageToken)+' **********')
        conn.commit()

        print('Queried: '+str(len(msgs))+' Emails')
        pathlib.Path('./output/raw').mkdir(parents=True,exist_ok=True)
        json_output_name = './output/raw/'+timestamp+'-'+str(len(msgs))+'.json'
        print('Writing result to: '+json_output_name)
        with open(json_output_name,'w') as file:
            file.write(msgs,indent=4)
    except Exception as e:
        print('Extract Function Error: '+str(e))
    finally:
        conn.close()

if __name__ == '__main__':
    extract()