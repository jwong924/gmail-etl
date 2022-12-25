import json
import datetime
import pathlib
import resources
import psycopg2
import sys
import os

#POSTGRESQL_HOST=127.0.0.1
#export POSTGRESQL_PORT=5432 
#export POSTGRESQL_USER='gmail_user'
#export POSTGRESQL_PASSWORD='Gmail$123'

def main():
    print('running script!')
    try:
        conn = psycopg2.connect(
            user=os.environ.get("POSTGRESQL_USER"),
            password=os.environ.get("POSTGRESQL_PASSWORD"),
            host=os.environ.get("POSTGRESQL_HOST"),
            port=int(os.environ.get("POSTGRESQL_PORT")),
            dbname='gmail'
        )
        cursor = conn.cursor()
    except psycopg2.Error as e:
        print(f'Exception error trying to connect to postgresql server: {e}')
        sys.exit(1)

    timestamp=str(datetime.datetime.now().strftime("%Y-%m-%dT%H%M%S"))
    msgs = []
    nextPageToken = ''
    r = json.loads(resources.get_list(nextPageToken))
    #print(r)
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    print(today)

    limit = 200
    count = 0
    msgs = []
    while count < limit:
        for item in r['messages']:
            response = None
            item_id = str(item['id'])
            cursor.execute('select * from emails WHERE id=%s',(item_id,))
            response = cursor.fetchone()
            if response:
                print(item['id'] +' has been queried with results: '+str(response))
            else:
                print(item['id'] + ' has not been queried')
                try:
                    print('querying: '+item['id'])
                    msgs.append(resources.get_msg(item['id']))
                    print('adding '+item['id']+' to db')
                    cursor.execute(
                    '''
                    insert into emails (id,date) values (%s,%s) 
                    ''',(item_id,today))
                    count+=1
                except Exception as e:
                    print(item['id']+' had an error: '+e)
        nextPageToken = r['nextPageToken']
        print('********** next page token: '+str(nextPageToken)+' **********')
        r = json.loads(resources.get_list('?pageToken='+nextPageToken))
    conn.commit()
    conn.close()
    print('Queried '+str(len(msgs))+' Emails')
    pathlib.Path('./output/raw').mkdir(parents=True,exist_ok=True)
    json_output_name = './output/raw/'+timestamp+'-'+str(len(msgs))+'.json'
    print('Writing: '+json_output_name)
    resources.write_file(msgs,json_output_name)
    return json_output_name

if __name__ == '__main__':
    main()