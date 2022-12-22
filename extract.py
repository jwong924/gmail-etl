import json
import datetime
import pathlib
import resources
import mariadb
import sys
import os

def main():
    print('running script!')
    try:
        conn = mariadb.connect(
            user=os.environ.get("MYSQL_USER"),
            password=os.environ.get("MYSQL_PASSWORD"),
            host=os.environ.get("MYSQL_HOST"),
            port=int(os.environ.get("MYSQL_PORT")),
            database='gmail'
        )
        cursor = conn.cursor()
    except mariadb.Error as e:
        print(f'Exception error trying to connect to mariadb: {e}')
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
            cursor.execute('select * from email WHERE id=?',(item_id,))
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
                    insert into email (id,date) values (?,?) 
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