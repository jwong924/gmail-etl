import requests
import json
import base64 as base64
from bs4 import BeautifulSoup as BeautifulSoup
from resources.get_token import get_token

# Query into completed pages
def find_item(key,value,content):
    count = 0
    for item in content:
        if value == item[key]:
            return [True,count]
        count+=1
    return [False,count]
  
# Get a list of emails by page
def get_list(query):
    try:
        with open('token.json') as file:
            token = json.load(file)['token']
        headers = {'Authorization':'Bearer '+token}
    except:
        token = json.loads(get_token())['token']
    r = requests.get('https://gmail.googleapis.com/gmail/v1/users/me/messages'+query, headers=headers)
    if r.status_code != 200:
        token = json.loads(get_token())['token']
        headers = {'Authorization':'Bearer '+token}
        r = requests.get('https://gmail.googleapis.com/gmail/v1/users/me/messages'+query, headers=headers)
    return r.text

# Get msg details
def get_msg(id):
    try:
        with open('token.json') as file:
            token = json.load(file)['token']
        headers = {'Authorization':'Bearer '+token}
    except:
        token = json.loads(get_token())['token']
    r = requests.get('https://gmail.googleapis.com/gmail/v1/users/me/messages/'+id, headers=headers)
    if r.status_code != 200: 
        token = json.loads(get_token())['token']
        headers = {'Authorization':'Bearer '+token}
        r = requests.get('https://gmail.googleapis.com/gmail/v1/users/me/messages/'+id, headers=headers)
    return r.text

# Loop through Msg List items
def get_msgs(msg_list):
    msgs = []
    for item in msg_list['messages']:
        try:
            msgs.append(get_msg(item['id']))
        except:
            print('error: '+ str(item['id']))
    return msgs
# Find JSON values by key
def find_values(key, json_repr):
    results=[]
    def _decode_dict(a_dict):
        try:
            results.append(a_dict[key])
        except KeyError:
            pass
        return a_dict
    json.loads(json_repr, object_hook=_decode_dict)
    return results

# Read a file
def read_file(file):
    print('reading: '+file)
    try:
        with open(file,'r') as file:
            content = file.read()
        content = json.loads(content)
        statusCode = 200
    except (FileNotFoundError):
        content = 'file not found'
        statusCode = 404
    except:
        content='unkown error'
        statusCode=400
    return json.dumps({'statusCode':statusCode,'body':content})

# Write file to provided location
def write_file(content, loc):
    with open(loc,'w') as file:
        file.write(json.dumps(content, indent=4))
    return {'statusCode':200}