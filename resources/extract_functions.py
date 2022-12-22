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
