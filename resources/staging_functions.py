import json
import dateutil.parser
import base64 as base64
from bs4 import BeautifulSoup as BeautifulSoup

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

#Format date from string
def format_date(date):
    return dateutil.parser.parse(date).strftime('%D %H:%M:%S')

# Standard transform and extract common metadata
def standard_transform_msg(msg):
    msg = json.loads(msg)
    msg_data = {'id':msg['id']}
    msg_data.update({'mimeType':msg['payload']['mimeType']})
    #return msg
    msg_headers = msg['payload']['headers']
    for item in msg_headers:
        if 'subject' in item['name'].lower():
            msg_data.update({'subject':item['value']})
        if 'date' in item['name'].lower():
            formatDate = format_date(item['value'])
            msg_data.update({'date_string':formatDate})
        if 'from' in item['name'].lower():
            msg_data.update({'from':item['value']})

    # Decode base64 Email body
    # Find all 'data' key in JSON and return an array of values
    body = find_values('data',json.dumps(msg))
    text = []
    for item in body:
        text.append(base64.urlsafe_b64decode(item).decode('utf-8'))
    text = ' '.join(text)
    #return text
        
    # Parse Email body to HTML with BeautifulSoup
    soup = BeautifulSoup(text,'html.parser')
    #msg_data.update({'body':soup.prettify()})
    #return soup
    cleanBody = soup.get_text(strip=True).encode('ascii','ignore').decode('utf-8').replace('\r','').replace('\n','')
    msg_data.update({'body':cleanBody})
    return msg_data
    
def extract_indeed(msg):
    msg = json.loads(msg)
    # Identify the relevant table from INDEED Emails
    # Find all 'data' key in JSON and return an array of values
    body = find_values('data',json.dumps(msg))
    text = []
    for item in body:
        text.append(base64.urlsafe_b64decode(item).decode('utf-8'))
    text = ' '.join(text)
    elements = []
    soup = BeautifulSoup(text,'html.parser')
    for item in soup.find(attrs={'dir':'rtl'}).find_all(['a', 'p']):
        elements.append(item.text.strip())

    # Update metadata
    try:data = {'role':elements[1],'org':elements[3],'location':elements[2].split(' - ')[1]}
    except:data = {}
    return data

def extract_linkedin(msg):
    msg = json.loads(msg)
    body = find_values('data',json.dumps(msg))
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
    if is_application_sent:elements = [x.get_text() for x in soup.find('td').find_all('p')]
    try:data = {'role':elements[1],'org':elements[2]}
    except:data={}
    return data