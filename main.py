import json
import datetime
import pathlib
#import sys
import resources

def main():
    print('running script!')
    timestamp=str(datetime.datetime.now().strftime("%Y-%m-%dT%H%M%S"))

    msg_list = json.loads(resources.get_list(''))
    nextPageToken = {
        'date': str(datetime.date.today()),
        'nextPageToken':msg_list['nextPageToken'],
        'status':'Next'
    }
    pageToken_json = json.loads(resources.read_file('pageToken_list.json'))
    if pageToken_json['statusCode'] != 200:
        print(pageToken_json)
        if pageToken_json['statusCode'] == 404:
            print('creating pageToken_list.json')
            resources.write_file([],'pageToken_list.json')
            pageToken_json = []
        #sys.exit()

    msgs = []

    # Initial run
    msgs = msgs + resources.get_msgs(msg_list)

    # Set page processing limit
    count = 1
    limit = 5
    page = 1
    upper_limit = 100
    # Ongoing Runs
    while count < limit and page < upper_limit:
        print('Query page: '+str(page))
        # Set format for nextPageTokens
        nextPageToken = {
            'date': str(datetime.date.today()),
            'nextPageToken':msg_list['nextPageToken'],
            'status':'Next'
        }
        # Check if the nextPageToken is in the saved list
        found=resources.find_item('nextPageToken',nextPageToken['nextPageToken'],pageToken_json['body'])
        #print(found)
        if found[0]:
            print(nextPageToken['nextPageToken']+' is an existing new page')
        elif not found[0]:
            print(nextPageToken['nextPageToken']+' is a new page! Adding token to list')
            pageToken_json['body'].append(nextPageToken)

        # Check if nextPageToken has been queried or not
        if(pageToken_json['body'][found[1]]['status'] == 'Next'):
            print('page has not been queried yet')
            print('processing page: '+ nextPageToken['nextPageToken'])
            msg_list=json.loads(resources.get_list('?pageToken='+nextPageToken['nextPageToken']))
            msgs = msgs + resources.get_msgs(msg_list)
            pageToken_json['body'][found[1]]['status'] = 'Completed'
            count += 1
        else: 
            print('page has been queried')
            msg_list=json.loads(resources.get_list('?pageToken='+nextPageToken['nextPageToken']))
        page += 1
    pathlib.Path('./output/raw').mkdir(parents=True,exist_ok=True)
    json_output_name = './output/raw/raw-'+timestamp+'.json'
    print('Writing: '+json_output_name)
    resources.write_file(msgs,json_output_name)
    print('Updating pageToken_json.json list')
    resources.write_file(pageToken_json['body'],'pageToken_list.json')

if __name__ == '__main__':
    main()