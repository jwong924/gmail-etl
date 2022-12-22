import json

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