import resources
import os
import json
import pathlib

def main():
    # Set or Create Paths
    pathlib.Path('./output/raw/processed').mkdir(parents=True,exist_ok=True)
    raw_path = './output/raw/'
    pathlib.Path('./output/stage').mkdir(parents=True,exist_ok=True)
    stage_path = './output/stage/'
    # Get all filenames in ./output/raw directory
    filenames = next(os.walk(raw_path), (None, None, []))[2]
    print(str(len(filenames))+' files to be processed, files: ' + str(filenames))

    # Loop through all files
    for filename in filenames:
        print('opening '+filename)
        with open(raw_path+filename) as file:
            contents = json.loads(file.read())
        formatted_msgs = []
        # Format each item and append to formatted_msgs
        for item in contents:
            formatted_msg = resources.standard_transform_msg(item)
            if('indeedapply@indeed.com' in formatted_msg['from']):
                #print(formatted_msg)
                formatted_msg.update(resources.extract_indeed(item))
            if('jobs-noreply@linkedin.com' in formatted_msg['from']):
                #print(formatted_msg)
                formatted_msg.update(resources.extract_linkedin(item))
            formatted_msgs.append(formatted_msg)
        # Write formatted_msgs to ./output/stage/staged directory
        print(f('writing formatted output of {filename} to stage directory'))
        resources.write_file(formatted_msgs,stage_path+filename)
        # Move raw files to processed directory
        print(f'moving {filename} to processed directory')
        os.rename(raw_path+filename,raw_path+'processed/'+filename)

if __name__ == '__main__':
    main()