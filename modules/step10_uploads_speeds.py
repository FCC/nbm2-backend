import os
import json
#############################
# created by Ahmad Aburizaiza
#############################
######################################################
# Step 1 -> reading variables from parent_config.json
######################################################
with open('./step10_config.json') as f:
    conf = json.load(f)

username = conf['uploads']['username']
fcc_secret_token = conf['uploads']['fcc_secret_token']
input_files = conf['uploads']['input_files']
tileset_names = conf['uploads']['tileset_names']
tileset_map_ids = conf['uploads']['tileset_map_ids']
##########################################
# Step 2 -> setting up the credentials url
##########################################
credential_url="https://api.mapbox.com/uploads/v1/"+username+"/credentials?access_token="+fcc_secret_token
###############################################
# Step 3 -> looping through the files to upload
###############################################
for i,v in enumerate(input_files):
    print("step 3", i, v)
    # retrieving connection credintials from Mapbox
    os.system('curl --output ./mapbox.json '+credential_url)
    # saving the credintials to ./mapbox.json
    with open('./mapbox.json') as f:
        mapbox_configs = json.load(f)

    bucket=mapbox_configs['bucket']
    key=mapbox_configs['key']
    accessKeyId=mapbox_configs['accessKeyId']
    secretAccessKey=mapbox_configs['secretAccessKey']
    sessionToken=mapbox_configs['sessionToken']
    url=mapbox_configs['url']
    # setting up environment variables
    os.environ['AWS_ACCESS_KEY_ID'] = accessKeyId
    os.environ['AWS_SECRET_ACCESS_KEY'] = secretAccessKey
    os.environ['AWS_SESSION_TOKEN'] = sessionToken
    # concatinating variables for the following two commands:
    # 1. upload from FCC's EC2 instance to Mapbox's S3 bucket
    # 2. upload from Mapbox's S3 bucket to FCC's Mapbox account
    user_dot_map_id = username+'.'+tileset_map_ids[i]
    data = '{"url":"https://'+bucket+'.s3.amazonaws.com/'+key+'","tileset":"'+user_dot_map_id+'","name":"'+tileset_names[i]+'"}'
    upload_url = "https://api.mapbox.com/uploads/v1/"+username+"?access_token="+fcc_secret_token
    # upload from FCC's EC2 instance to Mapbox's S3 bucket
    os.system('aws s3 cp '+input_files[i]+' s3://'+bucket+'/'+key+' --region us-east-1')
    # upload from Mapbox's S3 bucket to FCC's Mapbox account
    os.system('curl -X POST -H "Content-Type: application/json" -H "Cache-Control: no-cache" -d \''+data+'\' "'+upload_url+'"')