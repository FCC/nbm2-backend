import socket
import json
import os

db_config = {}
queue_IP_address = socket.gethostbyname(socket.gethostname())

# database information
db_config['db_schema']     = ""                                              # should change from test schema to prod schema
db_config['db_host']       = ""    # should almost never change
db_config['db_user']       = ""                                               # should almost never change
db_config['db_password']   = ""                                               # should almost never change
db_config['db']            = ""                                                  # should almost never change
db_config['SRID']          = ""                                                        # should almost never change
db_config['db_port']       = ""                                                        # should almost never change

# connection information for distributed computing
db_config['queue_ip']          = str(queue_IP_address)
db_config['distributed_port']  = 50000
db_config['queue_auth_key']  = ''

my_json = str(os.path.dirname(os.path.realpath(__file__)))+'/db_config.json'
with open(my_json, 'w') as cf:
    json.dump(db_config, cf)

