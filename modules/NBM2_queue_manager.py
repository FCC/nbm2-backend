from multiprocessing.managers import BaseManager
from queue import Queue
import socket
import json

queue_IP_address = socket.gethostbyname(socket.gethostname())

with open('db_config.json') as f:
    db_config = json.load(f)


with open('process_config.json') as f:
    config = json.load(f)


ip_file = config['temp_csvs_dir_path'] + 'queue_ip.txt'
with open(ip_file, 'w') as my_file:
    my_file.write(queue_IP_address)

input_queue = Queue()
output_queue = Queue()
message_queue = Queue()

manager = BaseManager(  address=('', db_config['distributed_port']), 
                        authkey= bytes(db_config['queue_auth_key'], 'utf-8'))
manager.register('input_queue',callable = lambda:input_queue)
manager.register('output_queue',callable = lambda:output_queue)
manager.register('message_queue',callable = lambda:message_queue)

print ('running a queue here')
server = manager.get_server()
server.serve_forever()
