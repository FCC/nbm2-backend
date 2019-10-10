import step0_main_blockmaster           as s0
import step1_main_provider_table        as s1
import step2_main_make_block_numprov    as s2
import step3_main_tract_county_numprov  as s3
import step4_main_geo_mapbox_tiles      as s4
import step5_main_create_geometry       as s5
import step6_main_create_speed_mbtiles  as s6
from NBM2_process_config import config
from NBM2_db_config import db_config
import NBM2_functions as nbmf
import socket
import json
import time
import os
import gc 

# connect to the queue
while True:
    try:
        continue_run, input_queue, output_queue, message_queue = nbmf.startMultiProcessingQueue(db_config)
        print("INFO - MAIN (MASTER): CONNECTED TO DISTRIBUTED QUEUE")
        break
    except:
        print('INFO - MAIN (MASTER): NO QUEUE DETECTED - ENSURE IT IS RUNNING')
        time.sleep(1)

# run through the input, message, and output queues to purge them of any tasks
# that may be left over from a previously terminated run
try:
    for q in [input_queue, output_queue,message_queue]:
        for _ in range(q.qsize()):
            try:
                q.get_nowait()
            except:
                pass
    print('INFO - MAIN (MASTER): CONFIRMED QUEUES ARE EMPTY')
except:
    continue_run = False 
    my_message = """
        ERROR - MAIN (MASTER): EXCEPTION WHILE CHECKING QUEUES - TERMINATE 
        QUEUES AND RESTART THEM
        """
    print(' '.join(my_message.split()))    

# iterate through the 7 steps
os.system("sudo sh -c 'echo 1 >/proc/sys/vm/drop_caches'")
if continue_run:
    for i in range(7):
        try:
            if config['steps']['step%s' % i]:
                print("INFO - MAIN (MASTER): PREPARING TO RUN STEP %s" % i)
                current_main = eval("s%s.myMain" % i)
                continue_run = current_main(config, db_config, input_queue, output_queue, message_queue)
                gc.collect()
                if not continue_run:
                    print("ERROR - STEP %s (MASTER): THERE WAS AN ERROR IN RUNNING THE STEP MODULE - CHECK THE LOGS" % i)
                    break
                else:
                    print("INFO - MAIN (MASTER): COMPLETED STEP %s OF 6 STEPS" % i)
                    os.system("sudo sh -c 'echo 1 >/proc/sys/vm/drop_caches'")
            else:
                print("WARNING - MAIN (MASTER): SKIPPING STEP %s" % i) 
        except: 
            pass
            