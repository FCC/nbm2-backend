from multiprocessing.managers import BaseManager
import NBM2_functions as nbmf 
import multiprocessing as mp
import servant_step0 as ss0
import servant_step2 as ss2
import servant_step3 as ss3
import servant_step5 as ss5 
import servant_step6 as ss6
import traceback 
import json
import time
import gc 


def poolProcessor(worker_name, input_queue, output_queue, config, db_config, 
                    pool_size):
    try:
        processes = [mp.Process(target=worker_name, args=(input_queue,output_queue,config, db_config)) for x in range(pool_size)]

        # start the pool processes
        for p in processes:
            p.start()

        # join the pool processes
        for p in processes:
            p.join()

        # temrinate the pool processes
        for p in processes:
            p.terminate()

    except:
        print(traceback.format_exc())

    gc.collect()
    return

def myMain(input_queue, output_queue, message_queue, config, db_config):
    while True:
        try:
            task = message_queue.get()
            if task == 'load_complex_shape':            # step 0 task 4
                print('running routine to process block and place shape files')
                poolProcessor(ss0.loadComplexShapes, input_queue, output_queue, config, db_config, 6)
                print('completed processing block and place shape files\n')
            if task == 'load_other_files':              # step 0 task 5
                print('running routine to process other shape files and csv files')
                poolProcessor(ss0.loadOtherFiles, input_queue, output_queue, config, db_config, 3)
                print('completed loading other shape files and csv files\n')
            if task == 'parse_blockdf':                 # step 0 task 6
                print('running routine to parse block data into county level geojsons')
                poolProcessor(ss0.parseBlockDF, input_queue, output_queue, config, db_config, 3)
                print('completed parsing block data into county level geojsons\n')
            if task == 'initial_spatial_intersection':  # step 0 task 7
                print('running routines for performing initial spatial intersections')
                poolProcessor(ss0.basicSpatialIntersection, input_queue, output_queue, config, db_config, 8)
                print('completed performing initial spatial intersections\n')
            if task == 'assign_water_blocks':           # step 0 task 8
                print('running routines for assigning water blocks to congressional districts')
                poolProcessor(ss0.assignWaterBlocks, input_queue, output_queue, config, db_config, 8)
                print('completed assigning water blocks to congressional districts\n')
            elif task == 'parse_fbd':                   # step 0 task 13
                print('running routines for parsing fixed broadband data to county level data')
                poolProcessor(ss0.parseFBD, input_queue, output_queue, config, db_config, 8)
                print('completed parsing fixed broadband data to county level\n')
            elif task == 'create_block_numprov':        # step 2 task 4 
                print('running processes for creating the block numprov files')
                poolProcessor(ss2.makeBlockNumProv, input_queue, output_queue, config, db_config, 3)
                print('completed making block numprov\n')
            elif task == 'create_tract_numprov':         # step 3 task 2 
                print('running processes for making tract and county numprov files')
                poolProcessor(ss3.makeLargeTracts,input_queue, output_queue, config, db_config, 3)
                print('completed making tract and county numprov files\n')
            elif task == 'tract_sort':                  # step 5 task 6
                print('running processes for dissolving at the tract level')
                poolProcessor(ss5.genTractGeoJson, input_queue, output_queue, config, db_config, 32)
                print('completed dissolving at the tract level\n')
            elif task == 'provider_files':              # step 5 task 12
                print('running processes for dissolving at the county and hoconum level')
                poolProcessor(ss5.genCountyHoCoNumGeoJson, input_queue, output_queue, config, db_config, 32)
                print('completed dissolving at the county and hoconum level\n')
            elif task == 'initial_geojson':             # step 6 task 1
                print('running processes for making initial geojson files')
                poolProcessor(ss6.genInitialGeoJson, input_queue, output_queue, config, db_config, 8)
                print('completed making initial geojson files\n')
            elif task == 'zoom_mbtiles':                # step 6 task 2a
                print('running processes for creating zoom files')
                poolProcessor(ss6.genZoomTiles, input_queue, output_queue, config, db_config, 4)
                print('completed making zoom files\n')
            elif task == 'large_zoom_mbtiles':          # step 6 task 2b
                print('running processes for making final zoom files')
                poolProcessor(ss6.genLargeZoomTiles, input_queue, output_queue, config, db_config, 2)
                print('completed making final zoom files\n')
            elif task == 'speed_mbtile':                # step 6 task 3
                print('running proceses for making speed level map box tiles')
                poolProcessor(ss6.genInterimTiles, input_queue, output_queue, config, db_config, 3)
                print('completed making speed level map box tiles\n')
            elif task == 'prep_providers':              # step 6 task 4
                print('running processes for preparing provider files')
                poolProcessor(ss6.genPrepProviderTiles, input_queue, output_queue, config, db_config, 2)
                print('completed prepping provider files')
            elif task == 'make_providers':              # step 6 task 5
                print('running processes to make provider files')
                poolProcessor(ss6.genFinalTiles, input_queue, output_queue, config, db_config, 2)
                print('completed making provider files')
            elif task is None:
                break
            else:
                pass
            # sleep to let the other servants get their task
            gc.collect()
            time.sleep(5)
        except:
            # sleep until a new set of tasks come in to be worked
            time.sleep(10)    

if __name__ == '__main__':

    # connect to the distributed queue
    with open('process_config.json') as f:
            config = json.load(f)  

    with open('db_config.json') as f:
            db_config = json.load(f)  

    while True: 
        try:
            continue_run,input_queue, output_queue, message_queue = nbmf.startMultiProcessingQueue(db_config)
            print("connected to", db_config['queue_ip'])
            break
        except:
            print('No Queues yet')
            time.sleep(1)

    if continue_run:
        myMain(input_queue, output_queue, message_queue, config, db_config)

        print('data processing complete - terminating workers and servant')
