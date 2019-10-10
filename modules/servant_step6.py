import multiprocessing as mp
import pandas as pd 
import traceback 
import socket
import time 
import os

# used in step 6 task 1
def genInitialGeoJson(input_queue, output_queue, config, db_config):
    """
    creates geojson files that contain zoom level information 

    Arguments In:
        input_queue:    a multiprocessing queue that can be shared across
                        multiple servers and cores.  All information to be
                        processed is loaded into the queue
        output_queue:   a multiprocessing queue that can be shared across
                        multiple servers and cores.  All results from the 
                        various processes are loaded into the queue
        config:         a dictionary variabel that contains the configuration
                        parameters for the step
        db_config:      a dictionary that contains the configuration
                        information for the database and queue

    Arguments Out: 
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be exectuted    
    """        
    # capture the process name
    my_name = mp.current_process().name
    my_IP_address = socket.gethostbyname(socket.gethostname())

    while True:
        try:
            # get the next element out of the queue
            element = input_queue.get()
            if element[0] is None: break

            # extract the terms from the queue
            try:
                speed       = element[0]
                u_speed     = element[0].split('_')[0]
                file_type   = element[1]
            except:
                output_queue.put(traceback.format_exc())
            try:
                temp_time = time.localtime()
                if file_type == 'county':
                    my_message = '%s - STEP 6 (%s - %s): %s CREATING COUNTY DATA (FOR ZOOM 0-4) FOR %s SPEED'
                    os.system('time tippecanoe-json-tool -c '+\
                        config['temp_csvs_dir_path']+'county_numprov/county_numprov_sort_round_%s_%s.csv ' % (speed, config['fbd_vintage']) +\
                        config['temp_speed_geojson_path']+'county_%s_500k_%s.sort.geojson | gzip > ' % (config['census_vintage'],db_config['SRID'])+\
                        config['temp_speed_geojson_path']+'counties_500k_%s.geojson.gz' % u_speed)

                elif file_type == 'tract':
                    my_message = '%s - STEP 6 (%s - %s): %s CREATING TRACT DATA (FOR ZOOM 5-8) FOR %s SPEED'
                    os.system('time tippecanoe-json-tool -c '+\
                        config['temp_csvs_dir_path']+'tract_numprov/tract_numprov_sort_round_%s_%s.csv ' % (speed, config['fbd_vintage']) +\
                        config['temp_speed_geojson_path']+'tract_%s_500k_%s.sort.geojson | gzip > ' % (config['census_vintage'],db_config['SRID'])+\
                        config['temp_speed_geojson_path']+'tracts_500k_%s.geojson.gz' % u_speed)

                elif file_type == 'not big':
                    my_message = '%s - STEP 6 (%s - %s): %s CREATING NOT-BIG TRACT DATA (FOR ZOOM 9) FOR %s SPEED'
                    os.system('time tippecanoe-json-tool -c '+\
                        config['temp_csvs_dir_path']+'tract_numprov/tract_numprov_sort_round_%s_%s.csv ' % (speed, config['fbd_vintage']) +\
                        config['temp_speed_geojson_path']+'notbig_tracts_5e8_%s_%s.sort.geojson | gzip > ' % (config['census_vintage'],db_config['SRID'])+\
                        config['temp_speed_geojson_path']+'tracts_%s.geojson.gz' % u_speed)

                elif file_type == 'big':
                    my_message = '%s - STEP 6 (%s - %s): %s CREATING LARGE TRACT DATA (FOR ZOOM 9) FOR %s SPEED'
                    os.system('time tippecanoe-json-tool -c '+\
                        config['temp_csvs_dir_path']+'block_numprov/block_numprov_%s_%s.csv ' % (speed, config['fbd_vintage']) +\
                        config['temp_speed_geojson_path']+'bigtract_blocks_5e8_%s_%s.sort.geojson | gzip > ' % (config['census_vintage'],db_config['SRID'])+\
                        config['temp_speed_geojson_path']+'big_tract_blocks_%s.geojson.gz' % u_speed)

                elif file_type == 'block':
                    my_message = '%s - STEP 6 (%s - %s): %s CREATING BLOCK DATA (FOR ZOOM 11) FOR %s SPEED'
                    os.system('time tippecanoe-json-tool -c '+\
                        config['temp_csvs_dir_path']+'block_numprov/block_numprov_%s_%s.csv ' % (speed, config['fbd_vintage']) +\
                        config['temp_speed_geojson_path']+'block_us_%s.sort.geojson| gzip > ' % config['census_vintage']+\
                        config['temp_speed_geojson_path']+'blocks_%s.geojson.gz' % u_speed)

                else:
                    pass

                # successfully completed the task.  update the message and send to the queue
                my_message = my_message % ('INFO', my_IP_address, my_name, 'COMPLETED', speed)
                output_queue.put((my_message, temp_time, time.localtime()))    

            except:
                # failed the task.  update the message and send to the queue
                my_message = my_message % ('ERROR', my_IP_address, my_name, 'FAILED', speed)
                my_message = my_message + '\n' + traceback.format_exc()
                output_queue.put((my_message, temp_time, time.localtime()))                

        except:
            # queue is empty wait for a task to be assigned
            time.sleep(1)

    return True

def genZoomTiles(input_queue, output_queue, config, db_config):
    """
    creates the zoom level mapbox tiles 

    Arguments In:
        input_queue:    a multiprocessing queue that can be shared across
                        multiple servers and cores.  All information to be
                        processed is loaded into the queue
        output_queue:   a multiprocessing queue that can be shared across
                        multiple servers and cores.  All results from the 
                        various processes are loaded into the queue
        config:         a dictionary variabel that contains the configuration
                        parameters for the step
        db_config:      a dictionary that contains the configuration
                        information for the database and queue

    Arguments Out: 
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be exectuted    
    """            
    # capture the process name
    my_name = mp.current_process().name
    my_IP_address = socket.gethostbyname(socket.gethostname())

    while True:
        try:
            # get the next element out of the queue
            element = input_queue.get()
            if element[0] is None: break

            # extract the terms from the queue
            try:
                speed       = element[0]
                u_speed     = element[0].split('_')[0]
                file_type   = element[1]
            except: 
                output_queue.put(traceback.format_exc())
            try:
                temp_time = time.localtime()
                if file_type == 'county':
                    my_message = '%s - STEP 6 (%s - %s): %s CREATING MAP BOX TILES FOR ZOOM 0-4 (COUNTIES) FOR %s SPEED'
                    os.system('gzip -dc '+config['temp_speed_geojson_path']+'counties_500k_%s.geojson.gz ' % u_speed +\
                        '| time tippecanoe -P -Z 0 -z 4 --detect-shared-borders -l %s_%s -x geoid%s -f -o ' % (config['fbd_vintage'],u_speed,config['census_vintage'][2:]) +\
                        config['temp_mbtiles_path']+'county_%s.mbtiles 2>&1 | tee ../logs/county_%s.log' % (u_speed, u_speed))

                elif file_type == 'tract_z5':
                    my_message = '%s - STEP 6 (%s - %s): %s CREATING MAP BOX TILES FOR ZOOM 5 (TRACTS, WITH COALESCE AS NEEDED) FOR %s SPEED'
                    os.system('gzip -dc '+config['temp_speed_geojson_path']+'tracts_500k_%s.geojson.gz ' % u_speed +\
                        '| time tippecanoe -P -Z 5 -z 5 -S 8 --detect-shared-borders --coalesce ' +\
                        '--coalesce-smallest-as-needed -l %s_%s -x tract_id -f -o ' % (config['fbd_vintage'],u_speed)+\
                        config['temp_mbtiles_path']+'tract_z5_%s.mbtiles 2>&1 | tee ../logs/tract_z5_%s.log' % (u_speed, u_speed))

                elif file_type == 'tract_z6':
                    my_message = '%s - STEP 6 (%s - %s): %s CREATING MAP BOX TILES FOR ZOOM 6-8 (TRACTS) FOR %s SPEED'
                    os.system('gzip -dc '+config['temp_speed_geojson_path']+'tracts_500k_%s.geojson.gz ' % u_speed +\
                        '| time tippecanoe -P -Z 6 -z 8 --detect-shared-borders -l %s_%s -x tract_id -f -o ' % (config['fbd_vintage'],u_speed) +\
                        config['temp_mbtiles_path']+'tract_z6_8_%s.mbtiles 2>&1 | tee ../logs/tract_z6_8_%s.log' % (u_speed, u_speed))
                
                elif file_type == 'tract_z9':
                    my_message = '%s - STEP 6 (%s - %s): %s CREATING MAP BOX TILES FOR ZOOM 9 (SMALLER TRACTS IN BIG TRACTS) FOR %s SPEED'
                    os.system('gzip -dc '+config['temp_speed_geojson_path']+'tracts_%s.geojson.gz ' % u_speed +\
                        '| time tippecanoe -P -Z 9 -z 9 --detect-shared-borders -l %s_%s -x tract_id -f -o ' % (config['fbd_vintage'],u_speed) +\
                        config['temp_mbtiles_path']+'tract_z9_%s.mbtiles 2>&1 | tee ../logs/tract_z9_%s.log' % (u_speed, u_speed))

                else:
                    pass

                # successfully completed the task.  update the message and send to the queue
                my_message = my_message % ('INFO', my_IP_address, my_name, 'COMPLETED', speed)
                output_queue.put((my_message, temp_time, time.localtime()))

            except:
                # failed the task.  update the message and send to the queue
                my_message = my_message % ('ERROR', my_IP_address, my_name, 'FAILED', speed)
                my_message = my_message + '\n' + traceback.format_exc()
                output_queue.put((my_message, temp_time, time.localtime())) 

        except:
            # queue is empty wait for a task to be assigned
            time.sleep(1)
    return

def genLargeZoomTiles(input_queue, output_queue, config, db_config):
    """
    creates large tract zoom files for each speed 

    Arguments In:
        input_queue:    a multiprocessing queue that can be shared across
                        multiple servers and cores.  All information to be
                        processed is loaded into the queue
        output_queue:   a multiprocessing queue that can be shared across
                        multiple servers and cores.  All results from the 
                        various processes are loaded into the queue
        config:         a dictionary variabel that contains the configuration
                        parameters for the step
        db_config:      a dictionary that contains the configuration
                        information for the database and queue

    Arguments Out: 
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be exectuted    
    """        
    # capture the process name
    my_name = mp.current_process().name
    my_IP_address = socket.gethostbyname(socket.gethostname())

    while True:
        try:
            # get the next element out of the queue
            element = input_queue.get()
            if element[0] is None: break

            # extract the terms from the queue
            try:
                speed       = element[0]
                u_speed     = element[0].split('_')[0]
                file_type   = element[1]
            except: 
                output_queue.put(traceback.format_exc())
            try:
                temp_time = time.localtime()
                if file_type == 'block_z9':
                    my_message = '%s - STEP 6 (%s - %s): %s CREATING MAP BOX TILES FOR ZOOM 9 (BLOCKS IN BIG TRACTS) FOR %s SPEED'
                    os.system('gzip -dc '+config['temp_speed_geojson_path']+'big_tract_blocks_%s.geojson.gz ' % u_speed +\
                        '| time tippecanoe -P -Z 9 -z 9 --detect-shared-borders -l %s_%s -x geoid%s -f -o ' % (config['fbd_vintage'],u_speed,config['census_vintage'][2:]) +\
                        config['temp_mbtiles_path']+'block_z9_%s.mbtiles 2>&1 | tee ../logs/block_z9_%s.log' % (u_speed, u_speed))

                elif file_type == 'block_z10':
                    my_message = '%s - STEP 6 (%s - %s): %s CREATING MAP BOX TILES FOR ZOOM 10 (BLOCKS, WITH SIMPLIFICATION AS NEEDED) FOR %s SPEED'
                    os.system('gzip -dc '+config['temp_speed_geojson_path']+'blocks_%s.geojson.gz ' % u_speed +\
                        '| time tippecanoe -P -Z 10 -z 10 -S 8 --detect-shared-borders -l %s_%s --coalesce ' % (config['fbd_vintage'],u_speed) +\
                        '--coalesce-smallest-as-needed -x geoid%s -f -o ' % config['census_vintage'][2:] +\
                        config['temp_mbtiles_path']+'block_z10_%s.mbtiles 2>&1 | tee ../logs/block_z10_%s.log' % (u_speed, u_speed))

                elif file_type == 'block_z11':
                    my_message = '%s - STEP 6 (%s - %s): %s CREATING MAP BOX TILES FOR ZOOM 11+ (BLOCKS, WITH HIGHER DETAIL AT HIGHEST ZOOM VIA -d) FOR %s SPEED'
                    os.system('gzip -dc '+config['temp_speed_geojson_path']+'blocks_%s.geojson.gz ' % u_speed +\
                        '| time tippecanoe -P -Z 11 -z 14 -d 14 --detect-shared-borders -l %s_%s -f -o ' % (config['fbd_vintage'],u_speed) +\
                        config['temp_mbtiles_path']+'block_z11_14_%s.mbtiles 2>&1 | tee ../logs/block_z11_14_%s.log' % (u_speed, u_speed))

                else:
                    pass

                # successfully completed the task.  update the message and send to the queue
                my_message = my_message % ('INFO', my_IP_address, my_name, 'COMPLETED', speed)
                output_queue.put((my_message, temp_time, time.localtime()))

            except:
                # failed the task.  update the message and send to the queue
                my_message = my_message % ('ERROR', my_IP_address, my_name, 'FAILED', speed)
                my_message = my_message + '\n' + traceback.format_exc()
                output_queue.put((my_message, temp_time, time.localtime())) 

        except:
            # queue is empty wait for a task to be assigned
            time.sleep(1)
    return

def genInterimTiles(input_queue, output_queue, config, db_config):
    """
    generates the single mapbox file for all zoom levels for each speed 

    Arguments In:
        input_queue:    a multiprocessing queue that can be shared across
                        multiple servers and cores.  All information to be
                        processed is loaded into the queue
        output_queue:   a multiprocessing queue that can be shared across
                        multiple servers and cores.  All results from the 
                        various processes are loaded into the queue
        config:         a dictionary variabel that contains the configuration
                        parameters for the step
        db_config:      a dictionary that contains the configuration
                        information for the database and queue

    Arguments Out: 
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be exectuted    
    """        
    # capture the process name
    my_name = mp.current_process().name
    my_IP_address = socket.gethostbyname(socket.gethostname())

    while True:
        try:
            # get the next element out of the queue
            element = input_queue.get()
            if element[0] is None: break

            # extract the terms from the queue
            speed       = element[0]
            u_speed     = element[0].split('_')[0]
            file_type   = element[1]
            try:
                temp_time = time.localtime()
                if file_type == 'interim':
                    my_message = '%s - STEP 6 (%s - %s): %s COMBINING FILES INTO ONE MBTILE FILE FOR SPEED %s'
                    os.system('tile-join -n %s_%s -o ' % (config['fbd_vintage'],u_speed) +\
                        config['output_mbtiles_path']+'%s_%s.mbtiles -f ' % (config['fbd_vintage'],u_speed) +\
                        config['temp_mbtiles_path']+'county_%s.mbtiles ' % u_speed +\
                        config['temp_mbtiles_path']+'tract_z5_%s.mbtiles ' % u_speed +\
                        config['temp_mbtiles_path']+'tract_z6_8_%s.mbtiles ' % u_speed +\
                        config['temp_mbtiles_path']+'tract_z9_%s.mbtiles ' % u_speed +\
                        config['temp_mbtiles_path']+'block_z9_%s.mbtiles ' % u_speed +\
                        config['temp_mbtiles_path']+'block_z10_%s.mbtiles ' % u_speed +\
                        config['temp_mbtiles_path']+'block_z11_14_%s.mbtiles' % u_speed)

                else:
                    pass

                # successfully completed the task.  update the message and send to the queue
                my_message = my_message % ('INFO', my_IP_address, my_name, 'COMPLETED', speed)
                output_queue.put((my_message, temp_time, time.localtime()))   

            except:
                # failed the task.  update the message and send to the queue
                my_message = my_message % ('ERROR', my_IP_address, my_name, 'FAILED', speed)
                my_message = my_message + '\n' + traceback.format_exc()
                output_queue.put((my_message, temp_time, time.localtime())) 

        except:
            # queue is empty wait for a task to be assigned
            time.sleep(1)

    return


def genPrepProviderTiles(input_queue, output_queue, config, db_config):
    """
    creates the initial provider detail mapbox tile file for each speed 

    Arguments In:
        input_queue:    a multiprocessing queue that can be shared across
                        multiple servers and cores.  All information to be
                        processed is loaded into the queue
        output_queue:   a multiprocessing queue that can be shared across
                        multiple servers and cores.  All results from the 
                        various processes are loaded into the queue
        config:         a dictionary variabel that contains the configuration
                        parameters for the step
        db_config:      a dictionary that contains the configuration
                        information for the database and queue

    Arguments Out: 
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be exectuted    
    """        
    # capture the process name
    my_name = mp.current_process().name
    my_IP_address = socket.gethostbyname(socket.gethostname())
 
    while True:
        try:
            # get the next element out of the queue
            element = input_queue.get()
            if element[0] is None: break

            # extract the terms from the queue
            file_type   = element

            try:
                temp_time = time.localtime()
                if file_type == 'large7':
                    my_message = '%s - STEP 6 (%s - %s): %s MAKING ZOOM 0 - 7 LARGE PROVIDER FILE'
                    os.system('tippecanoe -P -Z 0 -z 7 -S 8 -x county_fips --preserve-input-order ' +\
                        '--coalesce --detect-shared-borders -l %s_prov_lg -f -o ' % config['fbd_vintage'] +\
                        config['temp_mbtiles_path']+'%s_prov_lg_z0_7.mbtiles ' % config['fbd_vintage'] +\
                        config['temp_speed_geojson_path']+'%s_prov_lg.geojson 2>&1 | tee ../logs/prov_large_z7.log' % config['fbd_vintage'])

                elif file_type == 'large12':
                    my_message = '%s - STEP 6 (%s - %s): %s MAKING ZOOM 8 - 12 LARGE PROVIDER FILE'
                    os.system('tippecanoe -P -Z 8 -z 12 -x county_fips --preserve-input-order ' +\
                        '--coalesce -d 14 --detect-shared-borders -l %s_prov_lg -f -o ' % config['fbd_vintage'] +\
                        config['temp_mbtiles_path']+'%s_prov_lg_z8_12.mbtiles ' % config['fbd_vintage'] +\
                        config['temp_speed_geojson_path']+'%s_prov_lg.geojson 2>&1 | tee ../logs/prov_large.log' % config['fbd_vintage']) 

                elif file_type == 'other8':
                    my_message = '%s - STEP 6 (%s - %s): %s MAKING ZOOM 0 - 8 OTHER PROVIDER FILE'
                    os.system('tippecanoe -P -Z 0 -z 8 -S 8 -x county_fips --preserve-input-order ' +\
                        '--coalesce --detect-shared-borders -l %s_prov_other -f -o ' % config['fbd_vintage'] +\
                        config['temp_mbtiles_path']+'%s_prov_other_z0_8.mbtiles ' % config['fbd_vintage'] +\
                        config['temp_speed_geojson_path']+'%s_prov_other.geojson 2>&1 | tee ../logs/prov_other_z7.log' % config['fbd_vintage']) 

                elif file_type == 'other12':
                    my_message = '%s - STEP 6 (%s - %s): %s MAKING ZOOM 9 - 12 OTHER PROVIDER FILE'
                    os.system('tippecanoe -P -Z 9 -z 12 -x county_fips --preserve-input-order ' +\
                        '--coalesce -d 14 --detect-shared-borders -l %s_prov_other -f -o ' % config['fbd_vintage'] +\
                        config['temp_mbtiles_path']+'%s_prov_other_z9_12.mbtiles ' % config['fbd_vintage'] +\
                        config['temp_speed_geojson_path']+'%s_prov_other.geojson 2>&1 | tee ../logs/prov_other.log' % config['fbd_vintage']) 

                else:
                    pass

                # successfully completed the task.  update the message and send to the queue
                my_message = my_message % ('INFO', my_IP_address, my_name, 'COMPLETED')
                output_queue.put((my_message, temp_time, time.localtime()))  

            except:
                # failed the task.  update the message and send to the queue
                my_message = my_message % ('ERROR', my_IP_address, my_name, 'FAILED')
                my_message = my_message + '\n' + traceback.format_exc()
                output_queue.put((my_message, temp_time, time.localtime())) 

        except:
            # queue is empty wait for a task to be assigned
            time.sleep(1)               

    return

def genFinalTiles(input_queue, output_queue, config, db_config):
    """
    creates the final map box tile for each speed 

    Arguments In:
        input_queue:    a multiprocessing queue that can be shared across
                        multiple servers and cores.  All information to be
                        processed is loaded into the queue
        output_queue:   a multiprocessing queue that can be shared across
                        multiple servers and cores.  All results from the 
                        various processes are loaded into the queue
        config:         a dictionary variabel that contains the configuration
                        parameters for the step
        db_config:      a dictionary that contains the configuration
                        information for the database and queue

    Arguments Out: 
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be exectuted    
    """        
    # capture the process name
    my_name = mp.current_process().name
    my_IP_address = socket.gethostbyname(socket.gethostname())

    while True:
        try:
            # get the next element out of the queue
            element = input_queue.get()
            if element[0] is None: break

            # extract the terms from the queue
            file_type   = element
            try:
                temp_time = time.localtime()
                if file_type == 'other':
                    my_message = '%s - STEP 6 (%s - %s): %s MAKING FINAL OTHER PROVIDER MAP BOX TILE'
                    os.system('tile-join -n other_prov -o '+\
                        config['output_mbtiles_path']+'%s_prov_other.mbtiles -f ' % config['fbd_vintage'] +\
                        config['temp_mbtiles_path']+'%s_prov_other_z0_8.mbtiles ' % config['fbd_vintage'] +\
                        config['temp_mbtiles_path']+'%s_prov_other_z9_12.mbtiles' % config['fbd_vintage']) 
                
                elif file_type == 'large':
                    my_message = '%s - STEP 6 (%s - %s): %s MAKING FINAL LARGE PROVIDER MAP BOX TILE'
                    os.system('tile-join -n large_prov -o '+\
                        config['output_mbtiles_path']+'%s_prov_lg.mbtiles -f ' % config['fbd_vintage'] +\
                        config['temp_mbtiles_path']+'%s_prov_lg_z0_7.mbtiles ' % config['fbd_vintage'] +\
                        config['temp_mbtiles_path']+'%s_prov_lg_z8_12.mbtiles' % config['fbd_vintage'] )
                
                elif file_type == 'xlarge':
                    my_message = '%s - STEP 6 (%s - %s): %s MAKING INTERIM XLARGE BLOCKS MAP BOX TILE'
                    os.system('tippecanoe -P -Z 5 -z 9 -d 14 -x min_zoom --detect-shared-borders -l '+\
                        'xlarge_blocks_%s -f -o ' % config['census_vintage'] +\
                        config['output_mbtiles_path']+'xlarge_blocks_%s.mbtiles '  % config['census_vintage'] +\
                        config['temp_speed_geojson_path']+'xlarge_blocks_%s.geojson ' % config['census_vintage'] +\
                        '2>&1 | tee ../logs/xlarge_blocks_%s.log' % config['census_vintage'])

                else:
                    pass
                
                # successfully completed the task.  update the message and send to the queue
                my_message = my_message % ('INFO', my_IP_address, my_name, 'COMPLETED')
                output_queue.put((my_message, temp_time, time.localtime()))  

            except:
                # failed the task.  update the message and send to the queue
                my_message = my_message % ('ERROR', my_IP_address, my_name, 'FAILED')
                my_message = my_message + '\n' + traceback.format_exc()
                output_queue.put((my_message, temp_time, time.localtime())) 

        except:
            # queue is empty wait for a task to be assigned
            time.sleep(1)     
    return
