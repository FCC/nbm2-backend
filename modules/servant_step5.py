import multiprocessing as mp
import geopandas as gpd 
import pandas as pd 
import traceback 
import socket
import time 

#used in step 5 task 6
def genTractGeoJson(input_queue, output_queue,config, db_config):
    """
    creates the tract level geoJSON using the county level files 

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
        # get the next element out of the queue
        try:
            element = input_queue.get()
            if element[0] is None: break

            # extract the terms from the queue
            file_name = element[0]         
            config = element[1]
            start_time = element[2]
            county = file_name.split('.geojson')[0][-5:]
            try:
                # dissolve the blocks into the tract level 
                temp_time = time.localtime()
                temp_df = gpd.read_file(file_name)
                temp_df.sort_values('geoid%s' % config['census_vintage'][2:],
                                ascending=True,inplace=True)
                temp_df=temp_df[['tract_id','geometry']].loc[temp_df['ALAND%s'\
                    % config['census_vintage'][2:]] > 0].dissolve(by='tract_id')
                my_message = """
                    INFO - STEP 5 (%s - %s): COMPLETED DISSOLVING TRACTS IN %s 
                    COUNTY
                    """ % (my_IP_address, my_name, county)
                my_message = ' '.join(my_message.split()) 
                output_queue.put((1, my_message, temp_time, time.localtime(), temp_df))

            except:
                my_message = """
                    ERROR - STEP 5 (%s - %s):  FAILED TO DISSOLVE AT THE TRACT 
                    LEVEL FOR %s COUNTY
                    """ % ((my_IP_address, my_name, county))
                my_message = ' '.join(my_message.split())
                my_message += '\n' + traceback.format_exc()
                output_queue.put((0, my_message, temp_time, time.localtime()))

        except:
            #queue is empty - wait for one second and try again
            time.sleep(1)

    return True

# used in step 5 task 12
def genCountyHoCoNumGeoJson(input_queue, output_queue,config, db_config):
    """
    creates the county level geoJSON using the county level fbd files 

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
            block_name  = element[0]  
            fbd_name    = element[1]       
            config      = element[2]
            start_time  = element[3]
            county = block_name.split('.geojson')[0][-5:]  
            try:
                # get the fbd file for the given county
                temp_time = time.localtime()
                small_fbd_df=pd.read_csv(fbd_name, 
                        dtype={'BlockCode':str, 'HocoNum':str}, 
                        usecols=config['fbd_data_columns'])  
                small_prov_df=small_fbd_df[['HocoNum','BlockCode']]\
                                .loc[small_fbd_df.Consumer==1]
                small_prov_df.columns=['hoconum','block_fips']

                # get the block data for the given county
                small_block_df = gpd.read_file(block_name)

                # merge the fb data and block data together 
                temp_df=small_block_df[['geoid%s' % config['census_vintage'][2:],
                                        'geometry','county_id']]\
                        .merge(small_prov_df, 
                                left_on='geoid%s' % config['census_vintage'][2:], 
                                right_on='block_fips')

                # dissolve by HoCoNum
                temp_footprint_df=temp_df[['hoconum','county_id','geometry']]\
                                    .dissolve(by=['hoconum','county_id'])
                temp_footprint_df.reset_index(inplace=True)
                my_message = """
                    INFO - STEP 5 (%s - %s): COMPLETED DISSOLVING COUNTY AND 
                    HOCONUM FOR %s COUNTY
                    """ % (my_IP_address, my_name, county)
                my_message = ' '.join(my_message.split()) 
                output_queue.put((1, my_message, temp_time, time.localtime(), temp_footprint_df))
            except:
                my_message = """
                    ERROR - STEP 5 (%s - %s):  DISSOLVING COUNTY AND HOCONUM
                    FOR %s COUNTY
                    """ % ((my_IP_address, my_name, county))
                my_message = ' '.join(my_message.split()) 
                my_message += '\n' + traceback.format_exc()
                output_queue.put((0, my_message, temp_time, time.localtime()))
        except:
            # queue is empty - wait for 1 second and check again
            time.sleep(1) 

    return True