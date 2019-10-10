import multiprocessing as mp
import pandas as pd 
import traceback 
import pandas 
import socket
import time 

def outputGeoData(out_df, out_df_path, my_name, my_ip_address, geo, worker_speed, 
                    start_time, output_queue):
    """
    outputs the geometry dataframe into a csv format

    Arguments In:
        out_df:         a pandas dataframe that contains the data to be
                        output to the numprov file
        out_df_path:    a string variable that contains the full path to the
                        output file
        my_name:        a string variable that contains the unique 
                        identifier for the current process  
        my_ip_address:  a string variable that contains the IP address of
                        servant processes
        geo:            a string variable that indicates whether the 
                        geometry being processed is "tract" or "county"
        worker_speed:   a string variable that indicates which speed pair
                        is being worked by the process
        start_time:     a time.structure variable that indicates when the 
                        entire step started
        output_queue:   a multiprocessing queue variable that allows the 
                        servant processes to communicate with the master

    Arguments Out:
        continue_run:   a boolean variable that indicates whether the run
                        was a success
    """
    try:
        # output the files
        temp_time = time.localtime()
        out_df.to_csv(out_df_path)
        my_message = """
            INFO - STEP 3 (%s - %s): COMPLETED PRINTING %s GEOMETRIES 
            FOR %s
            """ % (my_ip_address, my_name, geo.upper(), worker_speed)
        output_queue.put((1,' '.join(my_message.split()), temp_time, 
            time.localtime(), time.mktime(time.localtime())-\
            time.mktime(start_time)))
        return True
    except:
        my_message = """
            ERROR - STEP 3 (%s - %s): FAILED PRINTING %s DATAFRAMES 
            FOR %s
            """ % (my_ip_address, my_name, geo.upper(), worker_speed)
        output_queue.put((2,' '.join(my_message.split())+\
            traceback.format_exc(), temp_time, time.localtime(), 
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False

def preProcessDF(geometry, outDF, output_queue):
    """
    Prepares the data frame that will be output to make the tract and county
    numprov files

    Arguments In:
        geometry:       a string variable that indicates whether the 
                        geometry being processed is "tract" or "county"
        outDF:          a pandas dataframe that contains the data to be
                        output to the numprov file
        output_queue:   a multiprocessing queue variable that allows the 
                        servant processes to communicate with the master

    Arguments Out:
        continue_run:   a boolean variable that indicates whether the run
                        was a success
        outDF:          a pandas dataframe that contains the data to be
                        output to the numprov file
    """
    try:
        outDF['is_populated']=True
        outDF.loc[outDF['%s_pop' % geometry] == 0,'is_populated']=False
        outDF.drop('%s_pop' % geometry, axis=1, inplace=True)
        outDF.fillna(0, inplace=True)
        outDF.sort_index(inplace=True)
        return True, outDF
    except:
        output_queue.put(traceback.format_exc())
        return False, None

def processColumns(my_name, my_ip_address, geometry, numprovDF, outDF, 
                output_queue, start_time, config):
    """
    adds technology columns to each of the numprov files for tract 
    and county

    Arguments In:
        my_name:        a string variable that contains the unique name for
                        the process
        my_ip_address:  a string variable that contains the IP address of
                        servant processes
        geometry:       a string variable that indicates whether the tracts
                        or counties are being processed
        numprovDF:      a pandas dataframe that contains the block numprov 
                        data
        outDF:          a pandas dataframe that contains the initial output
        output_queue:   a multiprocessing queue variable that allows the 
                        servant processes to communicate with the master
        start_time:     the time that the step started running
        config:         a dictionary that contains the configuration
                        information for the run

    Arguments Out:
        continue_run:   a boolean variable that indicates whether the run
                        was a success
        outDF:          a pandas dataframe that contains the initial output
    """
    try:
        geom = 'geoid%s' % config['census_vintage'][2:]
        numprovDF.set_index('%s_id' % geometry, inplace = True)
        numprovDF['%s_pop' % geometry] = numprovDF.groupby(['%s_id' % geometry])\
                                        ['pop'].sum() 
        for c in numprovDF.columns:
            if c not in [geom, 'pop', 'tract_pop', 'county_pop', 'tract_id', 
                        'county_id', 'weighted_pops', 'h2only_undev']:
                temp_time = time.localtime()
                numprovDF['weighted_pops'] = numprovDF['pop']*numprovDF[c]
                outDF.loc[eval('outDF.%s_pop' % geometry)>0, c]=(numprovDF\
                        .groupby(numprovDF.index)['weighted_pops']\
                        .sum()/(1.*eval('outDF.%s_pop' % geometry))).round(1)   
                my_message = """
                    INFO - STEP 3 (%s - %s): TECHNOLOGY %s ADDED TO %s LEVEL DATAFRAME
                    """ % (my_ip_address, my_name, c, geometry.upper())
                output_queue.put((0,' '.join(my_message.split()), temp_time, 
                    time.localtime(), time.mktime(time.localtime())-\
                    time.mktime(start_time)))
        return True, outDF
    except: 
        my_message = """
            INFO - STEP 3 (%s - %s): TECHNOLOGY %s FOR THE %s LEVEL DATAFRAME
            FAILED - PROCESSING ABORTED
            """ % (my_ip_address, my_name, c, geometry.upper())

        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        output_queue.put((2,my_message, temp_time, time.localtime(), 
            time.mktime(time.localtime())-time.mktime(start_time)))

        return False, None


def openNumprovFile(numprov_path, geom, my_name, my_ip_address, worker_speed, 
                    start_time, output_queue):
    """
    Opens the block numprov file and casts all numeric values as uint8

    Arguments In:
        numprov_path:   a string variable containing the full path to the 
                        required block_numprov_with_zero file  
        geom:           a string variable that indicates whether the 
                        geometry being processed is "tract" or "county"         
        my_name:        a string variable that contains the unique 
                        identifier for the current process
        my_ip_address:  a string variable that contains the IP address of
                        servant processes
        worker_speed:   a string variable that indicates which speed pair
                        is being worked by the process
        start_time:     a time.structure variable that indicates when the 
                        entire step started
        output_queue:   a multiprocessing queue variable that allows the 
                        servant processes to communicate with the master

    Arguments Out:
        continue_run:   a boolean variable that indicates whether the run
                        was a success
        block_numprov:  a pandas dataframe with the slightly modified in 
                        format block_numprov data
    """
    try:
        temp_time = time.localtime()
        # get the column names for each file
        get_cols = pd.read_csv(numprov_path, nrows = 0)
        keys = get_cols.columns.tolist()
        dictType = {key:'object' if key == geom else 'uint8' 
                    for key in keys}
        del get_cols

        # open the file and fix the column types
        block_numprov=pd.read_csv(numprov_path,dtype=dictType)
        my_message = """
            INFO - STEP 3 (%s - %s): OPENED BLOCK NUMPROV FILE FOR %s
            """ % (my_ip_address, my_name, worker_speed)
        output_queue.put((0,' '.join(my_message.split()), temp_time, 
            time.localtime(), time.mktime(time.localtime())-\
            time.mktime(start_time)))
        return True, block_numprov
    except:
        my_message = """
            ERROR - STEP 3 (%s - %s): FAILED OPENING BLOCK NUMPROV FILE
            FOR %s
            """ % (my_ip_address, my_name, worker_speed)
        output_queue.put((2,' '.join(my_message.split())+'\n'+\
            traceback.format_exc(), temp_time, time.localtime(), 
            time.mktime(time.localtime())-time.mktime(start_time))) 
        return False, None

def mergeWithDataFrame(block_numprov, blockm_df, geom, my_name, 
                    my_ip_address, worker_speed, start_time, output_queue):
    """
    Merges the block_numprov data and the blockmaster data and then adds
    two additional columns of data that contain the tract and county IDs

    Arguments In:
        block_numprov:  a pandas dataframe that contains the information 
                        for the block numprov file
        blockm_df:      a pandas dataframe that contains the information 
                        for the blockmaster data
        geom:           a string variable that contains the name of the 
                        unique identifier for the blockmaster and numprov
                        files    
        my_name:        a string variable that contains the unique 
                        identifier for the current process
        my_ip_address:      a string variable that contains the IP address of
                            servant processes
        worker_speed:   a string variable that indicates which speed pair
                        is being worked by the process
        start_time:     a time.structure variable that indicates when the 
                        entire step started
        output_queue:   a multiprocessing queue variable that allows the 
                        servant processes to communicate with the master 

    Arguments Out:
        continue_run:   a boolean variable that indicates whether the run
                        was a success
        block_numprov:  a pandas dataframe with the slightly modified in 
                        format block_numprov data

    """
    try:
        # merge the block numprov file and the blockmaster file
        temp_time = time.localtime()
        block_numprov=block_numprov.merge(blockm_df, how='left', left_on=geom, 
                                        right_on=geom)
        # add columns and data for the geometry IDs
        block_numprov['tract_id'] = block_numprov[geom].str[0:11]
        block_numprov['county_id'] = block_numprov[geom].str[0:5]

        my_message = """
            INFO - STEP 3 (%s - %s): MERGED BLOCKMASTER WITH NUMPROV FILE 
            FOR %s
            """ % (my_ip_address, my_name, worker_speed)
        output_queue.put((0,' '.join(my_message.split()), temp_time, 
            time.localtime(), time.mktime(time.localtime())-\
            time.mktime(start_time)))
        return True, block_numprov

    except:
        my_message = """
            ERROR - STEP 3 (%s - %s): FAILED MERGING BLOCKMASTER WITH NUMPROV
            FILE FOR %s
            """ % (my_ip_address, my_name, worker_speed)
        output_queue.put((2,' '.join(my_message.split())+'\n'+
            traceback.format_exc(), temp_time, time.localtime(), 
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None

def findPerCapitaProviders(my_name, my_ip_address, geo, block_numprov, output_queue, 
                            start_time, config, worker_speed, 
                            out_geo_df):
    """
    calculates the number of provers per capita for each of the technologies
    and speeds.  Also adds a "is populated" column

    Arguments In:
        my_name:        a string variable that contains the unique 
                        identifier for the current process
        my_ip_address:  a string variable that contains the IP address of
                        servant processes
        geo:            a string variable that indicates whether the 
                        geometry being processed is "tract" or "county"
        block_numprov:  a pandas dataframe with the slightly modified in 
                        format block_numprov data
        output_queue:   a multiprocessing queue variable that allows the 
                        servant processes to communicate with the master
        start_time:     a time.structure variable that indicates when the 
                        entire step started
        config:         a dictionary variabel that contains the configuration
                        parameters for the step
        worker_speed:   a string variable that indicates which speed pair
                        is being worked by the process
        out_geo_df:     a pandas dataframe that contains the data to be
                        output to the numprov file 

    Arguments Out:
        continue_run:   a boolean variable that indicates whether the run
                        was a success
        out_df:         a pandas dataframe that contains the data to be
                        output to the numprov file
    """
    try:
        # Add the columns
        temp_time = time.localtime()
        continue_run, out_df = processColumns(my_name, my_ip_address, geo, 
                                block_numprov, out_geo_df, output_queue, 
                                start_time, config)

        # add the "is populated" column
        if continue_run:    
            continue_run, out_df = preProcessDF(geo, out_df, output_queue)

        my_message = """
            INFO - STEP 3 (%s - %s): COMPLETED %s LEVEL PROCESSING FOR %s
            """ % (my_ip_address, my_name, geo.upper(), worker_speed)
        output_queue.put((0,' '.join(my_message.split()), temp_time, 
            time.localtime(), time.mktime(time.localtime())-\
            time.mktime(start_time)))
        return True, out_df
    except:
        my_message = """
            ERROR - STEP 3 (%s - %s): FAILED %s LEVEL PROCESSING FOR %s
            """ % (my_ip_address, my_name, geo.upper(), worker_speed)
        output_queue.put((2,' '.join(my_message.split())+\
            traceback.format_exc(), temp_time, time.localtime(), 
            time.mktime(time.localtime())-time.mktime(start_time)))   
        return False, None                 

def makeLargeTracts(input_queue, output_queue, config, db_config):
    """
    creates the large tracts geojson file

    Arguments In:
        input_queue:        a multiprocessing queue that can be shared across
                            multiple servers and cores.  All information to be
                            processed is loaded into the queue
        output_queue:       a multiprocessing queue that can be shared across
                            multiple servers and cores.  All results from the 
                            various processes are loaded into the queue
        config:         a dictionary variabel that contains the configuration
                        parameters for the step
        db_config:          a dictionary that contains the configuration
                            information for the database and queue

    Arguments Out: 
        N/A    
    """

    
    # capture the process name
    my_name = mp.current_process().name
    my_ip_address = socket.gethostbyname(socket.gethostname())

    while True:
        try:
            # get the next element out of the queue
            inputs = input_queue.get()
            try:
                if inputs[0] is None: break

                # extract the terms from the queue list
                numprov_path        = inputs[0] 
                blockm_df           = inputs[1]               
                out_tract_path      = inputs[2]                   
                out_county_path     = inputs[3]                  
                out_tract_df        = inputs[4]
                out_county_df       = inputs[5]  
                start_time          = inputs[6] 
                worker_speed        = inputs[7]
                config              = inputs[8]
                geom                = 'geoid%s' % config['census_vintage'][2:]

                continue_run, block_numprov = openNumprovFile(numprov_path, geom, 
                                            my_name, my_ip_address, worker_speed, 
                                            start_time, output_queue)

                if continue_run:
                    continue_run, block_numprov = mergeWithDataFrame(
                                        block_numprov, blockm_df, geom, my_name, 
                                        my_ip_address, worker_speed, start_time, 
                                        output_queue)                

                if continue_run:
                    for geo in ['tract', 'county']:
                        continue_run, out_df = findPerCapitaProviders(my_name, 
                                            my_ip_address, geo, block_numprov, 
                                            output_queue, start_time, config, 
                                            worker_speed, eval('out_%s_df' % geo))
                        
                        if continue_run:
                            continue_run = outputGeoData(out_df, 
                                            eval('out_%s_path' % geo), my_name, 
                                            my_ip_address, geo, worker_speed, 
                                            start_time, output_queue)

            except:
                pass

        except:
            # nothing in the queue, wait and check again
            time.sleep(1)

    return True