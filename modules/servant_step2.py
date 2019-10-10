import multiprocessing as mp
import pandas as pd
import numpy as np
import traceback
import socket
import pickle
import time 
import gc


def makeBlockNumprovFile(output_queue, workerSpeed, d_speed, u_speed, 
                        column_names,fbd_df, blockm_df, tech_dict, my_name, 
                        my_ip_address, start_time):
    """
    This routine creates the dataframe that serves as the basis for the block
    numprov file.

    Arguments In:
        output_queue:   a multiprocessing queue variable that is used to 
                        communicate with the master routine
        workerSpeed:    the label for the speed with whic data is 
                        communcated on fixed broadband
        d_speed:        a list of all the download speeds in MBsec
        u_speed:        a list of all the upload speeds in MBsec
        column_names:   a list of column names to be used for the dataframes
                        that make the numprov files 
        fbd_df:         the path to a serialized pandas dataframe that holds 
                        all of the fixed broadband data
        blockm_df:      a pandas dataframe that holds all of the blockmaster
                        data
        tech_dict:      a dictionary that maps all of the techologies to a
                        single letter label
        my_name:        a string that contains the unique identier that for 
                        a given child process    
        start_time:     a time structure that holds when the step 
                        started      

    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be exectuted
        df_return:      a pandas dataframe that contains the numprov data in
                        an unformatted structure.

    """

    try:
        temp_time = time.localtime()
    
        # Create a dataframe with the full set of fips_blocks to be merged later
        df_return=fbd_df[['HocoNum']].groupby(fbd_df.index).size()\
                                    .to_frame(name='HocoNum')
        
        # this block of code identifies all of the technology codes 
        # (see the config file) that are associated with a specific
        # set of technologies that define each column
        for techCode in column_names:
            temp_time_1 = time.localtime()
            techCode_i = [item for sublist in [x for x in \
                                            (t for t in (tech_dict[myKey]\
                                                for myKey in list(techCode)))]\
                                                for item in sublist]
        
            # Reducing the problem to only those rows which meet the 
            # specified upload/download speeds and conforms to the 
            # specific technology codes
            fbd_df['criteria_bool']=(fbd_df.max_download_speed>=d_speed) & \
                                    (fbd_df.max_upload_speed>=u_speed) & \
                                    (fbd_df.TechCode.isin(techCode_i))
            df_i=fbd_df[['HocoNum']].loc[fbd_df.criteria_bool]
            df_i=df_i.groupby(df_i.index).nunique()
            df_i=df_i.rename(columns={'HocoNum':techCode})
        
            # Merging with the full set of results
            df_return=df_return.merge(df_i,how='left',left_index=True,
                                    right_index=True)
            del df_i
            gc.collect()
            my_message = """
                INFO - STEP 2 (%s - %s): TASK 4 OF 5 - ADDED %s TO THE TEMP 
                DATAFRAME FOR %s SPEED
                """ % (my_ip_address, my_name, techCode, workerSpeed)
            my_message = ' '.join(my_message.split())
            response = (0, my_message, temp_time_1, time.localtime())
            output_queue.put(response)

        # Cleaning up the dataframe so it only contains the information
        # we need for the block_numprov files
        df_return.drop('HocoNum',axis=1,inplace=True)
        df_return.reset_index(inplace=True,drop=False)
        df_return = pd.merge(blockm_df[['BlockCode','h2only_undev']],
                            df_return,on='BlockCode', how='right')
        
        my_message = """
            INFO - STEP 2 (%s - %s): TASK 4 OF 5 - PROCESSED INITIAL NUMPROV 
            FILE FOR %s
            """ % (my_ip_address, my_name, workerSpeed)
        my_message = ' '.join(my_message.split())
        response = (0, my_message, temp_time, time.localtime(),
                    time.mktime(time.localtime())-time.mktime(start_time))
        output_queue.put(response)
        return True, df_return    
    except:
        my_message = """
            ERROR - STEP 2 (%s - %s): TASK 4 OF 5 - FAILED TO PROCESS INITIAL 
            NUMPROV FILE FOR %s
            """ % (my_ip_address, my_name, workerSpeed)
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        response = (2, my_message, temp_time, time.localtime())
        output_queue.put(response)    
        return False, None


def formatBlockNumprovFile(output_queue, df_return, workerSpeed, column_names, 
                            my_name, my_ip_address, config, start_time):
    """
    This routine formats the block numprov dataframe sa that all dataframes 
    have the same structure and can be read by later routines and processes

    Arguments In:
        output_queue:   a multiprocessing queue variable that is used to 
                        communicate with the master routine
        df_return:      a pandas dataframe that contains the numprov data in
                        an unformatted structure.
        workerSpeed:    the label for the speed with whic data is 
                        communcated on fixed broadband
        column_names:   a list of column names to be used for the dataframes
                        that make the numprov files 
        my_name:        a string that contains the unique identier that for 
                        a given child process 
        my_ip_address:  a string variable that contains the IP address of
                        servant processes
        config:			the json variable that contains all configration
    					data required for the data processing                        
        start_time:     a time structure that holds when the step 
                        started   
    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be exectuted
        df_return:      a pandas dataframe that contains the numprov data in
                        an unformatted structure.
    """
    try:
        # define the full set of column names required for the numprov file
        temp_time = time.localtime()
        full_column_names=['BlockCode','h2only_undev']+column_names
        df_return=df_return[full_column_names]
        
        # add the upload and download speeds to column names
        rename_dict={}
        rename_dict['BlockCode']='geoid%s'% config['census_vintage'][2:]
        for column_name in column_names:
            rename_dict[column_name]=column_name + '_' + workerSpeed
        df_return=df_return.rename(columns=rename_dict)
        
        # sort the dataframe to make sure the data comes out as we want it
        df_return.sort_values('geoid%s'% config['census_vintage'][2:],
                                inplace=True)

        my_message = """
            INFO - STEP 2 (%s - %s): TASK 4 OF 5 - COMPLETED FORMATTING %s, 
            """ % (my_ip_address, my_name, workerSpeed)
        my_message = ' '.join(my_message.split())
        response = (0, my_message, temp_time, time.localtime())
        output_queue.put(response)
        return True, df_return

    except:
        my_message = """
            ERROR - STEP 2 (%s - %s): TASK 4 OF 5 - FAILED TO FORMAT NUMPROV 
            FILE FOR %s
            """ % (my_ip_address, my_name, workerSpeed)
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        response = (2, my_message, temp_time, time.localtime())
        output_queue.put(response)  
        return False, None

def writeNumProvFile(output_queue, df_return, numprov_file_path, 
                    numprov_zero_file_path, workerSpeed, my_name, 
                    my_ip_address, start_time):
    """
    Writes out the block numprov files

    Arguments In:
        output_queue:           a multiprocessing queue variable that is 
                                used to communicate with the master routine
        df_return:              a pandas dataframe that contains the numprov 
                                data in an unformatted structure.
        numprov_file_path:      the full path for the numprov file that 
                                contains nulls
        numprov_zero_file_path: the full path for the numprov file that
                                contains zeroes
        workerSpeed:            the label for the speed with whic data is 
                                communcated on fixed broadband
        my_name:                a string that contains the unique identier 
                                that for a given child process 
        my_ip_address:          a string variable that contains the IP 
                                address of servant processes                                   
        start_time:             a time structure that holds when the step 
                                started   

    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be exectuted
        df_return:      a pandas dataframe that contains the numprov data in
                        an unformatted structure.
    """
    # h2only_dev column cannot contain 0's, we must remove them
    try:
        temp_time = time.localtime()
        df_return['h2only_undev']=df_return.h2only_undev.replace(0,np.nan)  
        df_return.to_csv(numprov_file_path,index=False, float_format='%.0f')

        my_message = """
            INFO - STEP 2 (%s - %s): TASK 4 OF 5 - COMPLETED WRITING BLOCK 
            NUMPROV FILE WITH NULLS FOR %s, 
            """ % (my_ip_address, my_name, workerSpeed)
        my_message = ' '.join(my_message.split())
        response = (0, my_message, temp_time, time.localtime())
        output_queue.put(response)

        temp_time = time.localtime()
        df_return.fillna(0, inplace=True)
        for col in df_return.columns[1:]:
            df_return[col]=df_return[col].astype('int8')
        df_return.to_csv(numprov_zero_file_path, index=False) 
        df_return['h2only_undev']=df_return.h2only_undev.replace(0,np.nan)  
        
        my_message = """
            INFO - STEP 2 (%s - %s): TASK 4 OF 5 - COMPLETED WRITING BLOCK 
            NUMPROV FILE WITH ZEROS FOR %s, 
            """ % (my_ip_address, my_name, workerSpeed)
        my_message = ' '.join(my_message.split())
        response = (0, my_message, temp_time, time.localtime())
        output_queue.put(response)

        return True, df_return

    except:
        my_message = """
            ERROR - STEP 2 (%s - %s): TASK 4 OF 5 - FAILED TO WRITE FORMATTED 
            NUMPROV FILE FOR %s
            """ % (my_ip_address, my_name, workerSpeed)
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        response = (2, my_message, temp_time, time.localtime())
        output_queue.put(response)
        return False, None

def capProvidersAreaTable(config, output_queue, df_return, blockm_df, blockm_keep, 
                            workerSpeed, my_name, my_ip_address, start_time):
    """
    For the area tables, there is no value in distinguishing different 
    levels over 3.  Therefore all provider numbers in excess of 3 are reset 
    to "3".  This mod is for the area table only, the numprov tables keep 
    the original value

    Arguments In:
        output_queue:   a multiprocessing queue variable that is 
                        used to communicate with the master routine
        df_return:      a pandas dataframe that contains the numprov 
                        data in an unformatted structure.
        blockm_df:      a pandas data frome that contains the 
                        blockmaster data
        blockm_keep:    a list of column names in the block master table
                        that needs to be retained for the area tables
        workerSpeed:    the label for the speed with whic data is 
                        communcated on fixed broadband
        my_name:        a string that contains the unique identier 
                        that for a given child process   
        my_ip_address:  a string variable that contains the IP 
                        address of servant processes                            
        start_time:     a time structure that holds when the step 
                        started   

    Arguments Out: 
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be exectuted
        df_return:      a pandas dataframe that contains the numprov 
                        data in an unformatted structure.
        big_df:         a pandas dataframe that contains key data columns
                        from the block master data frame and the df-return
                        dataframe
    """
    # get list of all tech-speed columns and cap the number of providers at 3
    try:
        temp_time = time.localtime()
        df_return.fillna(0, inplace=True) # <-- areaTable needs to have the 0's
        for col in df_return.columns[1:]:
            df_return[col]=df_return[col].astype('int8')
        for tech in df_return.columns[2:]:
            df_return.loc[df_return[tech]>3,tech]=3
        big_df=pd.merge(blockm_df[blockm_keep],df_return, how='left',\
                        left_on='BlockCode', 
                        right_on='geoid%s'% config['census_vintage'][2:])
        
        my_message = """
            INFO - STEP 2 (%s - %s): TASK 4 OF 5 - COMPLETED PREPPING 
            BLOCK_NUMPROV %s WITH TOP-CODE OF 3, 
            """ % (my_ip_address, my_name, workerSpeed)
        my_message = ' '.join(my_message.split())
        response = (0, my_message, temp_time, time.localtime())
        output_queue.put(response)
        return True, df_return, big_df
    except:
        my_message = """
            ERROR - STEP 2 (%s - %s): TASK 4 OF 5 - FAILED TO PREP BLOCK 
            NUMPROV %s WITH TOP-CODE of 3,
            """ % (my_ip_address, my_name, workerSpeed)
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        response = (2, my_message, temp_time, time.localtime())
        output_queue.put(response)     
        return False, None, None

def createAreaTable(output_queue, df_return, big_df, area_table_cols, geog_dict, 
                    temp_area_table_file_path, workerSpeed, my_name, 
                    my_ip_address, start_time):
    """
    The are table contains information for all speeds.  Since the routine has 
    been modified to be parallelized, there is high probability the area table
    will be written to simultaneously by different processes.  By making each 
    process write to its own temp area table, we can preclude the risk of 
    corrupting the file.  The temp area tables will be merged later in the 
    master routine.

    Arguments In:
        output_queue:               a multiprocessing queue variable that is 
                                    used to communicate with the master 
                                    routine
        df_return:                  a pandas dataframe that contains the
                                    numprov data in an unformatted structure
        big_df:                     a pandas dataframe that contains key 
                                    data columns from the block master data 
                                    frame and the df-return dataframe
        area_table_cols:            a list of column names for the area 
                                    tables
        geog_dict:                  a dictionary from the config file that 
                                    lists the elements of inforamtion that 
                                    will be included in the area table
        temp_area_table_file_path:  the full path for the area tables
        workerSpeed:                the label for the speed with whic data
                                    is communcated on fixed broadband
        my_name:                    a string that contains the unique 
                                    identier that for a given child process 
        my_ip_address:              a string variable that contains the IP 
                                    address of servant processes 
        start_time:                 a time structure that holds when the 
                                    step started   

    Arguments Out:
        continue_run:               a boolean variable that indicates if the 
                                    routine successfully completed and 
                                    whether the next steps should be 
                                    exectuted
    """
    try:
        # Prep area table for appending
        out_df=pd.DataFrame(columns=area_table_cols)
        out_df.to_csv(temp_area_table_file_path, index=False)

        # iterate over the specific geometries required in the area table
        for geog in geog_dict:
            temp_time = time.localtime()        
            for tech in df_return.columns[2:]:
                temp_time_1 = time.localtime() 
                out_df=big_df.groupby([geog,'urban_rural','tribal_non',tech])['pop']\
                            .sum().unstack().reset_index()
                out_df[geog]=out_df[geog].astype(object)
    
                # need to make sure all number columns are in the df
                for x in [0,1,2,3]:
                    if x not in out_df: out_df[x]=0
                out_df[[0,1,2,3]]=out_df[[0,1,2,3]].fillna(0).astype(int)
                out_df.rename(columns={ 0:'has_0',1:'has_1', 2:'has_2', 
                                        3:'has_3more',geog:'id'},\
                            inplace=True)
                
                # add columns
                out_df['type']=geog_dict[geog]
                out_df['tech']=tech.split('_')[0]
                if tech.split('_')[1]=='200':
                    out_df['speed']=0.2
                else:
                    out_df['speed']=tech.split('_')[1]
                out_df=out_df[area_table_cols]
                
                # write the file
                out_df.to_csv(temp_area_table_file_path, index=False, mode='a', 
                            header=False)

                my_message = """
                    INFO - STEP 2 (%s - %s): TASK 4 OF 5 - APPENDED %s DATA FOR 
                    %s GEOMETRY FOR %s SPEED 
                    """ % (my_ip_address, my_name, tech, geog, workerSpeed)
                my_message = ' '.join(my_message.split())
                response = (0, my_message, temp_time_1, time.localtime())
                output_queue.put(response)

                
            my_message = """
                INFO - STEP 2 (%s - %s): TASK 4 OF 5 - COMPLETED APPENDING %s 
                DATA FOR %s 
                """ % (my_ip_address, my_name, geog, workerSpeed)
            my_message = ' '.join(my_message.split())
            response = (0, my_message, temp_time, time.localtime())
            output_queue.put(response)

        my_message = """
            INFO - STEP 2 (%s - %s): TASK 4 OF 5 - COMPLETED CREATING TEMP AREA 
            TABLE FOR %s 
            """ % (my_ip_address, my_name, workerSpeed)
        my_message = ' '.join(my_message.split())
        response = (0, my_message, temp_time, time.localtime())
        output_queue.put(response)       

        del out_df
        gc.collect()

        return True

    except:
        my_message = """
            ERROR - STEP 2 (%s - %s): TASK 4 OF 5 - FAILED TO CREATE INTERIM 
            AREA TABLE FILE FOR %s
            """ % (my_ip_address, my_name, workerSpeed)
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        response = (2, my_message, temp_time, time.localtime())
        output_queue.put(response)  
    
        return False

def makeBlockNumProv(input_queue, output_queue, config, db_config):
    """
    processes output from worker processes and outputs the message contents
    to the terminal window

    Arguments In:
        input_queue:        a multiprocessing queue that can be shared across
                            multiple servers and cores.  All information to be
                            processed is loaded into the queue
        output_queue:       a multiprocessing queue that can be shared across
                            multiple servers and cores.  All results from the 
                            various processes are loaded into the queue
        config:			    the json variable that contains all configration
    					    data required for the data processing
        db_config:          a dictionary that contains the configuration
                            information for the database and queue

    Arguments Out:
        None
    """
    # supress warnings about chained assignments
    pd.options.mode.chained_assignment = None

    # capture the process name
    my_name = mp.current_process().name
    my_ip_address = socket.gethostbyname(socket.gethostname())
    continue_run = True

    while True:
        try:
            # get the next element out of the queue
            inputs = input_queue.get()
            try:
                if inputs[0] is None: break
                temp_time = time.localtime()

                # extract the terms from the queue list
                numprov_file_path         = inputs[0] 
                numprov_zero_file_path    = inputs[1]
                temp_area_table_file_path = inputs[2]
                workerSpeed               = inputs[3]
                d_speed                   = inputs[4]
                u_speed                   = inputs[5]
                column_names              = inputs[6]
                fbd_path                  = inputs[7]
                blockm_df                 = inputs[8]
                start_time                = inputs[9]  

                # get the config items
                tech_dict                 = config['tech_dict'] 
                area_table_cols           = config['area_table_cols']
                geog_dict                 = config['geog_dict']
                blockm_keep               = config['blockm_keep']  

                # unpack the pickles
                with open(fbd_path, 'rb') as my_jar:
                    fbd_df = pickle.load(my_jar)

                continue_run, df_return = makeBlockNumprovFile(output_queue, 
                                        workerSpeed, d_speed, u_speed, 
                                        column_names, fbd_df, blockm_df, 
                                        tech_dict, my_name, my_ip_address, 
                                        start_time)

                if continue_run:
                    continue_run, df_return = formatBlockNumprovFile(output_queue, 
                                        df_return, workerSpeed, column_names, 
                                        my_name, my_ip_address, config, 
                                        start_time)

                if continue_run:
                    continue_run, df_return = writeNumProvFile(output_queue, 
                                        df_return, numprov_file_path, 
                                        numprov_zero_file_path, workerSpeed, 
                                        my_name, my_ip_address, start_time)

                if continue_run:
                    continue_run, df_return, big_df = capProvidersAreaTable(
                                        config, output_queue, df_return, blockm_df, 
                                        blockm_keep, workerSpeed, my_name, 
                                        my_ip_address, start_time)

                if continue_run:
                    continue_run = createAreaTable(output_queue, df_return, 
                                        big_df, area_table_cols, geog_dict, 
                                        temp_area_table_file_path, workerSpeed, 
                                        my_name, my_ip_address, start_time)

                if continue_run:
                    my_message = """
                        INFO - STEP 2 (%s - %s): TASK 4 OF 5 - COMPLETED WRITING
                        %s TEMP AREA TABLE
                        """ % (my_ip_address, my_name, workerSpeed)
                    my_message = ' '.join(my_message.split())
                    output_queue.put((1,my_message, temp_time, time.localtime()))
                else:
                    my_message = """
                        ERROR - STEP 2 (%s - %s): TASK 4 OF 5 - FAILED WRITING %s 
                        TEMP AREA TABLE
                        """ % (my_ip_address, my_name, workerSpeed)
                    my_message = ' '.join(my_message.split())
                    output_queue.put((2,my_message, temp_time, time.localtime()))
                    break 
            except:
                # unhandled error - output a message to the queue
                my_message ="""
                    ERROR - STEP 2 (%s - %s): TASK 4 OF 5 - UNHANDLED ERROR
                    """ % (my_ip_address, my_name) 
                my_message = ' '.join(my_message.split())      
                my_message += '\n' + traceback.format_exc()
                output_queue.put((2, my_message))
                break

        except:
            # nothing in the queue, wait and check again
            time.sleep(1)

    return