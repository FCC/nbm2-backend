import NBM2_functions as nbmf 
import pandas as pd
import traceback
import time
import gc


def mergeSpeedAreaTables(append_list, config, start_time):
    """
    This routine merges the temp area tables created by each process spawned 
    off by the master and merges them into a single area table (the final 
    one).  It also deletes the temporary area tables

    Arguments In:
        append_list:    a list that contains the full path for each of the 
                        temp area tables
        config:         a dictionary that contains all of the configuration
                        parameters for the entire process
        start_time:     a time structure that holds when the step started

    Arguments Out:
        continue_run:   a boolean that indicates whether the process was 
                        successfully completed
    """
    # supress warnings about chained assignments
    pd.options.mode.chained_assignment = None

    try:
        temp_time = time.localtime()
        
        # create the header file for the final area table
        outdf=pd.DataFrame(columns=config['area_table_cols'])
        outdf.to_csv(config['output_dir_path'] + 'area_table_%s.csv'\
                    % config['fbd_vintage'], index=False, encoding="utf-8")
        del outdf
        gc.collect()

        # iterate over the temporary area tables
        for i in range(len(append_list)):
            temp_df = pd.read_csv(append_list[i],dtype={'id':object})
            temp_df.to_csv(config['output_dir_path'] + 'area_table_%s.csv'\
                    % config['fbd_vintage'], index=False, mode='a', 
                    header=False, encoding="utf-8")    
            nbmf.silentDelete(append_list[i])
            file_name = append_list[i].split('/')[-1:]
            my_message = """
                INFO - STEP 2 (MASTER): COMPLETED MERGING %s TEMP AREA TABLE 
                TO FINAL area_table_%s.csv
                """ % (file_name,config['fbd_vintage'])
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                    time.mktime(time.localtime())-time.mktime(start_time)))
        del temp_df
        gc.collect()

        my_message = """
            INFO - STEP 2 (MASTER): TASK 5 of 5 - COMPLETED MAKING AREA TABLE 
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))

        return True
    except:
        my_message = """
            ERROR - STEP 2 (MASTER): TASK 5 of 5 - FAILED MAKING AREA TABLE ON
            %s TEMP TABLE
            """ % append_list[i]
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))

        return False