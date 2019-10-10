import NBM2_functions as nbmf
import traceback 
import pickle 
import time 




def pickleFBD(fbd_df, config, start_time):
    """
    creates a pickle of the fbd dataframe.  Allows the distributed workers
    to more quickly process the data
    
    Arguments In: 
        fbd_df:             the path to a serialized pandas dataframe that holds 
                            all of the fixed broadband data
        config:			    the json variable that contains all configration
    					    data required for the data processing
    	start_time:	        the clock time that the step began using the 
    					    time.clock() format

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next steps
                            should be executed
    """

    try:
        temp_time = time.localtime()
        with open(config['temp_pickles']+'enhanced_fbd_df.pkl', 'wb') as my_jar:
            pickle.dump(fbd_df,my_jar,protocol=pickle.HIGHEST_PROTOCOL)

        my_message = """
            INFO - STEP 2 (MASTER):  TASK 3 of 5 - COMPLETED PICKLING ENHANCED 
            FIXED BROADBAND DATA
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
        return True 

    except:
        my_message = """
            ERROR - STEP 2 (MASTER):  TASK 3 of 5 - FAILED PICKLING ENHANCED 
            FIXED BROADBAND DATA
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
        return False 