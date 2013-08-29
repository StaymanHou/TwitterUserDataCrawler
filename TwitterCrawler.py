import threading
from Lib.TwitterFetcher import TwitterFetcherThread
from Lib.TwitterForeman import TwitterForemanThread
from Lib.MyDict import *
from time import sleep, localtime, strftime
from Queue import Queue
from ConfigParser import ConfigParser

config_file_path = 'config.ini'
log_file_path = 'log.txt'

    #vvvvvvvvvv    StrNow() functions    vvvvvvvvvv#
def StrNow():
    return strftime("[%Y-%m-%d_%H-%M-%S] ", localtime())

    #vvvvvvvvvv    UpdateMsg() functions    vvvvvvvvvv#
def UpdateMsg(Msg_str):
    print StrNow() +' '+ Msg_str
    open(log_file_path, 'a').write(StrNow()+Msg_str+'\n')
    return


Config = ConfigParser()
Config.read(config_file_path)

Work_DB_Path = Config.get('Initial_Setting','Work_DB_Path')
Data_DB_Path = Config.get('Initial_Setting','Data_DB_Path')

Crawler_Work_Queue_Size = Config.getint('Initial_Setting','Crawler_Work_Queue_Size')
Crawler_Data_Queue_Size = Config.getint('Initial_Setting','Crawler_Data_Queue_Size')

Worker_Num = Config.getint('Initial_Setting','Worker_Num')

Crawler_Work_Queue = Queue(Crawler_Work_Queue_Size)
Crawler_Data_Queue = Queue(Crawler_Data_Queue_Size)

Crawler_Resume = Config.getboolean('Initial_Setting','Crawler_Resume')

Init_Work = None

if not Crawler_Resume:
    Init_Work = []
    Seed_File_Path = Config.get('Initial_Setting','Seed_File_Path')
    fp = open(Seed_File_Path, 'r')
    while True:
        line = fp.readline()
        if len(line)==0:
            break
        if len(line.strip())>0:
            Init_Work.append(line.strip())
    fp.close()
    
Crawler_Work_Queue_Lock = threading.Lock()
Crawler_Data_Queue_Lock = threading.Lock()

Crawler_Check_Iteration = Config.getfloat('Crawler_Control','Crawler_Check_Iteration')
Crawler_Pause = Config.getboolean('Crawler_Control','Crawler_Pause')
Crawler_Terminate = Config.getboolean('Crawler_Control','Crawler_Terminate')
Work_Finish_Threshold = Config.getint('Crawler_Control','Work_Finish_Threshold')

Foreman_Config = {'FOREMAN_PAUSE_DURATION': 10,
                  'FOREMAN_NOWORK_DURATION': 20,
                  'FOREMAN_FINISH_DURATION': 30}
Foreman_Config['FOREMAN_PAUSE_DURATION'] = Config.getfloat('Foreman_Setting','FOREMAN_PAUSE_DURATION')
Foreman_Config['FOREMAN_NOWORK_DURATION'] = Config.getfloat('Foreman_Setting','FOREMAN_NOWORK_DURATION')
Foreman_Config['FOREMAN_FINISH_DURATION'] = Config.getfloat('Foreman_Setting','FOREMAN_FINISH_DURATION')

Fetcher_Config = {'FETCHER_PAUSE_DURATION': 5,
                  'FETCHER_DATAFULL_DURATION': 10,
                  'FETCHER_FINISH_DURATION': 0}
Fetcher_Config['FETCHER_PAUSE_DURATION'] = Config.getfloat('Fetcher_Setting','FETCHER_PAUSE_DURATION')
Foreman_Config['FETCHER_DATAFULL_DURATION'] = Config.getfloat('Fetcher_Setting','FETCHER_DATAFULL_DURATION')
Foreman_Config['FETCHER_FINISH_DURATION'] = Config.getfloat('Fetcher_Setting','FETCHER_FINISH_DURATION')

threads = []
threadID = 0

UpdateMsg('Crawler Start!')

thread = TwitterForemanThread(threadID, Foreman_Config, Crawler_Work_Queue, Crawler_Work_Queue_Size, Crawler_Work_Queue_Lock, Crawler_Data_Queue, Crawler_Data_Queue_Size, Crawler_Data_Queue_Lock, Work_DB_Path, Data_DB_Path, Init_Work = Init_Work)
thread.start()
threads.append(thread)
threadID += 1

for i in range(Worker_Num):
    thread = TwitterFetcherThread(threadID, Fetcher_Config, Crawler_Work_Queue, Crawler_Work_Queue_Lock, Crawler_Data_Queue, Crawler_Data_Queue_Lock)
    thread.start()
    threads.append(thread)
    threadID += 1

status_record = [0 for i in range(Worker_Num+1)]

while True:
    ####### checking quit
    flag_quit = True
    if status_record[0] > Work_Finish_Threshold or threads[0].total_status > Work_Finish_Threshold or threads[0].FOREMAN_STATUS == -3: flag_quit = False
    for i in range(Worker_Num):
        tid = i+1
        if status_record[tid]!=-5 or threads[tid].FETCHER_STATUS!=-5: flag_quit = False
    if flag_quit == True:
        UpdateMsg('Ready to quit. Terminating threads.')
        break
    ####### refresh status record
    status_record[0] = threads[0].total_status
    for i in range(Worker_Num):
        tid = i+1
        status_record[tid] = threads[tid].FETCHER_STATUS
    ####### print status
    buff = '[Checking]: Foreman: '
    buff += FOREMAN_STATUS_DICT[threads[0].FOREMAN_STATUS]+' | Workers: '
    temp_worker_status_dict = {}
    for i in range(Worker_Num):
        tid = i+1
        tst = FETCHER_STATUS_DICT[status_record[tid]]
        if tst not in temp_worker_status_dict:
            temp_worker_status_dict[tst] = 1
        else:
            temp_worker_status_dict[tst] += 1
    buff += str(temp_worker_status_dict)
    UpdateMsg(buff)
    ####### update config
    Config.read(config_file_path)
    Crawler_Check_Iteration = Config.getfloat('Crawler_Control','Crawler_Check_Iteration')
    Crawler_Pause = Config.getboolean('Crawler_Control','Crawler_Pause')
    Crawler_Terminate = Config.getboolean('Crawler_Control','Crawler_Terminate')
    Work_Finish_Threshold = Config.getint('Crawler_Control','Work_Finish_Threshold')

    Foreman_Config['FOREMAN_PAUSE_DURATION'] = Config.getfloat('Foreman_Setting','FOREMAN_PAUSE_DURATION')
    Foreman_Config['FOREMAN_NOWORK_DURATION'] = Config.getfloat('Foreman_Setting','FOREMAN_NOWORK_DURATION')
    Foreman_Config['FOREMAN_FINISH_DURATION'] = Config.getfloat('Foreman_Setting','FOREMAN_FINISH_DURATION')

    Fetcher_Config['FETCHER_PAUSE_DURATION'] = Config.getfloat('Fetcher_Setting','FETCHER_PAUSE_DURATION')
    Foreman_Config['FETCHER_DATAFULL_DURATION'] = Config.getfloat('Fetcher_Setting','FETCHER_DATAFULL_DURATION')
    Foreman_Config['FETCHER_FINISH_DURATION'] = Config.getfloat('Fetcher_Setting','FETCHER_FINISH_DURATION')
    ####### set new config
    threads[0].Foreman_Config = Foreman_Config
    for i in range(Worker_Num):
        tid = i+1
        threads[tid].Fetcher_Config = Fetcher_Config
    if Crawler_Pause:
        threads[0].FOREMAN_FLAGS |= FOREMAN_FLAGS_DICT['Pause']
        for i in range(Worker_Num):
            tid = i+1
            threads[tid].FETCHER_FLAGS |= FETCHER_FLAGS_DICT['Pause']
    else:
        threads[0].FOREMAN_FLAGS &= ~FOREMAN_FLAGS_DICT['Pause']
        for i in range(Worker_Num):
            tid = i+1
            threads[tid].FETCHER_FLAGS &= ~FETCHER_FLAGS_DICT['Pause']
    if Crawler_Terminate:
        break
    ######## sleep
    sleep(Crawler_Check_Iteration)

threads[0].FOREMAN_FLAGS |= FOREMAN_FLAGS_DICT['Terminate']
for i in range(Worker_Num):
    tid = i+1
    threads[tid].FETCHER_FLAGS |= FETCHER_FLAGS_DICT['Terminate']

# Wait for all threads to complete
for t in threads:
    t.join()

UpdateMsg('Exiting Crawler')

