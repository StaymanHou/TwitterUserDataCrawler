FETCHER_FLAGS_DICT = {'Normal':1,
                      'Terminate':2,
                      'Pause':4}
FETCHER_STATUS_DICT = {0:'Start',
                       -1:'Abnormal',
                       -2:'Terminate',
                       -3:'Pause',
                       1:'Working',
                       -4:'Queue_Cache_Lock',
                       -5:'Queue_Cache_Empty',
                       -6:'Data_Cache_Lock',
                       -7:'Data_Cache_Full',
                       2:'Caching',
                       -8:'Resting_After_Work'}
ACCINFO_STATUS_DICT = {1:'Normal',
                       -1:'Cant connect',
                       -2:'Suspended user',
                       -3:'302 Unknown',
                       -4:'User not exist',
                       -5:'Unknown response'}
FOREMAN_FLAGS_DICT = {'Normal':1,
                      'Terminate':2,
                      'Pause':4}
FOREMAN_STATUS_DICT = {0:'Start',
                       -1:'Abnormal',
                       -2:'Terminate',
                       -3:'Pause',
                       1:'Working',
                       -4:'Resting_No_Work',
                       -5:'Resting_After_Work'}

