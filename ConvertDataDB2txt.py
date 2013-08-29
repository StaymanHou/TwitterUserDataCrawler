import bsddb
from ConfigParser import ConfigParser
from lockfile import FileLock
config_file_path = 'config.ini'
Config = ConfigParser()
Config.read(config_file_path)
Data_DB_Path = Config.get('Initial_Setting','Data_DB_Path')

buff_update_per = 1000
buff_update_counter = 0
str_buff = ''
datadblock = FileLock(Data_DB_Path)
with datadblock:
    db = bsddb.hashopen(Data_DB_Path, 'r')
    fp = open('data.txt', 'a')
    for key in db.keys():
        if buff_update_counter >= buff_update_per:
            fp.write(str_buff)
            str_buff = ''
            buff_update_counter = 0
        str_buff += key+'\t'+db[key]+'\n'
        buff_update_counter += 1
    fp.write(str_buff)
    fp.close()
    db.close()

####### wait input to exit #######
print 'Press "Enter" to exit...'
raw_input()

