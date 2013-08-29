import bsddb
import json
from lockfile import FileLock

def dataliststore(temp_data_list, TwitterWorkDB, Data_DB_Path):
    TwitterWorkDB
    datadblock = FileLock(Data_DB_Path)
    rowlist = []
    worklist = []
    finishlist = []
    for temp_data in temp_data_list:
        buff = ''
        buff += str(temp_data[1])+'\t'
        if temp_data[2]!=None: buff += str(temp_data[2])
        buff += '\t'
        if temp_data[3]!=None: buff += str(temp_data[3])
        buff += '\t'
        if temp_data[4]!=None: buff += str(temp_data[4])
        buff += '\t'
        if temp_data[5]!=None: buff += temp_data[5].encode('ascii','replace')
        buff += '\t'
        buff += json.dumps(temp_data[6])+'\t'
        buff += json.dumps(temp_data[7])
        rowlist.append([temp_data[0],buff])
        worklist.extend(temp_data[6].keys())
        finishlist.append(temp_data[0])
    if len(rowlist)!=0:
        with datadblock:
            DataDB = bsddb.hashopen(Data_DB_Path, 'c')
            for row in rowlist:
                DataDB[row[0]] = row[1]
            DataDB.close()
    for workitem in set(worklist):
        TwitterWorkDB.put(workitem) #debug pass
    for finishitem in finishlist:
        TwitterWorkDB.finish(finishitem)
    return

class TwitterWorkDB(object):
    def __init__(self):
        self.DB = None
        self.finishpointer = '-3'
        self.pendingpointer = '-2'
        self.maxpointer = '-1'
        self.pendingmap = {}

    def Staticopen(dbpath, resume=True):
        WorkDB = TwitterWorkDB()
        WorkDB.DB = bsddb.hashopen(dbpath, 'c')
        if WorkDB.finishpointer not in WorkDB.DB:
            WorkDB.DB[WorkDB.finishpointer] = '0'
        if WorkDB.pendingpointer not in WorkDB.DB:
            WorkDB.DB[WorkDB.pendingpointer] = '0'
        if WorkDB.maxpointer not in WorkDB.DB:
            WorkDB.DB[WorkDB.maxpointer] = '0'
        if resume:
            WorkDB.DB[WorkDB.pendingpointer] = WorkDB.DB[WorkDB.finishpointer]
        else:
            for pointer in range(int(WorkDB.DB[WorkDB.finishpointer])+1,int(WorkDB.DB[WorkDB.pendingpointer])+1):
                if WorkDB.DB[str(pointer)][-1]=='F':
                    WorkDB.pendingmap[WorkDB.DB[str(pointer)][:-2]] = str(pointer)
        return WorkDB

    open = staticmethod(Staticopen)

    def close(self):
        self.DB.close()
        self.pendingmap = {}

    def empty(self):
        if long(self.DB[self.pendingpointer])>=long(self.DB[self.maxpointer]):
            return True
        return False

    def get(self):
        if self.empty():
            raise
        t_pp = long(self.DB[self.pendingpointer])
        while 1:
            t_pp += 1
            try: temp_data = self.DB[str(t_pp)][:-2]   #May 24 add try: to avoid key error caused by force close
            except: continue
            break
        self.pendingmap[temp_data.encode('ascii','replace')] = str(t_pp)
        self.DB[self.pendingpointer] = str(t_pp)
        return temp_data

    def put(self, data):
        #data = str(data)   --May 6 2013  will cause error if data contains unconvertable unicode
        data = data.encode('ascii','replace').lower()  #May 24 add .lower(): to avoid duplicated account crawling cause by different case
        if data not in self.DB:
            t_mp = long(self.DB[self.maxpointer])+1
            self.DB[self.maxpointer] = str(t_mp)
            self.DB[str(t_mp)] = data+'\tF'

    def finish(self, data):
        #data = str(data)   --May 6 2013  will cause error if data contains unconvertable unicode
        data = data.encode('ascii','replace')
        #print 'data: ', data, '\nmap: ', self.pendingmap#debug
        if data not in self.pendingmap:
            pass #     --May 29 2013:  used to be "raise" but it somehow stop the program running properly by raise a unexpected missing key
        self.DB[self.pendingmap[data]] = self.DB[self.pendingmap[data]][:-1]+'T'
        del self.pendingmap[data]
        t_fp = long(self.DB[self.finishpointer])
        t_mp = long(self.DB[self.maxpointer])
        while 1:
            t_fp += 1
            if t_fp>t_mp:
                break
            try: self.DB[str(t_fp)]   #May 24 add try: to avoid key error caused by force close
            except: continue
            if self.DB[str(t_fp)][-1]=='T':
                continue
            else:
                break
        t_fp -= 1
        self.DB[self.finishpointer] = str(t_fp)

