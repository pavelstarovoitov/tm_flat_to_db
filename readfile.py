#/usr/bin/python3
import mmap
import glob
import time
import psycopg2
from pgcopy import CopyManager
import io
from typing import Iterator, Any, Dict, Optional
import xml.dom.minidom
from io import BytesIO
from itertools import zip_longest
import datetime
import math
import struct

start = time.perf_counter()

print(f'{0.000:0.4}','transformation start')

def tm_parameters(filename):
    doc = xml.dom.minidom.parse(filename)
    param = doc.getElementsByTagName("Param")
    code = []
    indexlist = []  
    for i in param:
        code.append(i.getAttribute("Code").replace(".",'_').lower()) 
        indexlist.append(i.getAttribute("Index"))
    tm_name ={'code':code, 'index':indexlist}
    return tm_name


def create_tm_table(cursor, tm_code, table_name):
    mod=[]
    for i in tm_code:
        temp = i+" "+"REAL" 
        mod.append(temp)
    drop_table = "DROP TABLE IF EXISTS " +  table_name  + ";"
    create_table="CREATE TABLE IF NOT EXISTS " +  table_name  + " (time   TIMESTAMP," + ",".join(mod[0:]) + ");"
    hypertable = "SELECT  create_hypertable( "+ "'"+ table_name + "'" +" , 'time');"
    cursor.execute(drop_table)
    cursor.execute(create_table)
    cursor.execute(hypertable)

def convert_data(day):
    if type(day)==type('str'):
        day = int(day)
    delta = datetime.timedelta(day)
    epoch_start = '1950-01-01 00:00:00:000000'
    epoch_start =  datetime.datetime.strptime(epoch_start, '%Y-%m-%d  %H:%M:%S:%f')
    date = epoch_start + delta
    return date

def convert_to_time(t, date):
    micr =t
    if not date:
        date = datetime.datetime.now()
    delta = datetime.timedelta(microseconds=t*1000)
    t = date + delta
    return t



def transformation(path, beginfiles, endfiles):
    data=[]
    p =path.split('*')[0]+"2168_"+"*"
    for i in glob.glob(p)[0:1]:
        print(i)
        with open(i, 'r+b') as f:
            mm = mmap.mmap(f.fileno(),0)
            timelist=[]
            day = i.split('/')[1]
            date = convert_data(day)
            print('date',date)
            for i in range(19,mm.size(),32):
                a = bytearray(mm[i-8:i-16:-1])
                time1 = struct.unpack('!d', a)[0]*1000
                timelist.append((convert_to_time(time1, date)))
            mm.close()
            data.append(timelist)


    for i in glob.glob(path)[beginfiles:endfiles]:
        with open(i, 'r+b') as f:
            mm = mmap.mmap(f.fileno(),0)
            info= [None for  i in range(0,len(data[0]))]
            #print(i)
            timelist=[]
            info1=[]
            day = i.split('/')[1]
            date = convert_data(day)
            types= bytearray(mm[15:13:-1])
            types = struct.unpack('!h', types)[0]
            if types == 0:
                for i in range(23,mm.size(),32):
                    d2= bytearray(mm[i:i-8:-1])
                    a = bytearray(mm[i-12:i-20:-1])
                    time1 = struct.unpack('!d', a)[0]*1000
                    timelist.append(convert_to_time(time1, date))
                    d2 = struct.unpack('!d', d2)[0]
                    info1.append(d2)
            else:
                for i in range(23,mm.size(),32):
                    d2= bytearray(mm[i-4:i-8 :-1])
                    a = bytearray(mm[i-12:i-20:-1])
                    time1 = struct.unpack('!d', a)[0]*1000
                    timelist.append(convert_to_time(time1, date))
                    d2 = struct.unpack('!i', d2)[0]
                    info1.append(d2)

            
            begin =data[0].index(timelist[0])
            step =  (data[0].index(timelist[1])-begin)  if (data[0].index(timelist[1])-begin) else 1
            end = len(data[0])
            count=0
            for i in range(begin,end,step):
                if count>=len(info1):
                    # print(count)
                    break
                info[i]=info1[count]
                count+=1
            mm.close()
            data.append(info)

    return zip_longest(*data, fillvalue=None)


##########MAIN MAIN MAIN#######################################################################

if __name__== "__main__":

    tm_name = tm_parameters('TmParameters.xml')

    connection = psycopg2.connect(
        host="localhost",
        database="testload2",
        user="postgres",
        password='qw005870',
    )
    connection.autocommit = True

    paths=['./25720/*']#,'./25748/*']
    for path in paths:
        filelist =[x.split('/')[2].split('_')[0] for x in glob.glob(path)]
        tm_code = [tm_name['code'][tm_name['index'].index(x)] for x in filelist]       #Get all full list of TM CODES. Order the same as in the folder with binary files.
    
        begindata = 0 
        endat = 1550
        tm_list = tm_code[begindata:endat]
        cols = ('time', *tm_list)
        table_name = 'tm_value'

        with connection.cursor() as cursor:
            create_tm_table(cursor,tm_list,table_name)
        
        data = transformation(path,begindata,endat)

        elapsed = time.perf_counter() - start
        print(f' {elapsed:0.4}','transformation end,','copy to db start')

        with connection.cursor() as cursor:
            mgr = CopyManager(connection, table_name, cols)
            mgr.copy(data)

        elapsed = time.perf_counter() - start
        print(f'{elapsed:0.4}','copy to db end')

#######################################################################################################################
        begindata =1550#+1050
        endat = len(glob.glob(path))
        tm_list = tm_code[begindata:endat]
        cols = ('time', *tm_list)
        table_name = 'tm_value2'

        data = transformation(path,begindata, endat)

        with connection.cursor() as cursor:
            create_tm_table(cursor,tm_list,table_name)

        
        elapsed = time.perf_counter() - start
        print(f' {elapsed:0.4}','transformation end,','copy to db start')

        with connection.cursor() as cursor:
            mgr = CopyManager(connection, table_name,  cols)
            mgr.copy(data)


        elapsed = time.perf_counter() - start
        print(f'{elapsed:0.4}','copy to db end')