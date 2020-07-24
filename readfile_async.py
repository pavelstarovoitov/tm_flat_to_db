import glob
import xml.dom.minidom
import asyncio
import asyncio
import asyncpg
import mmap
import struct
import time
from itertools import zip_longest
import datetime

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

def convert_data(day):
    if type(day)==type('str'):
        day = int(day)
    delta = datetime.timedelta(day)
    epoch_start = '1950-01-01 00:00:00:000000'
    epoch_start =  datetime.datetime.strptime(epoch_start, '%Y-%m-%d  %H:%M:%S:%f')
    epoch_start = epoch_start.replace(tzinfo=datetime.timezone.utc)
    date = epoch_start + delta
    return date

def convert_to_time(t, date):
    micr = t
    if not date:
        date = datetime.datetime.now()
    delta = datetime.timedelta(microseconds=t*1000)
    t = date + delta
    return t


async def create_tm_table(conn, tm_code, table_name):
    mod=[]
    for i in tm_code:
        temp = i+" "+"REAL" 
        mod.append(temp)
    drop_table = "DROP TABLE IF EXISTS " +  table_name  + ";"
    create_table="CREATE TABLE IF NOT EXISTS " +  table_name  + " (time   TIMESTAMP WITH TIME ZONE," + ",".join(mod[0:]) + ");"
    hypertable = "SELECT  create_hypertable( "+ "'"+ table_name + "'" +" , 'time');"
    check_is_hypertable = "SELECT table_name FROM timescaledb_information.hypertable;"
    await conn.execute(drop_table)
    await conn.execute(create_table)
    await conn.execute(check_is_hypertable)
    hypertable_table =  await conn.fetch(check_is_hypertable)
    hlist = list(zip(*hypertable_table))
    if not hlist:
        await conn.execute(hypertable)
    if hlist and table_name not in  hlist[0]:
        await conn.execute(hypertable)


async def transformation(paths, beginfiles, endfiles):
    data=[]
    p =paths[0].split('/')[1]+"/2168_"+"*"
    for i in glob.glob(p)[0:1]:
        print(i)
        try:
            with open(i, 'r+b') as f:
                mm = mmap.mmap(f.fileno(),0)
                timelist=[]
                day = i.split('/')[0]
                date = convert_data(day)
                for i in range(23,mm.size(),32):
                    a = bytearray(mm[i-12:i-20:-1])
                    time1 = struct.unpack('!d', a)[0]*1000
                    timelist.append((convert_to_time(time1, date)))
                mm.close()
                data.append(timelist)
        except FileNotFoundError as e:
            print(e)


    LEN=len(data[0])
    day = paths[0].split('/')[1]
    date = convert_data(day)
    for i in paths[beginfiles:endfiles]:
        try:
            with open(i, 'r+b') as f:
                mm = mmap.mmap(f.fileno(),0)
                info= [None for  i in range(0,LEN)]
                info1=[]
                timelist=[]
                types= bytearray(mm[15:13:-1])
                types = struct.unpack('!h', types)[0]
                for i in range(23,23+32*2,32):
                    a = bytearray(mm[i-12:i-20:-1])
                    time1 = struct.unpack('!d', a)[0]*1000
                    timelist.append(convert_to_time(time1, date))
                    
                if types == 0:
                    for i in range(23,mm.size(),32):
                        d2= bytearray(mm[i:i-8:-1])
                        d2 = struct.unpack('!d', d2)[0]
                        info1.append(d2)
                else:
                    for i in range(23,mm.size(),32):
                        d2= bytearray(mm[i-4:i-8 :-1])
                        d2 = struct.unpack('!i', d2)[0]
                        info1.append(d2)

                begin =data[0].index(timelist[0])
                step =  (data[0].index(timelist[1])-begin)  if (data[0].index(timelist[1])-begin) else 1
                end=len(info1)
                count=0

                for i in range(begin,end,step):
                    info[i]=info1[count]
                    count+=1
                mm.close()
                data.append(info)
        except FileNotFoundError: 
            print('file not found')
            info= [None for  i in range(0,LEN)]
            data.append(info)

    return zip_longest(*data, fillvalue=None)



async def run():
    start = time.perf_counter()
    print(f'{0.000:0.4}','transformation start')

    conn = await asyncpg.connect(user='postgres', password='qw005870',database='testload2',host='localhost')


    folders = ['./25720/*','./25748/*']
    tm_name = tm_parameters('TmParameters.xml')
    tm_code = tm_name['code']
    paths=[]
    for folder in folders:
        paths =[folder.split('*')[0]+str(i)+"_"+folder.split('.')[1].split('/')[1]+'_00' for i in tm_name['index'] ]
        begindata = 0
        endat = 1550
        tm_list = tm_code[begindata:endat]
        cols = ('time', *tm_list)
        
        
        table_name = 'tm_value_async'
        await create_tm_table(conn, tm_list, table_name)

        data = await transformation(paths, begindata,endat)
        
        elapsed = time.perf_counter() - start
        print(f' {elapsed:0.4}','transformation end,','copy to db start')
        
        await conn.copy_records_to_table(table_name, records =data, columns= cols, timeout=60)
    
        elapsed = time.perf_counter() - start
        print(f'{elapsed:0.4}','copy to db end')
    #########################################################333

        begindata =1550
        endat = len(paths)
        tm_list = tm_code[begindata:endat]
        cols = ('time', *tm_list)

        table_name = 'tm_value_async2'

        await create_tm_table(conn, tm_list, table_name)

        data =  await transformation(paths,begindata, endat)

        elapsed = time.perf_counter() - start
        print(f' {elapsed:0.4}','transformation end,','copy to db start')
        
        await conn.copy_records_to_table(table_name, records =data, columns= cols, timeout=60)


        elapsed = time.perf_counter() - start
        print(f'{elapsed:0.4}','copy to db end')
    
    await conn.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(run())




