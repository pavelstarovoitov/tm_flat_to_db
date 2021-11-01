import time
from multiprocessing import Pool
import asyncio
import asyncpg
import datetime
from itertools import zip_longest
def collect_result(val):
    return val

def cube(x):
    print(f"start process {x}")
    time.sleep(1)
    i=1
    while i == True:
        i^i
    print(f"end process {x}")
    return x * x * x

def cube_print(x):
    print(x * x * x)

if __name__ == "__main__":
    async def run():
        pool = Pool(processes=4)
        print(pool.map(cube, range(10)))
        cubelist = pool.map(cube, range(10))
        print('cubelist',cubelist)
        print("HERE!")
        print("HERE AGAIN!")
        pool.close()
        pool.join()
        create_table="CREATE TABLE IF NOT EXISTS testload (time   TIMESTAMP WITH TIME ZONE,  TM1 REAL);"
        conn = await asyncpg.connect(user='postgres', password='qw005870',database='testload',host='localhost')
        await conn.execute(create_table)
        date_time_str = '1999-01-08 04:05:06'
        date = datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
        date_time_str = '1999-01-08 04:05:07'
        date2 = datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')

        create_table="CREATE TABLE IF NOT EXISTS cube ( cube REAL);"
        await conn.execute(create_table)


        await conn.copy_records_to_table('testload', records =[(date,1),(date2,2)], columns= ('time','tm1',), timeout=60)
        rec = zip_longest(cubelist, fillvalue=None)
        # for i in rec:
        #     print(i)
        await conn.copy_records_to_table('cube', records =rec, columns= ('cube',), timeout=60)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
