import os
import sys
import aiofiles
import asyncio
import multiprocessing
import threading
import re
import time
from queue import Queue

BLOCKSIZE = 1024*16
OUTFILE = '/home/iv/Documents/mydir/kursovaya/indexbinaries'
INFILES='/home/iv/Documents/mydir/kursovaya/filesforindexing2'

LOCK = threading.Lock()
DATAISREADY=threading.Event()
def processFlow(data,file,offset,number):
    dictionary = []
    with open(file) as input_file:
        os.lseek(input_file.fileno(),offset,os.SEEK_SET)
        flow = input_file.read(BLOCKSIZE)
    clear_flow = re.findall('\w+|\d+',flow)
    
    data[number] =(clear_flow[0],clear_flow[-1],)
    clear_flow.pop(0)
    clear_flow.pop(-1)
    for elem in clear_flow:
        if elem.lower() not in dictionary:
            dictionary.append(elem.lower())
    DATAISREADY.wait()
    if number == 0 or number == len(data)-1:
        val = data[number][0 if number==0 else 1].lower()
        if val not in dictionary:
            dictionary.append(val)
        
    if number!=0:
        val = data[number-1][1]+data[number][0].lower()
        if val not in dictionary:
            dictionary.append(val)
    res_string = ' '.join(sorted(dictionary))
    with open(os.path.join(OUTFILE,f'{number}.txt'), 'w') as outfile:
        
        outfile.write(res_string)
        
    
    
    
def multithreading_logic(pathtofile:str):
    count_threads = os.stat(pathtofile).st_size//BLOCKSIZE
    threads = []
    data = {}
    for i in range(count_threads):
        threads.append(threading.Thread(target=processFlow,args = (data,pathtofile,i*BLOCKSIZE+i,i,)))
    for thread in threads:
        thread.start()
    while len(data)!= count_threads:
        pass
    DATAISREADY.set()
    for thread in threads:
        thread.join()
    
    

if __name__ == '__main__':
    start = time.perf_counter()
    with multiprocessing.Pool() as multiprocessing_pool:
        multiprocessing_pool.map(
            multithreading_logic,
            (os.path.join(INFILES,path) for path in os.listdir(INFILES))
        ) 
    print(time.perf_counter() - start)