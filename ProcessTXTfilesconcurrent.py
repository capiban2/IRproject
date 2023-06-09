import os
import sys
import aiofiles
import asyncio
import multiprocessing
import threading
import re
import time
from queue import Queue

BLOCKSIZE = 1024*8
OUTFILE = '/home/iv/Documents/mydir/kursovaya/filesfortests2'
INFILES='/home/iv/Documents/mydir/kursovaya/indexbinaries'

LOCK = threading.Lock()
DATAISREADY=threading.Event()
def processFlow(data,dirout,file,offset,number):
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
            # if tmp.istitle():
            #     print(tmp)
    DATAISREADY.wait()
    if number == 0 or number == len(data)-1:
    
        if (val:=data[number][0 if number==0 else 1].lower()) not in dictionary:
            dictionary.append(val.lower())
        
    if number!=0:
        
        if (val:= data[number-1][1]+data[number][0].lower()) not in dictionary:
            dictionary.append(val.lower())
    # for elem in dictionary:
    #     if elem.istitle():
    #         print(elem)
    res_string = ' '.join(sorted(dictionary))
    with open(os.path.join(dirout,f'{number}{os.path.basename(file).split(".")[0]}.txt'), 'w') as outfile:
         
        outfile.write(res_string)
        #print(res_string)
        
    
    
    
def multithreadingLogic(received:tuple[str,str]):
    pathtofile = received[0]
    dirout = received[1]
    count_threads = os.stat(pathtofile).st_size//BLOCKSIZE
    threads = []
    data = {}
    for i in range(count_threads):
        threads.append(threading.Thread(target=processFlow,args = (data,dirout,pathtofile,i*BLOCKSIZE+i,i,)))
    for thread in threads:
        thread.start()
    while len(data)!= count_threads:
        pass
    DATAISREADY.set()
    for thread in threads:
        thread.join()
    
    

def treatTXTfiles(infiles:str, outfile : str):
    for filename in os.listdir(infiles):
        try:
            os.mkdir(os.path.join(outfile,filename.split('.')[0]))
            
        except Exception as e:
            print(e)
            continue
    #start = time.perf_counter()
    with multiprocessing.Pool() as multiprocessing_pool:
        multiprocessing_pool.map(
            multithreadingLogic,
            ((os.path.join(infiles,path),os.path.join(outfile,path.split('.')[0])) for path in os.listdir(infiles))
        ) 
        
# if __name__ == '__main__':
#     start = time.perf_counter()
#     treatTXTfiles('/home/iv/Documents/mydir/kursovaya/filesfortests2','/home/iv/Documents/mydir/kursovaya/indexbinaries')
#     print(time.perf_counter() - start)
#     #print(time.perf_counter() - start)